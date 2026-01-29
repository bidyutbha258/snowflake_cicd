-- Version 1.2 -- insert into logging 

CREATE OR REPLACE PROCEDURE POC_FINANCE_CTL.ADMIN.SP_EVOLVE_SILVER_DT("PIPELINE_NAME" VARCHAR, "ENTITY_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
function exec(sql, binds){ return snowflake.createStatement({sqlText: sql, binds}).execute(); }

/* ---------- helpers ---------- */
function stripBracketQuotes(p) {
  const s = String(p);
  if ((s.startsWith("[''") && s.endsWith("'']")) || (s.startsWith(''["'') && s.endsWith(''"]''))) return s.slice(2, -2);
  return s;
}
function toSafeCol(path) {
  let base = stripBracketQuotes(path);
  return (base.replace(/[^A-Za-z0-9]+/g, "_").replace(/^_+|_+$/g, "").toLowerCase()) || "col";
}
function makeJsonExpr(path) {
  const p = String(path);
  if ((p.startsWith("[''") && p.endsWith("'']")) || (p.startsWith(''["'') && p.endsWith(''"]''))) {
    const key = p.slice(2, -2);
    return `raw:"${key}"`;
  }
  const segs = p.split(''.'');
  const parts = segs.map(s => (/^[A-Za-z0-9_]+$/.test(s) ? s : `"${s}"`));
  return `raw:${parts.join('':'')}`;
}

/* ---------- config ---------- */
let env = ''POC'';
try {
  const rsEnv = exec(
    `SELECT parameter_value
       FROM POC_Finance_CTL.ADMIN.PIPELINE_CONFIG
      WHERE pipeline_name = ? AND parameter_name = ''active_env_prefix''`,
    [PIPELINE_NAME]
  );
  if (rsEnv.next()) env = String(rsEnv.getColumnValue(1));
} catch (e) { /* default POC */ }

/* Compose DB names from env */
const BRONZE_DB     = `${env}_Finance_BRONZE`;
const SILVER_DB     = `${env}_Finance_SILVER`;
const GOLD_DB       = `${env}_Finance_GOLD`;
const DQ_DB         = `${env}_Finance_DQ`;
const GOV_DB        = `${env}_Finance_GOVERNANCE`;
const WH            = `${env}_FINANCE_WH`;

/* Control tables remain centralized in POC_Finance_CTL.ADMIN */
const CTL_DB    = `POC_Finance_CTL`;
const CTL_SCHEMA= `ADMIN`;

/* entity mapping */
let ent = exec(
  `SELECT bronze_table, silver_dt
     FROM ${CTL_DB}.${CTL_SCHEMA}.ENTITIES
    WHERE entity_name = ?`, [ENTITY_NAME]);
if (!ent.next()) throw `Unknown entity ${ENTITY_NAME}`;
const bronze_table = ent.getColumnValue(1);
const silver_dt    = ent.getColumnValue(2);

/* FQNs */
const BRONZE_FQN = `${BRONZE_DB}.RAW.${bronze_table}`;
const REG_FQN    = `${CTL_DB}.${CTL_SCHEMA}.SILVER_PATH_REGISTRY`;
const LOG_FQN    = `${CTL_DB}.${CTL_SCHEMA}.SILVER_EVOLUTION_LOG`;
const DT_FQN     = `${SILVER_DB}.CORE.${silver_dt}`;
const run_id     = `${ENTITY_NAME}-${Date.now()}`;

/* 1) Discover JSON keys + inferred types */
const discoverSQL = `
WITH f AS (
  SELECT f.path, f.key, f.value, TYPEOF(f.value) AS t
  FROM ${BRONZE_FQN} r, LATERAL FLATTEN(INPUT => r.raw, RECURSIVE => TRUE) f
  WHERE f.key IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.path ORDER BY f.seq DESC) = 1
),
norm AS (
  SELECT
    REGEXP_REPLACE(path, ''^\\\\[|\\\\]$'', '''') AS raw_path,
    CASE WHEN t IN (''INTEGER'',''NUMBER'') THEN ''NUMBER''
         WHEN t = ''BOOLEAN'' THEN ''BOOLEAN''
         WHEN t IN (''TIMESTAMP_NTZ'',''TIMESTAMP_TZ'',''TIMESTAMP_LTZ'') THEN ''TIMESTAMP_NTZ''
         WHEN t = ''DATE'' THEN ''DATE''
         ELSE ''STRING'' END AS snowflake_type
  FROM f
)
SELECT DISTINCT raw_path, snowflake_type FROM norm
WHERE raw_path NOT LIKE ''%INDEX%''`;
let rs = exec(discoverSQL, []);

/* Upsert to registry */
while (rs.next()) {
  const raw_path = String(rs.getColumnValue(1));
  const typ      = String(rs.getColumnValue(2));
  const col      = toSafeCol(raw_path);

  exec(`
    MERGE INTO ${REG_FQN} t
    USING (SELECT ? ent, ? path, ? col, ? typ) s
      ON t.entity_name = s.ent AND t.path = s.path
    WHEN MATCHED THEN UPDATE SET
      last_seen_ts = CURRENT_TIMESTAMP,
      snowflake_type = IFF(t.snowflake_type=''STRING'' AND s.typ<>''STRING'', s.typ, t.snowflake_type)
    WHEN NOT MATCHED THEN INSERT(entity_name, path, column_name, snowflake_type,
                                 first_seen_ts, last_seen_ts, last_inferred_ts, active)
    VALUES (s.ent, s.path, s.col, s.typ,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE)
  `, [ENTITY_NAME, raw_path, col, typ]);
}

/* 2) Build SELECT list from registry */
let sel = exec(`
  SELECT path, column_name, snowflake_type
    FROM ${REG_FQN}
   WHERE entity_name = ? AND active = TRUE
   ORDER BY column_name`, [ENTITY_NAME]);

let selectCols = [];
while (sel.next()) {
  const path = String(sel.getColumnValue(1));
  const col  = String(sel.getColumnValue(2));
  const typ  = String(sel.getColumnValue(3));

  const jsonExpr = makeJsonExpr(path);
  const castExpr = (typ === ''TIMESTAMP_NTZ'') ? `TRY_TO_TIMESTAMP_NTZ(${jsonExpr})`
                                             : `${jsonExpr}::${typ}`;
  selectCols.push(`${castExpr} AS ${col}`);
}
selectCols.push(`_ingest_ts AS _src_ingest_ts`);

/* 3) Create or Replace the Silver Dynamic Table */
const ddl = `
CREATE OR REPLACE DYNAMIC TABLE ${DT_FQN}
  TARGET_LAG = ''10 MINUTES''
  WAREHOUSE  = ${WH}
  REFRESH_MODE = AUTO
  INITIALIZE = ON_CREATE
AS
SELECT
  ${selectCols.join('',\\n  '')}
FROM ${BRONZE_FQN};`;
exec(ddl, []);

/* log */
exec(`INSERT INTO ${LOG_FQN}(run_id, entity_name, action, details)
      SELECT ?, ?, ''REBUILD_DT'', ''Rebuilt with ${selectCols.length} columns''`,
      [run_id, ENTITY_NAME]);

/* optional manual refresh */
exec(`ALTER DYNAMIC TABLE ${DT_FQN} REFRESH`, []);
exec(`INSERT INTO ${LOG_FQN}(run_id, entity_name, action, details)
      SELECT ?, ?, ''REFRESH'', ''Manual refresh triggered''`,
      [run_id, ENTITY_NAME]);

return `OK: ${env}: rebuilt ${DT_FQN}`;
';

