CREATE OR REPLACE PROCEDURE POC_FINANCE_CTL.ADMIN.SP_EVOLVE_SILVER_DT("PIPELINE_NAME" VARCHAR, "ENTITY_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
function exec(sql, binds){ 
  return snowflake.createStatement({sqlText: sql, binds: binds}).execute(); 
}

/* ---------- helpers ---------- */
function stripBracketQuotes(p) {
  var s = String(p);
  if ((s.indexOf("[''") === 0 && s.lastIndexOf("'']") === s.length - 2) || (s.indexOf(''["'') === 0 && s.lastIndexOf(''"]'') === s.length - 2)) {
    return s.slice(2, -2);
  }
  return s;
}
function toSafeCol(path) {
  var base = stripBracketQuotes(path);
  var out = base.replace(/[^A-Za-z0-9]+/g, "_").replace(/^_+|_+$/g, "").toLowerCase();
  return out || "col";
}
function makeJsonExpr(path) {
  var p = String(path);
  if ((p.indexOf("[''") === 0 && p.lastIndexOf("'']") === p.length - 2) || (p.indexOf(''["'') === 0 && p.lastIndexOf(''"]'') === p.length - 2)) {
    var key = p.slice(2, -2);
    return "raw:\\"" + key + "\\"";
  }
  var segs = p.split(''.'');
  var parts = [];
  for (var i = 0; i < segs.length; i++) {
    var s = segs[i];
    parts.push(/^[A-Za-z0-9_]+$/.test(s) ? s : ("\\"" + s + "\\""));
  }
  return "raw:" + parts.join('':'');
}

/* ---------- config ---------- */
var env = ''POC'';
try {
  var rsEnv = exec(
    "SELECT parameter_value " +
    "  FROM POC_Finance_CTL.ADMIN.PIPELINE_CONFIG " +
    " WHERE pipeline_name = ? AND parameter_name = ''active_env_prefix''",
    [PIPELINE_NAME]
  );
  if (rsEnv.next()) env = String(rsEnv.getColumnValue(1));
} catch (e) {
  // default to ''POC''
}

/* Compose DB names from env */
var BRONZE_DB     = env + "_Finance_BRONZE";
var SILVER_DB     = env + "_Finance_SILVER";
var GOLD_DB       = env + "_Finance_GOLD";
var DQ_DB         = env + "_Finance_DQ";
var GOV_DB        = env + "_Finance_GOVERNANCE";
var WH            = env + "_FINANCE_WH";

/* Control tables remain centralized in POC_Finance_CTL.ADMIN */
var CTL_DB        = "POC_Finance_CTL";
var CTL_SCHEMA    = "ADMIN";

/* entity mapping */
var ent = exec(
  "SELECT bronze_table, silver_dt " +
  "  FROM " + CTL_DB + "." + CTL_SCHEMA + ".ENTITIES " +
  " WHERE entity_name = ?", 
  [ENTITY_NAME]
);
if (!ent.next()) throw "Unknown entity " + ENTITY_NAME;
var bronze_table = ent.getColumnValue(1);
var silver_dt    = ent.getColumnValue(2);

/* FQNs */
var BRONZE_FQN = BRONZE_DB + ".RAW." + bronze_table;
var REG_FQN    = CTL_DB + "." + CTL_SCHEMA + ".SILVER_PATH_REGISTRY";
var LOG_FQN    = CTL_DB + "." + CTL_SCHEMA + ".SILVER_EVOLUTION_LOG";
var LOAD_MONITOR_LOG_FQN = CTL_DB + "." + CTL_SCHEMA + ".LOAD_LOG_MONITOR";
var DT_FQN     = SILVER_DB + ".CORE." + silver_dt;

/* Run id */
var run_id     = ENTITY_NAME + "-" + Date.now();

/* ---------- monitoring vars ---------- */
var new_rows_inserted = 0;        // rows newly arriving in Bronze since max Silver ingest ts
var rows_updated      = 0;        // DTs are append/transform; set 0 unless you use MERGE elsewhere
var Stage             = "load_from_bronze_to_silver";
var Source_Object     = BRONZE_FQN;
var Destination_Object= DT_FQN;
var load_status       = 1;        // 1=success, 0=failure
var message           = "";

/* ---------- 1) Discover JSON keys + inferred types from Bronze ---------- */
var discoverSQL = ""
+ "WITH f AS ( \\n"
+ "  SELECT f.path, f.key, f.value, TYPEOF(f.value) AS t \\n"
+ "  FROM " + BRONZE_FQN + " r, LATERAL FLATTEN(INPUT => r.raw, RECURSIVE => TRUE) f \\n"
+ "  WHERE f.key IS NOT NULL \\n"
+ "  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.path ORDER BY f.seq DESC) = 1 \\n"
+ "), \\n"
+ "norm AS ( \\n"
+ "  SELECT \\n"
+ "    REGEXP_REPLACE(path, ''^\\\\[|\\\\]$'', '''') AS raw_path, \\n"
+ "    CASE WHEN t IN (''INTEGER'',''NUMBER'') THEN ''NUMBER'' \\n"
+ "         WHEN t = ''BOOLEAN'' THEN ''BOOLEAN'' \\n"
+ "         WHEN t IN (''TIMESTAMP_NTZ'',''TIMESTAMP_TZ'',''TIMESTAMP_LTZ'') THEN ''TIMESTAMP_NTZ'' \\n"
+ "         WHEN t = ''DATE'' THEN ''DATE'' \\n"
+ "         ELSE ''STRING'' END AS snowflake_type \\n"
+ "  FROM f \\n"
+ ") \\n"
+ "SELECT DISTINCT raw_path, snowflake_type FROM norm \\n"
+ "WHERE raw_path NOT LIKE ''%INDEX%''";
var rs = exec(discoverSQL, []);

/* Upsert to registry */
while (rs.next()) {
  var raw_path = String(rs.getColumnValue(1));
  var typ      = String(rs.getColumnValue(2));
  var col      = toSafeCol(raw_path);

  exec(
    "MERGE INTO " + REG_FQN + " t \\n" +
    "USING (SELECT ? ent, ? path, ? col, ? typ) s \\n" +
    "  ON t.entity_name = s.ent AND t.path = s.path \\n" +
    "WHEN MATCHED THEN UPDATE SET \\n" +
    "  last_seen_ts = CURRENT_TIMESTAMP, \\n" +
    "  snowflake_type = IFF(t.snowflake_type=''STRING'' AND s.typ<>''STRING'', s.typ, t.snowflake_type) \\n" +
    "WHEN NOT MATCHED THEN INSERT( \\n" +
    "  entity_name, path, column_name, snowflake_type, \\n" +
    "  first_seen_ts, last_seen_ts, last_inferred_ts, active \\n" +
    ") \\n" +
    "VALUES ( \\n" +
    "  s.ent, s.path, s.col, s.typ, \\n" +
    "  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE \\n" +
    ")", 
    [ENTITY_NAME, raw_path, col, typ]
  );
}

/* ---------- 2) Build SELECT list from registry ---------- */
var sel = exec(
  "SELECT path, column_name, snowflake_type \\n" +
  "  FROM " + REG_FQN + " \\n" +
  " WHERE entity_name = ? AND active = TRUE \\n" +
  " ORDER BY column_name",
  [ENTITY_NAME]
);

var selectCols = [];
while (sel.next()) {
  var path = String(sel.getColumnValue(1));
  var col  = String(sel.getColumnValue(2));
  var typ  = String(sel.getColumnValue(3));

  var jsonExpr = makeJsonExpr(path);
  var castExpr = (typ === "TIMESTAMP_NTZ") ? ("TRY_TO_TIMESTAMP_NTZ(" + jsonExpr + ")")
                                           : (jsonExpr + "::" + typ);
  selectCols.push(castExpr + " AS " + col);
}
/* capture source ingest ts from Bronze as a separate column in Silver */
selectCols.push("_ingest_ts AS _src_ingest_ts");

/* ---------- 3) Detect new Bronze rows relative to current Silver ---------- */
/* Reads current max _src_ingest_ts from Silver DT if it exists */
var max_silver_ts = null;
try {
  var rsMax = exec("SELECT MAX(_src_ingest_ts) FROM " + DT_FQN, []);
  if (rsMax.next()) max_silver_ts = rsMax.getColumnValue(1);
} catch (e) {
  /* DT may not exist yet on first run; that''s fine. */
}

/* Count rows in Bronze newer than Silver''s max _src_ingest_ts */
try {
  var rsNew = exec(
    "SELECT COUNT(*) \\n" +
    "  FROM " + BRONZE_FQN + " b \\n" +
    " WHERE b._ingest_ts > COALESCE(TO_TIMESTAMP_NTZ(?), ''1970-01-01''::TIMESTAMP_NTZ)",
    [max_silver_ts]
  );
  if (rsNew.next()) new_rows_inserted = Number(rsNew.getColumnValue(1));
  message = (new_rows_inserted > 0) ? "Successfully loaded" : "No new rows found in Bronze";
} catch (e) {
  // If Bronze query fails, mark as failure but continue to attempt DT build to avoid masking issues
  load_status = 0;
  message = "Failure checking Bronze new rows: " + e.toString();
}

/* ---------- 4) Create or Replace the Silver Dynamic Table ---------- */
var ddl = ""
+ "CREATE OR REPLACE DYNAMIC TABLE " + DT_FQN + " \\n"
+ "  TARGET_LAG = ''10 MINUTES'' \\n"
+ "  WAREHOUSE  = " + WH + " \\n"
+ "  REFRESH_MODE = AUTO \\n"
+ "  INITIALIZE = ON_CREATE \\n"
+ "AS \\n"
+ "SELECT \\n"
+ "  " + selectCols.join(",\\n  ") + " \\n"
+ "FROM " + BRONZE_FQN + ";";

try {
  exec(ddl, []);

  /* evolution log: rebuild */
  try {
    exec(
      "INSERT INTO " + LOG_FQN + "(run_id, entity_name, action, details) " +
      "SELECT ?, ?, ''REBUILD_DT'', ?",
      [run_id, ENTITY_NAME, "Rebuilt with " + selectCols.length + " columns"]
    );
  } catch (e2) { /* swallow log error */ }

  /* optional manual refresh (note: asynchronous) */
  exec("ALTER DYNAMIC TABLE " + DT_FQN + " REFRESH", []);

  /* evolution log: refresh */
  try {
    exec(
      "INSERT INTO " + LOG_FQN + "(run_id, entity_name, action, details) " +
      "SELECT ?, ?, ''REFRESH'', ''Manual refresh triggered''",
      [run_id, ENTITY_NAME]
    );
  } catch (e3) { /* swallow log error */ }

} catch (e) {
  load_status = 0;
  if (message && message.indexOf("Failure checking Bronze") === 0) {
    message = message + " | DT rebuild error: " + e.toString();
  } else {
    message = "Failure during DT rebuild: " + e.toString();
  }
}

/* ---------- 5) Insert Monitoring Log ---------- */
/* rows_updated is 0 for DT-based flows; set by MERGE if you switch approach later */
try {
  exec(
    "INSERT INTO " + LOAD_MONITOR_LOG_FQN + " \\n" +
    "  (RUN_ID, Stage, Source_Object, Destination_Object, \\n" +
    "   load_status, new_rows_inserted, rows_updated, message) \\n" +
    "SELECT ?, ?, ?, ?, ?, ?, ?, ?",
    [
      run_id,
      Stage,
      Source_Object,
      Destination_Object,
      load_status,
      new_rows_inserted,
      rows_updated,
      message
    ]
  );
} catch (e) {
  // keep SP return stable even if logging fails
}

/* ---------- 6) Return summary ---------- */
return "OK: " + env + ": evolved " + DT_FQN + "; new_rows_in_bronze=" + new_rows_inserted + "; status=" + load_status;
';