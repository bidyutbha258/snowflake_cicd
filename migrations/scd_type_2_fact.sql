/* ======================================================================
   CUSTOMER ACCOUNT FACT: Bronze → Silver → Gold (with Watermark & Task)
   Includes: Procedures, Test Data, Validations, Cleanup (Single Script)
   ====================================================================== */

-- USE ROLE ACCOUNTADMIN;
-- USE WAREHOUSE POC_FINANCE_WH;

/* ============================================================
   0) (Optional) PIPELINE CONFIG (env resolution)
   ============================================================ */
-- Create the config table if not exists and seed env prefix for PIPELINE_MAIN (optional)
CREATE TABLE IF NOT EXISTS POC_FINANCE_CTL.ADMIN.PIPELINE_CONFIG (
  PIPELINE_NAME    STRING,
  PARAMETER_NAME   STRING,
  PARAMETER_VALUE  STRING,
  UPDATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
MERGE INTO POC_FINANCE_CTL.ADMIN.PIPELINE_CONFIG t
USING (
  SELECT 'PIPELINE_MAIN' AS PIPELINE_NAME, 'active_env_prefix' AS PARAMETER_NAME, 'POC' AS PARAMETER_VALUE
) s
ON t.PIPELINE_NAME = s.PIPELINE_NAME AND t.PARAMETER_NAME = s.PARAMETER_NAME
WHEN NOT MATCHED THEN
  INSERT (PIPELINE_NAME, PARAMETER_NAME, PARAMETER_VALUE) VALUES (s.PIPELINE_NAME, s.PARAMETER_NAME, s.PARAMETER_VALUE);

/* ============================================================
   1) BRONZE LAYER
   ============================================================ */

-- 1.1 RAW fact (JSON source)
CREATE TABLE IF NOT EXISTS POC_FINANCE_BRONZE.RAW.CUSTOMER_ACCOUNT_FACT_RAW (
  RAW         VARIANT,
  _INGEST_TS  TIMESTAMP_NTZ
);
ALTER TABLE POC_FINANCE_BRONZE.RAW.CUSTOMER_ACCOUNT_FACT_RAW
  SET DATA_RETENTION_TIME_IN_DAYS = 1;

-- 1.2 Persistent TRANSIENT Stage (structured; for troubleshooting/traceability)
CREATE TRANSIENT TABLE IF NOT EXISTS POC_FINANCE_BRONZE.RAW.FACT_CUSTOMER_ACCOUNT_STAGE_TR (
  RUN_ID             VARCHAR NOT NULL,
  PIPELINE_NAME      VARCHAR,
  ENV                VARCHAR,

  SOURCE_EVENT_ID    VARCHAR,
  ACCOUNT_ID         VARCHAR,
  CUSTOMER_ID        VARCHAR,
  PRODUCT_ID         VARCHAR,
  TRANSACTION_TS     TIMESTAMP_NTZ,
  QUANTITY           NUMBER(18,3),
  AMOUNT             NUMBER(18,2),
  CURRENCY           VARCHAR,
  CHANNEL            VARCHAR,
  ATTRIBUTES_JSON    VARIANT,

  RAW_INGEST_TS      TIMESTAMP_NTZ,
  STAGED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
ALTER TABLE POC_FINANCE_BRONZE.RAW.FACT_CUSTOMER_ACCOUNT_STAGE_TR
  SET DATA_RETENTION_TIME_IN_DAYS = 1;

-- 1.3 Procedure: INSERT INTO Stage FROM RAW (dedup by SOURCE_EVENT_ID)
CREATE OR REPLACE PROCEDURE POC_FINANCE_CTL.ADMIN.SP_LOAD_STAGE_CUSTOMER_ACCOUNT_FROM_RAW(
  "PIPELINE_NAME"   VARCHAR,
  "RUN_ID"          VARCHAR,
  "LOOKBACK_HOURS"  VARCHAR    -- pass string; parsed in JS (avoid NUMBER(38,0) issue)
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
function exec(sql, binds){ return snowflake.createStatement({sqlText: sql, binds: binds}).execute(); }
function toIntOrDefault(str, defVal) { if (str===null||str===undefined) return defVal; var n=parseInt(String(str),10); return isNaN(n)?defVal:n; }

var lookback = toIntOrDefault(LOOKBACK_HOURS, 48);  /* default 48 hours */

/* Resolve env prefix */
var env = 'POC';
try {
  var rsEnv = exec(
    "SELECT parameter_value FROM POC_FINANCE_CTL.ADMIN.PIPELINE_CONFIG " +
    "WHERE pipeline_name = ? AND parameter_name = 'active_env_prefix'",
    [PIPELINE_NAME]
  );
  if (rsEnv.next()) env = String(rsEnv.getColumnValue(1));
} catch(e){}

/* FQNs */
var RAW_FQN   = env + "_FINANCE_BRONZE.RAW.CUSTOMER_ACCOUNT_FACT_RAW";
var STAGE_FQN = "POC_FINANCE_BRONZE.RAW.FACT_CUSTOMER_ACCOUNT_STAGE_TR";

/* Idempotent clear for this RUN_ID */
try { exec("DELETE FROM " + STAGE_FQN + " WHERE RUN_ID = ?", [RUN_ID]); } catch(ignore){}

/* Insert latest (deduped) from RAW into Stage */
var inserted = 0;
try {
  var sql = ""
    + "INSERT INTO " + STAGE_FQN + " ( "
    + "  RUN_ID, PIPELINE_NAME, ENV, "
    + "  SOURCE_EVENT_ID, ACCOUNT_ID, CUSTOMER_ID, PRODUCT_ID, "
    + "  TRANSACTION_TS, QUANTITY, AMOUNT, CURRENCY, CHANNEL, ATTRIBUTES_JSON, "
    + "  RAW_INGEST_TS "
    + ") "
    + "WITH src AS ( "
    + "  SELECT "
    + "    ? AS RUN_ID, ? AS PIPELINE_NAME, ? AS ENV, "
    + "    RAW:\"source_event_id\"::string    AS SOURCE_EVENT_ID, "
    + "    RAW:\"account_id\"::string         AS ACCOUNT_ID, "
    + "    RAW:\"customer_id\"::string        AS CUSTOMER_ID, "
    + "    RAW:\"product_id\"::string         AS PRODUCT_ID, "
    + "    TRY_TO_TIMESTAMP_NTZ(RAW:\"transaction_ts\"::string) AS TRANSACTION_TS, "
    + "    RAW:\"quantity\"::number           AS QUANTITY, "
    + "    RAW:\"amount\"::number             AS AMOUNT, "
    + "    RAW:\"currency\"::string           AS CURRENCY, "
    + "    RAW:\"channel\"::string            AS CHANNEL, "
    + "    RAW:\"attributes\"                 AS ATTRIBUTES_JSON, "
    + "    _INGEST_TS                         AS RAW_INGEST_TS "
    + "  FROM " + RAW_FQN + " "
    + "  WHERE RAW:\"source_event_id\" IS NOT NULL "
    + "    AND _INGEST_TS >= DATEADD('hour', ?, CURRENT_TIMESTAMP()) "
    + "), dedup AS ( "
    + "  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY SOURCE_EVENT_ID ORDER BY RAW_INGEST_TS DESC) AS rn "
    + "  FROM src s "
    + ") "
    + "SELECT "
    + "  RUN_ID, PIPELINE_NAME, ENV, "
    + "  SOURCE_EVENT_ID, ACCOUNT_ID, CUSTOMER_ID, PRODUCT_ID, "
    + "  TRANSACTION_TS, QUANTITY, AMOUNT, CURRENCY, CHANNEL, ATTRIBUTES_JSON, "
    + "  RAW_INGEST_TS "
    + "FROM dedup WHERE rn = 1";

  var ins = exec(sql, [RUN_ID, PIPELINE_NAME, env, -lookback]);
  inserted = ins.getRowCount();
} catch (e) {
  return "Failed to stage from RAW: " + e.toString();
}

return "Staged " + inserted + " row(s) from RAW into " + STAGE_FQN + " for RUN_ID=" + RUN_ID + ".";
$$;

/* ============================================================
   2) SILVER LAYER
   ============================================================ */

-- 2.1 Silver fact (event grain)
CREATE TABLE IF NOT EXISTS POC_FINANCE_SILVER.CORE.F_CUSTOMER_ACCOUNT (
  FACT_SK           NUMBER IDENTITY START 1 INCREMENT 1,

  SOURCE_EVENT_ID   VARCHAR NOT NULL,
  ACCOUNT_ID        VARCHAR NOT NULL,
  CUSTOMER_ID       VARCHAR NOT NULL,
  PRODUCT_ID        VARCHAR NOT NULL,

  CUSTOMER_SK       NUMBER,
  PRODUCT_SK        NUMBER,

  TRANSACTION_TS    TIMESTAMP_NTZ NOT NULL,
  DATE_ID           DATE          AS (TO_DATE(TRANSACTION_TS)),

  QUANTITY          NUMBER(18,3),
  AMOUNT            NUMBER(18,2),
  CURRENCY          VARCHAR,
  CHANNEL           VARCHAR,

  RAW_INGEST_TS     TIMESTAMP_NTZ,
  LOAD_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_BY        VARCHAR        DEFAULT CURRENT_USER(),

  CONSTRAINT UK_F_CUSTOMER_ACCOUNT UNIQUE (SOURCE_EVENT_ID)
);

-- 2.2 Minimal Product SCD2 dimension (skeleton)
CREATE TABLE IF NOT EXISTS POC_FINANCE_SILVER.CORE.S_PRODUCT_DT (
  PRODUCT_SK        NUMBER IDENTITY START 1 INCREMENT 1,
  PRODUCT_ID        VARCHAR NOT NULL,
  PRODUCT_NAME      VARCHAR,
  PRODUCT_CATEGORY  VARCHAR,
  BRAND             VARCHAR,
  EFFECTIVE_FROM_TS TIMESTAMP_NTZ NOT NULL,
  EFFECTIVE_TO_TS   TIMESTAMP_NTZ NOT NULL DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
  IS_CURRENT        BOOLEAN NOT NULL DEFAULT TRUE
);

-- (Assumes Customer SCD2 `S_CUSTOMER_DT` exists from prior work)

-- 2.3 Procedure: Load Silver FROM RAW (idempotent; PIT SCD2 lookups)
CREATE OR REPLACE PROCEDURE POC_FINANCE_CTL.ADMIN.SP_LOAD_SILVER_CUSTOMER_ACCOUNT_FACT(
  "PIPELINE_NAME" VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
function exec(sql, binds){ return snowflake.createStatement({sqlText: sql, binds: binds}).execute(); }

/* Resolve env */
var env = 'POC';
try {
  var rsEnv = exec(
    "SELECT parameter_value FROM POC_FINANCE_CTL.ADMIN.PIPELINE_CONFIG " +
    "WHERE pipeline_name = ? AND parameter_name = 'active_env_prefix'",
    [PIPELINE_NAME]
  );
  if (rsEnv.next()) env = String(rsEnv.getColumnValue(1));
} catch(e){}

/* FQNs */
var RAW_FQN      = env + "_FINANCE_BRONZE.RAW.CUSTOMER_ACCOUNT_FACT_RAW";
var SILVER_FACT  = env + "_FINANCE_SILVER.CORE.F_CUSTOMER_ACCOUNT";
var DIM_CUST     = env + "_FINANCE_SILVER.CORE.S_CUSTOMER_DT";
var DIM_PROD     = env + "_FINANCE_SILVER.CORE.S_PRODUCT_DT";

var inserted = 0;

try {
  var ins = exec(
    "WITH src AS ( " +
    "  SELECT " +
    "    RAW:\"source_event_id\"::string    AS SOURCE_EVENT_ID, " +
    "    RAW:\"account_id\"::string         AS ACCOUNT_ID, " +
    "    RAW:\"customer_id\"::string        AS CUSTOMER_ID, " +
    "    RAW:\"product_id\"::string         AS PRODUCT_ID, " +
    "    TRY_TO_TIMESTAMP_NTZ(RAW:\"transaction_ts\"::string) AS TRANSACTION_TS, " +
    "    RAW:\"quantity\"::number           AS QUANTITY, " +
    "    RAW:\"amount\"::number             AS AMOUNT, " +
    "    RAW:\"currency\"::string           AS CURRENCY, " +
    "    RAW:\"channel\"::string            AS CHANNEL, " +
    "    _INGEST_TS                         AS RAW_INGEST_TS " +
    "  FROM " + RAW_FQN + " " +
    "  WHERE RAW:\"source_event_id\" IS NOT NULL " +
    "), dedup AS ( " +
    "  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY SOURCE_EVENT_ID ORDER BY RAW_INGEST_TS DESC) AS rn " +
    "  FROM src s " +
    ") " +
    "INSERT INTO " + SILVER_FACT + " ( " +
    "  SOURCE_EVENT_ID, ACCOUNT_ID, CUSTOMER_ID, PRODUCT_ID, " +
    "  CUSTOMER_SK, PRODUCT_SK, TRANSACTION_TS, QUANTITY, AMOUNT, CURRENCY, CHANNEL, RAW_INGEST_TS " +
    ") " +
    "SELECT " +
    "  d.SOURCE_EVENT_ID, d.ACCOUNT_ID, d.CUSTOMER_ID, d.PRODUCT_ID, " +
    "  c.CUSTOMER_SK, p.PRODUCT_SK, d.TRANSACTION_TS, d.QUANTITY, d.AMOUNT, d.CURRENCY, d.CHANNEL, d.RAW_INGEST_TS " +
    "FROM dedup d " +
    "LEFT JOIN " + DIM_CUST + " c " +
    "  ON c.CUSTOMER_ID = d.CUSTOMER_ID " +
    " AND d.TRANSACTION_TS >= c.EFFECTIVE_FROM_TS " +
    " AND d.TRANSACTION_TS <  c.EFFECTIVE_TO_TS " +
    "LEFT JOIN " + DIM_PROD + " p " +
    "  ON p.PRODUCT_ID = d.PRODUCT_ID " +
    " AND d.TRANSACTION_TS >= p.EFFECTIVE_FROM_TS " +
    " AND d.TRANSACTION_TS <  p.EFFECTIVE_TO_TS " +
    "WHERE d.rn = 1 " +
    "  AND d.SOURCE_EVENT_ID NOT IN (SELECT SOURCE_EVENT_ID FROM " + SILVER_FACT + ")",
    []
  );
  inserted = ins.getRowCount();
} catch (e) {
  return "Failed to load Silver fact: " + e.toString();
}

return "Inserted " + inserted + " new fact row(s) into Silver.";
$$;

/* ============================================================
   3) GOLD LAYER (table + view + watermark + loader + task)
   ============================================================ */

-- 3.1 Gold daily aggregate table
CREATE SCHEMA IF NOT EXISTS POC_FINANCE_GOLD.MART;
CREATE TABLE IF NOT EXISTS POC_FINANCE_GOLD.MART.F_CUSTOMER_ACCOUNT_DAILY (
  DATE_ID        DATE,
  CUSTOMER_ID    VARCHAR,
  PRODUCT_ID     VARCHAR,
  CUSTOMER_SK    NUMBER,
  PRODUCT_SK     NUMBER,

  TXN_COUNT      NUMBER,
  TOTAL_QTY      NUMBER(18,3),
  TOTAL_AMOUNT   NUMBER(18,2),

  LOAD_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_BY     VARCHAR DEFAULT CURRENT_USER(),

  CONSTRAINT PK_F_CUSTOMER_ACCOUNT_DAILY PRIMARY KEY (DATE_ID, CUSTOMER_ID, PRODUCT_ID)
);

-- 3.2 Gold row-level enriched view (current attributes)
CREATE OR REPLACE VIEW POC_FINANCE_GOLD.MART.V_F_CUSTOMER_ACCOUNT_ENRICHED AS
SELECT
  f.FACT_SK,
  f.SOURCE_EVENT_ID,
  f.ACCOUNT_ID,
  f.CUSTOMER_ID,
  f.PRODUCT_ID,
  f.CUSTOMER_SK,
  f.PRODUCT_SK,
  f.TRANSACTION_TS,
  f.DATE_ID,
  f.QUANTITY,
  f.AMOUNT,
  f.CURRENCY,
  f.CHANNEL,
  f.RAW_INGEST_TS,
  f.LOAD_TS,

  c.FIRST_NAME       AS CUSTOMER_FIRST_NAME,
  c.LAST_NAME        AS CUSTOMER_LAST_NAME,
  c.COUNTRY          AS CUSTOMER_COUNTRY,
  c.SEGMENT          AS CUSTOMER_SEGMENT,
  c.VIP_STATUS       AS CUSTOMER_VIP_STATUS,

  p.PRODUCT_NAME,
  p.PRODUCT_CATEGORY,
  p.BRAND

FROM POC_FINANCE_SILVER.CORE.F_CUSTOMER_ACCOUNT f
LEFT JOIN POC_FINANCE_SILVER.CORE.S_CUSTOMER_DT c
  ON c.CUSTOMER_SK = f.CUSTOMER_SK
 AND c.IS_CURRENT = TRUE
LEFT JOIN POC_FINANCE_SILVER.CORE.S_PRODUCT_DT p
  ON p.PRODUCT_SK = f.PRODUCT_SK
 AND p.IS_CURRENT = TRUE;

-- 3.3 Control Watermark table
CREATE TABLE IF NOT EXISTS POC_FINANCE_CTL.ADMIN.FACT_LOAD_WATERMARKS (
  FACT_NAME     VARCHAR NOT NULL,
  ENV           VARCHAR NOT NULL,
  LAST_DATE_ID  DATE,
  UPDATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_FACT_LOAD_WATERMARKS PRIMARY KEY (FACT_NAME, ENV)
);
MERGE INTO POC_FINANCE_CTL.ADMIN.FACT_LOAD_WATERMARKS t
USING (SELECT 'F_CUSTOMER_ACCOUNT_DAILY' AS FACT_NAME, 'POC' AS ENV) s
ON t.FACT_NAME = s.FACT_NAME AND t.ENV = s.ENV
WHEN NOT MATCHED THEN
  INSERT (FACT_NAME, ENV, LAST_DATE_ID) VALUES (s.FACT_NAME, s.ENV, NULL);

-- 3.4 Procedure: Incremental Gold loader (VARCHAR params; watermark + grace; delete+insert)
CREATE OR REPLACE PROCEDURE POC_FINANCE_CTL.ADMIN.SP_LOAD_GOLD_CUSTOMER_ACCOUNT_DAILY(
  "PIPELINE_NAME" VARCHAR,
  "LOOKBACK_DAYS" VARCHAR,     -- pass '3'
  "GRACE_DAYS"    VARCHAR      -- pass '2'
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
function exec(sql, binds){ return snowflake.createStatement({sqlText: sql, binds: binds}).execute(); }
function toIntOrDefault(str, defVal) { if (str===null||str===undefined) return defVal; var n=parseInt(String(str),10); return isNaN(n)?defVal:n; }

var lookbackDays = toIntOrDefault(LOOKBACK_DAYS, 3);
var graceDays    = toIntOrDefault(GRACE_DAYS, 2);

/* Resolve env */
var env = 'POC';
try {
  var rsEnv = exec(
    "SELECT parameter_value FROM POC_FINANCE_CTL.ADMIN.PIPELINE_CONFIG " +
    "WHERE pipeline_name = ? AND parameter_name = 'active_env_prefix'",
    [PIPELINE_NAME]
  );
  if (rsEnv.next()) env = String(rsEnv.getColumnValue(1));
} catch (e) {}

var SILVER_FACT = env + "_FINANCE_SILVER.CORE.F_CUSTOMER_ACCOUNT";
var GOLD_FACT   = env + "_FINANCE_GOLD.MART.F_CUSTOMER_ACCOUNT_DAILY";
var WM_TBL      = "POC_FINANCE_CTL.ADMIN.FACT_LOAD_WATERMARKS";
var FACT_NAME   = "F_CUSTOMER_ACCOUNT_DAILY";

/* Ensure target */
try {
  exec("CREATE SCHEMA IF NOT EXISTS " + env + "_FINANCE_GOLD.MART", []);
  exec(
    "CREATE TABLE IF NOT EXISTS " + GOLD_FACT + " ( " +
    "  DATE_ID        DATE, " +
    "  CUSTOMER_ID    VARCHAR, " +
    "  PRODUCT_ID     VARCHAR, " +
    "  CUSTOMER_SK    NUMBER, " +
    "  PRODUCT_SK     NUMBER, " +
    "  TXN_COUNT      NUMBER, " +
    "  TOTAL_QTY      NUMBER(18,3), " +
    "  TOTAL_AMOUNT   NUMBER(18,2), " +
    "  LOAD_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), " +
    "  UPDATED_BY     VARCHAR DEFAULT CURRENT_USER(), " +
    "  CONSTRAINT PK_F_CUSTOMER_ACCOUNT_DAILY PRIMARY KEY (DATE_ID, CUSTOMER_ID, PRODUCT_ID) " +
    ")",
    []
  );
} catch (e) { return "Failed to ensure GOLD table: " + e.toString(); }

/* Dates */
var end_date = null;
try { var rsEnd = exec("SELECT CURRENT_DATE()", []); if (rsEnd.next()) end_date = rsEnd.getColumnValue(1); }
catch (e) { return "Failed CURRENT_DATE(): " + e.toString(); }

/* Ensure watermark row */
try {
  exec(
    "MERGE INTO " + WM_TBL + " t " +
    "USING (SELECT ? AS FACT_NAME, ? AS ENV) s " +
    "ON t.FACT_NAME = s.FACT_NAME AND t.ENV = s.ENV " +
    "WHEN NOT MATCHED THEN INSERT (FACT_NAME, ENV, LAST_DATE_ID) VALUES (s.FACT_NAME, s.ENV, NULL)",
    [FACT_NAME, env]
  );
} catch (e) { return "Failed ensure watermark row: " + e.toString(); }

/* Compute window start_date */
var start_date = null;
try {
  var rsWM = exec("SELECT LAST_DATE_ID FROM " + WM_TBL + " WHERE FACT_NAME = ? AND ENV = ?", [FACT_NAME, env]);
  var last = null; if (rsWM.next()) last = rsWM.getColumnValue(1);

  if (last) {
    var rs1 = exec("SELECT DATEADD('day', ?, ?::DATE)", [ -graceDays, last ]);
    var back1 = null; if (rs1.next()) back1 = rs1.getColumnValue(1);

    var rs2 = exec("SELECT DATEADD('day', ?, CURRENT_DATE())", [ -lookbackDays ]);
    var back2 = null; if (rs2.next()) back2 = rs2.getColumnValue(1);

    var rs3 = exec("SELECT IFF(?::DATE > ?::DATE, ?::DATE, ?::DATE)", [ back1, back2, back1, back2 ]);
    if (rs3.next()) start_date = rs3.getColumnValue(1);
  } else {
    var rs4 = exec("SELECT DATEADD('day', ?, CURRENT_DATE())", [ -lookbackDays ]);
    if (rs4.next()) start_date = rs4.getColumnValue(1);
  }
} catch (e) { return "Failed to compute start_date: " + e.toString(); }

/* Safety */
try {
  var rsSafe = exec("SELECT IFF(?::DATE <= ?::DATE, 1, 0)", [start_date, end_date]);
  if (rsSafe.next()) {
    var ok = Number(rsSafe.getColumnValue(1));
    if (ok !== 1) {
      var rsFB = exec("SELECT DATEADD('day', -3, CURRENT_DATE())", []);
      if (rsFB.next()) start_date = rsFB.getColumnValue(1);
    }
  }
} catch (e) {}

var deleted = 0, inserted = 0;

/* Delete window */
try {
  var del = exec("DELETE FROM " + GOLD_FACT + " WHERE DATE_ID BETWEEN ?::DATE AND ?::DATE", [start_date, end_date]);
  deleted = del.getRowCount();
} catch (e) { return "Failed to delete GOLD rows: " + e.toString(); }

/* Insert recomputed aggregates */
try {
  var ins = exec(
    "INSERT INTO " + GOLD_FACT + " (DATE_ID, CUSTOMER_ID, PRODUCT_ID, CUSTOMER_SK, PRODUCT_SK, TXN_COUNT, TOTAL_QTY, TOTAL_AMOUNT) " +
    "SELECT " +
    "  f.DATE_ID, f.CUSTOMER_ID, f.PRODUCT_ID, " +
    "  ANY_VALUE(f.CUSTOMER_SK), ANY_VALUE(f.PRODUCT_SK), " +
    "  COUNT(*), SUM(f.QUANTITY), SUM(f.AMOUNT) " +
    "FROM " + SILVER_FACT + " f " +
    "WHERE f.DATE_ID BETWEEN ?::DATE AND ?::DATE " +
    "GROUP BY f.DATE_ID, f.CUSTOMER_ID, f.PRODUCT_ID",
    [start_date, end_date]
  );
  inserted = ins.getRowCount();
} catch (e) { return "Failed to insert GOLD aggregates: " + e.toString(); }

/* Update watermark */
try {
  exec("UPDATE " + WM_TBL + " SET LAST_DATE_ID = ?, UPDATED_AT = CURRENT_TIMESTAMP() WHERE FACT_NAME = ? AND ENV = ?",
      [end_date, FACT_NAME, env]);
} catch (e) { return "Failed to update watermark: " + e.toString(); }

return "GOLD load complete. Env=" + env + ", Window=[" + start_date + " .. " + end_date + "], deleted=" + deleted + ", inserted=" + inserted + ".";
$$;

-- 3.5 Task to schedule the Gold loader (pass VARCHAR args)
CREATE OR REPLACE TASK POC_FINANCE_GOLD.MART.TASK_LOAD_F_CUSTOMER_ACCOUNT_DAILY
  WAREHOUSE = POC_FINANCE_WH
  SCHEDULE  = 'USING CRON 15 1 * * * UTC'  -- daily at 01:15 UTC
AS
  CALL POC_FINANCE_CTL.ADMIN.SP_LOAD_GOLD_CUSTOMER_ACCOUNT_DAILY('PIPELINE_MAIN', '3', '2');

ALTER TASK POC_FINANCE_GOLD.MART.TASK_LOAD_F_CUSTOMER_ACCOUNT_DAILY RESUME;

/* ============================================================
   4) (Optional) Minimal SCD2 dimension seeds for testing
   ============================================================ */

-- Customer SCD2 seed (MERGE to avoid dup errors)
MERGE INTO POC_FINANCE_SILVER.CORE.S_CUSTOMER_DT t
USING (
  SELECT 'C-1003' AS CUSTOMER_ID, 'Noah' AS FIRST_NAME, 'Singh' AS LAST_NAME, 'noah.singh@example.com' AS EMAIL,
         '+1-604-555-0300' AS PHONE, '1992-12-01' AS DOB, 'CA' AS COUNTRY, 'PREMIUM' AS SEGMENT,
         '+1-604-999-0900' AS LANDLINE, 'Single' AS MARITAL_STATUS, 'N' AS VIP_STATUS
) s
ON t.CUSTOMER_ID = s.CUSTOMER_ID AND t.IS_CURRENT = TRUE
WHEN NOT MATCHED THEN
  INSERT (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, DOB, COUNTRY, SEGMENT, LANDLINE, MARITAL_STATUS, VIP_STATUS,
          _SRC_INGEST_TS, EFFECTIVE_FROM_TS, EFFECTIVE_TO_TS, IS_CURRENT, ATTRIBUTE_HASH)
  VALUES (s.CUSTOMER_ID, s.FIRST_NAME, s.LAST_NAME, s.EMAIL, s.PHONE, s.DOB, s.COUNTRY, s.SEGMENT, s.LANDLINE, s.MARITAL_STATUS, s.VIP_STATUS,
          CURRENT_TIMESTAMP(), '1970-01-01'::TIMESTAMP_NTZ, '9999-12-31'::TIMESTAMP_NTZ, TRUE,
          HASH(s.COUNTRY, s.DOB, s.EMAIL, s.FIRST_NAME, s.LANDLINE, s.LAST_NAME, s.MARITAL_STATUS, s.PHONE, s.SEGMENT, s.VIP_STATUS));

MERGE INTO POC_FINANCE_SILVER.CORE.S_CUSTOMER_DT t
USING (
  SELECT 'C-2004' AS CUSTOMER_ID, 'Ava' AS FIRST_NAME, 'Chen' AS LAST_NAME, 'ava.chen@example.com' AS EMAIL,
         '+1-604-555-0400' AS PHONE, '1990-06-06' AS DOB, 'CA' AS COUNTRY, 'STANDARD' AS SEGMENT,
         '+1-604-999-0400' AS LANDLINE, 'Married' AS MARITAL_STATUS, 'N' AS VIP_STATUS
) s
ON t.CUSTOMER_ID = s.CUSTOMER_ID AND t.IS_CURRENT = TRUE
WHEN NOT MATCHED THEN
  INSERT (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, DOB, COUNTRY, SEGMENT, LANDLINE, MARITAL_STATUS, VIP_STATUS,
          _SRC_INGEST_TS, EFFECTIVE_FROM_TS, EFFECTIVE_TO_TS, IS_CURRENT, ATTRIBUTE_HASH)
  VALUES (s.CUSTOMER_ID, s.FIRST_NAME, s.LAST_NAME, s.EMAIL, s.PHONE, s.DOB, s.COUNTRY, s.SEGMENT, s.LANDLINE, s.MARITAL_STATUS, s.VIP_STATUS,
          CURRENT_TIMESTAMP(), '1970-01-01'::TIMESTAMP_NTZ, '9999-12-31'::TIMESTAMP_NTZ, TRUE,
          HASH(s.COUNTRY, s.DOB, s.EMAIL, s.FIRST_NAME, s.LANDLINE, s.LAST_NAME, s.MARITAL_STATUS, s.PHONE, s.SEGMENT, s.VIP_STATUS));

-- Product SCD2 seed
MERGE INTO POC_FINANCE_SILVER.CORE.S_PRODUCT_DT t
USING (SELECT 'P-2001' AS PRODUCT_ID, 'Widget A' AS PRODUCT_NAME, 'Accessories' AS PRODUCT_CATEGORY, 'BrandX' AS BRAND) s
ON t.PRODUCT_ID = s.PRODUCT_ID AND t.IS_CURRENT = TRUE
WHEN NOT MATCHED THEN
  INSERT (PRODUCT_ID, PRODUCT_NAME, PRODUCT_CATEGORY, BRAND, EFFECTIVE_FROM_TS, EFFECTIVE_TO_TS, IS_CURRENT)
  VALUES (s.PRODUCT_ID, s.PRODUCT_NAME, s.PRODUCT_CATEGORY, s.BRAND, '1970-01-01'::TIMESTAMP_NTZ, '9999-12-31'::TIMESTAMP_NTZ, TRUE);

MERGE INTO POC_FINANCE_SILVER.CORE.S_PRODUCT_DT t
USING (SELECT 'P-2002' AS PRODUCT_ID, 'Widget B' AS PRODUCT_NAME, 'Accessories' AS PRODUCT_CATEGORY, 'BrandY' AS BRAND) s
ON t.PRODUCT_ID = s.PRODUCT_ID AND t.IS_CURRENT = TRUE
WHEN NOT MATCHED THEN
  INSERT (PRODUCT_ID, PRODUCT_NAME, PRODUCT_CATEGORY, BRAND, EFFECTIVE_FROM_TS, EFFECTIVE_TO_TS, IS_CURRENT)
  VALUES (s.PRODUCT_ID, s.PRODUCT_NAME, s.PRODUCT_CATEGORY, s.BRAND, '1970-01-01'::TIMESTAMP_NTZ, '9999-12-31'::TIMESTAMP_NTZ, TRUE);

MERGE INTO POC_FINANCE_SILVER.CORE.S_PRODUCT_DT t
USING (SELECT 'P-3001' AS PRODUCT_ID, 'Gadget Pro' AS PRODUCT_NAME, 'Devices' AS PRODUCT_CATEGORY, 'BrandZ' AS BRAND) s
ON t.PRODUCT_ID = s.PRODUCT_ID AND t.IS_CURRENT = TRUE
WHEN NOT MATCHED THEN
  INSERT (PRODUCT_ID, PRODUCT_NAME, PRODUCT_CATEGORY, BRAND, EFFECTIVE_FROM_TS, EFFECTIVE_TO_TS, IS_CURRENT)
  VALUES (s.PRODUCT_ID, s.PRODUCT_NAME, s.PRODUCT_CATEGORY, s.BRAND, '1970-01-01'::TIMESTAMP_NTZ, '9999-12-31'::TIMESTAMP_NTZ, TRUE);

/* ============================================================
   5) TEST DATA → RAW (JSON) + RUN
   ============================================================ */

-- Clean previous test events by prefix
DELETE FROM POC_FINANCE_BRONZE.RAW.CUSTOMER_ACCOUNT_FACT_RAW
WHERE RAW:"source_event_id"::string LIKE 'TST-FA-%';

-- Insert test events (includes duplicate EVT id and cross-day events)
INSERT INTO POC_FINANCE_BRONZE.RAW.CUSTOMER_ACCOUNT_FACT_RAW (RAW, _INGEST_TS)
SELECT PARSE_JSON('{
  "source_event_id":"TST-FA-0001",
  "account_id":"AC-90001","customer_id":"C-1003","product_id":"P-2001",
  "transaction_ts":"2026-01-27T10:00:00Z","quantity":2,"amount":199.98,
  "currency":"CAD","channel":"WEB","attributes":{"promo_code":"NY26","region":"BC"}
}'), CURRENT_TIMESTAMP();

INSERT INTO POC_FINANCE_BRONZE.RAW.CUSTOMER_ACCOUNT_FACT_RAW (RAW, _INGEST_TS)
SELECT PARSE_JSON('{
  "source_event_id":"TST-FA-0002",
  "account_id":"AC-90001","customer_id":"C-1003","product_id":"P-2002",
  "transaction_ts":"2026-01-27T11:30:00Z","quantity":1,"amount":49.99,
  "currency":"CAD","channel":"MOBILE","attributes":{"promo_code":null,"region":"BC"}
}'), CURRENT_TIMESTAMP();

INSERT INTO POC_FINANCE_BRONZE.RAW.CUSTOMER_ACCOUNT_FACT_RAW (RAW, _INGEST_TS)
SELECT PARSE_JSON('{
  "source_event_id":"TST-FA-0003",
  "account_id":"AC-90002","customer_id":"C-2004","product_id":"P-2001",
  "transaction_ts":"2026-01-27T12:10:00Z","quantity":3,"amount":299.97,
  "currency":"CAD","channel":"POS","attributes":{"store_id":"60401","region":"BC"}
}'), CURRENT_TIMESTAMP();

-- Duplicate same event id (dedup should keep latest by _INGEST_TS within RAW)
INSERT INTO POC_FINANCE_BRONZE.RAW.CUSTOMER_ACCOUNT_FACT_RAW (RAW, _INGEST_TS)
SELECT PARSE_JSON('{
  "source_event_id":"TST-FA-0003",
  "account_id":"AC-90002","customer_id":"C-2004","product_id":"P-2001",
  "transaction_ts":"2026-01-27T12:10:00Z","quantity":3,"amount":299.97,
  "currency":"CAD","channel":"POS","attributes":{"store_id":"60401","region":"BC"}
}'), DATEADD('minute', 5, CURRENT_TIMESTAMP());

INSERT INTO POC_FINANCE_BRONZE.RAW.CUSTOMER_ACCOUNT_FACT_RAW (RAW, _INGEST_TS)
SELECT PARSE_JSON('{
  "source_event_id":"TST-FA-0004",
  "account_id":"AC-90003","customer_id":"C-1003","product_id":"P-3001",
  "transaction_ts":"2026-01-28T09:20:00Z","quantity":1,"amount":999.00,
  "currency":"CAD","channel":"WEB","attributes":{"gift_wrap":true}
}'), CURRENT_TIMESTAMP();

-- Stage FROM RAW (RUN_ID = 'ca-run-test-1', lookback 48h)
CALL POC_FINANCE_CTL.ADMIN.SP_LOAD_STAGE_CUSTOMER_ACCOUNT_FROM_RAW('PIPELINE_MAIN','ca-run-test-1','48');

-- Inspect Stage for RUN_ID
SELECT SOURCE_EVENT_ID, ACCOUNT_ID, CUSTOMER_ID, PRODUCT_ID, TRANSACTION_TS, RAW_INGEST_TS
FROM POC_FINANCE_BRONZE.RAW.FACT_CUSTOMER_ACCOUNT_STAGE_TR
WHERE RUN_ID = 'ca-run-test-1'
ORDER BY RAW_INGEST_TS;

-- Load SILVER from RAW
CALL POC_FINANCE_CTL.ADMIN.SP_LOAD_SILVER_CUSTOMER_ACCOUNT_FACT('PIPELINE_MAIN');

-- Validate Silver
SELECT SOURCE_EVENT_ID, ACCOUNT_ID, CUSTOMER_ID, PRODUCT_ID, TRANSACTION_TS, DATE_ID,
       QUANTITY, AMOUNT, CUSTOMER_SK, PRODUCT_SK
FROM POC_FINANCE_SILVER.CORE.F_CUSTOMER_ACCOUNT
WHERE SOURCE_EVENT_ID LIKE 'TST-FA-%'
ORDER BY TRANSACTION_TS;

-- Load GOLD (incremental; VARCHAR params)
CALL POC_FINANCE_CTL.ADMIN.SP_LOAD_GOLD_CUSTOMER_ACCOUNT_DAILY('PIPELINE_MAIN','3','2');

-- Validate GOLD
SELECT *
FROM POC_FINANCE_GOLD.MART.F_CUSTOMER_ACCOUNT_DAILY
WHERE DATE_ID BETWEEN '2026-01-27' AND '2026-01-28'
ORDER BY DATE_ID, CUSTOMER_ID, PRODUCT_ID;

/* ============================================================
   6) Late-arriving event test
   ============================================================ */

-- Late event for 2026-01-27 arrives now
INSERT INTO POC_FINANCE_BRONZE.RAW.CUSTOMER_ACCOUNT_FACT_RAW (RAW, _INGEST_TS)
SELECT PARSE_JSON('{
  "source_event_id":"TST-FA-0005",
  "account_id":"AC-90004","customer_id":"C-1003","product_id":"P-2001",
  "transaction_ts":"2026-01-27T16:45:00Z","quantity":1,"amount":99.99,
  "currency":"CAD","channel":"WEB","attributes":{"region":"BC"}
}'), CURRENT_TIMESTAMP();

-- Reload Silver; event added
CALL POC_FINANCE_CTL.ADMIN.SP_LOAD_SILVER_CUSTOMER_ACCOUNT_FACT('PIPELINE_MAIN');

-- Re-run GOLD; grace window recomputes 2026-01-27
CALL POC_FINANCE_CTL.ADMIN.SP_LOAD_GOLD_CUSTOMER_ACCOUNT_DAILY('PIPELINE_MAIN','3','2');

-- Validate that totals for 2026-01-27 increased correctly
SELECT *
FROM POC_FINANCE_GOLD.MART.F_CUSTOMER_ACCOUNT_DAILY
WHERE DATE_ID = '2026-01-27'
ORDER BY CUSTOMER_ID, PRODUCT_ID;

/* ============================================================
   7) Idempotency checks (re-run Stage/Silver/Gold)
   ============================================================ */

CALL POC_FINANCE_CTL.ADMIN.SP_LOAD_STAGE_CUSTOMER_ACCOUNT_FROM_RAW('PIPELINE_MAIN','ca-run-test-2','48');
SELECT COUNT(*) AS staged_rows
FROM POC_FINANCE_BRONZE.RAW.FACT_CUSTOMER_ACCOUNT_STAGE_TR
WHERE RUN_ID = 'ca-run-test-2';

CALL POC_FINANCE_CTL.ADMIN.SP_LOAD_SILVER_CUSTOMER_ACCOUNT_FACT('PIPELINE_MAIN');
CALL POC_FINANCE_CTL.ADMIN.SP_LOAD_GOLD_CUSTOMER_ACCOUNT_DAILY('PIPELINE_MAIN','3','2');

/* ============================================================
   8) (Optional) Trigger the Task once
   ============================================================ */
-- EXECUTE TASK POC_FINANCE_GOLD.MART.TASK_LOAD_F_CUSTOMER_ACCOUNT_DAILY;

/* ============================================================
   9) Cleanup / Reset (optional for repeatable tests)
   ============================================================ */
-- DELETE FROM POC_FINANCE_SILVER.CORE.F_CUSTOMER_ACCOUNT WHERE SOURCE_EVENT_ID LIKE 'TST-FA-%';
-- DELETE FROM POC_FINANCE_BRONZE.RAW.CUSTOMER_ACCOUNT_FACT_RAW WHERE RAW:"source_event_id"::string LIKE 'TST-FA-%';
-- DELETE FROM POC_FINANCE_BRONZE.RAW.FACT_CUSTOMER_ACCOUNT_STAGE_TR WHERE RUN_ID IN ('ca-run-test-1','ca-run-test-2');
-- DELETE FROM POC_FINANCE_GOLD.MART.F_CUSTOMER_ACCOUNT_DAILY WHERE DATE_ID BETWEEN '2026-01-27' AND '2026-01-28';
-- UPDATE POC_FINANCE_CTL.ADMIN.FACT_LOAD_WATERMARKS SET LAST_DATE_ID = NULL, UPDATED_AT = CURRENT_TIMESTAMP()
-- WHERE FACT_NAME = 'F_CUSTOMER_ACCOUNT_DAILY' AND ENV = 'POC';