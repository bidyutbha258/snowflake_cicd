CREATE OR REPLACE PROCEDURE POC_FINANCE_CTL.ADMIN.SP_PROCESS_SCD2_CUSTOMER_FROM_ENTITIES("PIPELINE_NAME" VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
function exec(sql, binds){ return snowflake.createStatement({sqlText: sql, binds: binds}).execute(); }

/* ---------- Resolve env prefix from pipeline config ---------- */
var env = 'POC';
try {
  var rsEnv = exec(
    "SELECT parameter_value " +
    "  FROM POC_Finance_CTL.ADMIN.PIPELINE_CONFIG " +
    " WHERE pipeline_name = ? AND parameter_name = 'active_env_prefix'",
    [PIPELINE_NAME]
  );
  if (rsEnv.next()) env = String(rsEnv.getColumnValue(1));
} catch (e) {}

/* ---------- Entities filter: ONLY 'customer' with OBJECT_TYPE='SCD_TYPE_2' ---------- */
var ent = exec(
  "SELECT bronze_table, silver_dt " +
  "  FROM POC_Finance_CTL.ADMIN.ENTITIES " +
  " WHERE entity_name = 'customer' AND object_type = 'SCD_TYPE_2'",
  []
);
if (!ent.next()) {
  return "Skipped: ENTITIES has no row where entity_name='customer' and object_type='SCD_TYPE_2'.";
}
var bronze_table = String(ent.getColumnValue(1));   // e.g., CUSTOMERS_RAW
var silver_dt    = String(ent.getColumnValue(2));   // e.g., S_CUSTOMER_DT

/* ---------- Compose FQNs ---------- */
var BRONZE_DB   = env + "_Finance_BRONZE";
var SILVER_DB   = env + "_Finance_SILVER";
var BRONZE_FQN  = BRONZE_DB + ".RAW."  + bronze_table;
var SILVER_FQN  = SILVER_DB + ".CORE." + silver_dt;

/* NOTE: transient staging table is in POC_FINANCE_BRONZE.RAW per your latest note */
var STAGE_FQN   = "POC_FINANCE_BRONZE.RAW.SCD2_CUSTOMER_STAGE_TR";

var CTL_DB      = "POC_Finance_CTL";
var CTL_SCHEMA  = "ADMIN";
var LOAD_LOG_FQN= CTL_DB + "." + CTL_SCHEMA + ".LOAD_LOG_MONITOR";

/* ---------- Monitoring metadata ---------- */
var run_id   = "customer-" + Date.now();
var Stage    = "load_from_bronze_to_silver";
var load_status = 1;
var message  = "";
var new_rows_inserted = 0;   // inserts of new current versions
var rows_updated      = 0;   // expirations of prior current versions
var Source_Object     = BRONZE_FQN;
var Destination_Object= SILVER_FQN;

/* ---------- Stage: clear this run's rows then load latest-per-customer into TRANSIENT table ---------- */
if (load_status === 1) {
  try {
    /* Idempotent clear for this run */
    exec("DELETE FROM " + STAGE_FQN + " WHERE RUN_ID = ?", [run_id]);

    /* Insert latest (deduped) from Bronze for this run
       IMPORTANT: JSON deref uses double-quoted keys (RAW:\"country\") */
    exec(
      "INSERT INTO " + STAGE_FQN + " ( " +
      "  RUN_ID, PIPELINE_NAME, ENTITY_NAME, ENV, " +
      "  COUNTRY, CUSTOMER_ID, DOB, EMAIL, FIRST_NAME, LANDLINE, LAST_NAME, MARITAL_STATUS, PHONE, SEGMENT, VIP_STATUS, _SRC_INGEST_TS, ATTRIBUTE_HASH " +
      ") " +
      "WITH src AS ( " +
      "  SELECT " +
      "    ? AS RUN_ID, ? AS PIPELINE_NAME, 'customer' AS ENTITY_NAME, ? AS ENV, " +
      "    RAW:\"country\"::string        AS COUNTRY, " +
      "    RAW:\"customer_id\"::string    AS CUSTOMER_ID, " +
      "    RAW:\"dob\"::string            AS DOB, " +
      "    RAW:\"email\"::string          AS EMAIL, " +
      "    RAW:\"first_name\"::string     AS FIRST_NAME, " +
      "    RAW:\"landline\"::string       AS LANDLINE, " +
      "    RAW:\"last_name\"::string      AS LAST_NAME, " +
      "    RAW:\"marital_status\"::string AS MARITAL_STATUS, " +
      "    RAW:\"phone\"::string          AS PHONE, " +
      "    RAW:\"segment\"::string        AS SEGMENT, " +
      "    RAW:\"vip_status\"::string     AS VIP_STATUS, " +
      "    _INGEST_TS                     AS _SRC_INGEST_TS " +
      "  FROM " + BRONZE_FQN + " " +
      "  WHERE RAW:\"customer_id\" IS NOT NULL " +
      "), dedup AS ( " +
      "  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY _SRC_INGEST_TS DESC) AS rn " +
      "  FROM src s " +
      ") " +
      "SELECT " +
      "  RUN_ID, PIPELINE_NAME, ENTITY_NAME, ENV, " +
      "  COUNTRY, CUSTOMER_ID, DOB, EMAIL, FIRST_NAME, LANDLINE, LAST_NAME, MARITAL_STATUS, PHONE, SEGMENT, VIP_STATUS, _SRC_INGEST_TS, " +
      "  HASH(COUNTRY, DOB, EMAIL, FIRST_NAME, LANDLINE, LAST_NAME, MARITAL_STATUS, PHONE, SEGMENT, VIP_STATUS) AS ATTRIBUTE_HASH " +
      "FROM dedup WHERE rn = 1",
      [run_id, PIPELINE_NAME, env]
    );
  } catch (e) {
    load_status = 0;
    message = (message ? message + " | " : "") + "Failed to stage into TRANSIENT table: " + e.toString();
  }
}

/* ---------- SCD2 Step 1: expire changed current rows ---------- */
if (load_status === 1) {
  try {
    var updStmt = exec(
      "UPDATE " + SILVER_FQN + " t " +
      "SET EFFECTIVE_TO_TS = s._SRC_INGEST_TS, " +
      "    IS_CURRENT = FALSE, " +
      "    LOAD_TS = CURRENT_TIMESTAMP(), " +
      "    UPDATED_BY = CURRENT_USER() " +
      "FROM " + STAGE_FQN + " s " +
      "WHERE s.RUN_ID = ? " +
      "  AND t.CUSTOMER_ID = s.CUSTOMER_ID " +
      "  AND t.IS_CURRENT = TRUE " +
      "  AND NVL(t.ATTRIBUTE_HASH, 0) <> NVL(s.ATTRIBUTE_HASH, 0)",
      [run_id]
    );
    rows_updated = updStmt.getRowCount();
  } catch (e) {
    load_status = 0;
    message = (message ? message + " | " : "") + "Failed to expire current rows: " + e.toString();
  }
}

/* ---------- SCD2 Step 2: insert new current rows (new or changed) ---------- */
if (load_status === 1) {
  try {
    var insStmt = exec(
      "INSERT INTO " + SILVER_FQN + " ( " +
      "  CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, DOB, COUNTRY, SEGMENT, LANDLINE, MARITAL_STATUS, VIP_STATUS, _SRC_INGEST_TS, " +
      "  EFFECTIVE_FROM_TS, EFFECTIVE_TO_TS, IS_CURRENT, ATTRIBUTE_HASH, LOAD_TS, UPDATED_BY " +
      ") " +
      "SELECT " +
      "  s.CUSTOMER_ID, s.FIRST_NAME, s.LAST_NAME, s.EMAIL, s.PHONE, s.DOB, s.COUNTRY, s.SEGMENT, s.LANDLINE, s.MARITAL_STATUS, s.VIP_STATUS, s._SRC_INGEST_TS, " +
      "  s._SRC_INGEST_TS AS EFFECTIVE_FROM_TS, '9999-12-31'::TIMESTAMP_NTZ AS EFFECTIVE_TO_TS, TRUE AS IS_CURRENT, s.ATTRIBUTE_HASH, CURRENT_TIMESTAMP(), CURRENT_USER() " +
      "FROM " + STAGE_FQN + " s " +
      "LEFT JOIN " + SILVER_FQN + " t " +
      "  ON t.CUSTOMER_ID = s.CUSTOMER_ID " +
      " AND t.IS_CURRENT = TRUE " +
      " AND NVL(t.ATTRIBUTE_HASH, 0) = NVL(s.ATTRIBUTE_HASH, 0) " +
      "WHERE s.RUN_ID = ? " +
      "  AND t.CUSTOMER_ID IS NULL",
      [run_id]
    );
    new_rows_inserted = insStmt.getRowCount();
  } catch (e) {
    load_status = 0;
    message = (message ? message + " | " : "") + "Failed to insert new rows: " + e.toString();
  }
}

/* ---------- Determine monitoring message ---------- */
if (message === "") {
  message = (new_rows_inserted > 0) ? "Successfully loaded" : "No new rows found in Bronze";
}

/* ---------- Monitoring insert ---------- */
try {
  exec(
    "INSERT INTO " + LOAD_LOG_FQN + " " +
    " (RUN_ID, Stage, Source_Object, Destination_Object, load_status, new_rows_inserted, rows_updated, message) " +
    " SELECT ?, ?, ?, ?, ?, ?, ?, ?",
    [run_id, Stage, Source_Object, Destination_Object, load_status, new_rows_inserted, rows_updated, message]
  );
} catch (e) {}

/* ---------- Return summary ---------- */
return "OK: " + env + ": SCD2 load into " + SILVER_FQN +
       "; inserted=" + new_rows_inserted +
       "; expired=" + rows_updated +
       "; status=" + load_status +
       "; msg=" + message +
       "; stage=" + STAGE_FQN +
       "; run_id=" + run_id;
$$;