USE DATABASE POC_Finance_BRONZE;
USE SCHEMA RAW;

-- 0) Clean (optional) customer C-1003 from Silver for repeatable demo
-- DELETE FROM POC_FINANCE_SILVER.CORE.S_CUSTOMER_DT WHERE CUSTOMER_ID = 'C-1003';

-- 1) First version (baseline)
INSERT INTO CUSTOMERS_RAW (RAW, _INGEST_TS)
SELECT PARSE_JSON('{
  "customer_id":"C-1003","first_name":"Noah","last_name":"Singh",
  "email":"noah.singh@example.com","phone":"+1-604-555-0300",
  "dob":"1992-12-01","country":"CA","segment":"PREMIUM",
  "landline":"+1-604-999-0900","marital_status":"Single","vip_status":"N"
}'), DATEADD('minute', -30, CURRENT_TIMESTAMP());

select * from POC_FINANCE_BRONZE.RAW.CUSTOMERS_RAW order by 2 desc;

select * from POC_FINANCE_BRONZE.RAW.SCD2_CUSTOMER_STAGE_TR;
 select * from POC_FINANCE_SILVER.CORE.S_CUSTOMER_DT;
-- Run SCD2 load
CALL POC_FINANCE_CTL.ADMIN.SP_PROCESS_SCD2_CUSTOMER_FROM_ENTITIES('PIPELINE_MAIN');

-- 2) Change attributes (e.g., segment, phone) → should EXPIRE prior and INSERT a new current row
INSERT INTO CUSTOMERS_RAW (RAW, _INGEST_TS)
SELECT PARSE_JSON('{
  "customer_id":"C-1003","first_name":"Noah","last_name":"Singh",
  "email":"noah.singh@example.com","phone":"+1-604-555-7777",
  "dob":"1992-12-01","country":"CA","segment":"ELITE",
  "landline":"+1-604-999-0900","marital_status":"Single","vip_status":"Y"
}'), DATEADD('minute', -20, CURRENT_TIMESTAMP());

CALL POC_FINANCE_CTL.ADMIN.SP_PROCESS_SCD2_CUSTOMER_FROM_ENTITIES('PIPELINE_MAIN');

-- 3) No change (same attributes) → should NOT create a new SCD row
INSERT INTO CUSTOMERS_RAW (RAW, _INGEST_TS)
SELECT PARSE_JSON('{
  "customer_id":"C-1003","first_name":"Noah","last_name":"Singh",
  "email":"noah.singh@example.com","phone":"+1-604-555-7777",
  "dob":"1992-12-01","country":"CA","segment":"ELITE",
  "landline":"+1-604-999-0900","marital_status":"Single","vip_status":"Y"
}'), DATEADD('minute', -10, CURRENT_TIMESTAMP());

CALL POC_FINANCE_CTL.ADMIN.SP_PROCESS_SCD2_CUSTOMER_FROM_ENTITIES('PIPELINE_MAIN');

-- 4) Another change (e.g., email change) → new current version again
INSERT INTO CUSTOMERS_RAW (RAW, _INGEST_TS)
SELECT PARSE_JSON('{
  "customer_id":"C-1003","first_name":"Noah","last_name":"Singh",
  "email":"n.singh@example.com","phone":"+1-604-555-7777",
  "dob":"1992-12-01","country":"CA","segment":"ELITE",
  "landline":"+1-604-999-0900","marital_status":"Single","vip_status":"Y"
}'), CURRENT_TIMESTAMP();

CALL POC_FINANCE_CTL.ADMIN.SP_PROCESS_SCD2_CUSTOMER_FROM_ENTITIES('PIPELINE_MAIN');