select * from POC_FINANCE_CTL.ADMIN.SILVER_PATH_REGISTRY order by 1;

-- Schema evolution example

select * from POC_FINANCE_CTL.ADMIN.SILVER_PATH_REGISTRY where entity_name='customer' order by last_inferred_ts desc; -- 9 columns

select * from POC_FINANCE_CTL.ADMIN.ENTITIES;

-- Last 10 columns , after adding 1 field to customer JSON, 'action'= REBUILD_DT and 'Details' should show 'Rebuilt with 11 columns' 
select * from POC_FINANCE_CTL.ADMIN.SILVER_EVOLUTION_LOG where entity_name ='customer' order by event_ts desc; --

-- 
-- Add 1 field to RAW 'customer' JSON. 
INSERT INTO POC_Finance_BRONZE.RAW.CUSTOMERS_RAW(RAW, _INGEST_TS)
SELECT PARSE_JSON('{
  "customer_id":"C-1003","first_name":"Noah","last_name":"Singh",
  "email":"noah.singh@example.com","phone":"+1-604-555-0300",
  "dob":"1992-12-01","country":"CA","segment":"PREMIUM","landline":"+1-604-999-0900","marital_status":"Single"
}'), DATEADD('day', 30, CURRENT_TIMESTAMP());

INSERT INTO POC_Finance_BRONZE.RAW.CUSTOMERS_RAW(RAW, _INGEST_TS)
SELECT PARSE_JSON('{
  "customer_id":"C-2090","first_name":"BidsY","last_name":"X",
  "email":"bids.b@mytest.com","phone":"+1-980-111-111",
  "dob":"1999-12-31","country":"CA","segment":"Home Service","landline":"+1-980-111-112"
}'), DATEADD('day', 30, CURRENT_TIMESTAMP());

select * from POC_FINANCE_BRONZE.RAW.CUSTOMERS_RAW;

select * from POC_FINANCE_SILVER.CORE.S_CUSTOMER_DT;

select * from POC_FINANCE_GOLD.MARTS.DIM_CUSTOMER;
--delete from POC_FINANCE_SILVER.CORE.S_CUSTOMER_DT where customer_id='C-1003';

execute POC_FINANCE_CTL.ADMIN.T_EVOLVE_ALL_SILVER;

select * from POC_FINANCE_DQ.CORE.COLUMNS_TO_TRACK;
select * from POC_FINANCE_DQ.CORE.DQ_SP_DEBUG_LOG;

select * from POC_FINANCE_DQ.CORE.RULES;

select * from POC_FINANCE_DQ.CORE.RULE_RESULTS;
-- -- Schema drift example

CREATE OR REPLACE TABLE POC_FINANCE_CTL.ADMIN.Load_Log_Monitor
(Log_id  NUMBER  IDENTITY START = 1 INCREMENT = 1,
RUN_ID  VARCHAR,
EVENT_TIME      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
Stage VARCHAR,
Source_Object Varchar,
Destination_Object VARCHAR,
new_rows_inserted int,
rows_updated int,
load_status boolean,
message varchar);

-- 
-- INSERT INTO POC_FINANCE_CTL.ADMIN.Load_Log_Monitor
-- (RUN_ID, Stage,Source_Object,Destination_Object,load_status,message)
-- values
-- (Run_id,'load_from_bronze_to_silver','POC_FINANCE_BRONZE.RAW.CUSTOMERS_RAW','POC_FINANCE_SILVER.CORE.S_CUSTOMER_DT',1, 'Successfully loaded');

select * from POC_FINANCE_CTL.ADMIN.Load_Log_Monitor;

select * from POC_FINANCE_CTL.ADMIN.SILVER_EVOLUTION_LOG limit 1;

select el.*,lm.* from POC_FINANCE_CTL.ADMIN.SILVER_EVOLUTION_LOG as el
join POC_FINANCE_CTL.ADMIN.Load_Log_Monitor as lm
on el.run_id=lm.run_id;


  --CALL POC_Finance_CTL.ADMIN.SP_EVOLVE_SILVER_DT('banking','customer');


  select * from POC_FINANCE_CTL.ADMIN.V_NO_NEW_ROWS_CANDIDATES;