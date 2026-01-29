-- 

-- USE DATABASE POC_Finance_SILVER;  -- or the active env DB (see proc below)
-- USE SCHEMA CORE;

-- DROP DYNAMIC TABLE POC_FINANCE_SILVER.CORE.S_CUSTOMER_DT;

CREATE TABLE IF NOT EXISTS POC_FINANCE_SILVER.CORE.S_CUSTOMER_DT (
  CUSTOMER_SK         NUMBER IDENTITY START 1 INCREMENT 1,
  CUSTOMER_ID         VARCHAR NOT NULL,
  FIRST_NAME          VARCHAR,
  LAST_NAME           VARCHAR,
  EMAIL               VARCHAR,
  PHONE               VARCHAR,
  DOB                 VARCHAR,
  COUNTRY             VARCHAR,
  SEGMENT             VARCHAR,
  LANDLINE            VARCHAR,
  MARITAL_STATUS      VARCHAR,
  VIP_STATUS          VARCHAR,
  _SRC_INGEST_TS      TIMESTAMP_NTZ,

  /* SCD2 fields */
  EFFECTIVE_FROM_TS   TIMESTAMP_NTZ NOT NULL,
  EFFECTIVE_TO_TS     TIMESTAMP_NTZ NOT NULL DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
  IS_CURRENT          BOOLEAN       NOT NULL DEFAULT TRUE,
  ATTRIBUTE_HASH      NUMBER,  -- HASH of tracked attributes (excludes key & dates)

  /* Ops metadata */
  LOAD_TS             TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UPDATED_BY          STRING        DEFAULT CURRENT_USER()
);

-- Optional helpful constraints (informational in Snowflake)
ALTER TABLE CUSTOMERS ADD CONSTRAINT IF NOT EXISTS UK_CUSTOMERS_CURR UNIQUE (CUSTOMER_ID, EFFECTIVE_FROM_TS);

ALTER TABLE POC_FINANCE_CTL.ADMIN.ENTITIES
ADD COLUMN OBJECT_TYPE VARCHAR;


USE DATABASE POC_Finance_CTL;
USE SCHEMA ADMIN;

-- Persistent transient table to store staged 'customer' rows by RUN_ID.
-- All textual fields are VARCHAR, aligned with your Silver table.
CREATE TRANSIENT TABLE IF NOT EXISTS SCD2_CUSTOMER_STAGE_TR (
  RUN_ID           VARCHAR NOT NULL,           -- correlate a specific execution
  PIPELINE_NAME    VARCHAR,
  ENTITY_NAME      VARCHAR,                    -- 'customer'
  ENV              VARCHAR,                    -- active env prefix (e.g., POC)
  -- staged attributes from Bronze (latest per CUSTOMER_ID)
  COUNTRY          VARCHAR,
  CUSTOMER_ID      VARCHAR NOT NULL,
  DOB              VARCHAR,                    -- VARCHAR by your Silver schema
  EMAIL            VARCHAR,
  FIRST_NAME       VARCHAR,
  LANDLINE         VARCHAR,
  LAST_NAME        VARCHAR,
  MARITAL_STATUS   VARCHAR,
  PHONE            VARCHAR,
  SEGMENT          VARCHAR,
  VIP_STATUS       VARCHAR,
  _SRC_INGEST_TS   TIMESTAMP_NTZ,
  ATTRIBUTE_HASH   NUMBER,                     -- hash of tracked attributes
  STAGED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()  -- audit
);

-- Keep time travel to a minimum for cost (tune per policy)
ALTER TABLE SCD2_CUSTOMER_STAGE_TR SET DATA_RETENTION_TIME_IN_DAYS = 1;


Update POC_FINANCE_CTL.ADMIN.ENTITIES
set OBJECT_TYPE='SCD_TYPE_2'
where entity_name='customer';

select * from SCD2_CUSTOMER_STAGE_TR;