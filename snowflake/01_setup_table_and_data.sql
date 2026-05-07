-- =============================================================================
-- snowflake/01_setup_table_and_data.sql
--
-- Step 1 of the Snowflake Horizon side: create a Snowflake-managed Iceberg
-- table accessible via the Polaris REST catalog, and load a small fixed
-- dataset (8 rows, identical to the Databricks side).
--
-- Replace placeholders below with values from your environment.
--
-- Prerequisites:
--   * An EXTERNAL VOLUME pointing at an S3/GCS/Azure location your account
--     can write to. Create one with CREATE EXTERNAL VOLUME if needed.
--   * A warehouse to run DDL on.
--   * The Snowflake role you'll use for Phase A (full access) and the role
--     you'll use for Phase B (restricted) -- both already exist and the
--     restricted role has SELECT granted on the table.
--   * The user you authenticate as from OSS Spark holds:
--       USAGE on the database, schema, warehouse
--       SELECT on the iceberg table
--       USAGE on the external volume
--     ...for both roles you intend to test.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE <database>;        -- e.g. DEMO
USE SCHEMA   <schema>;          -- e.g. PUBLIC
USE WAREHOUSE <warehouse>;      -- e.g. POLICY_TEST_WH

CREATE OR REPLACE ICEBERG TABLE policy_test_table (
  user_id     NUMBER,
  email       STRING,
  ip_address  STRING,
  country     STRING,
  event_type  STRING
)
  CATALOG          = 'SNOWFLAKE'
  EXTERNAL_VOLUME  = '<external_volume_name>'
  BASE_LOCATION    = 'policy_test_table';

INSERT INTO policy_test_table VALUES
  (1, 'alice@example.com',   '192.168.1.10',  'US', 'login'),
  (2, 'bob@example.com',     '10.0.0.5',      'US', 'login'),
  (3, 'carol@example.com',   '172.16.0.20',   'CA', 'vault_open'),
  (4, 'dave@example.co.uk',  '203.0.113.42',  'UK', 'login'),
  (5, 'eve@example.de',      '198.51.100.7',  'DE', 'failed_login'),
  (6, 'frank@example.fr',    '192.0.2.55',    'FR', 'login'),
  (7, 'grace@example.jp',    '203.0.113.99',  'JP', 'vault_open'),
  (8, 'heidi@example.au',    '198.51.100.88', 'AU', 'login');

-- Sanity check before any policies are applied.
SELECT * FROM policy_test_table ORDER BY user_id;

-- ---------------------------------------------------------------------------
-- Grant SELECT to the restricted role you'll use in Phase B.
-- Replace <restricted_role> with e.g. POLICY_TEST_ANALYST.
-- ---------------------------------------------------------------------------
-- GRANT USAGE  ON DATABASE  <database>      TO ROLE <restricted_role>;
-- GRANT USAGE  ON SCHEMA    <database>.<schema>     TO ROLE <restricted_role>;
-- GRANT SELECT ON TABLE     <database>.<schema>.policy_test_table TO ROLE <restricted_role>;
-- GRANT USAGE  ON WAREHOUSE <warehouse>     TO ROLE <restricted_role>;
-- GRANT USAGE  ON EXTERNAL VOLUME <external_volume_name> TO ROLE <restricted_role>;
