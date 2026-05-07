-- =============================================================================
-- 01_setup_table_and_data.sql
--
-- Step 1 of the test: create a Unity Catalog-managed Iceberg table and load
-- a small, fixed dataset. Run this in a Databricks SQL Editor or notebook
-- as a workspace admin or catalog owner.
--
-- Replace <catalog> below with a catalog you own that has external data
-- access enabled. The schema name `policy_test` is hard-coded for clarity;
-- change it if you prefer.
-- =============================================================================

USE CATALOG <catalog>;
CREATE SCHEMA IF NOT EXISTS policy_test;
USE SCHEMA policy_test;

DROP TABLE IF EXISTS policy_test_table;

CREATE TABLE policy_test_table (
  user_id     BIGINT,
  email       STRING,
  ip_address  STRING,
  country     STRING,
  event_type  STRING
)
USING ICEBERG;  -- UC-managed Iceberg (securable_kind = TABLE_DELTA_ICEBERG_MANAGED)

INSERT INTO policy_test_table VALUES
  (1, 'alice@example.com',   '192.168.1.10',   'US', 'login'),
  (2, 'bob@example.com',     '10.0.0.5',       'US', 'login'),
  (3, 'carol@example.com',   '172.16.0.20',    'CA', 'vault_open'),
  (4, 'dave@example.co.uk',  '203.0.113.42',   'UK', 'login'),
  (5, 'eve@example.de',      '198.51.100.7',   'DE', 'failed_login'),
  (6, 'frank@example.fr',    '192.0.2.55',     'FR', 'login'),
  (7, 'grace@example.jp',    '203.0.113.99',   'JP', 'vault_open'),
  (8, 'heidi@example.au',    '198.51.100.88',  'AU', 'login');

-- Phase A sanity check: full data, no policies yet.
SELECT * FROM policy_test_table ORDER BY user_id;
