-- =============================================================================
-- snowflake/02_apply_policies.sql
--
-- Step 2 of the Snowflake Horizon side: apply a row access policy and two
-- column masking policies, equivalent to the Databricks UC row filter and
-- column masks in databricks/02_apply_row_filter_and_masks.sql.
--
--   Row access policy: non-admin role only sees US/CA rows
--   Email mask:        alice@example.com -> a***@example.com
--   IP    mask:        192.168.1.10      -> ***.***.***.10
--
-- Run AFTER 01_setup_table_and_data.sql. Run BEFORE re-running the OSS
-- Spark client as the restricted role (Phase B).
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE <database>;
USE SCHEMA   <schema>;
USE WAREHOUSE <warehouse>;

-- ---------------------------------------------------------------------------
-- 1) Row access policy: only US/CA rows for non-admin callers.
--    ACCOUNTADMIN bypasses; any other role only sees rows where country IN ('US','CA').
-- ---------------------------------------------------------------------------
CREATE OR REPLACE ROW ACCESS POLICY p_country_us_ca
  AS (country STRING) RETURNS BOOLEAN ->
    CURRENT_ROLE() = 'ACCOUNTADMIN'
    OR country IN ('US', 'CA');

-- ---------------------------------------------------------------------------
-- 2) Column masking policy: email -> a***@domain for non-admin callers.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MASKING POLICY mask_email AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
    WHEN val IS NULL OR POSITION('@' IN val) = 0 THEN val
    ELSE LEFT(val, 1) || '***' || SUBSTRING(val, POSITION('@' IN val))
  END;

-- ---------------------------------------------------------------------------
-- 3) Column masking policy: ip_address -> ***.***.***.<last_octet>.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MASKING POLICY mask_ip AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
    WHEN val IS NULL THEN val
    ELSE '***.***.***.' || SPLIT_PART(val, '.', -1)
  END;

-- ---------------------------------------------------------------------------
-- 4) Attach the policies to the table.
-- ---------------------------------------------------------------------------
ALTER ICEBERG TABLE policy_test_table
  ADD ROW ACCESS POLICY p_country_us_ca ON (country);

ALTER ICEBERG TABLE policy_test_table
  MODIFY COLUMN email      SET MASKING POLICY mask_email;

ALTER ICEBERG TABLE policy_test_table
  MODIFY COLUMN ip_address SET MASKING POLICY mask_ip;

-- ---------------------------------------------------------------------------
-- 5) Verify enforcement on Snowflake compute.
--    As ACCOUNTADMIN: 8 rows, raw.
--    As <restricted_role>: 3 rows (US/CA), masked email + IP.
-- ---------------------------------------------------------------------------
SELECT * FROM policy_test_table ORDER BY user_id;

-- USE ROLE <restricted_role>;
-- SELECT * FROM <database>.<schema>.policy_test_table ORDER BY user_id;
