-- =============================================================================
-- 02_apply_row_filter_and_masks.sql
--
-- Step 2 of the test: apply a Unity Catalog row filter and two column masks.
-- This is the equivalent of Snowflake's row access policies + masking
-- policies on the Snowflake-managed Iceberg side of the comparison.
--
--   Row filter: only US/CA rows for non-admin callers
--   Email mask: alice@example.com -> a***@example.com
--   IP    mask: 192.168.1.10      -> ***.***.***.10
--
-- Run AFTER 01_setup_table_and_data.sql, BEFORE re-running the OSS Spark
-- client (Phase B).
-- =============================================================================

USE CATALOG <catalog>;
USE SCHEMA policy_test;

-- ---------------------------------------------------------------------------
-- 1) Row filter: only US/CA rows for non-admin callers.
--    Members of `admins` and `analytics_admin` bypass the filter.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION row_filter_us_ca(country STRING)
RETURNS BOOLEAN
RETURN
  is_account_group_member('admins')
  OR is_account_group_member('analytics_admin')
  OR country IN ('US', 'CA');

-- ---------------------------------------------------------------------------
-- 2) Column mask: email -> a***@domain for non-admin callers.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('admins')
      OR is_account_group_member('analytics_admin')
      THEN email
    WHEN email IS NULL OR instr(email, '@') = 0
      THEN email
    ELSE concat(substring(email, 1, 1), '***', substring(email, instr(email, '@')))
  END;

-- ---------------------------------------------------------------------------
-- 3) Column mask: ip_address -> ***.***.***.<last_octet> for non-admin callers.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mask_ip(ip STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('admins')
      OR is_account_group_member('analytics_admin')
      THEN ip
    WHEN ip IS NULL THEN ip
    ELSE concat('***.***.***.', element_at(split(ip, '\\.'), -1))
  END;

-- ---------------------------------------------------------------------------
-- 4) Attach the policies to the table.
-- ---------------------------------------------------------------------------
ALTER TABLE policy_test_table
  SET ROW FILTER row_filter_us_ca ON (country);

ALTER TABLE policy_test_table
  ALTER COLUMN email      SET MASK mask_email;

ALTER TABLE policy_test_table
  ALTER COLUMN ip_address SET MASK mask_ip;

-- ---------------------------------------------------------------------------
-- 5) Verify enforcement on Databricks compute.
--    A non-admin caller should now see 3 rows with masked email + IP.
-- ---------------------------------------------------------------------------
DESCRIBE EXTENDED policy_test_table;
SELECT * FROM policy_test_table ORDER BY user_id;

-- ---------------------------------------------------------------------------
-- 6) Make sure the principal you'll use from OSS Spark has EXTERNAL USE SCHEMA
--    so you can prove the failure is due to FGAC, not external-access scope.
--    Replace <your_principal> with a workspace user, group, or service principal.
-- ---------------------------------------------------------------------------
-- GRANT EXTERNAL USE SCHEMA ON SCHEMA <catalog>.policy_test
--   TO `<your_principal>`;
