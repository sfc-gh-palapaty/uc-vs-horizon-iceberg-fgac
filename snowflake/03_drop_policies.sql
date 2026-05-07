-- =============================================================================
-- snowflake/03_drop_policies.sql
--
-- Cleanup: detach the row access policy and column masking policies so the
-- Snowflake side returns to Phase A behavior.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE <database>;
USE SCHEMA   <schema>;

ALTER ICEBERG TABLE policy_test_table
  DROP ROW ACCESS POLICY p_country_us_ca;

ALTER ICEBERG TABLE policy_test_table
  MODIFY COLUMN email      UNSET MASKING POLICY;

ALTER ICEBERG TABLE policy_test_table
  MODIFY COLUMN ip_address UNSET MASKING POLICY;

-- Optional: drop the policy objects entirely.
-- DROP ROW ACCESS POLICY IF EXISTS p_country_us_ca;
-- DROP MASKING POLICY     IF EXISTS mask_email;
-- DROP MASKING POLICY     IF EXISTS mask_ip;
