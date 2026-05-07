-- =============================================================================
-- 03_drop_policies.sql
--
-- Cleanup: remove row filter and column masks so Phase A of the OSS Spark
-- test is reproducible after Phase B.
-- =============================================================================

USE CATALOG <catalog>;
USE SCHEMA policy_test;

ALTER TABLE policy_test_table DROP ROW FILTER;
ALTER TABLE policy_test_table ALTER COLUMN email      DROP MASK;
ALTER TABLE policy_test_table ALTER COLUMN ip_address DROP MASK;

-- Optional: drop the UDFs.
-- DROP FUNCTION IF EXISTS row_filter_us_ca;
-- DROP FUNCTION IF EXISTS mask_email;
-- DROP FUNCTION IF EXISTS mask_ip;
