-- ============================================================================
-- Databricks-side DDL: Catalog Federation (direct S3 read)
-- ============================================================================
-- Run this in a Databricks SQL editor or notebook with the same user as
-- the query-federation script. You can reuse the same CONNECTION created
-- by 01_databricks_query_federation.sql; the only thing that distinguishes
-- "Catalog Federation" from "Query Federation" is the foreign-catalog
-- options (specifically `authorized_paths`, plus the requirement that the
-- table's metadata layout meets the direct-read criteria).
--
-- IMPORTANT: this is the path that bypasses Snowflake's row-access policies
-- and column masks. See `findings.md` for the empirical proof.
-- ============================================================================

-- Prerequisite (Databricks side):
--
-- The cloud-storage path that backs Snowflake's external volume must be
-- registered in Unity Catalog as an external location, with a storage
-- credential UC can use. If you don't have one yet:
--
--   CREATE STORAGE CREDENTIAL <cred_name>
--     WITH AWS_IAM_ROLE 'arn:aws:iam::<acct>:role/<role>';
--
--   CREATE EXTERNAL LOCATION <ext_loc_name>
--     URL 's3://<snowflake_external_volume_bucket>/<prefix>/'
--     WITH (CREDENTIAL <cred_name>);
--
-- The IAM role must be allowed to read the bucket where Snowflake's
-- external volume writes its parquet and metadata files. That's a
-- cross-account configuration if your Snowflake external volume is in a
-- different AWS account than your Databricks workspace.

-- 1. (Optional, only if you don't already have one) Create the foreign
--    connection. Reuse <query_fed_connection_name> from
--    01_databricks_query_federation.sql if it exists.
--
-- CREATE CONNECTION <catalog_fed_connection_name> TYPE SNOWFLAKE
--   ... -- same options as the query-federation script

-- 2. Create the foreign catalog WITH authorized_paths. This is what
--    makes UC try the catalog-federation path: ask Snowflake for the
--    metadata.json location, then read parquet directly from S3 via the
--    storage credential bound to <ext_loc_name>.
CREATE FOREIGN CATALOG <catalog_fed_catalog_name>
  USING CONNECTION <query_fed_connection_name>
  OPTIONS (
    'database'         '<snowflake_database>',
    'authorized_paths' 's3://<snowflake_external_volume_bucket>/<prefix>/'
    -- you can list multiple paths separated by commas inside the same
    -- string. Tables whose underlying location isn't covered by an
    -- authorized path will silently fall back to Query Federation.
  );

-- 3. Sanity check the discovery.
SHOW SCHEMAS IN <catalog_fed_catalog_name>;
SHOW TABLES  IN <catalog_fed_catalog_name>.<schema>;

-- 4. Verify Provider = 'iceberg' on the policy-bearing table.
--    If you see Provider = 'snowflake' instead, UC fell back to Query
--    Federation for this table (see "Fallback mode" in the README) and
--    you will see policy-enforced results in step 03 — that's the
--    fallback safety net, not catalog federation actually working. Try
--    a table whose metadata layout meets the direct-read criteria.
DESCRIBE EXTENDED <catalog_fed_catalog_name>.<schema>.<policy_test_table>;
-- Look for:
--   Provider iceberg
--   Type     EXTERNAL
--   "Metadata location" = s3://... (the metadata.json Snowflake exposed)
