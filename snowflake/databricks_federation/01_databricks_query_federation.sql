-- ============================================================================
-- Databricks-side DDL: Query Federation (JDBC pushdown to Snowflake)
-- ============================================================================
-- Run this in a Databricks SQL editor or notebook attached to a workspace
-- whose metastore admin (or a user with CREATE CONNECTION / CREATE CATALOG)
-- you are running as.
--
-- Behavior: every SELECT against the resulting foreign catalog is rewritten
-- and pushed down to the Snowflake virtual warehouse over JDBC. Snowflake
-- compute evaluates the query, applies the row-access policy and column
-- masks, and returns the governed result set.
--
-- Replace placeholders (in <angle brackets>) with values that fit your
-- environment.
-- ============================================================================

-- 1. Create the foreign connection.
--    Bind it to a non-admin Snowflake role so the FGAC policies actually
--    apply (Snowflake's policies typically exempt ACCOUNTADMIN /
--    SECURITYADMIN / SYSADMIN, so binding to a privileged role would let
--    everything through and obscure the test).
CREATE CONNECTION <query_fed_connection_name> TYPE SNOWFLAKE
  COMMENT 'Query Federation into Snowflake bound to a non-admin role for the FGAC enforcement test'
  OPTIONS (
    host        '<snowflake_account>.snowflakecomputing.com',
    port        '443',
    user        '<snowflake_user>',
    password    '<snowflake_password>',  -- prefer Databricks secrets / DBSQL credential UI in production
    sfWarehouse '<snowflake_warehouse>',
    sfRole      '<restricted_role>'      -- e.g. ANALYST_RO; do NOT use ACCOUNTADMIN here
  );

-- 2. Create a foreign catalog that mirrors the Snowflake database.
--    Note: NO authorized_paths. That's what keeps this in pure Query
--    Federation mode (Provider: snowflake on the resulting tables).
CREATE FOREIGN CATALOG <query_fed_catalog_name>
  USING CONNECTION <query_fed_connection_name>
  OPTIONS ('database' '<snowflake_database>');

-- 3. Sanity check: the foreign catalog should now expose the schemas and
--    tables that <restricted_role> can see in Snowflake.
SHOW SCHEMAS IN <query_fed_catalog_name>;
SHOW TABLES  IN <query_fed_catalog_name>.<schema>;

-- 4. Verify Provider = 'snowflake' on the table you'll probe.
DESCRIBE EXTENDED <query_fed_catalog_name>.<schema>.<policy_test_table>;
-- Look for: Provider snowflake / Type FOREIGN
