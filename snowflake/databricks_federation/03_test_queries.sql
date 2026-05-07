-- ============================================================================
-- Probe queries — run the SAME SQL against the query-fed and catalog-fed
-- catalogs and compare. The two should give wildly different answers if
-- catalog federation is actually exercising direct-S3 read; if they
-- agree, UC fell back to query federation (check Provider with DESCRIBE
-- EXTENDED) and you haven't actually tested the catalog-fed path.
-- ============================================================================

-- A. Row count and country distribution.
--    With Snowflake's row-access policy active and the connection bound
--    to a non-admin role, only US/CA rows should be visible. That's
--    what query federation will return. Catalog federation will return
--    *all* rows because the parquet files contain them.
SELECT 'query_fed'   AS path, COUNT(*) AS visible_rows,
       COUNT(DISTINCT country_code) AS distinct_countries
FROM <query_fed_catalog_name>.<schema>.<policy_test_table>
UNION ALL
SELECT 'catalog_fed' AS path, COUNT(*) AS visible_rows,
       COUNT(DISTINCT country_code) AS distinct_countries
FROM <catalog_fed_catalog_name>.<schema>.<policy_test_table>;

-- B. Country histogram. Query fed: only US, CA. Catalog fed: every
--    country in the underlying parquet.
SELECT 'query_fed'   AS path, country_code, COUNT(*) AS n
  FROM <query_fed_catalog_name>.<schema>.<policy_test_table>   GROUP BY country_code
UNION ALL
SELECT 'catalog_fed' AS path, country_code, COUNT(*) AS n
  FROM <catalog_fed_catalog_name>.<schema>.<policy_test_table> GROUP BY country_code
ORDER BY path, country_code;

-- C. Mask check — count how many email/IP cells came back unmasked.
--    Query fed: every email starts with one letter then '***' (or is
--    NULL). Catalog fed: every email is a real address — masking was
--    bypassed because the masking policy is a query-time function in
--    Snowflake and was never serialized into the parquet.
SELECT 'query_fed'   AS path,
       COUNT(*) FILTER (WHERE email      NOT LIKE '%***%') AS unmasked_emails,
       COUNT(*) FILTER (WHERE ip_address NOT LIKE '%***%') AS unmasked_ips
FROM <query_fed_catalog_name>.<schema>.<policy_test_table>
UNION ALL
SELECT 'catalog_fed' AS path,
       COUNT(*) FILTER (WHERE email      NOT LIKE '%***%') AS unmasked_emails,
       COUNT(*) FILTER (WHERE ip_address NOT LIKE '%***%') AS unmasked_ips
FROM <catalog_fed_catalog_name>.<schema>.<policy_test_table>;

-- D. Row-by-row comparison. Read both and look at the same user_id side
--    by side. Catalog fed gives you the raw email and raw IP; query fed
--    gives you the masked versions Snowflake's engine produced.
SELECT user_id, email AS query_fed_email, ip_address AS query_fed_ip
  FROM <query_fed_catalog_name>.<schema>.<policy_test_table>
ORDER BY user_id;

SELECT user_id, email AS catalog_fed_email, ip_address AS catalog_fed_ip
  FROM <catalog_fed_catalog_name>.<schema>.<policy_test_table>
ORDER BY user_id;

-- E. Defeat the row-access policy via the catalog-fed path. The row
--    filter says: "only US/CA visible to <restricted_role>". The
--    catalog-fed read returns every row, so this should return non-zero.
SELECT 'rows_that_the_row_filter_should_have_hidden' AS what,
       COUNT(*) AS n
FROM <catalog_fed_catalog_name>.<schema>.<policy_test_table>
WHERE country_code NOT IN ('US','CA');

-- F. Same query against the query-fed catalog — should return 0,
--    proving Snowflake's policy is doing its job when its query engine
--    is actually in the path.
SELECT 'rows_that_the_row_filter_should_have_hidden' AS what,
       COUNT(*) AS n
FROM <query_fed_catalog_name>.<schema>.<policy_test_table>
WHERE country_code NOT IN ('US','CA');
