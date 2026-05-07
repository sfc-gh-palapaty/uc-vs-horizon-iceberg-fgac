# Captured run: Databricks federation against a Snowflake-policied Iceberg table

All identifiers in this file have been genericized. Real account URLs,
catalog names, user names, and S3 paths were redacted to placeholders
matching the variables in `01_databricks_query_federation.sql` and
`02_databricks_catalog_federation.sql`.

## Setup

- **Snowflake table**: managed Iceberg table at
  `<snowflake_database>.<schema>.<policy_test_table>` with 8 rows
  spanning 7 countries (US, CA, UK, DE, FR, JP, AU). Schema:
  `user_id`, `email`, `ip_address`, `country_code`, `event_type`.
- **Snowflake policies**:
  - Row-access policy: `country_code IN ('US','CA')` for non-admin roles.
  - Masking policy on `email`: keep first letter, replace local-part
    with `***`. Admins exempt.
  - Masking policy on `ip_address`: replace first three octets with
    `***`. Admins exempt.
- **Snowflake role used by the Databricks connection**: `<restricted_role>`,
  which is *not* in the admin exempt list.
- **Snowflake-side baseline** (the same table queried directly in
  Snowflake under `<restricted_role>`):

  ```text
  USER_ID | EMAIL                | IP_ADDRESS         | COUNTRY_CODE | EVENT_TYPE
  --------+----------------------+--------------------+--------------+-----------
  1       | a***@example.com     | ***.***.***.10     | US           | login
  2       | b***@example.com     | ***.***.***.5      | US           | login
  3       | c***@example.com     | ***.***.***.20     | CA           | vault_open
  ```

  3 rows, only US/CA, both PII columns masked. This is what we expect
  *any* trustworthy reader of the table to see when authenticated as
  `<restricted_role>`.

## Path 1 — Databricks Query Federation (CONNECTION TYPE = SNOWFLAKE)

`DESCRIBE EXTENDED` shows:

```text
Type     FOREIGN
Provider snowflake
```

i.e. UC is rewriting each query and pushing it down via JDBC to a
Snowflake virtual warehouse. The Snowflake query engine sees the
connection's role (`<restricted_role>`) and applies the policies before
returning the result set.

```sql
SELECT * FROM <query_fed_catalog_name>.<schema>.<policy_test_table>
ORDER BY user_id;
```

```text
USER_ID | EMAIL                | IP_ADDRESS         | COUNTRY_CODE | EVENT_TYPE
--------+----------------------+--------------------+--------------+-----------
1       | a***@example.com     | ***.***.***.10     | US           | login
2       | b***@example.com     | ***.***.***.5      | US           | login
3       | c***@example.com     | ***.***.***.20     | CA           | vault_open
```

```sql
SELECT COUNT(*), COUNT(DISTINCT country_code) FROM <query_fed_catalog_name>.<schema>.<policy_test_table>;
-- 3, 2

SELECT country_code, COUNT(*) FROM <query_fed_catalog_name>.<schema>.<policy_test_table> GROUP BY country_code;
-- US 2
-- CA 1

SELECT COUNT(*) FROM <query_fed_catalog_name>.<schema>.<policy_test_table>
WHERE country_code NOT IN ('US','CA');
-- 0
```

**Interpretation**: identical to the Snowflake-side baseline. Snowflake's
row-access policy and column masks are honored. Query federation is
**safe**: Databricks is just a SQL caller; Snowflake's governance plane
is fully in the data path.

## Path 2 — Databricks Catalog Federation (FOREIGN CATALOG with `authorized_paths`)

`DESCRIBE EXTENDED` shows:

```text
Type              EXTERNAL
Provider          iceberg
Metadata location s3://<bucket>/<prefix>/.../metadata/00001-...metadata.json
```

i.e. UC asked Snowflake (over the same JDBC connection) for the
`metadata.json` location of the Snowflake-managed table, and is now
reading the parquet **directly from S3** with its own UC storage
credential. Snowflake's query engine is no longer in the path.

```sql
SELECT * FROM <catalog_fed_catalog_name>.<schema>.<policy_test_table>
ORDER BY user_id;
```

```text
USER_ID | EMAIL              | IP_ADDRESS      | COUNTRY_CODE | EVENT_TYPE
--------+--------------------+-----------------+--------------+-------------
1       | alice@example.com  | 192.168.1.10    | US           | login
2       | bob@example.com    | 10.0.0.5        | US           | login
3       | carol@example.com  | 172.16.0.20     | CA           | vault_open
4       | dave@example.co.uk | 203.0.113.42    | UK           | login
5       | eve@example.de     | 198.51.100.7    | DE           | failed_login
6       | frank@example.fr   | 192.0.2.55      | FR           | login
7       | grace@example.jp   | 203.0.113.99    | JP           | vault_open
8       | heidi@example.au   | 198.51.100.88   | AU           | login
```

```sql
SELECT COUNT(*), COUNT(DISTINCT country_code) FROM <catalog_fed_catalog_name>.<schema>.<policy_test_table>;
-- 8, 7

SELECT country_code, COUNT(*) FROM <catalog_fed_catalog_name>.<schema>.<policy_test_table> GROUP BY country_code;
-- AU 1
-- CA 1
-- DE 1
-- FR 1
-- JP 1
-- UK 1
-- US 2

SELECT COUNT(*) FROM <catalog_fed_catalog_name>.<schema>.<policy_test_table>
WHERE country_code NOT IN ('US','CA');
-- 5

SELECT COUNT(*) FILTER (WHERE email      NOT LIKE '%***%') AS unmasked_emails,
       COUNT(*) FILTER (WHERE ip_address NOT LIKE '%***%') AS unmasked_ips
FROM <catalog_fed_catalog_name>.<schema>.<policy_test_table>;
-- unmasked_emails 8 / unmasked_ips 8
```

**Interpretation**: every guarantee Snowflake offers on this table for
non-admin roles has been bypassed.

- The **row-access policy** is gone — UK, DE, FR, JP, AU rows that
  Snowflake would have hidden are present. The "rows the filter should
  have hidden" probe returns 5 instead of 0.
- The **email masking policy** is gone — every email comes back as a
  real address. 8/8 unmasked.
- The **IP masking policy** is gone — every IP comes back as a real
  address. 8/8 unmasked.
- The country histogram includes every country in the parquet, not the
  governed subset.

The reason is mechanical, not a Databricks bug or a Snowflake bug:

1. Snowflake row-access policies and masking policies are
   **query-time** rewrites in the Snowflake query engine. They are not
   row-level deletions, not column-level encryption, not encoded into
   the parquet files.
2. The parquet files written by Snowflake under `<external_volume>` /
   `<base_location>` therefore contain **the complete, raw, ungoverned
   rows**.
3. When asked for the `metadata.json` location even under a non-admin
   role, Snowflake returns the unredacted path. (Verified directly with
   `SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('<table>')` under
   `<restricted_role>`: same path as ACCOUNTADMIN sees.)
4. Catalog Federation hands that path to Databricks, which reads it via
   its own UC storage credential. Snowflake's query engine is not in
   the loop, so neither is its FGAC enforcement.

Compare this to how Databricks Unity Catalog's own Iceberg REST endpoint
behaves when the table has UC row filters or column masks: UC strips
vended credentials and blanks the `manifest-list` field of the
`loadTable` response, deliberately making the table unreadable from
external Iceberg clients. That's a fail-secure design choice. Snowflake
makes a different design choice — expose the metadata, trust the engine
to honor the policy at query time — and Catalog Federation is exactly
the case where that assumption breaks.

## Why this matters

If you have row filters or column masks on Snowflake-managed Iceberg
tables and you let Databricks (or any other engine) configure Catalog
Federation against them, you're trusting **only** the cloud-storage
permissions on the bucket. Your Snowflake row filters do not protect
that path. Your Snowflake column masks do not protect that path.

Practical guidance:

- **Anchor governance to the engine you trust at query time.** If you
  rely on Snowflake row-access and masking policies, prefer Query
  Federation; reject any access path that hands raw S3 reads to a
  foreign engine.
- **Don't grant cross-engine S3 reads to your external-volume bucket
  unless you also enforce policies at the parquet layer** (e.g. write
  the data already filtered/masked, or maintain governed materialized
  views and only expose those).
- **Verify which mode you're actually in.** `DESCRIBE EXTENDED` on the
  Databricks side tells you `Provider: iceberg` (catalog federation,
  direct S3 read) vs `Provider: snowflake` (query federation, JDBC
  pushdown). The visible result is identical for unrestricted tables;
  only policy-bearing tables expose the difference.
