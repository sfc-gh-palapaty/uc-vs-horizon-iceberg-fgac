# Captured run: OSS Spark + Snowflake Spark connector against a Horizon-policied Iceberg table

This file captures the actual run output from
`spark_horizon_with_snowflake_connector.py` against the same
Snowflake-managed Iceberg table used in
`spark_horizon_policy_test.py` (the pure-Iceberg-REST sibling). Same
table, same policies, different OSS Spark client wiring.

The *pure* Apache Iceberg REST sibling is now the one that cannot read
this table — Polaris refuses `loadTable` with HTTP 403 once row-access
or column-mask policies are attached. See
[`findings_pure_iceberg_rest.md`](findings_pure_iceberg_rest.md) for
that side of the comparison. This file documents the path that *does*
work for OSS Spark on a policied Snowflake-managed Iceberg table:
`SnowflakeFallbackCatalog`, which falls back to JDBC and lets
Snowflake compute apply the policies.

All identifiers in this file are genericized.

## Setup

Spark configuration (key fields):

```python
.config("spark.jars.packages",
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION},"
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION},"
        f"net.snowflake:snowflake-jdbc:{SNOWFLAKE_JDBC_VERSION},"
        f"net.snowflake:spark-snowflake_2.12:{SNOWFLAKE_CONNECTOR_VERSION}")

.config("spark.sql.catalog.horizoncatalog",
        "org.apache.spark.sql.snowflake.catalog.SnowflakeFallbackCatalog")
.config("spark.sql.catalog.horizoncatalog.catalog-impl",
        "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.horizoncatalog.type", "rest")
.config("spark.sql.catalog.horizoncatalog.uri",
        "https://<account>.snowflakecomputing.com/polaris/api/catalog")
.config("spark.sql.catalog.horizoncatalog.scope",
        f"session:role:{role}")             # role-binds the Iceberg path
.config("spark.sql.catalog.horizoncatalog.header.X-Iceberg-Access-Delegation",
        "vended-credentials")

.config("spark.snowflake.sfRole",      role)   # role-binds the JDBC fallback
.config("spark.snowflake.sfWarehouse", "<warehouse>")
.config("spark.snowflake.sfURL",       "<account>.snowflakecomputing.com")
.config("spark.snowflake.sfDatabase",  "<database>")
.config("spark.snowflake.sfSchema",    "<schema>")
```

The catalog implementation is `SnowflakeFallbackCatalog` from the
Snowflake Spark connector. It wraps Apache Iceberg's `SparkCatalog`
(via `catalog-impl`) and routes Iceberg-readable tables through the
Iceberg REST + S3 path; anything Iceberg can't handle falls back to the
Snowflake Spark connector's JDBC path. Both paths are bound to the
same Snowflake role, so both end up applying the same set of policies
when a non-admin role runs the query.

## Phase A — `ACCOUNTADMIN` (admin role, exempt from policies)

```text
================================================================================
  ROLE: ACCOUNTADMIN
  QUERY: SELECT * FROM horizoncatalog.PUBLIC.<policy_test_table> ORDER BY user_id
================================================================================
+-------+-------------------+--------------+------------+------------+------------+-------------------+
|USER_ID|EMAIL              |FULL_NAME     |IP_ADDRESS  |COUNTRY_CODE|LOGIN_METHOD|EVENT_TIMESTAMP    |
+-------+-------------------+--------------+------------+------------+------------+-------------------+
|1      |alice@example.com  |Alice Johnson |192.168.1.10|US          |SSO         |2026-05-01 10:00:00|
|2      |bob@example.com    |Bob Smith     |10.0.0.25   |CA          |MFA         |2026-05-01 11:00:00|
|3      |carol@example.com  |Carol Williams|172.16.0.5  |GB          |SSO         |2026-05-02 09:30:00|
|4      |dave@example.com   |Dave Brown    |192.168.2.1 |DE          |Password    |2026-05-02 14:00:00|
|5      |eve@example.com    |Eve Davis     |10.10.10.10 |US          |MFA         |2026-05-03 08:45:00|
|6      |frank@example.com  |Frank Miller  |172.16.1.1  |FR          |SSO         |2026-05-03 16:20:00|
|7      |grace@example.com  |Grace Lee     |192.168.3.3 |JP          |MFA         |2026-05-04 07:15:00|
|8      |hank@example.com   |Hank Taylor   |10.20.30.40 |AU          |Password    |2026-05-04 12:00:00|
+-------+-------------------+--------------+------------+------------+------------+-------------------+

  Rows returned: 8
  Sample email : alice@example.com   -> masked? False
  Sample IP    : 192.168.1.10        -> masked? False
```

8 rows, no masking, no filtering. Expected — the row-access policy and
both masking policies exempt the admin role.

## Phase B — restricted role (policies active)

```text
================================================================================
  ROLE: <restricted_role>
  QUERY: SELECT * FROM horizoncatalog.PUBLIC.<policy_test_table> ORDER BY user_id
================================================================================
+-------+------------------+-------------+--------------+------------+------------+-------------------+
|USER_ID|EMAIL             |FULL_NAME    |IP_ADDRESS    |COUNTRY_CODE|LOGIN_METHOD|EVENT_TIMESTAMP    |
+-------+------------------+-------------+--------------+------------+------------+-------------------+
|1      |a***@example.com  |Alice Johnson|***.***.***.10|US          |SSO         |2026-05-01 10:00:00|
|2      |b***@example.com  |Bob Smith    |***.***.***.25|CA          |MFA         |2026-05-01 11:00:00|
|5      |e***@example.com  |Eve Davis    |***.***.***.10|US          |MFA         |2026-05-03 08:45:00|
+-------+------------------+-------------+--------------+------------+------------+-------------------+

  Rows returned: 3
  Sample email : a***@example.com    -> masked? True
  Sample IP    : ***.***.***.10      -> masked? True
```

- Row-access policy (`country IN ('US','CA')`) honored: 3 of 8 rows. UK,
  DE, FR, JP, AU rows are gone.
- Email masking policy honored: every email starts with one letter then
  `***`. Real local-part is not visible.
- IP masking policy honored: first three octets replaced with `***`.

Identical to what `<restricted_role>` would see directly in Snowflake.

## Where enforcement is happening (and which path actually carried this query)

`SnowflakeFallbackCatalog` is a routing wrapper. For a Snowflake-managed
Iceberg table it tries the Iceberg REST path first (cheap, no
Snowflake compute) and uses one of two enforcement-aware mechanisms,
depending on the Iceberg version bundled with the connector and on
what the catalog server supports:

1. **Iceberg REST Scan API** (server-side scan planning). With Spark
   on Iceberg 1.11+, the connector can call Polaris's scan-planning
   endpoint instead of `loadTable`. Polaris evaluates the policies for
   the active role during scan planning and returns concrete data-file
   references that already reflect the row filter and column masks.
   Spark reads only the listed files. This is the documented Iceberg
   protocol mechanism for governed external reads (see
   [duckdb/duckdb-iceberg#977](https://github.com/duckdb/duckdb-iceberg/issues/977)
   for an external write-up of the same flow).

2. **JDBC pushdown to a Snowflake virtual warehouse.** When the Scan
   API path isn't available — older Iceberg, an Iceberg client that
   doesn't implement scan planning, or a table layout the Scan API
   doesn't cover — the connector falls back to a JDBC query against a
   Snowflake warehouse. The SQL engine evaluates the policies at query
   time and returns the governed rows over JDBC.

Both paths are role-bound — `spark.sql.catalog.<cat>.scope` on the
Iceberg side and `spark.snowflake.sfRole` on the JDBC side both wire
to the same Snowflake role.

The captured run in this file pins
`iceberg-spark-runtime-3.5_2.12:1.9.1`, which is *pre*-Scan-API on
the Iceberg-runtime side. The connector therefore took the JDBC
fallback for this run. Verified after the fact by reading
`INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER` on the Snowflake side —
each Spark `SELECT *` shows up as a real warehouse query bound to
the right role:

```text
SELECT "USER_ID", "EMAIL", "FULL_NAME", "IP_ADDRESS", "COUNTRY_CODE",
       "LOGIN_METHOD", "EVENT_TIMESTAMP"
FROM   PUBLIC.<policy_test_table>     -- ROLE_NAME=<restricted_role>, WAREHOUSE=<warehouse>
SELECT "USER_ID", "EMAIL", "FULL_NAME", "IP_ADDRESS", "COUNTRY_CODE",
       "LOGIN_METHOD", "EVENT_TIMESTAMP"
FROM   PUBLIC.<policy_test_table>     -- ROLE_NAME=ACCOUNTADMIN,        WAREHOUSE=<warehouse>
```

That literal-SELECT, fully-qualified-column shape is the Snowflake
Spark connector's JDBC pushdown signature, not a Scan-API metadata
call. There's no Iceberg `loadTable` for this table in either phase;
the Iceberg REST attempt for `loadTable` would have hit the 403
documented in [`findings_pure_iceberg_rest.md`](findings_pure_iceberg_rest.md),
the connector noticed the failure mode, and routed both phases over
JDBC.

Enforcement, therefore, happens in **Snowflake's SQL engine on the
warehouse** for this specific run: row filter and masking policies are
evaluated at query time against the active role, and only the governed
result rows leave the warehouse. The Spark side never receives raw
rows, never receives a manifest list, never receives vended S3
credentials. Same enforcement story as Databricks Query Federation.

For non-policied Snowflake-managed Iceberg tables, the same connector
*does* take the Iceberg REST path (cheaper, no warehouse), reads
parquet directly from S3 with vended credentials, and never spins up
a warehouse. So the routing is adaptive: pick whichever path the
catalog will actually serve under the current policy state.

## Why this isn't a contradiction with the Catalog Federation bypass

Catalog Federation in Databricks UC bypasses Snowflake's policies (see
`databricks_federation/findings.md`) because UC does **not** go through
Polaris's `loadTable` *or* through Snowflake's SQL engine. UC asks
Snowflake for the unredacted `metadata.json` path via a JDBC handshake
that doesn't consult policy state, and then reads the parquet directly
using its own UC storage credential.

The OSS Spark + Snowflake Spark connector path keeps the request inside
the Snowflake-warehouse code path the entire time — JDBC fallback runs
real SQL on Snowflake compute, and the SQL engine applies the policies
before any rows are returned. The bypass only happens when an external
reader skips both Polaris and Snowflake compute and reaches the parquet
by some other means (Catalog Federation, Glue, Hadoop, direct path).

## Reproducing

```bash
export SNOWFLAKE_ACCOUNT_URL="https://<account>.snowflakecomputing.com"
export SNOWFLAKE_PAT="<programmatic-access-token>"
export SNOWFLAKE_USER="<user>"
export SNOWFLAKE_DATABASE="<database>"
export SNOWFLAKE_SCHEMA="<schema>"
export SNOWFLAKE_WAREHOUSE="<warehouse>"
export SNOWFLAKE_REGION="us-east-1"
export SNOWFLAKE_TABLE="<policy_test_table>"
export SNOWFLAKE_RESTRICTED_ROLE="<restricted_role>"
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

python3 snowflake/spark_horizon_with_snowflake_connector.py
```

The script runs both phases automatically (admin then restricted
role). Total wall-clock time ~50 seconds including Spark dependency
download.
