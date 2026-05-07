# Captured run: OSS Spark, *pure* Apache Iceberg REST, against a Horizon-policied Iceberg table

This file captures the actual run output from
`spark_horizon_policy_test.py` — the OSS-Spark client that talks to
Snowflake Polaris using only the Apache Iceberg REST spec, with no
Snowflake Spark connector and no JDBC fallback in the picture.

The companion file
[`findings_snowflake_spark_connector.md`](findings_snowflake_spark_connector.md)
captures the same workload through the
`SnowflakeFallbackCatalog` (Iceberg REST + JDBC fallback) path. The
contrast between the two is the point of having both files: same OSS
Spark host, same Snowflake table, same policies, two different OSS
Spark catalog wirings, two very different outcomes.

All identifiers in this file are genericized.

## TL;DR

When the test table has row-access and column-mask policies attached,
Polaris's `loadTable` returns **`403 Forbidden: Authorization failed`**
to a pure-Iceberg client — for *both* the admin role and the
restricted role. The same `loadTable` call against a sibling table in
the same database with no policies attached returns a normal Iceberg
snapshot with vended S3 credentials. So the 403 is policy-driven, not
account-/role-/network-driven.

This is the same fail-secure shape Databricks Unity Catalog uses, just
implemented at a different layer of the protocol — UC scrubs the
response body to 200 OK with empty fields; Polaris returns an HTTP 403.
Either way, a pure Iceberg REST client cannot read a policied
Snowflake-managed Iceberg table from outside Snowflake compute.

## Setup

The PySpark client configuration (key fields):

```python
.config("spark.jars.packages",
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION},"
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}")

.config("spark.sql.catalog.horizon", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.horizon.type", "rest")
.config("spark.sql.catalog.horizon.uri",
        "https://<account>.snowflakecomputing.com/polaris/api/catalog")
.config("spark.sql.catalog.horizon.warehouse",  "<database>")
.config("spark.sql.catalog.horizon.scope",      f"session:role:{role}")
.config("spark.sql.catalog.horizon.credential", "<programmatic-access-token>")
.config("spark.sql.catalog.horizon.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO")
.config("spark.sql.catalog.horizon.header.X-Iceberg-Access-Delegation",
        "vended-credentials")
```

This is *only* the Apache Iceberg REST OAuth + `loadTable` flow. No
JDBC connector, no `SnowflakeFallbackCatalog`, no Snowflake compute.

## Phase A — `ACCOUNTADMIN` (admin role, exempt from the policies)

```text
================================================================================
  ROLE: ACCOUNTADMIN
  QUERY: SELECT * FROM horizon.PUBLIC.<policy_test_table> ORDER BY user_id
================================================================================
  QUERY FAILED: Py4JJavaError: An error occurred while calling o63.sql.
: org.apache.iceberg.exceptions.ForbiddenException: Forbidden: Authorization failed
    at org.apache.iceberg.rest.ErrorHandlers$DefaultErrorHandler.accept(ErrorHandlers.java:236)
    at org.apache.iceberg.rest.ErrorHandlers$TableErrorHandler.accept(ErrorHandlers.java:123)
    at org.apache.iceberg.rest.HTTPClient.throwFailure(HTTPClient.java:215)
    at org.apache.iceberg.rest.RESTSessionCatalog.loadTable(RESTSessionCatalog.java:397)
    ...
```

The OAuth token exchange against `/polaris/api/catalog/v1/oauth/tokens`
*succeeds* (Polaris issues a bearer JWT bound to
`session:role:ACCOUNTADMIN`). It's the subsequent `GET
/v1/<database>/namespaces/PUBLIC/tables/<policy_test_table>` call —
`loadTable` — that comes back as HTTP 403 with body:

```json
{"error": {"message": "Authorization failed",
           "type":    "ForbiddenException",
           "code":    403}}
```

Note that this is in spite of `ACCOUNTADMIN` being the role the row
filter and both column masks explicitly exempt. The 403 is not a
policy *evaluation* outcome (the policies say admin sees everything);
it's Polaris refusing to serve the table to *any* external Iceberg
caller while these policies are attached.

## Phase B — restricted role (policies active)

```text
================================================================================
  ROLE: <restricted_role>
  QUERY: SELECT * FROM horizon.PUBLIC.<policy_test_table> ORDER BY user_id
================================================================================
  QUERY FAILED: Py4JJavaError: An error occurred while calling o63.sql.
: org.apache.iceberg.exceptions.ForbiddenException: Forbidden: Authorization failed
    ... (identical stack)
```

Same 403, same `Authorization failed`, same `ForbiddenException`. Both
roles get the same outcome: the request is refused at the REST layer
before any data could be returned.

## Forensic check — same Polaris, non-policied sibling table

To confirm the 403 is policy-driven and not account-/network-/role-driven,
the same OAuth token from the Phase A run was used to call `loadTable`
on a sibling table in the same `DEMO.PUBLIC` namespace that has *no*
policies attached:

```text
=== loadTable on <non_policied_table> (no policies attached) ===
config keys              : ['client.region', 'expiration-time', 's3.access-key-id',
                            's3.secret-access-key', 's3.session-token']
vended S3 access-key-id  : PRESENT
vended S3 session-token  : PRESENT
storage-credentials count: 0
snapshot.manifest-list   : 's3://<bucket>/<prefix>/<table>/metadata/snap-<id>.avro'
```

```text
=== loadTable on <policy_test_table>   (row filter + email mask + IP mask attached) ===
ERROR: {"error": {"message": "Authorization failed",
                  "type":    "ForbiddenException",
                  "code":    403}}
```

Same OAuth bearer, same role on the OAuth scope, same vended-credentials
header, same Polaris endpoint, same database/schema. The non-policied
table loads cleanly with vended creds and a real manifest-list pointer.
The policied table is refused with HTTP 403. The policies are the only
input that changes between the two requests, and they fully account for
the difference in outcome.

## Where enforcement is happening

For pure Iceberg REST clients reading a Snowflake-managed Iceberg table
that has row-access or masking policies attached:

> Polaris's `loadTable` returns HTTP 403 Forbidden. The external client
> never receives a snapshot, never receives vended credentials, and
> never gets to read parquet. There is no role-scoped "governed
> snapshot" served to clients that go through `loadTable` while
> policies are attached.

Mechanically, this means Snowflake fail-secures *at the Iceberg REST
boundary* for any client that uses `loadTable`-style Iceberg REST flow
— the same shape as Databricks Unity Catalog, just implemented as an
HTTP 403 instead of a 200 OK with redacted fields. The protocol-level
error is more honest (an explicit `ForbiddenException` instead of a
downstream `Invalid S3 URI`) but the operational result is the same:
pure Iceberg-spec readers that rely on `loadTable` cannot consume a
policied Snowflake-managed Iceberg table.

This is independently corroborated by
[duckdb/duckdb-iceberg#977](https://github.com/duckdb/duckdb-iceberg/issues/977),
which reports the same 403 from a completely different Iceberg client:

> However, tables with Snowflake masking policies or row access
> policies return **HTTP 403 on `GetTableInformation`** — the catalog
> refuses to vend table metadata/credentials because the external
> engine cannot enforce policies on raw Parquet reads.

Same endpoint shape (DuckDB calls it `GetTableInformation`, Iceberg
1.x calls it `loadTable`; both are `GET /v1/<warehouse>/namespaces/<ns>/tables/<table>`),
same outcome.

## Aside: the Iceberg REST Scan API path

The Iceberg REST spec includes a separate flow — server-side scan
planning, the "Scan API" — that is the protocol-level escape hatch
for exactly this scenario. Instead of `loadTable` returning a snapshot
and vending S3 credentials and letting the client plan its own scan,
the catalog server itself does the scan planning and hands the client a
list of concrete data-file references that already reflect the
policy. The client just reads the listed files. The duckdb-iceberg
issue describes Snowflake's role-aware behavior on this API:

> Apache Spark (with Iceberg 1.11+) supports the Scan API, which
> enables **server-side scan planning**. The catalog server
> (Snowflake) evaluates policies during the scan phase and returns
> only the authorized data references.

That mechanism is what makes governed external reads of a policied
table conceptually possible *via the Iceberg spec itself*. The
caveat is that adoption on the client side is uneven today: the
Snowflake Spark connector and Spark+Iceberg 1.11+ implement it,
DuckDB 1.5.2 does not (per
[duckdb/duckdb-iceberg#977](https://github.com/duckdb/duckdb-iceberg/issues/977)),
older Spark+Iceberg combinations do not, and most pure-Iceberg-REST
catalog clients in the wild today still rely on `loadTable` and
therefore see the 403 documented in this file.

Our `spark_horizon_policy_test.py` script pins
`iceberg-spark-runtime-3.5_2.12:1.9.1`, which is pre-Scan-API. So
the 403 captured here is what an older Iceberg client gets, *and*
also what any Iceberg-REST client that hasn't adopted the Scan API
flow gets, regardless of version. To exercise the Scan API path
empirically, see
[`findings_snowflake_spark_connector.md`](findings_snowflake_spark_connector.md):
the connector either uses JDBC pushdown or Scan-API server-side
planning depending on the bundled Iceberg version, and either way
Snowflake compute is in the loop and the policy is applied.

To read the table from OSS Spark while these policies remain attached,
the read has to go through some Snowflake-aware code path that
ultimately runs SQL on a Snowflake virtual warehouse. The
`SnowflakeFallbackCatalog`-based client documented in
[`findings_snowflake_spark_connector.md`](findings_snowflake_spark_connector.md)
takes exactly that route — JDBC fallback to Snowflake compute, where
the SQL engine evaluates the policies for the active role. The Spark
read returns the governed result (3 rows / masked columns for the
restricted role; 8 raw rows for `ACCOUNTADMIN`). Everything that
*works* for the policied table on OSS Spark goes through Snowflake
compute one way or another.

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
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

# Phase A
export SNOWFLAKE_ROLE="ACCOUNTADMIN"
python3 snowflake/spark_horizon_policy_test.py
# -> ForbiddenException: Forbidden: Authorization failed

# Phase B
export SNOWFLAKE_ROLE="<restricted_role>"
python3 snowflake/spark_horizon_policy_test.py
# -> ForbiddenException: Forbidden: Authorization failed

# Forensic side-by-side (curl):
bash snowflake/probe_polaris_iceberg_rest.sh   # against the policied table  -> 403
# (modify SNOWFLAKE_TABLE to point at a non-policied sibling and re-run
#  to see the contrast: vended creds + real manifest-list)
```

To restore the readable-by-Iceberg-REST behavior, drop the policies
(`snowflake/03_drop_policies.sql`); the table immediately becomes a
normal Iceberg table again, and pure-Iceberg clients can read it with
vended creds.

## Relationship to the Databricks UC side of this repo

| Engine boundary | What happens to the external Iceberg-REST `loadTable` request | HTTP shape |
|---|---|---|
| Databricks Unity Catalog → policied table | UC scrubs the response — vended creds removed, `manifest-list` set to empty string. Iceberg client trips on `Invalid S3 URI, cannot determine scheme:` downstream. | **200 OK, body redacted** |
| Snowflake Polaris → Snowflake-managed policied table | Polaris refuses the request entirely. No metadata returned, no vended creds returned. Iceberg client surfaces `ForbiddenException: Authorization failed`. | **403 Forbidden** |

Both behaviors are fail-secure: an external Iceberg-spec client
cannot read raw parquet for a policied table on either platform. The
two implementations differ only in *how* the refusal is wired (response
scrubbing vs HTTP 403) and in how the failure surfaces to the caller.
