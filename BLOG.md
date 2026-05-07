# Open Iceberg, governed: Snowflake Horizon vs Databricks Unity Catalog when a foreign engine reads a policied table

> *"Apache Iceberg is open, so my governance plane shouldn't lock me into one
> compute engine."* — that's the pitch every vendor makes for managed Iceberg
> these days. This post is a short, reproducible test of how true that
> actually is. We attach the same fine-grained access controls (row
> filters and column masks) to an Iceberg table on each vendor, and then
> read the table from a *foreign* engine — first OSS Apache Spark via the
> vendor's Iceberg REST catalog, and then, on the Snowflake side, from
> Databricks via Unity Catalog Federation.

The two vendors take different approaches in how they fail-secure
external readers and in *which* external readers they support, and the
differences matter for any team planning a multi-engine architecture
on top of either platform. This post walks through the test, the
mechanism, and the implications.

The full code and SQL is on GitHub:
**[`uc-vs-horizon-iceberg-fgac`](https://github.com/sfc-gh-palapaty/uc-vs-horizon-iceberg-fgac)**.

---

## The setup, in one paragraph

A small Iceberg table — 8 rows, one row per fictional user, columns
`user_id, email, ip_address, country, event_type` — created twice: once as
a Snowflake-managed Iceberg table inside Snowflake Horizon, once as a
Unity Catalog-managed Iceberg table (`USING ICEBERG`) inside a Databricks
workspace. On both sides we attach the same three policies:

- **Row filter:** non-admin callers see only `country IN ('US','CA')`
- **Column mask on `email`:** `alice@example.com` → `a***@example.com`
- **Column mask on `ip_address`:** `192.168.1.10` → `***.***.***.10`

Then we point an OSS Apache Spark process — the same code, the same Iceberg
1.9.1 jars, running on a laptop — at each platform's Iceberg REST catalog
and run `SELECT * FROM <table>` as a non-admin caller.

The expected behavior, if both platforms truly enforce governance on
Iceberg the way their marketing implies, is the same: the external Spark
client should get back a *governed* view of the table — 3 rows, masked
email, masked IP.

What actually happens is more interesting: **both vendors fail-secure
on the pure Iceberg REST path**. Neither serves a governed view to a
plain Apache Iceberg client when row-access or column-mask policies
are attached. They differ in *how* they fail-secure (HTTP 403 vs
response stripping) and in *which alternative paths* they offer for
external compute that wants to read the same table while honoring the
policies.

---

## The Databricks Unity Catalog side

Setup:

```sql
CREATE TABLE <catalog>.policy_test.policy_test_table (
  user_id BIGINT, email STRING, ip_address STRING,
  country STRING, event_type STRING
) USING ICEBERG;  -- securable_kind = TABLE_DELTA_ICEBERG_MANAGED

CREATE OR REPLACE FUNCTION row_filter_us_ca(country STRING) RETURNS BOOLEAN
RETURN is_account_group_member('admins')
    OR is_account_group_member('analytics_admin')
    OR country IN ('US','CA');

CREATE OR REPLACE FUNCTION mask_email(email STRING) RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('admins')
    OR is_account_group_member('analytics_admin') THEN email
  WHEN email IS NULL OR instr(email,'@')=0        THEN email
  ELSE concat(substring(email,1,1), '***', substring(email, instr(email,'@')))
END;

ALTER TABLE policy_test_table SET ROW FILTER row_filter_us_ca ON (country);
ALTER TABLE policy_test_table ALTER COLUMN email      SET MASK mask_email;
ALTER TABLE policy_test_table ALTER COLUMN ip_address SET MASK mask_ip;
```

OSS Spark connects to UC's Iceberg REST endpoint:

```python
.config("spark.sql.catalog.uc.uri",
        f"{WORKSPACE_URL}/api/2.1/unity-catalog/iceberg-rest")
.config("spark.sql.catalog.uc.warehouse", UC_CATALOG)
.config("spark.sql.catalog.uc.token", PAT_TOKEN)
.config("spark.sql.catalog.uc.header.X-Iceberg-Access-Delegation",
        "vended-credentials")
```

### Phase A — no policies attached yet

Both Databricks SQL (in-workspace) and OSS Spark (via Iceberg REST) return
the full 8 rows, raw. The Iceberg client gets vended S3 credentials in the
REST response and reads parquet directly from S3. So far so good — the
wire path works.

### Phase B — same table, after attaching the row filter and column masks

Databricks SQL (in-workspace) for a non-admin caller returns exactly what
you'd want and expect: 3 rows (US/CA), email and IP masked. UC's
in-engine, name-based policy enforcement is doing its job.

OSS Spark, hitting the same workspace, the same catalog, the same table,
with the same caller — fails:

```
QUERY FAILED: Py4JJavaError: An error occurred while calling o62.showString.
: org.apache.iceberg.exceptions.ValidationException:
        Invalid S3 URI, cannot determine scheme:
    at org.apache.iceberg.aws.s3.S3URI.<init>(S3URI.java:75)
    at org.apache.iceberg.aws.s3.S3InputFile.fromLocation(S3InputFile.java:41)
    at org.apache.iceberg.aws.s3.S3FileIO.newInputFile(S3FileIO.java:176)
    at org.apache.iceberg.BaseSnapshot.cacheManifests(BaseSnapshot.java:176)
    ...
```

That error doesn't look like a Unity Catalog policy decision at first
glance — it looks like an Iceberg client bug. It isn't.

### What UC actually does — peek at the REST response

The most honest way to see what's happening is to skip Spark entirely and
hit UC's Iceberg REST endpoint directly with `curl`:

```bash
curl -s \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "X-Iceberg-Access-Delegation: vended-credentials" \
  "$DATABRICKS_HOST/api/2.1/unity-catalog/iceberg-rest/v1/catalogs/$UC_CATALOG/namespaces/policy_test/tables/policy_test_table"
```

A few key fields, side by side:

| Field in `loadTable` response                | Phase A (no policies)                        | Phase B (policies attached)        |
|---                                           |---                                           |---                                 |
| HTTP status                                  | 200 OK                                       | **200 OK**                         |
| `config.s3.access-key-id`                    | PRESENT                                      | **ABSENT**                         |
| `config.s3.session-token`                    | PRESENT                                      | **ABSENT**                         |
| `config.client.region`                       | PRESENT                                      | **ABSENT**                         |
| `storage-credentials[]`                      | empty (creds in `config`)                    | empty                              |
| `metadata.snapshots[-1].manifest-list`       | real S3 path, e.g. `s3://…/snap-…avro` (265 chars) | **empty string `""`**        |
| `metadata-location`                          | real S3 path                                 | real S3 path (kept, but useless without creds) |

The HTTP response is still `200 OK`. The metadata pointer at the top of
the JSON looks valid. But every actionable field has been redacted by UC:
no vended S3 credentials, and the latest snapshot's `manifest-list` —
the pointer the Iceberg client uses to plan a scan — is the empty string.

That empty string is what the Iceberg client trips on. `S3URI.<init>` is
called with `""`, fails synchronously with `Invalid S3 URI, cannot
determine scheme:` (note the trailing colon and empty value), and the
client error bubbles up. From the client's perspective it looks like a
malformed table; from the platform's perspective it's a deliberate
authorization decision.

---

## The Snowflake Horizon side

Setup, abridged:

```sql
-- Snowflake-managed Iceberg table, accessible via Polaris REST.
CREATE OR REPLACE ICEBERG TABLE demo.public.policy_test_table (...)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'my_s3_volume'
  BASE_LOCATION = 'policy_test_table';

-- Row access policy: non-admin role sees only US/CA rows.
CREATE OR REPLACE ROW ACCESS POLICY p_country_us_ca
  AS (c STRING) RETURNS BOOLEAN ->
    CURRENT_ROLE() = 'ACCOUNTADMIN' OR c IN ('US','CA');

-- Column masks.
CREATE OR REPLACE MASKING POLICY mask_email AS (v STRING) RETURNS STRING ->
  CASE WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN v
       ELSE LEFT(v, 1) || '***' || SUBSTRING(v, POSITION('@' IN v)) END;

ALTER ICEBERG TABLE demo.public.policy_test_table
  ADD ROW ACCESS POLICY p_country_us_ca ON (country);
ALTER ICEBERG TABLE demo.public.policy_test_table
  MODIFY COLUMN email      SET MASKING POLICY mask_email;
ALTER ICEBERG TABLE demo.public.policy_test_table
  MODIFY COLUMN ip_address SET MASKING POLICY mask_ip;
```

OSS Spark configures its catalog against Snowflake Polaris with
`scope=session:role:<role>`:

```python
.config("spark.sql.catalog.horizon.uri", f"{SF_URL}/polaris/api/catalog")
.config("spark.sql.catalog.horizon.scope", f"session:role:{role}")
.config("spark.sql.catalog.horizon.credential", PAT_TOKEN)
.config("spark.sql.catalog.horizon.header.X-Iceberg-Access-Delegation", "vended-credentials")
```

### Phase A — no policies attached yet

`SELECT * FROM horizon.demo.public.policy_test_table` as `ACCOUNTADMIN`
returns all 8 rows, raw. Polaris's `loadTable` returns vended S3
credentials and a real `manifest-list` pointer; the Iceberg client
plans the scan and reads parquet directly. Same wire path as the
Databricks Phase A — works fine on an unpoliced Snowflake-managed
Iceberg table.

### Phase B — same table, after attaching the row filter and column masks

OSS Spark, hitting Polaris with the OAuth scope bound to *any*
Snowflake role — `ACCOUNTADMIN`, the role the policies explicitly
exempt, included — fails:

```
QUERY FAILED: Py4JJavaError: An error occurred while calling o63.sql.
: org.apache.iceberg.exceptions.ForbiddenException: Forbidden: Authorization failed
    at org.apache.iceberg.rest.ErrorHandlers$DefaultErrorHandler.accept(ErrorHandlers.java:236)
    at org.apache.iceberg.rest.ErrorHandlers$TableErrorHandler.accept(ErrorHandlers.java:123)
    at org.apache.iceberg.rest.HTTPClient.throwFailure(HTTPClient.java:215)
    at org.apache.iceberg.rest.RESTSessionCatalog.loadTable(RESTSessionCatalog.java:397)
    ...
```

The OAuth token exchange against `/polaris/api/catalog/v1/oauth/tokens`
*succeeds* — Polaris issues a JWT bound to the role on the OAuth scope.
What gets refused is the next call: `loadTable`. Polaris returns HTTP
403 with body:

```json
{"error": {"message": "Authorization failed",
           "type":    "ForbiddenException",
           "code":    403}}
```

To verify the 403 is policy-driven and not account-/role-/network-driven,
the same OAuth bearer was used to call `loadTable` against a sibling
table in the same database that has *no* policies attached:

```text
=== loadTable on <non_policied_table>  (no policies attached) ===
config keys              : ['client.region', 'expiration-time', 's3.access-key-id',
                            's3.secret-access-key', 's3.session-token']
vended S3 access-key-id  : PRESENT
vended S3 session-token  : PRESENT
snapshot.manifest-list   : 's3://<bucket>/<prefix>/<table>/metadata/snap-<id>.avro'

=== loadTable on <policy_test_table>   (policies attached) ===
ERROR: {"error": {"message": "Authorization failed",
                  "type":    "ForbiddenException", "code": 403}}
```

Same OAuth token, same role, same vended-credentials header, same
Polaris endpoint, same database/schema. Only difference: one table
has FGAC policies attached, the other doesn't. **The policied one is
refused at the REST layer.** Polaris does not attempt to serve a
"role-scoped governed snapshot" — there is no metadata returned at
all.

So the empirical reality on the pure Apache Iceberg REST path is:

> Both vendors fail-secure when an external Iceberg-spec client tries
> to read a managed Iceberg table that has row-access or column-mask
> policies attached. UC stripts the response body to 200 OK with empty
> fields; Polaris returns 403. The table is unreadable from a pure
> Iceberg-REST consumer either way.

### The Snowflake Spark connector path

The story doesn't end there for OSS Spark. Snowflake also ships a
Spark connector — `net.snowflake:spark-snowflake_2.12` — that
includes a hybrid catalog implementation, `SnowflakeFallbackCatalog`.
This catalog wraps Iceberg's `SparkCatalog` and adds two enforcement-
aware mechanisms for reaching policy-protected tables:

1. The **Iceberg REST Scan API** (server-side scan planning), with
   Iceberg 1.11+ on the Spark side. Instead of calling `loadTable`,
   Spark calls Polaris's scan-planning endpoint; Polaris evaluates the
   policies for the active role and returns concrete data-file
   references that already reflect the row filter and column masks.
   Spark reads only those files. This is the protocol-level escape
   hatch the Iceberg REST spec was extended to provide for exactly
   this scenario.
2. **JDBC pushdown to a Snowflake virtual warehouse**, as a fallback
   when the Scan API path isn't available for the client or table
   layout. The SQL engine evaluates the policies at query time and
   returns the governed rows over JDBC.

Both paths are role-bound:

```python
.config("spark.sql.catalog.h",            "org.apache.spark.sql.snowflake.catalog.SnowflakeFallbackCatalog")
.config("spark.sql.catalog.h.catalog-impl","org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.h.type",        "rest")
.config("spark.sql.catalog.h.uri",         f"{SF_URL}/polaris/api/catalog")
.config("spark.sql.catalog.h.scope",       f"session:role:{role}")  # role-binds the Iceberg path

.config("spark.snowflake.sfRole",          role)                    # role-binds the JDBC fallback
.config("spark.snowflake.sfWarehouse",     "<warehouse>")
.config("spark.snowflake.sfPassword",      PAT_TOKEN)
.config("spark.snowflake.sfURL",           "<account>.snowflakecomputing.com")
```

`SELECT * FROM h.public.policy_test_table` as `ACCOUNTADMIN` returns
the full 8 rows, raw. As the restricted role, the same query returns
3 rows (US/CA only) with masked email and masked IP — exactly what
the role would see in-warehouse:

```
+-------+------------------+-------------+--------------+------------+------------+-------------------+
|USER_ID|EMAIL             |FULL_NAME    |IP_ADDRESS    |COUNTRY_CODE|LOGIN_METHOD|EVENT_TIMESTAMP    |
+-------+------------------+-------------+--------------+------------+------------+-------------------+
|1      |a***@example.com  |Alice Johnson|***.***.***.10|US          |SSO         |...                |
|2      |b***@example.com  |Bob Smith    |***.***.***.25|CA          |MFA         |...                |
|5      |e***@example.com  |Eve Davis    |***.***.***.10|US          |MFA         |...                |
+-------+------------------+-------------+--------------+------------+------------+-------------------+
```

Querying `INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER` immediately after
the run shows the read actually executed on a Snowflake virtual
warehouse:

```
SELECT "USER_ID", "EMAIL", "FULL_NAME", "IP_ADDRESS", "COUNTRY_CODE",
       "LOGIN_METHOD", "EVENT_TIMESTAMP"
FROM   PUBLIC.policy_test_table     -- ROLE_NAME=<restricted_role>, WAREHOUSE=<warehouse>
SELECT "USER_ID", "EMAIL", "FULL_NAME", "IP_ADDRESS", "COUNTRY_CODE",
       "LOGIN_METHOD", "EVENT_TIMESTAMP"
FROM   PUBLIC.policy_test_table     -- ROLE_NAME=ACCOUNTADMIN,        WAREHOUSE=<warehouse>
```

That literal `SELECT "USER_ID", "EMAIL", … FROM PUBLIC.…` shape is the
Snowflake Spark connector's JDBC pushdown signature. Our test pins
`iceberg-spark-runtime-3.5_2.12:1.9.1`, which is pre-Scan-API on
the Iceberg-runtime side, so for this run the connector took path 2
(JDBC fallback) for both phases. With a newer Iceberg runtime that
supports server-side scan planning, the connector would prefer path 1
(Scan API) and the warehouse query history would look very different
(metadata-shaped rather than full SELECT) — but the user-visible
result is the same governed view either way.

For non-policied tables on the same connector, the plain Iceberg REST
path *does* succeed — the connector takes the cheap route there and
reads parquet directly with vended creds and no warehouse spin-up.
The routing is adaptive: pick whichever path the catalog will
actually serve under the current policy state.

Captured run output:
[`snowflake/findings_pure_iceberg_rest.md`](https://github.com/sfc-gh-palapaty/uc-vs-horizon-iceberg-fgac/blob/main/snowflake/findings_pure_iceberg_rest.md)
(the 403 path) and
[`snowflake/findings_snowflake_spark_connector.md`](https://github.com/sfc-gh-palapaty/uc-vs-horizon-iceberg-fgac/blob/main/snowflake/findings_snowflake_spark_connector.md)
(the connector path).

---

## Why the two systems behave the way they do

### Both fail-secure at the pure Iceberg REST boundary

Row-access and column-mask policies on a managed Iceberg table are
**query-time** constructs: Snowflake's row-access policies are SQL
expressions evaluated by Snowflake's planner; Databricks's row filters
and column masks are UDFs the UC engine wraps around the relation
before scanning it. Neither set of policies is encoded into the
parquet files. The parquet files are still the raw, complete dataset.

So when an external Iceberg-spec client asks for the table via the
catalog's REST endpoint, the platform has three theoretical options:

1. **Vend creds + metadata pointing at the raw files anyway**, watch
   the policies be silently bypassed. Both vendors *deliberately do
   not* do this. (And as the Catalog Federation case below shows,
   when something else does end up in this position, the bypass is
   real.)
2. **Synthesize a governed snapshot** — write fresh manifest files
   that reference only the rows/columns the role should see, vend
   creds scoped to those files, and serve that snapshot back. This is
   the elegant option in theory, but it requires non-trivial machinery
   per request — masking is a column-level rewrite that doesn't fit
   the Iceberg snapshot model cleanly, and row filters on dynamic
   `CURRENT_ROLE()` predicates would need a fresh snapshot per role.
   Neither vendor does this today.
3. **Fail-secure: refuse the `loadTable` request.** Both vendors took
   this path on the standard Iceberg REST `loadTable` flow. They
   differ only in the protocol shape:

   | | Databricks UC | Snowflake Polaris |
   |---|---|---|
   | HTTP status | 200 OK | 403 Forbidden |
   | Response body | full table metadata, but `manifest-list = ""` and no vended S3 creds | `{"error": {"message": "Authorization failed", "type": "ForbiddenException", "code": 403}}` |
   | What the Iceberg client surfaces | `ValidationException: Invalid S3 URI, cannot determine scheme:` (downstream) | `ForbiddenException: Authorization failed` (direct) |
   | Honesty of the failure | low — looks like a malformed table | high — explicit `Forbidden` from the protocol |

The Snowflake form is more transparent at the protocol level; the UC
form is older and surfaces only as a downstream client exception.
Operationally they have the same effect on this flow: an Iceberg-spec
client that uses `loadTable` cannot read raw parquet for a policied
table.

This is independently corroborated from a completely different
client. [duckdb/duckdb-iceberg#977](https://github.com/duckdb/duckdb-iceberg/issues/977)
reports DuckDB 1.5.2 (with the Iceberg extension) hitting Snowflake's
HIRC endpoint and getting `HTTP Forbidden_403` with message
`Authorization failed` on `GetTableInformation` — the exact same 403
our PySpark client got, from a totally different Iceberg
implementation. So the 403 is a property of the catalog + the
table's policy state, not of any one client's wiring.

### The Scan API is the protocol's escape hatch — and Snowflake implements it

There's a fourth option the Iceberg community standardized
specifically for this case, and Snowflake implements it: the
**Iceberg REST Scan API**, also known as server-side scan planning.
Instead of `loadTable` returning a snapshot pointer plus credentials
and letting the client plan its own scan, the catalog server itself
does the scan planning. It evaluates the policies for the active
role, computes the set of data-file references that the role is
allowed to see (already filtered, already masked), and returns that
list to the client. The client then reads only the listed files.
Polaris implements this for Snowflake-managed Iceberg tables; Spark
with Iceberg 1.11+ supports it on the client side; Snowflake's Spark
connector takes advantage of it.

So the more nuanced statement of what's happening on the Snowflake
side is:

- An Iceberg-spec client that uses **only `loadTable`** gets the
  fail-secure 403. (Older Spark+Iceberg, DuckDB 1.5.2, anything that
  predates server-side scan planning.)
- An Iceberg-spec client that **also uses the Scan API** gets a
  governed-snapshot-equivalent: a list of data files that already
  reflect the policy, vended credentials scoped to those files, and
  reads parquet directly from S3. No Snowflake compute used; the
  policy is enforced by Polaris during scan planning.

UC's documented Iceberg REST endpoint does not currently advertise a
comparable Scan API path for policied tables. So the asymmetry is
narrower and sharper than "Snowflake fails-secure too": both vendors
fail-secure on `loadTable`, but Snowflake offers a documented
Iceberg-protocol-compliant way for adopting clients to read the
policied table; UC effectively requires consumers to be Databricks
compute itself.

### Summary of the alternative paths for external compute

Putting the `loadTable` story and the Scan API story together for the
Snowflake side, an external Iceberg-spec consumer of a policied
Snowflake-managed table actually has three protocol-level paths
available depending on its capabilities:

| Client capability | Path used | Where the policy is enforced | Outcome on a policied table |
|---|---|---|---|
| `loadTable` only (Iceberg <= 1.10, DuckDB 1.5.2, most pure-Iceberg-REST clients today) | Iceberg REST `loadTable` | n/a — request refused | **403 Forbidden** |
| Iceberg REST Scan API (Iceberg 1.11+ on Spark, Snowflake Spark connector) | Iceberg REST scan planning | Polaris during scan planning, returns role-scoped data-file list + scoped vended creds | **Governed read**, no Snowflake compute |
| Snowflake Spark connector with JDBC fallback enabled | JDBC pushdown to a Snowflake warehouse | Snowflake SQL engine at query time | **Governed read**, on Snowflake compute |

UC does not document a comparable Scan API or connector-fallback path
today for policied tables. The supported consumers of a UC-managed
policied table are Databricks compute itself (interactive cluster, SQL
warehouse, Databricks Connect) via name-based SQL. Path-based access
(`spark.read.format("delta").load("s3://…")`), the Iceberg REST path,
and any other path that bypasses the engine are all blocked or
unsupported on policied tables.

So both vendors fail-secure on `loadTable`. Snowflake additionally
implements the Iceberg REST spec's standardized escape hatch for
policied tables (Scan API) and offers the Snowflake Spark connector
as a JDBC-based fallback. The size of the multi-engine governance
story therefore depends as much on *which Iceberg client you're
using* as on which platform you're querying: a Spark-1.11+/connector
client lands in the governed-read column on Snowflake; a
`loadTable`-only client lands in the 403 column on Snowflake (and in
the equivalent fail-secure column on UC).

The behavior is documented:

> You cannot use Iceberg REST catalog or Unity REST APIs to access tables
> with row filters or column masks.
>
> — [Databricks docs — Row filters and column masks, Limitations](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks#limitations)

And the corresponding Databricks Knowledge Base article:

> `[UNAUTHORIZED_ACCESS] Path-based access to table <catalog>.<schema>.<table>
> with row filter or column mask not supported. … Only name-based access
> (`catalog.schema.table`) is allowed.`
>
> — [KB article](https://kb.databricks.com/en_US/delta/unauthorized-access-exception-when-trying-to-access-a-unity-catalog-table-with-row-filters-or-column-masks)

The Iceberg community considered standardizing row filters and column
masks in the Iceberg REST spec itself. The issue
[apache/iceberg#10909](https://github.com/apache/iceberg/issues/10909)
was closed as `not_planned`, so this is a vendor-by-vendor concern for
the foreseeable future.

---

## Implications for multi-engine architectures

Plenty of teams reach for managed Iceberg specifically because they
want a single governance plane that works across compute engines:
Spark on EMR, Trino, Flink, PyIceberg in scripts, DuckDB in
notebooks, Snowflake reading via catalog-linked databases, and so
on. The empirical results above narrow the design space along two
axes — which Iceberg flow the client implements, and which platform
governs the table:

- **Plain `loadTable`-based Iceberg clients** (Spark with
  `type=rest`, PyIceberg, Trino's Iceberg REST connector,
  DuckDB, anything that hasn't adopted server-side scan planning)
  **cannot read a policied managed Iceberg table on either platform**.
  Both UC and Polaris fail-secure on `loadTable` itself. The
  duckdb-iceberg issue
  ([#977](https://github.com/duckdb/duckdb-iceberg/issues/977))
  documents this exact failure for DuckDB against Snowflake's HIRC.
  This is the dominant client capability in the field today, so for
  practical purposes "FGAC + arbitrary Iceberg client = unreadable"
  is the right operational assumption right now.
- **Iceberg clients that implement server-side scan planning (Scan
  API)** — Spark 3.5+ on Iceberg 1.11+, the Snowflake Spark connector
  — **can read a policied Snowflake-managed Iceberg table** and get a
  governed result back. This is an Iceberg-spec-compliant flow, not
  Snowflake-proprietary; the catalog-side bottleneck is which
  vendors implement it, and the client-side bottleneck is Iceberg
  client adoption. UC doesn't currently advertise a Scan API path
  for policied tables, so this option doesn't exist on the UC side.
- **External compute that re-enters the governing engine** can read
  the table even without Scan API support — JDBC pushdown to
  Snowflake (via the Snowflake Spark connector or Databricks Query
  Federation), or name-based SQL on Databricks compute (interactive
  cluster, SQL warehouse, Databricks Connect). The security boundary
  here is the governing engine's SQL evaluator, not the Iceberg REST
  layer.
- **The trap** is *external compute that can reach the parquet files
  directly* without re-entering the governing engine and without
  going through Scan API. That path bypasses the policies entirely,
  because the policies are query-time constructs and the parquet
  files have no idea they exist. Catalog Federation, AWS Glue,
  Hadoop catalog, and any reader with independent credentials to the
  underlying bucket are all in this category. The Catalog Federation
  case below makes this concrete.

---

## A twist: Snowflake-policied table read from Databricks via UC Federation

UC offers two first-class integrations into Snowflake — and they take
dramatically different paths to the data.

### Path 1 — Query Federation (JDBC pushdown)

```sql
CREATE CONNECTION sf_conn TYPE SNOWFLAKE OPTIONS (
  host        '<account>.snowflakecomputing.com',
  user        '<user>',
  password    '<secret>',
  sfWarehouse '<warehouse>',
  sfRole      '<restricted_role>'   -- bound to a non-admin role
);

CREATE FOREIGN CATALOG sf_query_fed USING CONNECTION sf_conn
  OPTIONS ('database' '<database>');
```

`DESCRIBE EXTENDED sf_query_fed.public.policy_test_table` shows
`Provider: snowflake, Type: FOREIGN`. Every `SELECT` is rewritten and
pushed down over JDBC to a Snowflake virtual warehouse, executes on
Snowflake compute, and returns the governed result.

```sql
SELECT * FROM sf_query_fed.public.policy_test_table ORDER BY user_id;
-- 1  a***@example.com  ***.***.***.10  US  login
-- 2  b***@example.com  ***.***.***.5   US  login
-- 3  c***@example.com  ***.***.***.20  CA  vault_open
```

Three rows, masked. Identical to what Snowflake itself returns to the
restricted role. Snowflake's policies are honored because Snowflake's
query engine is in the data path. This is essentially the same shape
as the Snowflake-Spark-connector / `SnowflakeFallbackCatalog` JDBC
fallback path, just with Databricks playing the role of the Spark
driver.

### Path 2 — Catalog Federation (direct S3 read)

The DDL is almost identical but adds an `authorized_paths` option, which
lets UC ask Snowflake for the table's `metadata.json` location and read
the parquet directly from S3 with its own UC storage credential:

```sql
CREATE FOREIGN CATALOG sf_catalog_fed USING CONNECTION sf_conn
  OPTIONS (
    'database'         '<database>',
    'authorized_paths' 's3://<external_volume_bucket>/<prefix>/'
  );
```

`DESCRIBE EXTENDED sf_catalog_fed.public.policy_test_table` shows
`Provider: iceberg, Type: EXTERNAL` along with the metadata location
Snowflake disclosed. The **same SELECT** now returns:

```sql
SELECT * FROM sf_catalog_fed.public.policy_test_table ORDER BY user_id;
-- 1  alice@example.com   192.168.1.10    US   login
-- 2  bob@example.com     10.0.0.5        US   login
-- 3  carol@example.com   172.16.0.20     CA   vault_open
-- 4  dave@example.co.uk  203.0.113.42    UK   login
-- 5  eve@example.de      198.51.100.7    DE   failed_login
-- 6  frank@example.fr    192.0.2.55      FR   login
-- 7  grace@example.jp    203.0.113.99    JP   vault_open
-- 8  heidi@example.au    198.51.100.88   AU   login
```

All eight rows. Email, unmasked. IP, unmasked. The row filter that
should have hidden UK / DE / FR / JP / AU is gone. The masking policies
are gone. The Snowflake table itself hasn't changed; only Databricks's
access path has.

### Why this happens

Snowflake's row-access policies and masking policies are **query-time**
rewrites in Snowflake's SQL engine. They are not row-level deletions,
not column-level encryption, not encoded into the parquet files. The
parquet files written under the table's external volume contain the
complete, raw rows — exactly what an admin would see in-warehouse.

Catalog Federation goes around the Snowflake query engine. UC asks
Snowflake (over the JDBC connection) for the latest `metadata.json`
location, and Snowflake returns the unredacted path *even when the
asking role is not in the policies' admin exempt list* — verified
directly with `SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(...)` under
both ACCOUNTADMIN and the non-admin role; same path. UC then reads the
parquet directly using its own storage credential. Snowflake's FGAC
enforcement is no longer in the path, so it doesn't apply.

This is a notable asymmetry. Polaris, asked through the Iceberg REST
endpoint, *refuses* to serve any external Iceberg-REST client a
policied table — fail-secure HTTP 403, per the Snowflake-side test
above. But Snowflake's *metadata service* does not redact the parquet
location returned over JDBC: it trusts Snowflake's own engine to
honor the policy at query time, which works for Snowflake compute but
not for an engine that reads the parquet directly. Catalog Federation
exploits exactly that gap.

### Fallback mode is a safety net, not the rule

Databricks documents that some tables fall back from Catalog Federation
to Query Federation: tables whose metadata location lives outside the
declared table location, tables whose paths use special characters, and
a few other criteria. When fallback kicks in, the query goes back over
JDBC and Snowflake's policies enforce again. So you may see policy
enforcement even on a foreign catalog you set up for catalog
federation — and conclude, incorrectly, that the path is safe. It
isn't; you got lucky on the table layout.

Always check `DESCRIBE EXTENDED <catalog>.<schema>.<table>`. If you see
`Provider: iceberg`, you're reading parquet directly and Snowflake's
FGAC is not protecting that path.

### Summary

| Snowflake policy | Direct in Snowflake | Snowflake → OSS Spark, pure Iceberg REST | Snowflake → OSS Spark, Snowflake Spark connector | Snowflake → Databricks Query Federation | Snowflake → Databricks Catalog Federation |
|---|---|---|---|---|---|
| Row-access policy | Enforced | **Refused (403)** | Enforced (JDBC fallback) | Enforced (JDBC pushdown) | **Bypassed** |
| Email masking policy | Enforced | **Refused (403)** | Enforced (JDBC fallback) | Enforced | **Bypassed** |
| IP masking policy | Enforced | **Refused (403)** | Enforced (JDBC fallback) | Enforced | **Bypassed** |
| Provider on Databricks side | n/a | n/a | n/a | `snowflake` | `iceberg` |

Customers running Snowflake FGAC and considering Databricks as a reader
should default to Query Federation, treat Catalog Federation as
unsuitable for any table whose security depends on row filters or
column masks, and audit existing Catalog Federation foreign catalogs to
make sure no policied tables are exposed.

The full DDL, probe queries, and captured run output for this scenario
are in
[`snowflake/databricks_federation/`](https://github.com/sfc-gh-palapaty/uc-vs-horizon-iceberg-fgac/tree/main/snowflake/databricks_federation).

---

## Reproducing the test

Everything in this post is reproducible. The repository
[`uc-vs-horizon-iceberg-fgac`](https://github.com/sfc-gh-palapaty/uc-vs-horizon-iceberg-fgac)
contains both sides of the comparison, structured symmetrically:

```
databricks/                            snowflake/
├── 01_setup_table_and_data.sql        ├── 01_setup_table_and_data.sql
├── 02_apply_row_filter_and_masks.sql  ├── 02_apply_policies.sql
├── 03_drop_policies.sql               ├── 03_drop_policies.sql
├── spark_uc_policy_test.py            ├── spark_horizon_policy_test.py
└── probe_uc_iceberg_rest.sh           ├── spark_horizon_with_snowflake_connector.py
                                       └── probe_polaris_iceberg_rest.sh
```

### Databricks side

```bash
# Step 1 (Databricks SQL editor): edit <catalog>, run databricks/01_setup_table_and_data.sql

# Step 2 (laptop):
export DATABRICKS_HOST="https://<your-workspace>.cloud.databricks.com"
export DATABRICKS_TOKEN="<personal-access-token-or-OAuth-bearer>"
export UC_CATALOG="<your-catalog>"
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

python3 databricks/spark_uc_policy_test.py    # Phase A: 8 rows, raw
./databricks/probe_uc_iceberg_rest.sh         # Phase A: READABLE

# Step 3 (Databricks SQL editor): run databricks/02_apply_row_filter_and_masks.sql

python3 databricks/spark_uc_policy_test.py    # Phase B: ValidationException: Invalid S3 URI
./databricks/probe_uc_iceberg_rest.sh         # Phase B: BLOCKED (response scrubbed)

# Optional cleanup: databricks/03_drop_policies.sql
```

### Snowflake side

```bash
# Step 1 (Snowsight / SQL editor): edit placeholders, run snowflake/01_setup_table_and_data.sql

# Step 2 (laptop):
export SNOWFLAKE_ACCOUNT_URL="https://<account>.snowflakecomputing.com"
export SNOWFLAKE_PAT="<programmatic-access-token>"
export SNOWFLAKE_USER="<user>"
export SNOWFLAKE_DATABASE="<database>"
export SNOWFLAKE_SCHEMA="PUBLIC"
export SNOWFLAKE_WAREHOUSE="<warehouse>"
export SNOWFLAKE_REGION="us-east-1"
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

# Phase A: BEFORE policies are attached. Pure Iceberg REST works.
export SNOWFLAKE_ROLE=ACCOUNTADMIN
python3 snowflake/spark_horizon_policy_test.py   # Phase A: 8 rows, raw
./snowflake/probe_polaris_iceberg_rest.sh        # Phase A: READABLE

# Step 3 (Snowsight): run snowflake/02_apply_policies.sql

# Phase B: pure Iceberg REST -- now refused.
python3 snowflake/spark_horizon_policy_test.py   # Phase B: ForbiddenException: Authorization failed
./snowflake/probe_polaris_iceberg_rest.sh        # Phase B: BLOCKED (error.code=403)

# Phase B alternative: Snowflake Spark connector with JDBC fallback.
export SNOWFLAKE_RESTRICTED_ROLE=<restricted_role>
python3 snowflake/spark_horizon_with_snowflake_connector.py
# Phase A 8 rows raw, Phase B 3 rows masked -- governed result via JDBC.

# Optional cleanup: snowflake/03_drop_policies.sql
```

Total elapsed time on each side: well under five minutes, including the
Spark dependency download.

### Snowflake-from-Databricks federation side

```bash
# Step 1 (Snowflake): table + policies already exist (snowflake/01_*.sql + 02_*.sql)
# Step 2 (Databricks SQL editor, as a user with CREATE CONNECTION):
#   - edit placeholders in snowflake/databricks_federation/01_databricks_query_federation.sql
#   - run it (creates CONNECTION + FOREIGN CATALOG, no authorized_paths)
#   - edit placeholders in snowflake/databricks_federation/02_databricks_catalog_federation.sql
#   - run it (creates a FOREIGN CATALOG with authorized_paths)
# Step 3 (Databricks SQL editor):
#   - run snowflake/databricks_federation/03_test_queries.sql against both catalogs
#   - compare row counts, country histograms, and email/IP columns

# DESCRIBE EXTENDED <catalog>.<schema>.<table> tells you whether you're
# really exercising catalog federation (Provider: iceberg) or whether UC
# silently fell back to query federation for that specific table layout
# (Provider: snowflake). Only tables in iceberg-provider mode actually
# demonstrate the FGAC bypass.
```

---

## Closing

"Iceberg is open" is true, and "Iceberg is governed" is true on both
platforms. The interesting question is whether those two things are
*compatible* on a per-table basis when a foreign engine is in the
picture — and the empirical answer has more nuance than the
marketing on either side suggests.

The picture in three statements:

- **Both Databricks Unity Catalog and Snowflake Polaris fail-secure on
  the standard Iceberg REST `loadTable` flow** for tables with
  row-access or column-mask policies attached. UC scrubs the response
  body to `200 OK` with empty fields; Polaris returns `403 Forbidden`.
  Either way, the dominant class of Iceberg-spec clients in the field
  today (anything `loadTable`-based — Spark+Iceberg ≤ 1.10,
  PyIceberg, DuckDB, plain `type=rest` Spark catalogs) cannot read a
  policied table on either platform. The DuckDB issue
  ([duckdb-iceberg#977](https://github.com/duckdb/duckdb-iceberg/issues/977))
  documents this for DuckDB+Snowflake; our test documents it for
  PySpark+Iceberg-1.9.1 against both Snowflake and Databricks.
- **The Iceberg REST spec has a standardized escape hatch — the
  Scan API, server-side scan planning — and Snowflake implements it.**
  Iceberg clients that adopt the Scan API (Spark 3.5+ on Iceberg 1.11+,
  the Snowflake Spark connector) get a governed external read of a
  policied Snowflake-managed table: Polaris evaluates the policy
  during scan planning, returns role-scoped data-file references, and
  the client reads those files directly. UC does not currently
  advertise a comparable Scan API path for policied tables. So the
  multi-engine governance story on Snowflake is real but
  client-version-gated; on UC it requires Databricks compute itself.
- **The trap, on either platform, is external compute that bypasses
  the governing engine and reads the parquet directly.** Catalog
  Federation does this for Snowflake-managed Iceberg tables: 8 raw
  rows, unmasked, regardless of policies. Anything else that hands an
  external engine raw cloud-storage access to the underlying files —
  AWS Glue, Hadoop catalog, an Iceberg client with its own AWS
  credentials, a synced Open Catalog instance — has the same shape.
  Policies are query-time constructs; the parquet has no idea they
  exist.

The practical takeaway for anyone planning a multi-engine architecture
is to be explicit about three things at design time, not at incident
time:

1. **Which engine you trust to enforce policy at query time** — and
   make sure every read path actually goes through that engine, or
   through an Iceberg Scan API that the catalog implements
   policy-aware.
2. **Which Iceberg client capabilities your downstream consumers
   actually have.** A "we pick managed Iceberg so any engine can
   read" plan that targets Snowflake FGAC is realistic for clients
   that adopt server-side scan planning, and unrealistic for clients
   that don't. Inventory your consumers; check whether they're
   `loadTable`-only or Scan-API-aware.
3. **Which paths around the engine exist in your environment.** Audit
   IAM on the underlying buckets, every `CONNECTION TYPE = SNOWFLAKE`
   foreign catalog with `authorized_paths`, every Glue / Open Catalog
   sync, every Iceberg client config that supplies its own cloud
   credentials. Any of those is a bypass channel for FGAC, regardless
   of how strict the catalog is.

UC keeps you safe as long as you stay on Databricks compute and
don't expose Iceberg REST to external readers of policied tables.
Snowflake keeps you safe as long as you stay on Snowflake compute,
or on Iceberg clients that adopt the Scan API, or on engines that
talk to it through Snowflake compute (Snowflake Spark connector,
Databricks Query Federation, JDBC). The boundaries where the
guarantees stop are the same on both sides: pure-`loadTable` Iceberg
clients (refused) and "go around the engine and read the parquet"
paths (silently ungoverned).

---

## References

- Databricks docs — [Row filters and column masks, Limitations](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks#limitations)
- Databricks docs — [Access Databricks tables from Apache Iceberg clients](https://docs.databricks.com/aws/external-access/iceberg)
- Databricks docs — [Unity Catalog credential vending for external system access](https://docs.databricks.com/en/external-access/credential-vending)
- Databricks docs — [What is catalog federation?](https://docs.databricks.com/aws/en/query-federation/catalog-federation)
- Databricks docs — [Enable Snowflake catalog federation](https://docs.databricks.com/aws/en/query-federation/snowflake-catalog-federation)
- Databricks KB — [Unauthorized access exception … row filters or column masks](https://kb.databricks.com/en_US/delta/unauthorized-access-exception-when-trying-to-access-a-unity-catalog-table-with-row-filters-or-column-masks)
- Apache Iceberg — [Issue #10909 "Support row filter & column masking in REST spec"](https://github.com/apache/iceberg/issues/10909) (closed: `not_planned`)
- Apache Iceberg — [REST OpenAPI spec including the scan-planning endpoints](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) (server-side scan planning / Scan API)
- DuckDB Iceberg extension — [Issue #977 "Support for Iceberg REST Catalog Scan API (server-side planning)"](https://github.com/duckdb/duckdb-iceberg/issues/977) — independent corroboration of Snowflake HIRC's 403 for policy-protected tables, plus the role of the Scan API as the protocol-level escape hatch
- Snowflake docs — [Apache Iceberg tables](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- Snowflake docs — [Row access policies](https://docs.snowflake.com/en/user-guide/security-row-intro)
- Snowflake docs — [Polaris (open-source) catalog](https://www.snowflake.com/blog/polaris-catalog-iceberg/)
- Snowflake docs — [Snowflake Connector for Spark](https://docs.snowflake.com/en/user-guide/spark-connector)

---

*Code: [github.com/sfc-gh-palapaty/uc-vs-horizon-iceberg-fgac](https://github.com/sfc-gh-palapaty/uc-vs-horizon-iceberg-fgac)*
*PDF version of this post is in the repo as `UC_Iceberg_Policy_Enforcement_Findings.pdf`.*
