# Open Iceberg, governed: Snowflake Horizon vs Databricks Unity Catalog when a foreign engine reads a policied table

> *"Apache Iceberg is open, so my governance plane shouldn't lock me into one
> compute engine."* — that's the pitch every vendor makes for managed Iceberg
> these days. This post is a short, reproducible test of how true that
> actually is. We attach the same fine-grained access controls (row
> filters and column masks) to an Iceberg table on each vendor, and then
> read the table from a *foreign* engine — first OSS Apache Spark via the
> vendor's Iceberg REST catalog, and then, on the Snowflake side, from
> Databricks via Unity Catalog Federation.

The two vendors take fundamentally different approaches and produce
different observable results — different enough that anyone planning a
multi-engine architecture on top of either platform should know exactly
which behavior they're signing up for. This post walks through the test,
the mechanism, and the implications.

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

That's what happens on Snowflake Horizon. On Databricks Unity Catalog the
result is materially different.

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

`SELECT * FROM horizon.demo.public.policy_test_table` as `ACCOUNTADMIN`
returns all 8 rows, raw. The same query as a non-admin role returns 3 rows
with `a***@example.com` and `***.***.***.10`. The Spark process is
unmodified between the two runs — only the role on the catalog scope
changes. Horizon's policy enforcement happens server-side in Snowflake's
data plane and is presented to Polaris as the canonical view of the table.
The external engine sees the governed data.

That's the result everyone expects. Now the other side.

---

## The Databricks Unity Catalog side

Setup is the equivalent SQL:

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

---

## What UC actually does — peek at the REST response

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

## Why the two systems diverge

The split is structural, not a feature gap that's about to be closed:

### Snowflake Horizon — *enforce on the data plane*

Snowflake's data plane stores Iceberg metadata that already reflects the
caller's policy view. When an external client asks Polaris for the table,
Snowflake produces a snapshot whose manifest list and data files
correspond to the filtered, masked view that role would have seen
in-warehouse. The credentials Polaris vends are scoped to those filtered
files. Spark reads parquet from S3 and gets back governed data.

This works because Snowflake's data layout for Iceberg is a result of its
query planner, not the raw underlying files. The query planner is the
same code path whether the consumer is a Snowflake virtual warehouse or
an external client.

### Databricks Unity Catalog — *enforce on the engine*

Unity Catalog enforces row filters and column masks by **rewriting query
plans inside Databricks compute**. The base table on S3 is unchanged: it's
still all 8 rows, raw. UC adds the policy at query time, in the engine,
in a name-based access path (`SELECT * FROM catalog.schema.table` rather
than `spark.read.format("delta").load("s3://…")`).

OSS Spark via Iceberg REST cannot be made to honor that. The client gets
a metadata pointer plus credentials, plans the scan itself, and reads
parquet directly from S3. UC has no opportunity to rewrite the plan.

So UC has two choices when an external client asks for a policied table:
**(a)** vend credentials anyway and watch the policies be silently
bypassed, or **(b)** refuse to vend a usable response. UC chose (b), and
chose to do it by stripping the response rather than returning a 403.
That's a defensible security decision and a poor developer-experience
decision: the failure is invisible at the HTTP layer and surfaces only
as an Iceberg-client exception that doesn't mention authorization.

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

Plenty of teams reach for managed Iceberg specifically because they want a
single governance plane that works across compute engines: Spark on EMR,
Trino, Flink, PyIceberg in scripts, Snowflake reading via catalog-linked
databases, and so on. The two vendors offer fundamentally different
contracts for that scenario:

- **Snowflake Horizon** treats the data plane as the enforcement plane.
  Any external Iceberg client gets the governed view. Multi-engine and
  fine-grained access control are not mutually exclusive — you can have
  governance and openness at the same time.
- **Databricks Unity Catalog** treats the engine as the enforcement
  plane. As soon as you attach a row filter or column mask to a table,
  that table effectively becomes Databricks-only: it's still readable
  from Databricks compute via name-based access, but unreadable from any
  external Iceberg client. You have to pick between governance and
  multi-engine access on a per-table basis.

That last point is the headline. UC FGAC is not a free lunch on top of
"open Iceberg". It is a feature that is incompatible with a class of
external consumers that the Iceberg REST endpoint nominally exists to
serve.

The usual workarounds — expose dynamic views with the policy logic
baked in, copy the table into a separate location for external
consumption, restrict external consumers to columns and rows you don't
need to mask — re-introduce exactly the duplication and surface area
that motivated putting FGAC on the base table in the first place.

If your design assumes UC as the single source of truth and external
engines are first-class consumers, this needs to be on the table early.

---

## A twist: Snowflake-policied table read from Databricks via UC Federation

Once you've internalized the difference, the obvious follow-up question
is the inverse one: if Snowflake is the governance plane and the table
sits in S3 in a Snowflake-managed Iceberg format, what does *Databricks*
see when it queries that table?

This matters because Databricks customers don't have to use OSS Spark to
reach a Snowflake-managed table. UC offers two first-class integrations
into Snowflake — and they take dramatically different paths to the data.

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
query engine is in the data path.

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

This is the architectural inverse of UC's own behavior. UC's Iceberg
REST endpoint scrubs `loadTable` for policied tables and refuses to
hand the raw parquet to external readers — fail-secure. Snowflake's
metadata service does not scrub; it trusts the engine to honor the
policy at query time, which works for Snowflake compute but not for an
engine that bypasses the engine.

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

| Snowflake policy | Direct in Snowflake | Snowflake → OSS Spark via Polaris | Snowflake → Databricks Query Federation | Snowflake → Databricks Catalog Federation |
|---|---|---|---|---|
| Row-access policy | Enforced | Enforced (governed snapshot) | Enforced (JDBC pushdown) | **Bypassed** |
| Email masking policy | Enforced | Enforced | Enforced | **Bypassed** |
| IP masking policy | Enforced | Enforced | Enforced | **Bypassed** |
| Provider on Databricks side | n/a | n/a | `snowflake` | `iceberg` |

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
└── probe_uc_iceberg_rest.sh           └── probe_polaris_iceberg_rest.sh
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
./databricks/probe_uc_iceberg_rest.sh         # Phase B: BLOCKED

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

# Phase A as the admin role
export SNOWFLAKE_ROLE=ACCOUNTADMIN
python3 snowflake/spark_horizon_policy_test.py   # Phase A: 8 rows, raw
./snowflake/probe_polaris_iceberg_rest.sh        # Phase A: READABLE

# Step 3 (Snowsight): run snowflake/02_apply_policies.sql

# Phase B as the restricted role
export SNOWFLAKE_ROLE=<restricted_role>          # e.g. POLICY_TEST_ANALYST
python3 snowflake/spark_horizon_policy_test.py   # Phase B: 3 rows, MASKED email + IP
./snowflake/probe_polaris_iceberg_rest.sh        # Phase B: READABLE (governed view)

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
picture, *and* on which side of that engine boundary policies are
actually enforced.

The two platforms make opposite choices, and both choices have
consequences:

- **Databricks Unity Catalog** treats the Databricks engine as the
  enforcement plane. Policied tables are governed inside Databricks,
  unreadable from an external Iceberg client. Fail-secure to outsiders;
  closed to multi-engine reads on policied tables.
- **Snowflake Horizon** treats the data plane as the enforcement plane
  *for engines that go through Snowflake* (native compute, OSS Spark via
  Polaris, Databricks Query Federation). Open to multi-engine reads.
  But that openness has an edge: any path that hands raw S3 access to
  another engine — like Databricks Catalog Federation — bypasses the
  policies entirely, because they are query-time constructs and the
  parquet files have no idea they exist.

The practical takeaway for anyone planning a multi-engine architecture
is to be explicit about *which engine you trust to enforce policy at
query time*, and then make sure every read path actually goes through
that engine. UC gives you that for free if you stay on Databricks
compute. Snowflake gives you that for free if you stay on Snowflake or
on engines that talk to it through Snowflake compute (OSS Spark via
Polaris, Databricks Query Federation). The traps are at the boundaries:
external Iceberg clients hitting UC's REST endpoint, and Databricks
Catalog Federation reading Snowflake-managed parquet directly.

---

## References

- Databricks docs — [Row filters and column masks, Limitations](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks#limitations)
- Databricks docs — [Access Databricks tables from Apache Iceberg clients](https://docs.databricks.com/aws/external-access/iceberg)
- Databricks docs — [Unity Catalog credential vending for external system access](https://docs.databricks.com/en/external-access/credential-vending)
- Databricks docs — [What is catalog federation?](https://docs.databricks.com/aws/en/query-federation/catalog-federation)
- Databricks docs — [Enable Snowflake catalog federation](https://docs.databricks.com/aws/en/query-federation/snowflake-catalog-federation)
- Databricks KB — [Unauthorized access exception … row filters or column masks](https://kb.databricks.com/en_US/delta/unauthorized-access-exception-when-trying-to-access-a-unity-catalog-table-with-row-filters-or-column-masks)
- Apache Iceberg — [Issue #10909 "Support row filter & column masking in REST spec"](https://github.com/apache/iceberg/issues/10909) (closed: `not_planned`)
- Snowflake docs — [Apache Iceberg tables](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- Snowflake docs — [Row access policies](https://docs.snowflake.com/en/user-guide/security-row-intro)
- Snowflake docs — [Polaris (open-source) catalog](https://www.snowflake.com/blog/polaris-catalog-iceberg/)

---

*Code: [github.com/sfc-gh-palapaty/uc-vs-horizon-iceberg-fgac](https://github.com/sfc-gh-palapaty/uc-vs-horizon-iceberg-fgac)*
*PDF version of this post is in the repo as `UC_Iceberg_Policy_Enforcement_Findings.pdf`.*
