# Open Iceberg, governed: Snowflake Horizon vs Databricks Unity Catalog when an external Spark engine reads a policied table

> *"Apache Iceberg is open, so my governance plane shouldn't lock me into one
> compute engine."* — that's the pitch every vendor makes for managed Iceberg
> these days. This post is a short, reproducible test of how true that
> actually is, on each of the two main vendors, when you attach
> fine-grained access controls (row filters and column masks) to an
> Iceberg table and then read it from OSS Apache Spark via the vendor's
> Iceberg REST catalog.

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

---

## Closing

"Iceberg is open" is true, and "Iceberg is governed" is true on both
platforms. The interesting question is whether those two things are
*compatible* on a per-table basis when an external engine is in the
picture. On Snowflake Horizon they are. On Databricks Unity Catalog they
are not, today, and the documented behavior makes that explicit. The
practical effect for anyone planning a multi-engine architecture is that
the choice of governance plane has direct consequences for which engines
can read which tables — a choice worth making with eyes open.

---

## References

- Databricks docs — [Row filters and column masks, Limitations](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks#limitations)
- Databricks docs — [Access Databricks tables from Apache Iceberg clients](https://docs.databricks.com/aws/external-access/iceberg)
- Databricks docs — [Unity Catalog credential vending for external system access](https://docs.databricks.com/en/external-access/credential-vending)
- Databricks KB — [Unauthorized access exception … row filters or column masks](https://kb.databricks.com/en_US/delta/unauthorized-access-exception-when-trying-to-access-a-unity-catalog-table-with-row-filters-or-column-masks)
- Apache Iceberg — [Issue #10909 "Support row filter & column masking in REST spec"](https://github.com/apache/iceberg/issues/10909) (closed: `not_planned`)
- Snowflake docs — [Apache Iceberg tables](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- Snowflake docs — [Polaris (open-source) catalog](https://www.snowflake.com/blog/polaris-catalog-iceberg/)

---

*Code: [github.com/sfc-gh-palapaty/uc-vs-horizon-iceberg-fgac](https://github.com/sfc-gh-palapaty/uc-vs-horizon-iceberg-fgac)*
*PDF version of this post is in the repo as `UC_Iceberg_Policy_Enforcement_Findings.pdf`.*
