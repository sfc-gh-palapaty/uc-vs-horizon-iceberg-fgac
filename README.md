# Unity Catalog vs Snowflake Horizon — Iceberg policy enforcement test

Reproducible empirical test of how Databricks Unity Catalog and Snowflake
Horizon enforce fine-grained access controls (FGAC — row filters and
column masks) on Iceberg tables when those tables are read from a
**different engine** than the one that owns the governance plane.

Five access paths are characterized empirically:

1. Databricks Unity Catalog table accessed by **OSS Apache Spark** via UC's Iceberg REST endpoint.
2. Snowflake-managed Iceberg table accessed by **OSS Apache Spark**, *pure Apache Iceberg REST* against Polaris.
3. Snowflake-managed Iceberg table accessed by **OSS Apache Spark** with the **Snowflake Spark connector** (`SnowflakeFallbackCatalog`, Iceberg REST + JDBC fallback).
4. Snowflake-managed Iceberg table accessed by **Databricks** via UC **Query Federation** (JDBC pushdown).
5. Snowflake-managed Iceberg table accessed by **Databricks** via UC **Catalog Federation** (direct S3 read).

**TL;DR**

| Boundary | Direction | Result |
|---|---|---|
| Databricks UC Iceberg REST → OSS Spark | UC governs, Spark reads | **Fail-secure (response scrubbed):** UC returns `200 OK` with no vended credentials and a blank `manifest-list`. The Iceberg client surfaces this downstream as `Invalid S3 URI, cannot determine scheme:`. Tables with row filters or column masks become unreadable from any external Iceberg-REST consumer. |
| Snowflake Horizon → OSS Spark (**pure Apache Iceberg REST**) | Snowflake governs, Spark calls Polaris directly | **Fail-secure (HTTP 403):** Polaris's `loadTable` returns `ForbiddenException: Authorization failed` for *any* role — including the admin role the policies explicitly exempt — while row-access or column-mask policies are attached. No metadata, no vended credentials, no parquet read. Conceptually the same fail-secure shape as UC, just expressed as a 403 instead of a scrubbed body. |
| Snowflake Horizon → OSS Spark with **Snowflake Spark connector** (`SnowflakeFallbackCatalog`) | Snowflake governs, Spark uses Iceberg REST + JDBC fallback | **Policy-enforcing.** The Iceberg REST attempt gets the same 403 as above, the connector catches that and falls back to JDBC. The query runs on a Snowflake virtual warehouse; the SQL engine evaluates the policies for the active role and returns the governed result (admin: 8 raw rows; restricted role: 3 rows, masked email + IP). |
| Snowflake → Databricks UC **Query Federation** (JDBC pushdown) | Snowflake governs, Databricks reads via Snowflake compute | **Policy-enforcing.** Snowflake's query engine applies the row filter and masks before the result reaches Databricks. |
| Snowflake → Databricks UC **Catalog Federation** (direct S3 read) | Snowflake governs, Databricks reads parquet directly | **Policies bypassed.** Databricks reads the raw parquet from S3 with its own UC storage credential, completely outside Polaris and Snowflake compute. The row-access and masking policies are query-time constructs and were never serialized into the files. |

The single sentence: **both vendors fail-secure on the standard
Iceberg REST `loadTable` flow for policied tables. The Iceberg REST
spec has a standardized escape hatch — server-side scan planning, the
"Scan API" — that Snowflake implements; UC currently does not. To
read a policied managed Iceberg table from an external engine today,
the read has to flow through either (a) an Iceberg client that
implements the Scan API (Spark 3.5+ on Iceberg 1.11+, the Snowflake
Spark connector — Snowflake side only), or (b) native compute on the
governing platform — Databricks compute (name-based SQL) for UC, or a
Snowflake warehouse (JDBC / Snowflake Spark connector / Query
Federation) for Snowflake.** The one external path that *appears* to
work but doesn't enforce policies is Catalog Federation, which goes
around both Polaris and Snowflake compute.

Independent corroboration of the pure-`loadTable` 403 from a totally
different Iceberg client:
[duckdb/duckdb-iceberg#977](https://github.com/duckdb/duckdb-iceberg/issues/977)
reports DuckDB 1.5.2 hitting Snowflake's HIRC endpoint and getting
exactly the same `HTTP Forbidden_403 / Authorization failed` we got
from PySpark + Iceberg 1.9.1.

UC's documented limitation, verbatim:

> You cannot use Iceberg REST catalog or Unity REST APIs to access tables
> with row filters or column masks.
>
> — [Databricks docs — Row filters and column masks, Limitations](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks#limitations)

Full write-up — including the field-by-field diff of UC's `loadTable`
response with vs. without policies attached, the empirical Polaris 403
captured against a policied vs. non-policied sibling table, and the
Catalog-Federation bypass — is in **[BLOG.md](BLOG.md)** and
**[UC_Iceberg_Policy_Enforcement_Findings.pdf](UC_Iceberg_Policy_Enforcement_Findings.pdf)**.

## Repository layout

```
.
├── BLOG.md                                    Long-form blog post
├── README.md                                  This file
├── LICENSE
├── UC_Iceberg_Policy_Enforcement_Findings.pdf Technical PDF write-up
├── UC_Iceberg_Policy_Enforcement_Findings.html
│
├── databricks/                                Unity Catalog side of the test
│   ├── 01_setup_table_and_data.sql            Create UC-managed Iceberg table + 8 rows
│   ├── 02_apply_row_filter_and_masks.sql      Define UDFs + ALTER TABLE ... SET ROW FILTER / SET MASK
│   ├── 03_drop_policies.sql                   Cleanup
│   ├── spark_uc_policy_test.py                OSS Spark client over UC Iceberg REST
│   └── probe_uc_iceberg_rest.sh               curl probe of UC's loadTable response
│
└── snowflake/                                 Snowflake Horizon side of the test
    ├── 01_setup_table_and_data.sql            Create Snowflake-managed Iceberg table + 8 rows
    ├── 02_apply_policies.sql                  Define ROW ACCESS / MASKING POLICIES + ALTER ICEBERG TABLE
    ├── 03_drop_policies.sql                   Cleanup
    ├── spark_horizon_policy_test.py           OSS Spark client, pure Apache Iceberg REST → Polaris
    ├── spark_horizon_with_snowflake_connector.py  OSS Spark client, SnowflakeFallbackCatalog (Iceberg REST + JDBC fallback)
    ├── findings_pure_iceberg_rest.md          Captured run: pure Iceberg REST → 403 on policied table
    ├── findings_snowflake_spark_connector.md  Captured run: connector path → JDBC fallback → governed result
    ├── probe_polaris_iceberg_rest.sh          curl probe of Polaris's loadTable response
    │
    └── databricks_federation/                 Databricks-as-the-consumer test
        ├── README.md                          Test narrative + prereqs
        ├── 01_databricks_query_federation.sql Databricks DDL: CONNECTION + FOREIGN CATALOG (JDBC pushdown)
        ├── 02_databricks_catalog_federation.sql Databricks DDL: FOREIGN CATALOG with authorized_paths (direct S3 read)
        ├── 03_test_queries.sql                Probe queries comparing the two paths side by side
        └── findings.md                        Captured empirical results — query fed enforces, catalog fed bypasses
```

The five paths share the same data, the same policy shapes, and the same
probe queries — so the only meaningful difference between runs is the
access path under test. That's what makes the comparison clean.

## Common prerequisites for both sides

- Java 11 or 17 on the laptop running the OSS Spark client
- Python 3.10+ with `pip install pyspark==3.5.*`
- `bash`, `curl`, `python3` for the probe scripts

The OSS Spark client uses Iceberg 1.9.1 + the Iceberg AWS bundle. If your
external volume / managed storage is on Azure or GCP, swap the AWS bundle
for the Azure / GCP equivalent.

## Running the Databricks side

Prerequisites specific to Databricks:
- Databricks workspace on AWS with **External data access** enabled on the
  metastore (`Catalog explorer → Metastore → Details`).
- A catalog you own. Edit the `<catalog>` placeholder in the SQL files.
- A workspace user, group, or service principal with `USE CATALOG`,
  `USE SCHEMA`, `SELECT` on the test table, and `EXTERNAL USE SCHEMA` on
  the schema.
- A Databricks PAT or OAuth bearer for that principal.

```bash
# 1. (Databricks SQL editor) edit <catalog>, run databricks/01_setup_table_and_data.sql

# 2. Phase A: prove the OSS Spark wire path works.
export DATABRICKS_HOST="https://<your-workspace>.cloud.databricks.com"
export DATABRICKS_TOKEN="<personal-access-token-or-OAuth-bearer>"
export UC_CATALOG="<your-catalog>"
export JAVA_HOME=$(/usr/libexec/java_home -v 11)   # macOS

python3 databricks/spark_uc_policy_test.py            # Expect: 8 rows, raw
./databricks/probe_uc_iceberg_rest.sh                 # Expect: READABLE

# 3. (Databricks SQL editor) run databricks/02_apply_row_filter_and_masks.sql

# 4. Phase B: re-run the same OSS Spark client and probe.
python3 databricks/spark_uc_policy_test.py            # Expect: ValidationException: Invalid S3 URI
./databricks/probe_uc_iceberg_rest.sh                 # Expect: BLOCKED (response scrubbed)

# 5. Optional: restore Phase A state.
#    -> databricks/03_drop_policies.sql
```

## Running the Snowflake side

Prerequisites specific to Snowflake:
- A Snowflake account with Iceberg + Polaris REST catalog enabled.
- An `EXTERNAL VOLUME` pointing at S3/GCS/Azure storage your account can
  write to. Edit `<external_volume_name>` in `snowflake/01_setup_table_and_data.sql`.
- An admin role (e.g. `ACCOUNTADMIN`) and a non-admin "restricted" role
  (e.g. `POLICY_TEST_ANALYST`) that has `SELECT` on the test table and
  `USAGE` on the database, schema, warehouse, and external volume.
- A Snowflake **programmatic access token (PAT)** for the user you'll
  authenticate as. The user should be able to assume both roles
  (mint with `ROLE_RESTRICTION = 'ANY'` or no restriction).

```bash
# 1. (Snowflake SQL editor / Snowsight) edit placeholders, run snowflake/01_setup_table_and_data.sql

# 2. Common environment for the OSS Spark client.
export SNOWFLAKE_ACCOUNT_URL="https://<account>.snowflakecomputing.com"
export SNOWFLAKE_PAT="<programmatic-access-token>"
export SNOWFLAKE_USER="<user>"
export SNOWFLAKE_DATABASE="<database>"
export SNOWFLAKE_SCHEMA="PUBLIC"
export SNOWFLAKE_WAREHOUSE="<warehouse>"
export SNOWFLAKE_REGION="us-east-1"
export JAVA_HOME=$(/usr/libexec/java_home -v 11)   # macOS

# Phase A: as ACCOUNTADMIN, BEFORE policies are attached. Pure Iceberg
# REST works end to end because the table is unpoliced.
export SNOWFLAKE_ROLE=ACCOUNTADMIN
python3 snowflake/spark_horizon_policy_test.py        # Expect: 8 rows, raw
./snowflake/probe_polaris_iceberg_rest.sh             # Expect: READABLE

# 3. (Snowflake SQL editor) run snowflake/02_apply_policies.sql

# 4. Phase B: re-run for either role -- admin or restricted. Polaris
#    refuses loadTable for the policied table, fail-secure.
export SNOWFLAKE_ROLE=ACCOUNTADMIN                    # or <restricted_role>
python3 snowflake/spark_horizon_policy_test.py        # Expect: ForbiddenException: Authorization failed
./snowflake/probe_polaris_iceberg_rest.sh             # Expect: BLOCKED (error.code=403)

# 5. To READ the policied table from OSS Spark, use the Snowflake Spark
#    connector path. Iceberg REST 403's, the connector falls back to
#    JDBC, and Snowflake compute applies the policies.
export SNOWFLAKE_RESTRICTED_ROLE=<restricted_role>    # e.g. POLICY_TEST_ANALYST
python3 snowflake/spark_horizon_with_snowflake_connector.py
# Expect: Phase A (ACCOUNTADMIN) 8 rows raw; Phase B (<restricted_role>)
# 3 rows masked. Verify routing by querying
# INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER on Snowflake -- the SELECTs
# show up as warehouse queries on the JDBC path, not Iceberg REST.

# 6. Optional: restore Phase A state.
#    -> snowflake/03_drop_policies.sql
```

## Running the Snowflake-from-Databricks federation test

This third scenario asks: with Snowflake's FGAC policies attached, what
does Databricks see when it accesses the same table via UC Federation?
There are two distinct integrations to test, and they behave very
differently. Full details, prerequisites, and DDL are in
[`snowflake/databricks_federation/README.md`](snowflake/databricks_federation/README.md);
empirical run output is in
[`snowflake/databricks_federation/findings.md`](snowflake/databricks_federation/findings.md).

In short:

```bash
# Snowflake side already has the policied table (snowflake/01_*.sql + 02_*.sql).

# Databricks side: open SQL editor as a metastore admin / CREATE CONNECTION user.
# Edit placeholders in:
#   snowflake/databricks_federation/01_databricks_query_federation.sql
#   snowflake/databricks_federation/02_databricks_catalog_federation.sql
# Run them in order, then run:
#   snowflake/databricks_federation/03_test_queries.sql
```

Expected behavior:
- The **query-fed** catalog (`Provider: snowflake` in `DESCRIBE EXTENDED`)
  returns 3 rows, masked — Snowflake enforced the policies via JDBC pushdown.
- The **catalog-fed** catalog (`Provider: iceberg` in `DESCRIBE EXTENDED`)
  returns all 8 rows, **unmasked** — direct parquet read from S3 bypassed
  Snowflake's policy enforcement entirely.

## Reading the results

The Phase B outcomes for OSS Spark, side by side:

|                                   | Snowflake Horizon, pure Iceberg REST | Snowflake Horizon, Snowflake Spark connector | Databricks UC Iceberg REST |
|---                                |---                                   |---                                            |---                          |
| OSS Spark `SELECT *`              | `ForbiddenException: Authorization failed` | 3 rows, masked email + IP (via JDBC fallback) | `ValidationException: Invalid S3 URI, cannot determine scheme:` |
| `loadTable` HTTP status           | **403**                              | 403 on first call; ignored, JDBC used         | 200 OK (body scrubbed)      |
| `loadTable` `config.s3.*`         | absent (response is an error body)   | n/a (JDBC path)                                | ABSENT                      |
| `loadTable` `manifest-list`       | absent (response is an error body)   | n/a (JDBC path)                                | empty string `""`           |
| Where the read actually runs      | nowhere — request refused            | Snowflake virtual warehouse (JDBC pushdown)    | nowhere — Iceberg client errors out |

Both vendors fail-secure on the standard Iceberg REST `loadTable`
flow. The Snowflake side additionally implements the Iceberg REST
**Scan API** (server-side scan planning) for policied tables, which
is the Iceberg-spec-compliant way for an external client to obtain
a governed read; clients that adopt that flow (Spark 3.5+ on Iceberg
1.11+, the Snowflake Spark connector) get a governed result without
going through Snowflake compute. Older / `loadTable`-only clients
(our `iceberg-spark-runtime-3.5_2.12:1.9.1` client, DuckDB 1.5.2 per
[duckdb-iceberg#977](https://github.com/duckdb/duckdb-iceberg/issues/977),
PyIceberg, etc.) get the 403. The Snowflake Spark connector also
provides a JDBC-pushdown fallback that routes through Snowflake
compute when the Scan API path isn't available — which is what our
empirical run actually exercised, since we pinned an Iceberg version
without Scan API support. The Databricks side has no equivalent Scan
API path or external-Spark fallback documented for policied tables
today; the supported path on UC is to use Databricks's own engine
via name-based SQL.

The federation test extends this with a third row: when Snowflake is
the governance plane and Databricks is the reader, the **integration
mode you pick matters more than the policies you wrote**.

|                                          | Snowflake → Databricks Query Federation | Snowflake → Databricks Catalog Federation |
|---                                       |---                                      |---                                         |
| Provider in `DESCRIBE EXTENDED`          | `snowflake`                             | `iceberg`                                  |
| Where the query runs                     | Snowflake virtual warehouse (JDBC push) | Databricks compute                         |
| Snowflake row-access policy honored?     | Yes (3/8 rows visible)                  | No (8/8 rows visible)                      |
| Snowflake column-mask policies honored?  | Yes (`a***@example.com`, `***.***.***.10`) | No (raw `alice@example.com`, raw `192.168.1.10`) |

## License

MIT — see [LICENSE](LICENSE).
