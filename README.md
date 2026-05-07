# Unity Catalog vs Snowflake Horizon — Iceberg policy enforcement test

Reproducible empirical test of how Databricks Unity Catalog and Snowflake
Horizon enforce fine-grained access controls (FGAC — row filters and
column masks) on Iceberg tables when those tables are read from a
**different engine** than the one that owns the governance plane:

1. Databricks Unity Catalog table accessed by **OSS Apache Spark** via UC's Iceberg REST endpoint.
2. Snowflake Horizon table accessed by **OSS Apache Spark** via Polaris REST.
3. Snowflake Horizon table accessed by **Databricks** via UC Federation (both Query Federation and Catalog Federation).

**TL;DR** — three separate boundaries, three very different outcomes:

| Boundary | Direction | Result |
|---|---|---|
| Databricks UC Iceberg REST → OSS Spark | UC governs, Spark reads | **Fail-secure**: UC returns no vended credentials and a blank `manifest-list`. The external client cannot read the table at all. |
| Snowflake Polaris → OSS Spark            | Snowflake governs, Spark reads | **Policy-enforcing**: Polaris serves a role-scoped, filtered + masked snapshot to the external client. |
| Snowflake → Databricks UC **Query Federation** (JDBC pushdown) | Snowflake governs, Databricks reads via Snowflake compute | **Policy-enforcing**: Snowflake's query engine applies the row filter and masks before the result reaches Databricks. |
| Snowflake → Databricks UC **Catalog Federation** (direct S3 read) | Snowflake governs, Databricks reads parquet directly | **Policies bypassed**: Databricks reads the raw parquet from S3, which contains every row and every cell unmasked. The Snowflake row-access and masking policies are query-time constructs and were never serialized into the files. |

UC's documented limitation, verbatim:

> You cannot use Iceberg REST catalog or Unity REST APIs to access tables
> with row filters or column masks.
>
> — [Databricks docs — Row filters and column masks, Limitations](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks#limitations)

The full write-up, including the field-by-field diff of UC's
`loadTable` response with vs. without policies attached, is in
**[BLOG.md](BLOG.md)** and **[UC_Iceberg_Policy_Enforcement_Findings.pdf](UC_Iceberg_Policy_Enforcement_Findings.pdf)**.

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
    ├── spark_horizon_policy_test.py           OSS Spark client over Polaris REST
    ├── probe_polaris_iceberg_rest.sh          curl probe of Polaris's loadTable response
    │
    └── databricks_federation/                 Databricks-as-the-consumer test
        ├── README.md                          Test narrative + prereqs
        ├── 01_databricks_query_federation.sql Databricks DDL: CONNECTION + FOREIGN CATALOG (JDBC pushdown)
        ├── 02_databricks_catalog_federation.sql Databricks DDL: FOREIGN CATALOG with authorized_paths (direct S3 read)
        ├── 03_test_queries.sql                Probe queries comparing the two paths side by side
        └── findings.md                        Captured empirical results — query fed enforces, catalog fed bypasses
```

The three sides have intentionally identical shape — same data, same
policy shapes, same probe queries — so the only meaningful difference
between runs is the access path under test. That's what makes the
comparison clean.

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
./databricks/probe_uc_iceberg_rest.sh                 # Expect: BLOCKED

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
  authenticate as. The user should be able to assume both roles.

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

# Phase A: as ACCOUNTADMIN (no policies effectively apply).
export SNOWFLAKE_ROLE=ACCOUNTADMIN
python3 snowflake/spark_horizon_policy_test.py        # Expect: 8 rows, raw
./snowflake/probe_polaris_iceberg_rest.sh             # Expect: READABLE

# 3. (Snowflake SQL editor) run snowflake/02_apply_policies.sql

# 4. Phase B: as the restricted role.
export SNOWFLAKE_ROLE=<restricted_role>               # e.g. POLICY_TEST_ANALYST
python3 snowflake/spark_horizon_policy_test.py        # Expect: 3 rows, MASKED email + IP
./snowflake/probe_polaris_iceberg_rest.sh             # Expect: READABLE (governed view)

# 5. Optional: restore Phase A state.
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
  returns 3 rows, masked — Snowflake enforced the policies.
- The **catalog-fed** catalog (`Provider: iceberg` in `DESCRIBE EXTENDED`)
  returns all 8 rows, **unmasked** — direct parquet read from S3 bypassed
  Snowflake's policy enforcement entirely.

## Reading the results

The Phase B outcomes for the original two-sided OSS-Spark test:

|                                  | Snowflake Horizon, restricted role     | Databricks Unity Catalog, non-admin caller |
|---                               |---                                     |---                                          |
| OSS Spark `SELECT *`             | 3 rows, masked email + IP              | `Invalid S3 URI, cannot determine scheme:` |
| `loadTable` `config.s3.*`        | PRESENT (scoped to filtered snapshot)  | ABSENT |
| `loadTable` `manifest-list`      | real S3 path                           | empty string `""` |

Both platforms have the same in-engine UX (admins see everything,
non-admins see filtered + masked). The difference is at the external
engine boundary: Snowflake serves the governed view, Databricks blocks
external readability entirely.

The federation test extends this with a third row: when Snowflake is the
governance plane and Databricks is the reader, the **integration mode you
pick matters more than the policies you wrote**.

|                                          | Snowflake → Databricks Query Federation | Snowflake → Databricks Catalog Federation |
|---                                       |---                                      |---                                         |
| Provider in `DESCRIBE EXTENDED`          | `snowflake`                             | `iceberg`                                  |
| Where the query runs                     | Snowflake virtual warehouse (JDBC push) | Databricks compute                         |
| Snowflake row-access policy honored?     | Yes (3/8 rows visible)                  | No (8/8 rows visible)                      |
| Snowflake column-mask policies honored?  | Yes (`a***@example.com`, `***.***.***.10`) | No (raw `alice@example.com`, raw `192.168.1.10`) |

## License

MIT — see [LICENSE](LICENSE).
