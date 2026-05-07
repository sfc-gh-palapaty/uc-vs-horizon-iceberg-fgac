# Unity Catalog vs Snowflake Horizon — Iceberg policy enforcement test

Reproducible empirical test of how Databricks Unity Catalog and Snowflake
Horizon enforce fine-grained access controls on Iceberg tables when those
tables are read from external engines (OSS Apache Spark) via the
platforms' respective Iceberg REST catalogs.

**TL;DR** — the two systems behave very differently:

| Capability | Snowflake Horizon (Polaris REST) | Databricks Unity Catalog (Iceberg REST) |
|---|---|---|
| Native compute reads policied table | Filtered + masked rows served | Filtered + masked rows served |
| External OSS Spark reads policied table | Filtered + masked rows served (server-side enforcement) | **Refused** — empty creds, blank manifest-list, client errors |
| External engine receives vended credentials | Yes — scoped to the policy's filtered/masked view | No — `config: {}` |
| External engine receives a real manifest-list | Yes — points at the filtered/masked snapshot | No — `manifest-list: ""` |
| Posture | Open Iceberg + governed FGAC across engines | Open Iceberg + FGAC only when callers stay on Databricks compute |

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
    └── probe_polaris_iceberg_rest.sh          curl probe of Polaris's loadTable response
```

The two sides have intentionally identical shape — same data, same policy
shapes, same OSS Spark client structure — so the only meaningful
difference between runs is the platform under test. That's what makes the
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

## Reading the results

The two Phase B outcomes side by side are the headline:

|                                  | Snowflake Horizon, restricted role     | Databricks Unity Catalog, non-admin caller |
|---                               |---                                     |---                                          |
| OSS Spark `SELECT *`             | 3 rows, masked email + IP              | `Invalid S3 URI, cannot determine scheme:` |
| `loadTable` `config.s3.*`        | PRESENT (scoped to filtered snapshot)  | ABSENT |
| `loadTable` `manifest-list`      | real S3 path                           | empty string `""` |

Both platforms have the same in-engine UX (admins see everything,
non-admins see filtered + masked). The difference is at the external
engine boundary: Snowflake serves the governed view, Databricks blocks
external readability entirely.

## License

MIT — see [LICENSE](LICENSE).
