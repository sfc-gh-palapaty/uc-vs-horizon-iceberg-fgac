"""
spark_horizon_policy_test.py

OSS Apache Spark client for the Snowflake Horizon side of the comparison.
Connects to a Snowflake account's Polaris REST catalog and reads a
Snowflake-managed Iceberg table, optionally as different roles.

Run this script twice:

  Phase A -- as the admin role (e.g. ACCOUNTADMIN), with no policies on
            the table OR with the admin role exempt from the policy.
            Expect: 8 rows, raw email + IP.

  Phase B -- as a non-admin role (e.g. POLICY_TEST_ANALYST), with the
            row access policy and column masks applied (run
            02_apply_policies.sql first).
            Expect: 3 rows (US/CA only), email + IP masked. Snowflake
            Horizon enforces the policy server-side and serves the
            governed view of the table to the external Spark client
            via Polaris with vended S3 credentials.

Compare to the Databricks Unity Catalog side (../databricks/spark_uc_policy_test.py)
where Phase B fails because UC strips vended credentials and the
manifest-list pointer from the Iceberg REST loadTable response.

----------------------------------------------------------------------------
Usage:

  export SNOWFLAKE_ACCOUNT_URL="https://<account>.snowflakecomputing.com"
  export SNOWFLAKE_PAT="<programmatic-access-token>"
  export SNOWFLAKE_USER="<user>"
  export SNOWFLAKE_DATABASE="<database>"      # used as Polaris warehouse name
  export SNOWFLAKE_SCHEMA="<schema>"          # e.g. PUBLIC
  export SNOWFLAKE_WAREHOUSE="<warehouse>"
  export SNOWFLAKE_REGION="us-east-1"          # cloud region of the external volume
  export SNOWFLAKE_ROLE="ACCOUNTADMIN"         # role to test as (Phase A vs Phase B)
  # Optional, defaults shown:
  export SNOWFLAKE_TABLE="policy_test_table"

  python3 spark_horizon_policy_test.py
----------------------------------------------------------------------------
"""

from __future__ import annotations

import os
import sys
import traceback

from pyspark.sql import SparkSession


SF_ACCOUNT_URL = os.environ.get("SNOWFLAKE_ACCOUNT_URL", "").rstrip("/")
SF_PAT         = os.environ.get("SNOWFLAKE_PAT", "")
SF_USER        = os.environ.get("SNOWFLAKE_USER", "")
SF_DATABASE    = os.environ.get("SNOWFLAKE_DATABASE", "")
SF_SCHEMA      = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
SF_WAREHOUSE   = os.environ.get("SNOWFLAKE_WAREHOUSE", "")
SF_REGION      = os.environ.get("SNOWFLAKE_REGION", "us-east-1")
SF_ROLE        = os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SF_TABLE       = os.environ.get("SNOWFLAKE_TABLE", "policy_test_table")

# Polaris REST catalog URI is at /polaris/api/catalog on the Snowflake
# account's hostname.
CATALOG_URI = f"{SF_ACCOUNT_URL}/polaris/api/catalog"

SPARK_CAT       = "horizon"
ICEBERG_VERSION = "1.9.1"


def create_spark_session(role: str) -> SparkSession:
    session_role = f"session:role:{role}"
    return (
        SparkSession.builder
        .master("local[*]")
        .config("spark.ui.port", "0")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.port", "0")
        .config("spark.blockManager.port", "0")
        .config(
            "spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION},"
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.defaultCatalog", SPARK_CAT)
        .config(f"spark.sql.catalog.{SPARK_CAT}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{SPARK_CAT}.type", "rest")
        .config(f"spark.sql.catalog.{SPARK_CAT}.uri", CATALOG_URI)
        .config(f"spark.sql.catalog.{SPARK_CAT}.warehouse", SF_DATABASE)
        .config(f"spark.sql.catalog.{SPARK_CAT}.scope", session_role)
        .config(f"spark.sql.catalog.{SPARK_CAT}.client.region", SF_REGION)
        .config(f"spark.sql.catalog.{SPARK_CAT}.credential", SF_PAT)
        .config(
            f"spark.sql.catalog.{SPARK_CAT}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            f"spark.sql.catalog.{SPARK_CAT}.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .getOrCreate()
    )


def query_table(spark: SparkSession, role: str) -> None:
    fqn = f"{SPARK_CAT}.{SF_SCHEMA}.{SF_TABLE}"
    print()
    print("=" * 80)
    print(f"  ROLE: {role}")
    print(f"  QUERY: SELECT * FROM {fqn} ORDER BY user_id")
    print("=" * 80)
    try:
        df = spark.sql(f"SELECT * FROM {fqn} ORDER BY user_id")
        df.show(truncate=False)
        rows = df.collect()
        print(f"  Rows returned: {len(rows)}")
        if rows:
            email_masked = "***" in str(rows[0]["EMAIL"])
            ip_masked    = "***" in str(rows[0]["IP_ADDRESS"])
            print(f"  Email masked? {email_masked}")
            print(f"  IP    masked? {ip_masked}")
    except Exception as exc:
        print(f"  QUERY FAILED: {type(exc).__name__}: {exc}")
        traceback.print_exc()


def main() -> None:
    missing = [
        name
        for name, val in [
            ("SNOWFLAKE_ACCOUNT_URL", SF_ACCOUNT_URL),
            ("SNOWFLAKE_PAT",         SF_PAT),
            ("SNOWFLAKE_USER",        SF_USER),
            ("SNOWFLAKE_DATABASE",    SF_DATABASE),
            ("SNOWFLAKE_WAREHOUSE",   SF_WAREHOUSE),
        ]
        if not val
    ]
    if missing:
        print(
            "ERROR: missing required environment variables: " + ", ".join(missing),
            file=sys.stderr,
        )
        print("See the docstring at the top of this file for usage.", file=sys.stderr)
        sys.exit(2)

    print("=" * 80)
    print("  SNOWFLAKE HORIZON / POLARIS REST -- POLICY ENFORCEMENT TEST")
    print("=" * 80)
    print(f"  Account   : {SF_ACCOUNT_URL}")
    print(f"  Database  : {SF_DATABASE} (Polaris warehouse name)")
    print(f"  Schema    : {SF_SCHEMA}")
    print(f"  Table     : {SF_TABLE}")
    print(f"  Role      : {SF_ROLE}")
    print()
    print("  This OSS Spark client connects to Snowflake Polaris at")
    print("  /polaris/api/catalog with vended credentials, scoped to the")
    print("  current SNOWFLAKE_ROLE. Re-run this script with a different")
    print("  SNOWFLAKE_ROLE to compare the admin and non-admin views.")
    print()

    spark = create_spark_session(SF_ROLE)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        query_table(spark, SF_ROLE)
    finally:
        spark.stop()

    print()
    print("=" * 80)
    print("  INTERPRETATION")
    print("=" * 80)
    print()
    print("  Phase A (admin role, e.g. ACCOUNTADMIN):")
    print("    OSS Spark sees 8 rows, fully unmasked. Snowflake's row access")
    print("    policy and column masks exempt ACCOUNTADMIN.")
    print()
    print("  Phase B (restricted role, with policies attached):")
    print("    OSS Spark sees 3 rows (US/CA only) with masked email + IP.")
    print("    Snowflake Horizon enforces the policy server-side and serves")
    print("    the governed view of the table to Polaris. The external Spark")
    print("    client gets the same governed result the role would see")
    print("    in-warehouse.")
    print()
    print("  Compare to ../databricks/spark_uc_policy_test.py: in the")
    print("  Databricks UC equivalent, Phase B fails entirely because UC")
    print("  strips vended S3 credentials and the manifest-list pointer from")
    print("  the loadTable response rather than enforcing policy.")
    print()


if __name__ == "__main__":
    main()
