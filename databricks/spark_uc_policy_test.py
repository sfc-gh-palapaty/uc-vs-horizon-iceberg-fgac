"""
spark_uc_policy_test.py

Connects to a Databricks workspace from OSS Spark via the Unity Catalog
Iceberg REST endpoint (/api/2.1/unity-catalog/iceberg-rest) and reads a
UC-managed Iceberg table.

Run this script twice:

  Phase A -- table has NO row filter / column masks attached.
            Expect: 8 rows returned with raw email + IP (proves the wire
            path works).

  Phase B -- table has a row filter and column masks attached
            (run 02_apply_row_filter_and_masks.sql first).
            Expect: a hard failure from the Iceberg client. UC strips the
            response of vended S3 credentials and the snapshot's
            manifest-list pointer, so the OSS Spark client cannot read
            the table.

This is the empirical answer to the question: "Does Unity Catalog enforce
fine-grained access controls on Iceberg tables when read from external
engines, the way Snowflake Horizon does?" -- short answer, no: UC fails
secure rather than enforce policy on the data plane.

See BLOG.md for the full write-up and the side-by-side with the Snowflake
Horizon test.

----------------------------------------------------------------------------
Usage:

  export DATABRICKS_HOST="https://<your-workspace>.cloud.databricks.com"
  export DATABRICKS_TOKEN="<personal-access-token-or-OAuth-bearer>"
  export UC_CATALOG="<catalog-with-the-test-schema>"
  # Optional, defaults shown:
  export UC_SCHEMA="policy_test"
  export UC_TABLE="policy_test_table"

  python3 spark_uc_policy_test.py

The PAT / bearer token's principal must hold:
  - USE CATALOG, USE SCHEMA, SELECT on the test table
  - EXTERNAL USE SCHEMA on the schema
The metastore must have External data access enabled.
----------------------------------------------------------------------------
"""

from __future__ import annotations

import os
import sys
import traceback

from pyspark.sql import SparkSession


WORKSPACE_URL = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
PAT_TOKEN     = os.environ.get("DATABRICKS_TOKEN", "")
UC_CATALOG    = os.environ.get("UC_CATALOG", "")
UC_SCHEMA     = os.environ.get("UC_SCHEMA", "policy_test")
UC_TABLE      = os.environ.get("UC_TABLE",  "policy_test_table")

SPARK_CAT       = "uc"
ICEBERG_VERSION = "1.9.1"


def create_spark_session() -> SparkSession:
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
        .config(
            f"spark.sql.catalog.{SPARK_CAT}.uri",
            f"{WORKSPACE_URL}/api/2.1/unity-catalog/iceberg-rest",
        )
        .config(f"spark.sql.catalog.{SPARK_CAT}.warehouse", UC_CATALOG)
        .config(f"spark.sql.catalog.{SPARK_CAT}.token", PAT_TOKEN)
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


def query_table(spark: SparkSession) -> None:
    fqn = f"{SPARK_CAT}.{UC_SCHEMA}.{UC_TABLE}"
    print()
    print("=" * 80)
    print(f"  QUERY: SELECT * FROM {fqn} ORDER BY user_id")
    print("  Expected if NO row filter / column mask is attached  -> 8 rows, raw data")
    print("  Expected if row filter / column mask IS attached     -> ERROR from UC")
    print("=" * 80)
    try:
        df = spark.sql(f"SELECT * FROM {fqn} ORDER BY user_id")
        df.show(truncate=False)
        rows = df.collect()
        print(f"  Rows returned: {len(rows)}")
        if rows:
            email_masked = "***" in str(rows[0]["email"])
            ip_masked    = "***" in str(rows[0]["ip_address"])
            print(f"  Email masked? {email_masked}")
            print(f"  IP    masked? {ip_masked}")
    except Exception as exc:
        print(f"  QUERY FAILED: {type(exc).__name__}: {exc}")
        traceback.print_exc()


def main() -> None:
    missing = [
        name
        for name, val in [
            ("DATABRICKS_HOST",  WORKSPACE_URL),
            ("DATABRICKS_TOKEN", PAT_TOKEN),
            ("UC_CATALOG",       UC_CATALOG),
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
    print("  UNITY CATALOG ICEBERG REST -- POLICY ENFORCEMENT TEST")
    print("=" * 80)
    print(f"  Workspace : {WORKSPACE_URL}")
    print(f"  Catalog   : {UC_CATALOG} (managed Iceberg via UC Iceberg REST)")
    print(f"  Schema    : {UC_SCHEMA}")
    print(f"  Table     : {UC_TABLE}")
    print()
    print("  This OSS Spark client connects via the Unity Catalog Iceberg REST")
    print("  endpoint /api/2.1/unity-catalog/iceberg-rest with vended credentials.")
    print()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        query_table(spark)
    finally:
        spark.stop()

    print()
    print("=" * 80)
    print("  INTERPRETATION")
    print("=" * 80)
    print()
    print("  Phase A (no policies):")
    print("    OSS Spark sees 8 rows, fully unmasked. UC's Iceberg REST endpoint")
    print("    vends short-lived S3 credentials and the client reads parquet")
    print("    directly from cloud storage.")
    print()
    print("  Phase B (row filter + column masks attached):")
    print("    UC scrubs the loadTable response: empty `config`, no")
    print("    `storage-credentials`, and the snapshot's `manifest-list` is the")
    print("    empty string. The Iceberg client then trips on")
    print("    `Invalid S3 URI, cannot determine scheme:` because there is no")
    print("    manifest-list path to read.")
    print()
    print("  Net: UC's design for external engines is fail-secure rather than")
    print("  policy-enforcing. The table becomes unreadable from any non-")
    print("  Databricks Iceberg client (OSS Spark, Trino, Flink, PyIceberg, ...)")
    print("  for as long as a row filter or column mask is attached.")
    print()
    print("  Reference:")
    print("    https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks#limitations")
    print("    > 'You cannot use Iceberg REST catalog or Unity REST APIs to")
    print("    >  access tables with row filters or column masks.'")
    print()


if __name__ == "__main__":
    main()
