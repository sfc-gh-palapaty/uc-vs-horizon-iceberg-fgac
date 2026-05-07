"""
spark_horizon_with_snowflake_connector.py

OSS Apache Spark client for the Snowflake Horizon side of the comparison
that uses the **Snowflake Spark connector** (`net.snowflake:spark-snowflake`)
in combination with the Iceberg Spark runtime, via the
`SnowflakeFallbackCatalog` shipped by the Snowflake Spark connector.

The other Snowflake-side OSS Spark client in this repo
(`spark_horizon_policy_test.py`) is *pure* Apache Iceberg. It talks to
Snowflake's Polaris REST endpoint and follows the Iceberg spec end to
end. For Snowflake-managed Iceberg tables WITHOUT policies attached,
that path works fine: Polaris vends S3 creds and Spark reads parquet
directly. For Snowflake-managed Iceberg tables WITH row-access or
column-mask policies attached, that path fails -- Polaris returns
HTTP 403 (`ForbiddenException: Authorization failed`) at `loadTable`,
fail-secure, much like Databricks Unity Catalog scrubs its response.
See `findings_pure_iceberg_rest.md` for the captured evidence.

This script wires up a *hybrid* catalog. `SnowflakeFallbackCatalog`
delegates Iceberg-readable tables to the Iceberg `SparkCatalog` (same
Polaris REST + S3 path as before) and falls back to the Snowflake JDBC
connector for anything Iceberg can't handle natively. For a policied
table on this account, Iceberg REST returns 403, the connector
catches that, and the read is silently routed over JDBC to a Snowflake
virtual warehouse where the SQL engine evaluates the policies for the
active role. For non-policied tables the connector takes the Iceberg
REST + S3 path (cheaper, no warehouse credits).

Both code paths are configured with role binding:

  Iceberg REST path: spark.sql.catalog.<cat>.scope = session:role:<role>
                     -> Polaris OAuth scope binds the request to the role.
                        For policied tables this currently returns 403.

  Snowflake JDBC fallback path:
                     spark.snowflake.sfRole = <role>
                     -> Snowflake's query engine evaluates the role's
                        policies at query time on the warehouse compute.
                        This is the path that actually carries reads of
                        policied tables today.

Run twice:

  Phase A -- as the admin role (ACCOUNTADMIN). Expect 8 raw rows
            (admin is exempt from the policies).
  Phase B -- as the restricted role (with row filter + masks active).
            Expect 3 rows, masked email and IP.

Both phases work end-to-end for policied tables BECAUSE of the JDBC
fallback. To verify the routing afterwards, query
INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER on the Snowflake side -- you
will see the SELECTs ran on the warehouse, not via Iceberg REST.

----------------------------------------------------------------------------
Usage:

  export SNOWFLAKE_ACCOUNT_URL="https://<account>.snowflakecomputing.com"
  export SNOWFLAKE_PAT="<programmatic-access-token>"  # used for both Polaris OAuth and JDBC password
  export SNOWFLAKE_USER="<user>"
  export SNOWFLAKE_DATABASE="<database>"      # used as Polaris warehouse name AND JDBC database
  export SNOWFLAKE_SCHEMA="<schema>"          # e.g. PUBLIC
  export SNOWFLAKE_WAREHOUSE="<warehouse>"
  export SNOWFLAKE_REGION="us-east-1"
  # Optional, defaults shown:
  export SNOWFLAKE_TABLE="policy_test_table"

  python3 spark_horizon_with_snowflake_connector.py
----------------------------------------------------------------------------
"""

from __future__ import annotations

import os
import sys
import time
import traceback

from pyspark.sql import SparkSession


SF_ACCOUNT_URL = os.environ.get("SNOWFLAKE_ACCOUNT_URL", "").rstrip("/")
SF_PAT         = os.environ.get("SNOWFLAKE_PAT", "")
SF_USER        = os.environ.get("SNOWFLAKE_USER", "")
SF_DATABASE    = os.environ.get("SNOWFLAKE_DATABASE", "")
SF_SCHEMA      = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
SF_WAREHOUSE   = os.environ.get("SNOWFLAKE_WAREHOUSE", "")
SF_REGION      = os.environ.get("SNOWFLAKE_REGION", "us-east-1")
SF_TABLE       = os.environ.get("SNOWFLAKE_TABLE", "policy_test_table")

# Polaris REST endpoint
CATALOG_URI = f"{SF_ACCOUNT_URL}/polaris/api/catalog"
# JDBC URL host -- the same account, sans scheme.
SF_HOST_FOR_JDBC = SF_ACCOUNT_URL.replace("https://", "").replace("http://", "")

SPARK_CAT = "horizoncatalog"

ICEBERG_VERSION             = "1.9.1"
SNOWFLAKE_JDBC_VERSION      = "3.24.0"
SNOWFLAKE_CONNECTOR_VERSION = "3.1.6"   # net.snowflake:spark-snowflake_2.12


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
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION},"
            f"net.snowflake:snowflake-jdbc:{SNOWFLAKE_JDBC_VERSION},"
            f"net.snowflake:spark-snowflake_2.12:{SNOWFLAKE_CONNECTOR_VERSION}"
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.defaultCatalog", SPARK_CAT)
        # The hybrid catalog: SnowflakeFallbackCatalog wraps the Iceberg
        # SparkCatalog and adds JDBC fallback through the Snowflake Spark
        # connector. The .catalog-impl configures the Iceberg implementation
        # the wrapper delegates Iceberg-native operations to.
        .config(f"spark.sql.catalog.{SPARK_CAT}",
                "org.apache.spark.sql.snowflake.catalog.SnowflakeFallbackCatalog")
        .config(f"spark.sql.catalog.{SPARK_CAT}.catalog-impl",
                "org.apache.iceberg.spark.SparkCatalog")
        # Iceberg REST configuration (used for Iceberg-native reads).
        .config(f"spark.sql.catalog.{SPARK_CAT}.type", "rest")
        .config(f"spark.sql.catalog.{SPARK_CAT}.uri", CATALOG_URI)
        .config(f"spark.sql.catalog.{SPARK_CAT}.warehouse", SF_DATABASE)
        .config(f"spark.sql.catalog.{SPARK_CAT}.scope", session_role)
        .config(f"spark.sql.catalog.{SPARK_CAT}.client.region", SF_REGION)
        .config(f"spark.sql.catalog.{SPARK_CAT}.credential", SF_PAT)
        .config(f"spark.sql.catalog.{SPARK_CAT}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{SPARK_CAT}.header.X-Iceberg-Access-Delegation",
                "vended-credentials")
        # Snowflake Spark connector configuration (used for JDBC fallback).
        .config("spark.snowflake.sfURL",       SF_HOST_FOR_JDBC)
        .config("spark.snowflake.sfUser",      SF_USER)
        .config("spark.snowflake.sfPassword",  SF_PAT)
        .config("spark.snowflake.sfDatabase",  SF_DATABASE)
        .config("spark.snowflake.sfSchema",    SF_SCHEMA)
        .config("spark.snowflake.sfRole",      role)
        .config("spark.snowflake.sfWarehouse", SF_WAREHOUSE)
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
            email = str(rows[0].asDict().get("EMAIL", rows[0].asDict().get("email", "")))
            ip    = str(rows[0].asDict().get("IP_ADDRESS", rows[0].asDict().get("ip_address", "")))
            print(f"  Sample email : {email}     -> masked? {'***' in email}")
            print(f"  Sample IP    : {ip}     -> masked? {'***' in ip}")
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
    print("  SNOWFLAKE HORIZON via SNOWFLAKE SPARK CONNECTOR")
    print("  (SnowflakeFallbackCatalog = Iceberg REST + JDBC fallback)")
    print("=" * 80)
    print(f"  Account   : {SF_ACCOUNT_URL}")
    print(f"  Database  : {SF_DATABASE}")
    print(f"  Schema    : {SF_SCHEMA}")
    print(f"  Table     : {SF_TABLE}")
    print()

    # ---- Phase A: admin role (no policies effectively apply) -----------
    print("=" * 80)
    print("  PHASE A: ACCOUNTADMIN -- expect 8 rows, raw email + IP")
    print("=" * 80)
    spark = create_spark_session("ACCOUNTADMIN")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        query_table(spark, "ACCOUNTADMIN")
    finally:
        spark.stop()
    time.sleep(2)

    # ---- Phase B: restricted role (policies active) -------------------
    role_b = os.environ.get("SNOWFLAKE_RESTRICTED_ROLE", "ONEPASSWORD_ANALYST")
    print()
    print("=" * 80)
    print(f"  PHASE B: {role_b} -- expect 3 rows, masked email + IP")
    print("=" * 80)
    spark = create_spark_session(role_b)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        query_table(spark, role_b)
    finally:
        spark.stop()

    print()
    print("=" * 80)
    print("  INTERPRETATION")
    print("=" * 80)
    print()
    print("  This client uses SnowflakeFallbackCatalog: Iceberg REST first,")
    print("  JDBC fallback if Iceberg REST can't serve the read.")
    print()
    print("  For Snowflake-managed Iceberg tables WITHOUT policies attached,")
    print("  the connector takes the Iceberg REST + S3 path (no Snowflake")
    print("  compute, vended creds, direct parquet read).")
    print()
    print("  For Snowflake-managed Iceberg tables WITH row-access or column-")
    print("  mask policies attached, the Iceberg REST loadTable returns HTTP")
    print("  403 (Forbidden). SnowflakeFallbackCatalog catches that and runs")
    print("  the read as a JDBC query against a Snowflake virtual warehouse.")
    print("  Snowflake's SQL engine evaluates the policies for the active")
    print("  role at query time and returns the governed result. Verify by")
    print("  querying INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER on Snowflake")
    print("  after the run.")
    print()
    print("  Both paths are role-bound. Either way, the policies apply for")
    print("  non-admin roles -- but the actual enforcement point is")
    print("  Snowflake compute on the JDBC fallback path, not the Iceberg")
    print("  REST path, for any policied table.")
    print()


if __name__ == "__main__":
    main()
