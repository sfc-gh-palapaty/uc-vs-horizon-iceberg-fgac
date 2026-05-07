# Test 3: Snowflake Iceberg + FGAC, accessed from Databricks via federation

This test extends the Snowflake side of the comparison with the question
that customers running multi-engine architectures actually care about:

> If my data sits in a Snowflake-managed Iceberg table with a row-access
> policy and column masking, **and Databricks is the consumer**, does
> Snowflake's governance plane still apply?

Databricks Unity Catalog (UC) exposes two distinct integrations with
Snowflake. They look similar on the surface, but they take fundamentally
different paths to the data and they enforce policies very differently:

| Path | What happens | Where the query runs | Where governance applies |
|------|--------------|----------------------|--------------------------|
| **Query Federation** | UC creates `CONNECTION TYPE = SNOWFLAKE`. Each `SELECT` is rewritten and pushed down over JDBC to a Snowflake virtual warehouse. | Snowflake compute | Snowflake's query engine evaluates the row-access policy and column masks at query time |
| **Catalog Federation** | UC reuses that same connection only to ask Snowflake for the latest `metadata.json` path, and then reads the parquet **directly from S3** using its own UC storage credential. | Databricks compute (Photon / Spark) | **Nothing** at query time. The parquet files written by Snowflake contain the **raw, ungoverned** rows because row filters and column masks are query-time constructs in Snowflake — they were never serialized into the files. |

The empirical run in this directory confirms what that table predicts.

## What's in this directory

| File | Purpose |
|------|---------|
| `01_databricks_query_federation.sql`   | DDL the Databricks side runs to set up Query Federation against Snowflake (CONNECTION + FOREIGN CATALOG, no `authorized_paths`). |
| `02_databricks_catalog_federation.sql` | DDL the Databricks side runs to set up Catalog Federation (same CONNECTION, FOREIGN CATALOG with `authorized_paths`). |
| `03_test_queries.sql`                  | The actual probe queries — the same SQL run against both catalogs to expose the difference. |
| `findings.md`                          | Captured outputs from a real run with all sensitive identifiers stripped. |

The Snowflake-side artifacts (table DDL, row-access policy, masking
policies) are reused from `../01_setup_table_and_data.sql` and
`../02_apply_policies.sql`.

## Prerequisites

On the **Snowflake** side:

1. The Iceberg table from `../01_setup_table_and_data.sql` exists and has the
   policies from `../02_apply_policies.sql` attached.
2. A non-admin role exists (e.g. `<restricted_role>`) with `USAGE` on the
   database/schema, `SELECT` on the table, and `USAGE` on the warehouse.
3. The role is granted to the user the Databricks connection will use.
4. The Snowflake account has a configured external volume backed by a cloud
   storage path that **Databricks Unity Catalog can also see** as an external
   location (otherwise Catalog Federation will fall back to Query Federation
   silently — see "Fallback mode" below).

On the **Databricks** side:

1. A storage credential and external location covering the bucket / prefix
   where Snowflake's external volume writes its Iceberg data.
2. `CREATE CONNECTION` and `CREATE CATALOG` privileges on the metastore.
3. A SQL warehouse (Pro or Serverless, image 2023.40+) or a Standard /
   Dedicated cluster on DBR 13.3+ (16.4 LTS+ recommended for catalog
   federation).

## Run order

```sql
-- Step 1: Query Federation (Snowflake compute does the work)
@./01_databricks_query_federation.sql

-- Step 2: Catalog Federation (Databricks reads parquet directly)
@./02_databricks_catalog_federation.sql

-- Step 3: same probe SQL against both catalogs
@./03_test_queries.sql
```

Then compare the row counts, country histograms, and email/IP columns
between the two catalogs. See `findings.md` for what we observed.

## Fallback mode (important)

Databricks documents a fallback rule for Catalog Federation: if the
Snowflake-managed table's metadata.json location is not "URI compatible",
or it lives outside the declared table location, or its scheme is not in
the supported list, UC silently falls back to Query Federation for that
table. You'll see `Provider: snowflake` in `DESCRIBE EXTENDED` instead of
`Provider: iceberg`, and policy enforcement will *appear* to work — but
only because the fallback put Snowflake's query engine back in the loop.

When verifying, always check `DESCRIBE EXTENDED <catalog>.<schema>.<table>`
and confirm which mode you're actually exercising.

## Why this matters

Snowflake's row-access policies and masking policies are **evaluated by
the Snowflake query engine at query time**. They are not row-level
deletions, they are not column-level encryption, and they are not encoded
in the parquet files. The parquet files written to S3 contain the raw
rows.

Catalog Federation goes around the Snowflake query engine. So:

- Anything you assume your row filter is hiding is sitting in the parquet
  files in S3, fully readable.
- Anything you assume your masking policy is redacting is sitting in the
  parquet files in S3, fully readable.
- The only access control left is the cloud-storage permissions on the
  bucket — which, if you've handed Databricks an authorized path covering
  that bucket, are now effectively delegated to Databricks UC.

This is the inverse of the Databricks Unity Catalog story documented in
`../../databricks/`. UC scrubs its REST response when policies are active
(fail-secure). Snowflake exposes the metadata path even to the
non-admin role and trusts the engine to honor the policy at query time.
The two design choices land in very different places when a foreign engine
is the reader.
