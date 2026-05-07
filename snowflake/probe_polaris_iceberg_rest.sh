#!/usr/bin/env bash
# snowflake/probe_polaris_iceberg_rest.sh
#
# Calls Snowflake Polaris's Iceberg REST loadTable endpoint directly and
# prints the fields that determine whether an external Iceberg engine can
# read the table:
#
#   - config.s3.* (vended S3 credentials)
#   - storage-credentials[]
#   - snapshot.manifest-list
#
# Run this as the admin role (Phase A) and then as a restricted role with
# policies attached (Phase B). The contrast IS the demo:
#
#   Phase A (admin):       vended creds PRESENT, real manifest-list -> READABLE
#   Phase B (restricted):  vended creds PRESENT, manifest-list points
#                          at the FILTERED + MASKED snapshot         -> READABLE
#
# Compare to ../databricks/probe_uc_iceberg_rest.sh, where Phase B returns
# empty creds and an empty manifest-list (BLOCKED).
#
# Required environment variables:
#   SNOWFLAKE_ACCOUNT_URL   e.g. https://<account>.snowflakecomputing.com
#   SNOWFLAKE_PAT           Snowflake programmatic access token
#   SNOWFLAKE_DATABASE      database (used as Polaris warehouse name)
#   SNOWFLAKE_ROLE          role to test as (e.g. ACCOUNTADMIN or POLICY_TEST_ANALYST)
# Optional (defaults shown):
#   SNOWFLAKE_SCHEMA        PUBLIC
#   SNOWFLAKE_TABLE         policy_test_table
#
# Requires: bash, curl, python3.

set -euo pipefail

: "${SNOWFLAKE_ACCOUNT_URL:?Set SNOWFLAKE_ACCOUNT_URL to your account URL}"
: "${SNOWFLAKE_PAT:?Set SNOWFLAKE_PAT to a Snowflake programmatic access token}"
: "${SNOWFLAKE_DATABASE:?Set SNOWFLAKE_DATABASE to the database holding the test schema}"
: "${SNOWFLAKE_ROLE:?Set SNOWFLAKE_ROLE to the role to test as}"

ACCOUNT_URL="${SNOWFLAKE_ACCOUNT_URL%/}"
SCHEMA="${SNOWFLAKE_SCHEMA:-PUBLIC}"
TABLE="${SNOWFLAKE_TABLE:-policy_test_table}"

echo "Probing: $ACCOUNT_URL"
echo "Table  : $SNOWFLAKE_DATABASE.$SCHEMA.$TABLE"
echo "Role   : $SNOWFLAKE_ROLE"
echo

# Polaris REST API uses the OAuth2 'scope=session:role:<role>' convention
# to bind the request to a specific Snowflake role. The catalog (Polaris
# warehouse) name maps to the Snowflake database name.
curl -s \
  -H "Authorization: Bearer $SNOWFLAKE_PAT" \
  -H "X-Iceberg-Access-Delegation: vended-credentials" \
  -H "Polaris-Role: $SNOWFLAKE_ROLE" \
  "$ACCOUNT_URL/polaris/api/catalog/v1/$SNOWFLAKE_DATABASE/namespaces/$SCHEMA/tables/$TABLE" \
  | python3 -c "
import json, sys
d = json.load(sys.stdin)
cfg = d.get('config') or {}
sc  = d.get('storage-credentials') or []
md  = d.get('metadata') or {}
snaps = md.get('snapshots') or []
ml = snaps[-1].get('manifest-list') if snaps else None

print('--- Polaris Iceberg REST loadTable response ---')
print('config keys              :', sorted(cfg.keys()))
print('vended S3 access-key-id  :', 'PRESENT' if cfg.get('s3.access-key-id') else 'ABSENT')
print('vended S3 session-token  :', 'PRESENT' if cfg.get('s3.session-token') else 'ABSENT')
print('storage-credentials count:', len(sc))
print('snapshot.manifest-list   :', repr(ml))
print()
print('Verdict for external Iceberg engine readability:')
ok = bool(cfg.get('s3.session-token')) and bool(ml)
print('  ', 'READABLE'   if ok else 'BLOCKED  (no creds and/or empty manifest-list)')
print()
print('Note: even when readable in Phase B, the snapshot pointer should be')
print('different from the admin-role snapshot because Snowflake serves a')
print('governed view that reflects the row filter and column masks.')
"
