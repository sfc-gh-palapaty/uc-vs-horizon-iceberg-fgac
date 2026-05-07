#!/usr/bin/env bash
# probe_uc_iceberg_rest.sh
#
# Calls Unity Catalog's Iceberg REST loadTable endpoint directly and prints
# the fields that determine whether an external Iceberg engine can read the
# table:
#
#   - config.s3.* (vended S3 credentials)
#   - storage-credentials[]
#   - snapshot.manifest-list
#
# Run this BEFORE applying policies (Phase A) and AFTER applying policies
# (Phase B). The contrast between the two outputs is the demo:
#
#   Phase A: vended creds PRESENT, manifest-list is a real S3 path -> READABLE
#   Phase B: vended creds ABSENT,  manifest-list is empty string   -> BLOCKED
#
# Required environment variables:
#   DATABRICKS_HOST   e.g. https://your-workspace.cloud.databricks.com
#   DATABRICKS_TOKEN  PAT or OAuth bearer for the test principal
#   UC_CATALOG        catalog containing the test schema
# Optional (defaults shown):
#   UC_SCHEMA         policy_test
#   UC_TABLE          policy_test_table
#
# Requires: bash, curl, python3.

set -euo pipefail

: "${DATABRICKS_HOST:?Set DATABRICKS_HOST to your workspace URL}"
: "${DATABRICKS_TOKEN:?Set DATABRICKS_TOKEN to a PAT or OAuth bearer}"
: "${UC_CATALOG:?Set UC_CATALOG to the catalog containing the test schema}"

WORKSPACE_URL="${DATABRICKS_HOST%/}"
SCHEMA="${UC_SCHEMA:-policy_test}"
TABLE="${UC_TABLE:-policy_test_table}"

echo "Probing: $WORKSPACE_URL"
echo "Table  : $UC_CATALOG.$SCHEMA.$TABLE"
echo

curl -s \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "X-Iceberg-Access-Delegation: vended-credentials" \
  "$WORKSPACE_URL/api/2.1/unity-catalog/iceberg-rest/v1/catalogs/$UC_CATALOG/namespaces/$SCHEMA/tables/$TABLE" \
  | python3 -c "
import json, sys
d = json.load(sys.stdin)
cfg = d.get('config') or {}
sc  = d.get('storage-credentials') or []
md  = d.get('metadata') or {}
snaps = md.get('snapshots') or []
ml = snaps[-1].get('manifest-list') if snaps else None

print('--- UC Iceberg REST loadTable response ---')
print('config keys              :', sorted(cfg.keys()))
print('vended S3 access-key-id  :', 'PRESENT' if cfg.get('s3.access-key-id') else 'ABSENT')
print('vended S3 session-token  :', 'PRESENT' if cfg.get('s3.session-token') else 'ABSENT')
print('storage-credentials count:', len(sc))
print('snapshot.manifest-list   :', repr(ml))
print()
print('Verdict for external Iceberg engine readability:')
ok = bool(cfg.get('s3.session-token')) and bool(ml)
print('  ', 'READABLE'   if ok else 'BLOCKED  (no creds and/or empty manifest-list)')
"
