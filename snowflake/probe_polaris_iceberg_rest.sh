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
# Run this against the policy_test_table BEFORE applying policies
# (Phase A) and then AFTER applying policies (Phase B). The contrast IS
# the demo:
#
#   Phase A (no policies attached):
#     loadTable returns 200 OK with vended S3 creds and a real
#     manifest-list pointer -> READABLE.
#
#   Phase B (row-access / column-mask policies attached, any role):
#     loadTable returns HTTP 403 ForbiddenException ("Authorization
#     failed") -> BLOCKED. Polaris refuses to serve the table to ANY
#     external Iceberg-REST caller -- including the admin role the
#     policies explicitly exempt -- while the policies are attached.
#
# This is fail-secure behavior at the Iceberg REST boundary. The
# contrast is now to the *non-policied* sibling: re-pointing
# SNOWFLAKE_TABLE at any Snowflake-managed Iceberg table without
# policies brings the loadTable response back to vended creds and a
# real manifest-list.
#
# Compare to ../databricks/probe_uc_iceberg_rest.sh: UC also fail-
# secures Phase B, but with a different protocol shape -- 200 OK with
# the response body scrubbed (empty manifest-list, no vended creds)
# instead of a 403.
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

# Polaris REST OAuth: exchange the PAT for a role-scoped bearer using the
# standard OAuth2 client_credentials grant. Polaris's
# session:role:<role> scope binds the issued token to the given
# Snowflake role for the duration of the request.
TOKEN=$(curl -s -X POST "$ACCOUNT_URL/polaris/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "client_secret=$SNOWFLAKE_PAT" \
  --data-urlencode "scope=session:role:$SNOWFLAKE_ROLE" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print(d.get('access_token','ERROR_'+str(d)))")

if [[ "$TOKEN" == ERROR_* ]]; then
  echo "OAuth token exchange failed:"
  echo "$TOKEN" | sed 's/^ERROR_//'
  exit 1
fi

# loadTable on the table.
RESP=$(curl -s \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Iceberg-Access-Delegation: vended-credentials" \
  "$ACCOUNT_URL/polaris/api/catalog/v1/$SNOWFLAKE_DATABASE/namespaces/$SCHEMA/tables/$TABLE")

echo "$RESP" | python3 -c "
import json, sys
raw = sys.stdin.read()
try:
    d = json.loads(raw)
except Exception:
    print('--- Polaris Iceberg REST loadTable response ---')
    print('Non-JSON response:')
    print(raw)
    print()
    print('Verdict for external Iceberg engine readability:')
    print('   BLOCKED  (Polaris returned a non-JSON / error response)')
    sys.exit(0)

# Polaris returns 403 with a JSON error body for policied tables.
err = d.get('error')
if err is not None:
    msg  = err.get('message') if isinstance(err, dict) else str(err)
    code = err.get('code')    if isinstance(err, dict) else None
    typ  = err.get('type')    if isinstance(err, dict) else None
    print('--- Polaris Iceberg REST loadTable response ---')
    print('error.code               :', code)
    print('error.type               :', typ)
    print('error.message            :', msg)
    print()
    print('Verdict for external Iceberg engine readability:')
    print('   BLOCKED  (Polaris refused loadTable, fail-secure)')
    print()
    print('This is the expected response for a Snowflake-managed Iceberg')
    print('table with row-access or column-mask policies attached. To')
    print('verify, re-run against a non-policied sibling table; loadTable')
    print('will return vended creds and a real manifest-list.')
    sys.exit(0)

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
print('A READABLE Phase A confirms the wire path works end to end. After')
print('attaching row-access or masking policies (snowflake/02_apply_policies.sql),')
print('this same call should return error.code=403 with')
print('error.type=ForbiddenException -- Polaris fail-secure on the')
print('policied table.')
"
