#!/usr/bin/env bash
# Smoke test against an already port-forwarded Service URL.
# No NodePort or Ingress is used. Exits non-zero on any failed assertion.
set -euo pipefail

BASE_URL=${BASE_URL:-http://127.0.0.1:18080}
BASE_URL=${BASE_URL%/}
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

health_code=$(curl -sS --max-time 5 -o "$tmpdir/health.json" -w '%{http_code}' "$BASE_URL/healthz")
[[ "$health_code" == "200" ]] || fail "GET /healthz returned $health_code"
grep -q '"status"' "$tmpdir/health.json" || fail "/healthz body missing status"

select_code=$(curl -sS --max-time 5 -o "$tmpdir/select.json" -w '%{http_code}' \
  -X POST "$BASE_URL/v1/select" \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"smoke-user","ad_slot_id":"slot-homepage-top","geo":"US","device":"mobile"}')
[[ "$select_code" == "200" ]] || fail "POST /v1/select returned $select_code"
grep -Eq '"filled"[[:space:]]*:[[:space:]]*(true|false)' "$tmpdir/select.json" \
  || fail "POST /v1/select response missing boolean filled"

metrics_code=$(curl -sS --max-time 5 -o "$tmpdir/metrics.txt" -w '%{http_code}' "$BASE_URL/metrics")
[[ "$metrics_code" == "200" ]] || fail "GET /metrics returned $metrics_code"
grep -q 'adselector_' "$tmpdir/metrics.txt" || fail "/metrics does not contain adselector_"

echo "PASS: healthz, select, and metrics checks succeeded against $BASE_URL"
