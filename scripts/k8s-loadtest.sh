#!/usr/bin/env bash
# Run the k6 Job against the in-cluster Service and verify the shared Redis
# budget invariant after the run.
#
# Why Redis is the source of truth: every replica calls the same Lua spend
# script against the same key. If the final value for every budget:* key is
# >= 0 and <= its seeded value, no pod (or combination of pods) spent more
# than the campaign budget. This is checked after traffic stops, so it is
# independent of how k6 distributed requests across pods.
set -euo pipefail

NAMESPACE=${NAMESPACE:-adselector}
RELEASE=${RELEASE:-adselector}
REPLICAS=${REPLICAS:-2}
QPS=${QPS:-200}
DURATION=${DURATION:-30s}
BASE_URL=${BASE_URL:-http://${RELEASE}:80}
JOB_NAME=${JOB_NAME:-k6-loadtest}
JOB_TIMEOUT=${JOB_TIMEOUT:-10m}
RESET_BUDGETS=${RESET_BUDGETS:-true}
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

KUBECTL=(kubectl)
if [[ -n "${KUBE_CONTEXT:-}" ]]; then
  KUBECTL+=(--context "$KUBE_CONTEXT")
fi
kc() { "${KUBECTL[@]}" -n "$NAMESPACE" "$@"; }

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

declare -A SEED=(
  [camp-us-mobile-1]=500000000
  [camp-global-desktop-1]=1000000000
  [camp-eu-any-device-1]=250000000
  [camp-catch-all-low-bid]=2000000000
)

redis_pod=$(kc get pods \
  -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/name=redis" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
[[ -n "$redis_pod" ]] || fail "Redis pod not found; this check requires redis.enabled=true in the chart"

deployment=$(kc get deploy \
  -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/name=adselector" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
[[ -n "$deployment" ]] || fail "AdSelector deployment not found"

echo "==> Resetting budget keys and scaling ${deployment} to ${REPLICAS} replica(s)"
if [[ "$RESET_BUDGETS" == "true" ]]; then
  for campaign in "${!SEED[@]}"; do
    kc exec "$redis_pod" -- redis-cli --raw SET "budget:${campaign}" "${SEED[$campaign]}" EX 93600 >/dev/null
  done
fi

# Pin the HPA to this replica count so autoscaling cannot race the measurement.
hpa=$(kc get hpa \
  -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/name=adselector" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -n "$hpa" ]]; then
  kc patch hpa "$hpa" --type merge \
    -p "{\"spec\":{\"minReplicas\":${REPLICAS},\"maxReplicas\":${REPLICAS}}}" >/dev/null
  echo "==> Pinned HPA ${hpa} to ${REPLICAS} replica(s)"
fi

kc scale deployment "$deployment" --replicas="$REPLICAS" >/dev/null
kc rollout status deployment/"$deployment" --timeout=180s

echo "==> Creating k6 ConfigMap and Job"
kc delete job "$JOB_NAME" --ignore-not-found >/dev/null
kc create configmap k6-loadtest \
  --from-file=loadtest-k8s.js="${ROOT_DIR}/scripts/loadtest-k8s.js" \
  --from-literal=BASE_URL="$BASE_URL" \
  --from-literal=QPS="$QPS" \
  --from-literal=DURATION="$DURATION" \
  --dry-run=client -o yaml | kc apply -f - >/dev/null

kc apply -f "${ROOT_DIR}/deploy/k8s/k6-job.yaml" >/dev/null

echo "==> Waiting for k6 Job (QPS=${QPS}, DURATION=${DURATION}, BASE_URL=${BASE_URL})"
if ! kc wait --for=condition=complete "job/${JOB_NAME}" --timeout="$JOB_TIMEOUT"; then
  echo "---- k6 logs ----" >&2
  kc logs "job/${JOB_NAME}" --tail=-1 >&2 || true
  fail "k6 Job did not complete; thresholds failed or the pod errored"
fi

echo "---- k6 summary ----"
kc logs "job/${JOB_NAME}" --tail=-1

echo "==> Checking Redis budget invariant"
if ! keys_raw=$(kc exec "$redis_pod" -- redis-cli --raw --scan --pattern 'budget:*'); then
  fail "could not scan Redis budget keys"
fi
mapfile -t keys < <(printf '%s\n' "$keys_raw" | tr -d '\r' | awk 'NF' | sort -u)
[[ "${#keys[@]}" -gt 0 ]] || fail "no budget:* keys found in Redis"

declare -A seen=()
total_spent=0
for key in "${keys[@]}"; do
  value=$(kc exec "$redis_pod" -- redis-cli --raw GET "$key" | tr -d '\r\n')
  [[ "$value" =~ ^[0-9]+$ ]] || fail "budget key ${key} has non-numeric/negative value '${value}'"
  (( value >= 0 )) || fail "budget key ${key} is negative (${value}) -- overspend!"

  campaign=${key#budget:}
  if [[ -n "${SEED[$campaign]+x}" ]]; then
    seed=${SEED[$campaign]}
    (( value <= seed )) || fail "${campaign} remaining ${value} exceeds seed ${seed}"
    spent=$((seed - value))
    total_spent=$((total_spent + spent))
    seen[$campaign]=1
    printf '  %-26s seed=%-12s remaining=%-12s spent=%s\n' "$campaign" "$seed" "$value" "$spent"
  else
    printf '  %-26s remaining=%s (no seed recorded)\n' "$campaign" "$value"
  fi
done

for campaign in "${!SEED[@]}"; do
  [[ -n "${seen[$campaign]+x}" ]] || fail "missing budget key for ${campaign}"
done
(( total_spent > 0 )) || fail "no budget was spent; the load test likely never reached the service"

echo "PASS: every budget key is within [0, seed]; total spent=${total_spent} micros across ${REPLICAS} replica(s)"
