#!/usr/bin/env bash
# Tier C check: prove the API drains within its grace period after SIGTERM.
# Build first: go build -o bin/adselector ./cmd/server
set -euo pipefail

BINARY=${BINARY:-./bin/adselector}
PORT=${PORT:-18081}
GRACE_SECONDS=${GRACE_SECONDS:-15}
ADDR="127.0.0.1:${PORT}"

if [[ ! -x "$BINARY" ]]; then
  echo "FAIL: binary not found or not executable: $BINARY"
  echo "Build it with: go build -o bin/adselector ./cmd/server"
  exit 1
fi

tmpdir=$(mktemp -d)
server_log="$tmpdir/server.log"
pid=""

cleanup() {
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid" 2>/dev/null || true
  fi
  if [[ -n "${request_loop_pid:-}" ]] && kill -0 "$request_loop_pid" 2>/dev/null; then
    kill "$request_loop_pid" 2>/dev/null || true
  fi
  rm -rf "$tmpdir"
}
trap cleanup EXIT

BUDGET_BACKEND=memory \
FREQCAP_BACKEND=noop \
KAFKA_ENABLED=false \
LISTEN_ADDR="$ADDR" \
  "$BINARY" >"$server_log" 2>&1 &
pid=$!

ready=0
for _ in $(seq 1 50); do
  if curl -fsS --max-time 1 "http://$ADDR/healthz" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.2
done
if [[ "$ready" != "1" ]]; then
  echo "FAIL: server did not become healthy"
  cat "$server_log"
  exit 1
fi

# Keep requests in flight while the process receives SIGTERM.
(
  while kill -0 "$pid" 2>/dev/null; do
    curl -s --max-time 1 -X POST "http://$ADDR/v1/select" \
      -H 'Content-Type: application/json' \
      -d '{"user_id":"shutdown-check","ad_slot_id":"slot-homepage-top","geo":"US","device":"mobile"}' \
      >/dev/null 2>&1 || true
    sleep 0.01
  done
) &
request_loop_pid=$!

sleep 0.5
start=$(date +%s)
kill -TERM "$pid"

set +e
wait "$pid"
exit_code=$?
set -e
end=$(date +%s)
elapsed=$((end - start))

kill "$request_loop_pid" 2>/dev/null || true
wait "$request_loop_pid" 2>/dev/null || true
request_loop_pid=""

if [[ "$exit_code" -ne 0 ]]; then
  echo "FAIL: process exit code $exit_code after SIGTERM (expected 0)"
  cat "$server_log"
  exit 1
fi

if [[ "$elapsed" -gt "$GRACE_SECONDS" ]]; then
  echo "FAIL: shutdown took ${elapsed}s, above ${GRACE_SECONDS}s"
  cat "$server_log"
  exit 1
fi

if ! grep -q 'shutdown complete' "$server_log"; then
  echo "FAIL: graceful shutdown log line missing"
  cat "$server_log"
  exit 1
fi

echo "PASS: SIGTERM handled, exit 0 after ${elapsed}s (grace ${GRACE_SECONDS}s)"
