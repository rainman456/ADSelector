# Verification (Tier C)

These are the commands that prove the repository works. They need Go,
Docker, Helm 3, and a local cluster. **No result has been recorded here**
and none of these commands was executed while writing the code. Copy the
relevant command, paste the output, and update
`docs/benchmarks/k8s-scaling.md` only with real numbers.

Use a Bash-compatible shell. On Unix-like systems make scripts executable
once:

```sh
chmod +x scripts/*.sh
```

Versions used by the repo: Go 1.24, Helm chart dependencies
`bitnami/redis 20.1.7` and `redpanda/redpanda 5.7.19`, k6 `0.54.0`,
kubeconform `v0.6.7`, metrics-server `v0.7.2`, Redis `7.4.1`, Redpanda via
the chart, ClickHouse `24.8.8.10`.

## 1. Go build and tests

```sh
go mod tidy
go build ./...
go vet ./...
go test ./...
go test -race -count=10 ./internal/budget/...
```

Expected: each command exits 0. `go mod tidy` creates `go.sum` (it is not
committed in the scaffold). The race run is the proof that the naive tracker
can double-spend under concurrency and that the atomic trackers do not.

## 2. Local Compose stack

```sh
docker compose up -d --build
docker compose ps
curl -s http://localhost:8080/healthz
curl -s http://localhost:8080/metrics | grep adselector_
curl -s -X POST http://localhost:8080/v1/select \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"u1","ad_slot_id":"slot-homepage-top","geo":"US","device":"mobile"}'
```

Expected: `adselector`, `redis`, and `kafka` are running; healthz returns
`{"status":"ok"}`; metrics contains `adselector_`; select returns HTTP 200
JSON with a boolean `filled` field. With the seed data, a US/mobile request
is normally filled by `camp-us-mobile-1`.

Run the local load test and record the summary:

```sh
k6 run --env BASE_URL=http://localhost:8080 --env QPS=200 --env DURATION=30s scripts/loadtest.js
```

Expected: k6 exits 0 (thresholds: p99 < 200 ms, HTTP failures < 1%). The
numbers are for your machine; do not copy them into the repo as benchmark
results.

Stop the stack:

```sh
docker compose down -v
```

## 3. Graceful shutdown

```sh
go build -o bin/adselector ./cmd/server
bash scripts/shutdown-check.sh
```

Expected: `PASS: SIGTERM handled, exit 0 after Ns (grace 15s)`. The script
sets `BUDGET_BACKEND=memory`, `FREQCAP_BACKEND=noop`,
`KAFKA_ENABLED=false`, so it needs no Redis or Kafka. It keeps requests in
flight, sends `SIGTERM`, and requires exit code 0 plus the
`shutdown complete` log line.

## 4. Helm chart statically

```sh
helm dependency build deploy/helm/adselector
helm lint deploy/helm/adselector

helm template adselector deploy/helm/adselector \
  --set redis.enabled=true --set kafka.enabled=false \
  | docker run --rm -i ghcr.io/yannh/kubeconform:v0.6.7 \
      -strict -ignore-missing-schemas -summary -

helm template adselector deploy/helm/adselector \
  --set redis.enabled=false --set kafka.enabled=true \
  --set config.kafkaEnabled=true \
  | docker run --rm -i ghcr.io/yannh/kubeconform:v0.6.7 \
      -strict -ignore-missing-schemas -summary -
```

Expected: `helm lint` reports no failures; both kubeconform runs report 0
invalid objects. The chart renders with Redis only by default, and with
Redpanda when Kafka is enabled.

## 5. Kind smoke test

```sh
make cluster-up CLUSTER=kind
make k8s-test CLUSTER=kind
```

Expected: kind cluster `adselector` is created, metrics-server v0.7.2 is
applied, the image is built and loaded, Helm install waits for readiness,
then the smoke test prints:

```
PASS: healthz, select, and metrics checks succeeded against http://127.0.0.1:18080
```

`make k8s-test` exits non-zero on any failed assertion. It uses only
`kubectl port-forward`; no NodePort or Ingress.

## 6. Minikube smoke test

```sh
make cluster-up CLUSTER=minikube
make k8s-test CLUSTER=minikube
```

Expected: minikube profile `adselector` starts, the metrics-server addon is
enabled, `minikube -p adselector image load` loads the image, Helm installs,
and the same `PASS` line is printed. Tear down with:

```sh
make cluster-down CLUSTER=minikube
```

## 7. Budget invariant and scaling

Run at one replica, then at N:

```sh
make k8s-loadtest CLUSTER=minikube REPLICAS=1 QPS=200 DURATION=30s
make k8s-loadtest CLUSTER=minikube REPLICAS=4 QPS=400 DURATION=30s
```

For kind, substitute `CLUSTER=kind`. The runner:

1. finds the Redis pod and resets the four seeded `budget:*` keys,
2. pins the HPA to the requested replica count and scales the Deployment,
3. creates the k6 ConfigMap and Job, waits for completion, and prints the
   k6 summary,
4. scans `budget:*` and asserts every value is in `[0, seeded]` and that at
   least one campaign spent.

Expected: k6 thresholds pass, then:

```
PASS: every budget key is within [0, seed]; total spent=... micros across N replica(s)
```

Record the real p50/p95/p99, RPS, error rate, and the invariant line in
`docs/benchmarks/k8s-scaling.md`. Until then those cells stay
"not yet measured".

## 8. Analytics companion (optional)

```sh
cd analytics
go mod tidy
go build ./...
go vet ./...
cd ..

docker compose --profile analytics up -d --build
docker compose --profile analytics ps

# Apply the schema if the ClickHouse image did not run the init script:
docker compose --profile analytics exec -T clickhouse \
  clickhouse-client --multiquery < analytics/migrations/0001_ad_events.sql

# Generate one served event through the core service:
curl -s -X POST http://localhost:8080/v1/select \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"analytics-check","ad_slot_id":"slot-homepage-top","geo":"DE","device":"mobile"}'

# Wait a few seconds for the Kafka consumer, then query:
docker compose --profile analytics exec -T clickhouse clickhouse-client \
  --query "SELECT campaign_id, toStartOfHour(served_at) AS hour, sum(bid_price_micros) AS spend_micros, count() AS impressions FROM ad_events GROUP BY campaign_id, hour ORDER BY hour DESC"
```

Expected: `analytics` is healthy on `:8081` (`/healthz`, `/metrics`), and
the query returns at least the `camp-us-mobile-1` or
`camp-eu-any-device-1` row depending on the geo/device you sent. The
consumer commits offsets only after a successful insert, so a crash can
duplicate rows; this is documented in `docs/analytics.md`.

Stop it:

```sh
docker compose --profile analytics down -v
```

## 9. CI

Push a branch or open a pull request. `.github/workflows/ci.yml` runs:

- `helm dependency build`, `helm lint`, and kubeconform for Kafka off/on,
- `go mod tidy`, build, vet, test, and a race test of `internal/budget`,
- kind install, metrics-server, image build/load, Helm install, the
  port-forward smoke test, and a 2-replica k6 run with the Redis budget
  invariant.

All actions, images, and chart dependencies are pinned. The workflow needs
no repository secrets.

## 10. Teardown

```sh
docker compose --profile analytics down -v
make cluster-down CLUSTER=minikube
make cluster-down CLUSTER=kind
```

Expected: `docker compose ps` is empty, and the named cluster no longer
appears in `minikube profile list` or `kind get clusters`.
