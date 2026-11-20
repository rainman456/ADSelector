# AdSelector

A low-latency ad-decisioning microservice. Given an impression request
(user + context signals + ad slot), it picks the best eligible ad candidate
across competing campaigns -- correctly enforcing per-campaign budget and
frequency caps even when thousands of concurrent requests are racing for
the same campaign's last few dollars of budget.

> **Status: implementation is in place, but this repository has not been
> built, run, or tested on the authoring machine.** The Go service, Helm
> chart, Kubernetes workflow, CI, and companion analytics module are
> written; the commands that prove them are Tier C and live in
> [`docs/verify.md`](docs/verify.md). Nothing below should be read as a
> measured result until those steps pass.

## Problem statement

Ad exchanges pick a winning ad tens of thousands of times a second, and
every winning pick has to debit that campaign's daily budget. The naive way
to track "how much budget does campaign X have left" is: read the current
balance, check if it covers this bid, and if so, write back the new
balance. That's fine for one request at a time. It is **not** fine when
hundreds of requests for the same popular campaign arrive within the same
few milliseconds:

```
Campaign X has $10.00 remaining.
Request A reads: $10.00 remaining -> "I can spend $1, go ahead"
Request B reads: $10.00 remaining -> "I can spend $1, go ahead"   (same $10, read before A wrote back!)
Request A writes: $9.00 remaining
Request B writes: $9.00 remaining   (B overwrote A's debit with its own, stale-based, $9.00)
```

Multiply that by hundreds of concurrent requests and a campaign that should
have been shut off at exactly $0.00 can serve well past its budget --
silently over-delivering impressions the advertiser never paid for. This
double-spend problem is the same shape as an overdraft bug in a banking
system, and it is the single hardest correctness problem in this project.
AdSelector's core deliverable is making that failure mode structurally
impossible, not just "usually fine in practice."

See [`docs/adr/0001-naive-vs-atomic-budget-enforcement.md`](docs/adr/0001-naive-vs-atomic-budget-enforcement.md)
for the full before/after story, including a Go test
(`internal/budget/naive_test.go`) that reproduces the double-spend under
concurrent load and proves the fix closes it.

## What this project is / is not

**Is:**
- A real HTTP API (`POST /v1/select`) with real request/response structs.
- Rules-based targeting (geo/device match, highest-bid-wins) -- no ML.
- Two budget-enforcement implementations behind one interface, built
  specifically to demonstrate a concurrency bug and its fix.
- Redis-backed frequency capping (INCR + EXPIRE, atomic via Lua).
- A thin, fire-and-forget Kafka event publish on each successful selection.
- Prometheus metrics, a docker-compose dev environment, and a load-test
  harness (Go + optional k6 script) to produce p99-latency-at-N-qps numbers.

**Is not:**
- A real ad-ranking / CTR-prediction ML system (a highest-bid-wins rules
  scorer is deliberately "enough" here -- see `internal/selector`).
- A frontend of any kind.
- A Kafka-based analytics pipeline inside the core service. Kafka is just
  an outbound event publish here. An optional, separately built companion
  consumer lives in `analytics/` (see [Analytics companion](#analytics-companion)).

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for a request-flow
diagram. In short:

```
client -> POST /v1/select
            -> campaign.Store.SelectCandidates   (targeting filter)
            -> sort by bid, highest first
            -> freqcap.FrequencyCapper.Allow      (per user+campaign, per day)
            -> budget.BudgetEnforcer.TrySpend     (atomic; the double-spend fix lives here)
            -> events.Publisher.PublishImpressionServed (async, fire-and-forget)
          <- SelectResponse{filled, campaign_id, creative_id, bid_price_micros}
```

## Repository layout

```
AdSelector/
├── cmd/
│   ├── server/main.go       entrypoint: wires store + budget + freqcap + events + metrics + HTTP server
│   └── loadtest/main.go     concurrent HTTP load generator producing p50/p90/p99 latency
├── internal/
│   ├── api/                 HTTP request/response types, handler, chi router
│   ├── campaign/            campaign model + Store interface + in-memory implementation
│   ├── budget/              THE CENTERPIECE: naive (broken) vs. atomic (fixed) budget enforcement
│   │   ├── interface.go     BudgetEnforcer interface
│   │   ├── naive.go         deliberately racy check-then-act implementation (demo only, never wired to main)
│   │   ├── atomic.go        correct in-memory CAS-loop implementation (single-replica default)
│   │   ├── redis.go         correct distributed implementation via a Redis Lua script
│   │   └── naive_test.go    goroutine + WaitGroup test proving naive double-spends, atomic doesn't
│   ├── freqcap/              per-user-per-campaign-per-day frequency capping (Redis INCR+EXPIRE via Lua)
│   ├── events/                fire-and-forget "impression_served" publisher (Kafka or noop)
│   ├── metrics/                Prometheus collectors + /metrics handler
│   └── selector/               ties targeting + freqcap + budget + events together
├── pkg/
│   └── client/                dependency-free Go SDK for POST /v1/select (used by cmd/loadtest)
├── deploy/
│   ├── helm/adselector/        chart: Deployment, Service, ConfigMap, PDB, HPA, Redis/Redpanda deps
│   └── k8s/                    metrics-server for kind, k6 Job
├── analytics/                  separate Go module: Kafka -> ClickHouse consumer + migrations
├── scripts/
│   ├── loadtest.js             k6 alternative to cmd/loadtest (local)
│   ├── loadtest-k8s.js         k6 script aimed at seeded campaigns (in-cluster)
│   ├── k8s-smoke.sh            port-forwarded health/select/metrics assertions
│   ├── k8s-loadtest.sh         k6 Job runner + Redis budget-invariant check
│   └── shutdown-check.sh       SIGTERM drain check
├── docs/
│   ├── architecture.md
│   ├── verify.md               Tier C commands and expected results
│   ├── analytics.md            ClickHouse schema, consumer, spend-per-hour query
│   ├── benchmarks/k8s-scaling.md
│   └── adr/                    decision records 0001-0004
├── .github/workflows/ci.yml    helm lint/kubeconform, go tests, kind smoke + invariant
├── Makefile                    cluster-up/down, k8s-build/load/deploy/test/loadtest/logs
├── docker-compose.yml           adselector + redis + kafka; optional analytics profile
├── Dockerfile
├── go.mod
└── .gitignore
```

## Local setup (Tier C, not yet run)

None of the following has been executed while writing this repository.
They are the commands to run next; [`docs/verify.md`](docs/verify.md) has
the full ordered checklist including Kubernetes.

```sh
# 1. Resolve and pin real dependency versions / generate go.sum
go mod tidy

# 2. Bring up Redis + Kafka (+ the API itself, containerized) for local dev
docker compose up -d

# 3. Build everything
go build ./...

# 4. Run the full test suite
go test ./...

# 5. Run just the concurrency demo, with the race detector, a few times to
#    reliably observe the naive tracker double-spending
go test -race -count=10 ./internal/budget/...

# 6. Run the server directly (instead of via docker compose)
REDIS_ADDR=localhost:6379 KAFKA_BROKER=localhost:9092 go run ./cmd/server

# 7. Hit it once it's up
curl -s -X POST localhost:8080/v1/select \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"u1","ad_slot_id":"slot-homepage-top","geo":"US","device":"mobile"}'

# 8. Load-test it (Go harness or k6)
go run ./cmd/loadtest -qps 200 -duration 30s
k6 run scripts/loadtest.js

# 9. Prove SIGTERM drains cleanly
go build -o bin/adselector ./cmd/server
bash scripts/shutdown-check.sh
```

## Configuration

`cmd/server` reads its config from environment variables (see
`cmd/server/main.go`):

| Variable          | Default                  | Purpose                                           |
|-------------------|---------------------------|---------------------------------------------------|
| `LISTEN_ADDR`     | `:8080`                   | HTTP listen address                                |
| `REDIS_ADDR`      | `localhost:6379`          | Redis address (freqcap, and budget if redis-backed)|
| `KAFKA_BROKER`    | `localhost:9092`          | Kafka bootstrap broker                             |
| `KAFKA_TOPIC`     | `impression_served`       | Topic for the fire-and-forget event publish        |
| `KAFKA_ENABLED`   | `true`                    | `false` uses `events.NoopPublisher` and never dials Kafka |
| `BUDGET_BACKEND`  | `memory`                  | `memory` (InMemoryAtomicTracker) or `redis` (RedisAtomicTracker) |
| `BUDGET_SEED`     | `reset`                   | `redis` startup seeding: `reset` overwrites, `init` only writes missing keys |
| `FREQCAP_BACKEND` | `redis`                   | `redis` or `noop` (for local dev without Redis)    |

The chart sets `BUDGET_BACKEND=redis`, `BUDGET_SEED=init`, and
`KAFKA_ENABLED=false` by default so two replicas can run safely without
Kafka. The binary defaults keep the original local behavior.

## Run on Kubernetes

The chart lives at `deploy/helm/adselector` (version 0.1.0). It deploys two
API replicas, a PDB (`minAvailable: 1`), and an HPA (`autoscaling/v2`, CPU
70%, min 2, max 10). Redis is a subchart enabled by default with auth off
for local use. Kafka is off by default; enabling the subchart runs a
single-broker Redpanda. Point at managed services with
`external.redisAddr` / `external.kafkaBroker` and `redis.enabled=false` /
`kafka.enabled=false`.

The Makefile defaults are `CLUSTER=minikube`, `RELEASE=adselector`,
`NAMESPACE=adselector`, `IMAGE=adselector:0.1.0`:

```sh
# minikube: build, load, install, port-forward, smoke test
make k8s-test CLUSTER=minikube BASE_URL=http://adselector:80
make k8s-logs CLUSTER=minikube

# kind: same flow; the Makefile applies deploy/k8s/metrics-server-kind.yaml
make k8s-test CLUSTER=kind
make k8s-logs CLUSTER=kind

# After k8s-test the smoke URL is:
kubectl --context minikube  -n adselector port-forward svc/adselector 18080:80
kubectl --context kind-adselector -n adselector port-forward svc/adselector 18080:80
curl -s http://127.0.0.1:18080/healthz
```

`make k8s-test` builds `IMAGE`, loads it with
`minikube -p adselector image load` or
`kind load docker-image --name adselector`, runs
`helm dependency build` + `helm upgrade --install --wait`, waits for
`rollout status`, port-forwards `svc/adselector` on `18080:80`, and runs
`scripts/k8s-smoke.sh` (healthz 200, `/v1/select` 200 JSON with `filled`,
`/metrics` containing `adselector_`). It exits non-zero on failure. No
NodePort or Ingress is used by the test path.

Manual install (image must already be built and loaded if it is not in a
registry):

```sh
docker build --build-arg VERSION=0.1.0 -t adselector:0.1.0 .
make k8s-load CLUSTER=kind
helm dependency build deploy/helm/adselector
helm upgrade --install adselector deploy/helm/adselector \
  --namespace adselector --create-namespace \
  --set image.repository=adselector --set image.tag=0.1.0 \
  --wait --timeout 5m
```

Kafka is opt-in and needs both flags:

```sh
helm upgrade --install adselector deploy/helm/adselector -n adselector \
  --set kafka.enabled=true --set config.kafkaEnabled=true \
  --set config.kafkaTopic=impression_served
```

`make k8s-up` is `cluster-up` + `k8s-deploy` exactly as the task defines
it; it assumes the image is pullable (use the GHCR repository or run
`k8s-build`/`k8s-load` first).

Scaling measurements and the invariant check:

```sh
make k8s-loadtest CLUSTER=minikube REPLICAS=1 QPS=200 DURATION=30s
make k8s-loadtest CLUSTER=minikube REPLICAS=4 QPS=400 DURATION=30s
```

Record results in [`docs/benchmarks/k8s-scaling.md`](docs/benchmarks/k8s-scaling.md).
They are all marked "not yet measured" until someone runs the commands.

## Budget invariant across replicas

The correctness argument does not depend on how many pods exist.

- With `BUDGET_BACKEND=memory`, each pod owns an independent CAS counter.
  That is correct inside one process and **wrong across replicas**, so the
  chart does not use it.
- With `BUDGET_BACKEND=redis` (the chart default), every pod calls the same
  Lua script (`internal/budget/redis.go`). Redis executes a script to
  completion without interleaving another client, so read-check-decrement
  is one atomic operation against one shared key. N pods share one counter;
  a campaign cannot spend past its budget.
- `BUDGET_SEED=init` (chart default) makes startup seeding use `SET NX`.
  A rolling update or scale-up cannot reset spend already recorded by
  another pod. The binary default `reset` keeps the original
  single-replica behavior.

`scripts/k8s-loadtest.sh` proves the property out-of-band. After the k6 Job
finishes, it reads every `budget:*` key from the Redis pod and asserts each
value is between 0 and its seeded value, and that at least one campaign
actually spent. The Job's k6 thresholds fail on more than 1% HTTP errors.
The runner also pins the HPA to the requested replica count for the run.

## Graceful shutdown

The server handles `SIGINT`/`SIGTERM`, stops accepting connections, and
drains in-flight requests with a 10s timeout (`cmd/server/main.go`). The
chart sets `terminationGracePeriodSeconds: 30`, `maxUnavailable: 0`, and
`maxSurge: 1`, so a rolling update keeps capacity while an old pod drains.
There is deliberately no `preStop` hook: the image is distroless and has no
shell. `scripts/shutdown-check.sh` starts the binary, sends traffic,
delivers `SIGTERM`, and checks exit 0 and the `shutdown complete` log line.

## Analytics companion

[`analytics/`](analytics) is a separate Go module
(`github.com/michaelstone897/adselector/analytics`). It consumes
`impression_served` with `segmentio/kafka-go`, batch-inserts into ClickHouse
(`clickhouse-go/v2`), and exposes `/healthz` and `/metrics` on `:8081`. The
core service does not import it; Kafka stays an outbound publish. See
[`docs/analytics.md`](docs/analytics.md) for the schema, environment, and the
campaign spend-per-hour query. Local optional stack:

```sh
docker compose --profile analytics up -d --build
```

## License

Portfolio project; no license file included yet.
