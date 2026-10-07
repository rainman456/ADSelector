# AdSelector

A low-latency ad-decisioning microservice. Given an impression request
(user + context signals + ad slot), it picks the best eligible ad candidate
across competing campaigns -- correctly enforcing per-campaign budget and
frequency caps even when thousands of concurrent requests are racing for
the same campaign's last few dollars of budget.

> **Status: scaffolded, not yet built, run, or tested.** This repository
> was generated as a portfolio project skeleton. Every file compiles-shaped
> real code, not stubs, but no build/run/test command has been executed
> against it yet. See [Setup](#setup-commands-not-yet-run) below.

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
- A Kafka-based analytics/aggregation pipeline (that belongs in a separate
  portfolio project; here Kafka is just an outbound event publish).

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
│   │   ├── atomic.go        correct in-memory CAS-loop implementation (the default)
│   │   ├── redis.go         correct distributed implementation via a Redis Lua script
│   │   └── naive_test.go    goroutine + WaitGroup test proving naive double-spends, atomic doesn't
│   ├── freqcap/              per-user-per-campaign-per-day frequency capping (Redis INCR+EXPIRE via Lua)
│   ├── events/                fire-and-forget "impression_served" publisher (Kafka-backed)
│   ├── metrics/                Prometheus collectors + /metrics handler
│   └── selector/               ties targeting + freqcap + budget + events together
├── pkg/
│   └── client/                dependency-free Go SDK for POST /v1/select (used by cmd/loadtest)
├── scripts/
│   └── loadtest.js             k6 alternative to cmd/loadtest
├── docs/
│   ├── architecture.md
│   └── adr/0001-naive-vs-atomic-budget-enforcement.md
├── docker-compose.yml           adselector + redis + kafka (KRaft mode, no zookeeper)
├── Dockerfile
├── go.mod
└── .gitignore
```

## Setup (commands NOT yet run)

None of the following has been executed while scaffolding this repository.
They're listed here as the commands that *would* be run next:

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
| `BUDGET_BACKEND`  | `memory`                  | `memory` (InMemoryAtomicTracker) or `redis` (RedisAtomicTracker) |
| `FREQCAP_BACKEND` | `redis`                   | `redis` or `noop` (for local dev without Redis)    |

## License

Portfolio project; no license file included yet.
