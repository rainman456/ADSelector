# ADR 0002: Keep analytics in a companion module

- **Status:** Accepted
- **Date:** 2026-11-19
- **Context:** The service publishes `impression_served` to Kafka. Should the
  aggregation pipeline (Kafka consumer, ClickHouse schema, spend reporting)
  live inside the API process?

## Context

The ad decision path has one hard requirement: answer `POST /v1/select`
quickly and never let a reporting failure reject an ad. A Kafka consumer
that batches into ClickHouse needs a database driver, a consumer group, a
batch buffer, and its own retry loop. Putting that in the API binary would
add dependencies and goroutines to the request-serving process, and a
ClickHouse outage would compete for memory and attention with the code that
must keep serving ads.

Kafka already decouples the producer from consumers. The API's contract is
"publish one event, fire and forget"; a downstream consumer can be built,
deployed, and scaled separately.

## Decision

Create `analytics/` as a **separate Go module**,
`github.com/michaelstone897/adselector/analytics`:

- `segmentio/kafka-go` consumer in group `adselector-analytics`.
- `ClickHouse/clickhouse-go/v2` batch inserts into `ad_events`.
- `/healthz` and `/metrics` on `:8081`.
- Its own `Dockerfile`, `go.mod`, and `migrations/0001_ad_events.sql`.
- Not imported by the core module and not part of the Helm chart.

The core module keeps only the existing outbound publisher. Reporting reads
`ad_events`; it is not consulted by the budget enforcer, so ClickHouse lag
cannot change an ad decision or a spend.

## Consequences

- The API image stays small and has no ClickHouse or consumer dependency.
- The consumer can be restarted or scaled without touching API pods.
- Delivery is at-least-once: offsets are committed after an insert, and a
  crash in between can duplicate rows. `MergeTree` has no dedupe key; a
  `ReplacingMergeTree` keyed on `event_id` would be the next step.
- There are two `go.mod` files and two build commands to maintain. CI builds
  and vets both.
- The core `internal/events` JSON struct and the consumer's struct must stay
  in sync manually; there is no schema registry. This is acceptable for a
  portfolio-sized event with nine fields and is documented in
  `docs/analytics.md`.
