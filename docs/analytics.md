# Analytics companion

`analytics/` is a separate Go module,
`github.com/michaelstone897/adselector/analytics`. It exists because the core
service intentionally stops at publishing `impression_served`; building a
real aggregation pipeline in the request path would add a dependency and a
failure mode the ad decision does not need. See
[`adr/0002-analytics-companion-module.md`](adr/0002-analytics-companion-module.md).

## What it does

```
adselector --Kafka--> impression_served
                             |
                             v
                  analytics (kafka-go consumer)
                             |
                             v
                  ClickHouse ad_events (MergeTree)
```

- Consumes `KAFKA_TOPIC` (default `impression_served`) in group
  `KAFKA_GROUP` (default `adselector-analytics`).
- Buffers up to `BATCH_SIZE` (default 500) events or `FLUSH_INTERVAL`
  (default 5s), then inserts them into `ad_events`.
- Commits Kafka offsets only after a successful insert. A crash between
  insert and commit can duplicate rows; this is at-least-once, not
  exactly-once.
- Exposes `/healthz` and `/metrics` on `LISTEN_ADDR` (default `:8081`).
  Metrics: `adselector_analytics_kafka_consumed_total`,
  `adselector_analytics_clickhouse_inserted_total`,
  `adselector_analytics_analytics_failed_total`.

## Schema

`migrations/0001_ad_events.sql`:

- `MergeTree`
- `PARTITION BY toYYYYMM(served_at)`
- `ORDER BY (campaign_id, served_at)`
- `TTL served_at + INTERVAL 180 DAY`

Apply it once per database:

```sh
clickhouse-client --multiquery < analytics/migrations/0001_ad_events.sql
```

The Compose ClickHouse service also mounts `analytics/migrations` into
`/docker-entrypoint-initdb.d`, which runs it on a fresh data directory.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `LISTEN_ADDR` | `:8081` | health and metrics HTTP listener |
| `KAFKA_BROKER` | `localhost:9092` | comma-separated brokers |
| `KAFKA_TOPIC` | `impression_served` | source topic |
| `KAFKA_GROUP` | `adselector-analytics` | consumer group |
| `CLICKHOUSE_ADDR` | `localhost:9000` | native protocol address |
| `CLICKHOUSE_DB` | `adselector` | database |
| `CLICKHOUSE_USER` | `default` | user |
| `CLICKHOUSE_PASSWORD` | empty | password |
| `BATCH_SIZE` | `500` | events per insert |
| `FLUSH_INTERVAL` | `5s` | max wait before a partial insert |
| `SHUTDOWN_GRACE` | `10s` | HTTP shutdown timeout |
| `INSERT_TIMEOUT` | `15s` | ClickHouse insert timeout |

All are also flags with the same names (`-batch-size`, `-flush-interval`,
and so on).

## Run locally

```sh
docker compose --profile analytics up -d --build
```

That starts Kafka, ClickHouse, the core service, and the consumer. Generate
an event through `POST /v1/select`, then query.

## Campaign spend per hour

```sql
SELECT campaign_id,
       toStartOfHour(served_at) AS hour,
       sum(bid_price_micros) AS spend_micros,
       count() AS impressions
FROM ad_events
GROUP BY campaign_id, hour
ORDER BY hour DESC
```

`bid_price_micros` is integer micros of the account currency
(1,000,000 = 1.00). The core service owns budget correctness; this query is
reporting only and can lag or double-count after a consumer crash.

## Limits

- No schema registry; the consumer decodes the JSON emitted by
  `internal/events`.
- No exactly-once sink; `MergeTree` has no dedupe key. A
  `ReplacingMergeTree` keyed on `event_id` would be the next step.
- No retention beyond the 180-day TTL.
- Not part of the Helm chart. It is a separate module and a Compose
  profile, so the default deployment stays small.
