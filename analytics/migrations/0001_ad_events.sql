CREATE DATABASE IF NOT EXISTS adselector;

CREATE TABLE IF NOT EXISTS adselector.ad_events
(
    event_id         String,
    served_at        DateTime64(3, 'UTC'),
    user_id          LowCardinality(String),
    ad_slot_id       LowCardinality(String),
    campaign_id      LowCardinality(String),
    creative_id      String,
    bid_price_micros Int64,
    geo              LowCardinality(String),
    device           LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(served_at)
ORDER BY (campaign_id, served_at)
TTL served_at + INTERVAL 180 DAY;
