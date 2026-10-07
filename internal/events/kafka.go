package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

// KafkaPublisher is the production Publisher implementation. It wraps a
// single kafka-go Writer configured for fire-and-forget semantics (Async:
// true) so that a slow or unavailable broker can never add latency to the
// /v1/select response path.
//
// This package is intentionally thin: AdSelector's value proposition is
// low-latency decisioning, not stream processing, so there is no batching
// strategy tuning, no schema registry integration, and no exactly-once
// semantics here. A real production system serving billable events would
// want those; this is a portfolio-scoped placeholder that shows the shape
// of the integration, not a production-grade event pipeline.
type KafkaPublisher struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// KafkaConfig configures the underlying kafka-go Writer.
type KafkaConfig struct {
	Brokers []string
	Topic   string
}

// NewKafkaPublisher builds a KafkaPublisher. It does not connect
// immediately -- kafka-go's Writer dials lazily on the first WriteMessages
// call -- so constructing this value is cheap and safe to do at startup
// even if Kafka isn't reachable yet.
func NewKafkaPublisher(cfg KafkaConfig, logger *slog.Logger) *KafkaPublisher {
	if logger == nil {
		logger = slog.Default()
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        true, // fire-and-forget: don't block the caller on a broker round trip
	}
	return &KafkaPublisher{writer: w, logger: logger}
}

var _ Publisher = (*KafkaPublisher)(nil)

// PublishImpressionServed serializes evt to JSON and hands it to the
// (async) kafka-go writer. Because the writer is configured with
// Async: true, this call returns as soon as the message is queued
// in-process; delivery errors surface asynchronously via the writer's
// internal error handling rather than through this method's return value
// in the common case. We still surface synchronous errors (e.g.
// serialization failures, or the enqueue call itself failing) directly.
func (p *KafkaPublisher) PublishImpressionServed(ctx context.Context, evt ImpressionServed) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("events: KafkaPublisher: marshal event %s: %w", evt.EventID, err)
	}

	msg := kafka.Message{
		Key:   []byte(evt.CampaignID),
		Value: payload,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Warn("events: failed to enqueue impression_served message",
			"event_id", evt.EventID, "campaign_id", evt.CampaignID, "err", err)
		return fmt.Errorf("events: KafkaPublisher: enqueue message: %w", err)
	}
	return nil
}

// Close flushes buffered messages and closes the underlying connection.
func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
