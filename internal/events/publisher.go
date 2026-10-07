package events

import "context"

// Publisher emits impression-served events. It is deliberately a thin
// interface -- AdSelector's job is real-time decisioning, not analytics.
// Building a real event-processing pipeline (aggregation, Spark/Flink
// jobs, dashboards, etc.) on top of these events is explicitly out of
// scope for this project; this is just the publish side of a
// fire-and-forget event.
type Publisher interface {
	// PublishImpressionServed sends evt to the underlying event bus. In the
	// Kafka implementation this is fire-and-forget from the caller's point
	// of view: it must never block or fail the HTTP request that triggered
	// it. Callers (see internal/selector) should invoke this from a
	// background goroutine and only surface publish failures via
	// logs/metrics, never by failing the ad-selection response.
	PublishImpressionServed(ctx context.Context, evt ImpressionServed) error

	// Close flushes any buffered messages and releases the underlying
	// connection/writer. Should be called once at server shutdown.
	Close() error
}
