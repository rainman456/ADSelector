package events

import "context"

// NoopPublisher discards impression events. It exists for deployments that
// intentionally run without Kafka (KAFKA_ENABLED=false, the Helm chart
// default) so no broker connection is attempted while the Publisher
// interface still has a safe, non-nil implementation.
type NoopPublisher struct{}

var _ Publisher = (*NoopPublisher)(nil)

// NewNoopPublisher builds a Publisher that accepts and drops every event.
func NewNoopPublisher() *NoopPublisher {
	return &NoopPublisher{}
}

func (*NoopPublisher) PublishImpressionServed(_ context.Context, _ ImpressionServed) error {
	return nil
}

func (*NoopPublisher) Close() error {
	return nil
}
