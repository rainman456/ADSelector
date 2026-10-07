package freqcap

import "context"

// NoopCapper allows every request through without tracking anything. It
// exists for local development or tests that don't have a Redis instance
// available and don't care about frequency-cap correctness -- it must
// never be used to serve real traffic, since it provides no capping at all.
type NoopCapper struct{}

var _ FrequencyCapper = NoopCapper{}

func (NoopCapper) Allow(_ context.Context, _ string, _ string, _ int) (bool, error) {
	return true, nil
}
