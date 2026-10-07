package budget

import "context"

// BudgetEnforcer atomically reserves spend against a campaign's remaining
// daily budget. This is the centerpiece of AdSelector's concurrency-safety
// story: many goroutines (or, in production, many API server instances)
// call TrySpend concurrently for the same campaign, and it must be
// impossible for two concurrent callers to both succeed when only enough
// budget remains for one of them.
//
// Three implementations exist on purpose:
//
//   - NaiveTracker (naive.go) -- a deliberately broken check-then-act
//     implementation, kept around for the demo/blog-post narrative in
//     docs/adr/0001-naive-vs-atomic-budget-enforcement.md. NOT used by
//     default anywhere in cmd/server/main.go.
//   - InMemoryAtomicTracker (atomic.go) -- a correct, lock-free (CAS-loop)
//     fix for NaiveTracker's race, for the local in-memory deployment mode.
//   - RedisAtomicTracker (redis.go) -- a correct implementation that
//     generalizes the same fix to a distributed deployment with N stateless
//     API replicas sharing one Redis instance, using a Lua script so the
//     read-check-decrement sequence is atomic from Redis's point of view.
type BudgetEnforcer interface {
	// TrySpend attempts to deduct amountMicros from campaignID's remaining
	// budget. ok reports whether the spend was accepted; when ok is false,
	// the caller should treat the campaign as exhausted for the rest of the
	// day (e.g. skip to the next candidate, or return "no fill").
	// remaining is the resulting balance after a successful spend, or the
	// last-known balance when ok is false (best-effort, for
	// observability/logging only -- never used for a subsequent decision).
	TrySpend(ctx context.Context, campaignID string, amountMicros int64) (ok bool, remaining int64, err error)

	// SetBudget (re)initializes campaignID's remaining budget, e.g. at
	// campaign creation or at the start of a new billing day.
	SetBudget(ctx context.Context, campaignID string, amountMicros int64) error

	// Remaining returns the current remaining budget for campaignID.
	Remaining(ctx context.Context, campaignID string) (int64, error)
}
