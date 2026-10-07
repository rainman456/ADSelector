package budget

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// NaiveTracker is a DELIBERATELY BROKEN BudgetEnforcer implementation.
//
// It is kept in this codebase on purpose, as the "before" half of the
// naive-vs-atomic demo described in
// docs/adr/0001-naive-vs-atomic-budget-enforcement.md. It is NOT wired up
// in cmd/server/main.go and must never be used to serve real traffic.
//
// The bug: TrySpend performs a classic check-then-act (a.k.a.
// time-of-check-to-time-of-use / TOCTOU) race. It reads the current
// balance, decides whether there's enough budget, and only *then* writes
// the new balance back -- as separate critical sections instead of one.
// Under concurrent load, two goroutines can both read the same
// "remaining = 10" balance, both decide "yes, I can spend my $10", and both
// write back a debit, over-spending the campaign's budget. This is exactly
// the "double-spend" failure mode a real-money ad-serving system cannot
// tolerate.
//
// A small artificial delay is inserted between the read and the write to
// widen the race window so unit tests (and a live demo) reliably reproduce
// the bug on typical hardware. Real production race conditions don't need
// any help from an artificial sleep -- they just happen less predictably,
// which is part of what makes them dangerous: this bug could sit in
// production for months before a traffic spike surfaces it.
type NaiveTracker struct {
	mu        sync.Mutex // guards map access itself, NOT the read-check-write sequence below
	remaining map[string]int64

	// RaceWindow is the artificial delay inserted between the balance read
	// and the balance write. Defaults to 1ms if zero. Set to 0 explicitly
	// via a nonzero-but-tiny value if you want to see the race happen more
	// rarely, closer to how it would in production.
	RaceWindow time.Duration
}

// NewNaiveTracker builds a NaiveTracker. See the type doc comment: this is
// the intentionally-racy implementation, not the one main.go should use.
func NewNaiveTracker() *NaiveTracker {
	return &NaiveTracker{
		remaining: make(map[string]int64),
	}
}

var _ BudgetEnforcer = (*NaiveTracker)(nil)

func (t *NaiveTracker) SetBudget(_ context.Context, campaignID string, amountMicros int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.remaining[campaignID] = amountMicros
	return nil
}

func (t *NaiveTracker) Remaining(_ context.Context, campaignID string) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	v, ok := t.remaining[campaignID]
	if !ok {
		return 0, fmt.Errorf("budget: NaiveTracker: unknown campaign %q", campaignID)
	}
	return v, nil
}

// TrySpend is the broken half of the demo. Note the separate critical
// sections: read, [sleep], write. Nothing prevents another goroutine from
// running its own read+decide in between this goroutine's read and its
// write -- that's the race.
func (t *NaiveTracker) TrySpend(_ context.Context, campaignID string, amountMicros int64) (bool, int64, error) {
	window := t.RaceWindow
	if window == 0 {
		window = time.Millisecond
	}

	// --- 1. CHECK: read the current balance under its own short-lived lock.
	t.mu.Lock()
	current, ok := t.remaining[campaignID]
	t.mu.Unlock()
	if !ok {
		return false, 0, fmt.Errorf("budget: NaiveTracker: unknown campaign %q", campaignID)
	}

	if current < amountMicros {
		// Not enough budget -- correctly rejected. The bug only affects the
		// accept path below.
		return false, current, nil
	}

	// Artificial gap between "I checked and there was enough" and "I acted
	// on that belief". In a real system this gap is just normal goroutine
	// scheduling, GC pauses, or network hops to a shared store -- no sleep
	// required for the bug to exist, only to make it reliably observable
	// in a fast unit test.
	time.Sleep(window)

	// --- 2. ACT: write back the debited balance, in a *different*,
	// unrelated critical section from the read above. Another goroutine
	// could have already spent against the same balance we read in step 1,
	// and we have no way of knowing that here.
	t.mu.Lock()
	t.remaining[campaignID] -= amountMicros
	newBalance := t.remaining[campaignID]
	t.mu.Unlock()

	return true, newBalance, nil
}
