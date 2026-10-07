package budget

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// InMemoryAtomicTracker is the correct, concurrency-safe fix for
// NaiveTracker's check-then-act race, for the local in-memory deployment
// mode (single process, no Redis).
//
// Instead of separate read/decide/write critical sections, each spend
// attempt is a single compare-and-swap (CAS) loop: read the current
// balance, compute the candidate new balance, and atomically install it
// ONLY if nobody else has changed the balance in the meantime. If the CAS
// fails (because another goroutine won the race), we simply retry with the
// fresh value. This makes "read current balance, decide, commit" a single
// indivisible operation from every other goroutine's point of view -- there
// is no window in which two goroutines can both believe they've reserved
// the last dollar of budget.
//
// This is the in-memory generalization of the same idea RedisAtomicTracker
// implements with a Lua script against a shared Redis instance (see
// redis.go) -- one process's CAS loop vs. Redis's single-threaded script
// execution, same underlying guarantee: "check and commit" as one step.
type InMemoryAtomicTracker struct {
	mu      sync.RWMutex // guards insertion/lookup in the map, not the spend itself
	budgets map[string]*atomic.Int64
}

// NewInMemoryAtomicTracker builds the CAS-loop based in-memory tracker.
func NewInMemoryAtomicTracker() *InMemoryAtomicTracker {
	return &InMemoryAtomicTracker{
		budgets: make(map[string]*atomic.Int64),
	}
}

var _ BudgetEnforcer = (*InMemoryAtomicTracker)(nil)

func (t *InMemoryAtomicTracker) SetBudget(_ context.Context, campaignID string, amountMicros int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	counter, ok := t.budgets[campaignID]
	if !ok {
		counter = &atomic.Int64{}
		t.budgets[campaignID] = counter
	}
	counter.Store(amountMicros)
	return nil
}

func (t *InMemoryAtomicTracker) Remaining(_ context.Context, campaignID string) (int64, error) {
	t.mu.RLock()
	counter, ok := t.budgets[campaignID]
	t.mu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("budget: InMemoryAtomicTracker: unknown campaign %q", campaignID)
	}
	return counter.Load(), nil
}

// TrySpend performs a lock-free compare-and-swap loop: the atomic
// equivalent of "SELECT balance ...; UPDATE ... SET balance = new WHERE
// balance = <value we just read>" -- if the row changed underneath us, we
// re-read and try again instead of blindly overwriting.
func (t *InMemoryAtomicTracker) TrySpend(_ context.Context, campaignID string, amountMicros int64) (bool, int64, error) {
	t.mu.RLock()
	counter, ok := t.budgets[campaignID]
	t.mu.RUnlock()
	if !ok {
		return false, 0, fmt.Errorf("budget: InMemoryAtomicTracker: unknown campaign %q", campaignID)
	}

	for {
		current := counter.Load()
		if current < amountMicros {
			return false, current, nil
		}
		newBalance := current - amountMicros
		if counter.CompareAndSwap(current, newBalance) {
			// Nobody else modified the counter between our Load and this
			// CompareAndSwap, so this spend is safely accounted for.
			return true, newBalance, nil
		}
		// Someone else won the race; `current` is now stale. Loop and
		// retry against the latest value. This retry loop is the entire
		// fix: it turns three separate critical sections into one atomic
		// read-modify-write with automatic retry on contention.
	}
}
