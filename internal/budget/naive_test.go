package budget

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// TestNaiveTracker_DoubleSpendsUnderConcurrency demonstrates the exact bug
// described in docs/adr/0001-naive-vs-atomic-budget-enforcement.md: with a
// budget sized for exactly half of numGoroutines spenders, launching
// numGoroutines concurrent goroutines against NaiveTracker results in MORE
// than half of them succeeding (or a negative final balance), because of
// the check-then-act race between the balance read and the balance write.
//
// This test has NOT been run (see repo README -- no test commands have
// been executed while scaffolding this project), but it is written to be
// run as-is with `go test ./internal/budget/...` once the module builds.
func TestNaiveTracker_DoubleSpendsUnderConcurrency(t *testing.T) {
	const (
		campaignID    = "camp-race"
		unitMicros    = int64(1_000_000) // $1.00 per simulated spend
		numGoroutines = 20
	)
	// Budget covers exactly half the goroutines. If TrySpend were correct,
	// exactly numGoroutines/2 calls would succeed and the rest would be
	// rejected.
	wantMaxSuccesses := int64(numGoroutines / 2)
	budgetAmount := unitMicros * wantMaxSuccesses

	tracker := NewNaiveTracker()
	ctx := context.Background()
	if err := tracker.SetBudget(ctx, campaignID, budgetAmount); err != nil {
		t.Fatalf("SetBudget: %v", err)
	}

	var (
		wg           sync.WaitGroup
		successCount atomic.Int64
	)
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			ok, _, err := tracker.TrySpend(ctx, campaignID, unitMicros)
			if err != nil {
				t.Errorf("TrySpend: %v", err)
				return
			}
			if ok {
				successCount.Add(1)
			}
		}()
	}
	wg.Wait()

	finalRemaining, err := tracker.Remaining(ctx, campaignID)
	if err != nil {
		t.Fatalf("Remaining: %v", err)
	}

	t.Logf("naive tracker: %d/%d spends succeeded (budget only covers %d); final remaining = %d",
		successCount.Load(), numGoroutines, wantMaxSuccesses, finalRemaining)

	// The whole point of this test: NaiveTracker is EXPECTED to misbehave
	// under concurrency. We assert on the observable symptom of the
	// double-spend -- either more spends succeeded than the budget allowed,
	// or the final balance went negative (both indicate the same
	// underlying race). Goroutine scheduling is inherently non-deterministic,
	// so on a very lucky run this could pass without reproducing the race;
	// re-run with `-count=10` (or lower RaceWindow) if that happens.
	if successCount.Load() <= wantMaxSuccesses && finalRemaining >= 0 {
		t.Skipf("did not observe a double-spend this run (goroutine scheduling is inherently "+
			"non-deterministic); got %d successes (budget for %d) and remaining=%d -- re-run, or use "+
			"`go test -race -count=10 ./internal/budget/...` to increase the chance of hitting the race window",
			successCount.Load(), wantMaxSuccesses, finalRemaining)
	}
}

// TestInMemoryAtomicTracker_NeverDoubleSpends is the "after" half of the
// demo: the exact same concurrent workload against the CAS-loop based
// tracker must always allow precisely budget/unit spends, never more, and
// the final balance must never go negative.
func TestInMemoryAtomicTracker_NeverDoubleSpends(t *testing.T) {
	const (
		campaignID    = "camp-fixed"
		unitMicros    = int64(1_000_000)
		numGoroutines = 20
	)
	wantSuccesses := int64(numGoroutines / 2)
	budgetAmount := unitMicros * wantSuccesses

	tracker := NewInMemoryAtomicTracker()
	ctx := context.Background()
	if err := tracker.SetBudget(ctx, campaignID, budgetAmount); err != nil {
		t.Fatalf("SetBudget: %v", err)
	}

	var (
		wg           sync.WaitGroup
		successCount atomic.Int64
	)
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			ok, _, err := tracker.TrySpend(ctx, campaignID, unitMicros)
			if err != nil {
				t.Errorf("TrySpend: %v", err)
				return
			}
			if ok {
				successCount.Add(1)
			}
		}()
	}
	wg.Wait()

	finalRemaining, err := tracker.Remaining(ctx, campaignID)
	if err != nil {
		t.Fatalf("Remaining: %v", err)
	}

	if got := successCount.Load(); got != wantSuccesses {
		t.Errorf("atomic tracker: got %d successful spends, want exactly %d (budget=%d, unit=%d)",
			got, wantSuccesses, budgetAmount, unitMicros)
	}
	if finalRemaining != 0 {
		t.Errorf("atomic tracker: final remaining = %d, want exactly 0 (no double-spend, no left-over)", finalRemaining)
	}
	if finalRemaining < 0 {
		t.Errorf("atomic tracker: final remaining went NEGATIVE (%d) -- this must never happen", finalRemaining)
	}
}
