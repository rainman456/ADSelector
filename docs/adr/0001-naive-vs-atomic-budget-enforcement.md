# ADR 0001: Naive vs. Atomic Budget Enforcement

- **Status:** Accepted
- **Date:** 2026-09-27
- **Context:** AdSelector's core correctness requirement -- campaign budgets
  must never be double-spent under concurrent load.

## Context

AdSelector picks a winning ad candidate for every impression request and,
when it does, debits that campaign's remaining daily budget by the winning
bid price. Under production traffic, a single popular campaign can easily
receive hundreds of concurrent `/v1/select` requests within the same few
milliseconds -- and every one of those requests, if it wins, needs to debit
the *same* shared counter.

I wanted to build this the way I suspect a lot of budget-tracking code
actually gets written the first time, on purpose, so I could show the bug,
not just assert it exists. So `internal/budget` ships **two**
implementations of the same `BudgetEnforcer` interface: a naive one
(`naive.go`) that I know is broken, and an atomic one (`atomic.go` /
`redis.go`) that fixes it. This document is the walkthrough.

## The naive implementation

```go
// simplified from internal/budget/naive.go
func (t *NaiveTracker) TrySpend(ctx context.Context, campaignID string, amount int64) (bool, int64, error) {
    t.mu.Lock()
    current := t.remaining[campaignID]
    t.mu.Unlock()

    if current < amount {
        return false, current, nil // correctly rejected
    }

    // <-- the gap. Nothing prevents another goroutine from reading and
    //     acting on the *same* `current` value right here.

    t.mu.Lock()
    t.remaining[campaignID] -= amount
    newBalance := t.remaining[campaignID]
    t.mu.Unlock()

    return true, newBalance, nil
}
```

Note that this code isn't lock-free or naively unlocked -- it *does* take a
mutex, twice. That's exactly what makes it a good demo of the bug: it's the
kind of code that looks safe at a glance ("look, I'm using a mutex!") and
passes review from someone skimming for "is there a `sync.Mutex`
somewhere?". The mutex protects each individual map access, but it does
**not** protect the sequence of "read, decide, write" as a single unit. The
critical section that actually needs to be atomic -- "check the balance
covers this spend, and reserve it" -- is split across two separate lock
acquisitions with a decision made in between them, unprotected.

### Why this matters at scale

With a campaign budget of $10 and 20 concurrent requests each trying to
spend $1:

```
Request  1..10: read $10..$1 in sequence, roughly, each sees "enough", spends
Request 11..20: ALSO read a stale "enough" value before requests 1..10's
                writes land, because reads and writes are separate critical
                sections -- so they ALSO spend
```

In the worst case, all 20 requests could read "$10 remaining, that's
enough" before any of them writes back a debit, and all 20 succeed against
a $10 budget -- a 2x over-delivery. In a real system this is real money: an
advertiser billed for exactly $10 of spend receives $20 of ad delivery, the
inventory those extra impressions consumed can't be un-served, and (worse)
this kind of bug tends to show up *only* under peak traffic, i.e. exactly
when it's most expensive and least convenient to discover.

### Proving it: `internal/budget/naive_test.go`

`TestNaiveTracker_DoubleSpendsUnderConcurrency` seeds a budget sized for
exactly 10 of 20 concurrent `TrySpend` calls (`$1` each against a `$10`
budget), fires all 20 from goroutines via a `sync.WaitGroup`, and asserts
that *either* more than 10 succeeded *or* the final balance went negative
-- both are symptoms of the same race. A tiny artificial delay
(`NaiveTracker.RaceWindow`, default 1ms) sits between the read and the
write specifically so this is reliably reproducible in a fast unit test
run, rather than relying on getting unlucky with goroutine scheduling. Real
production traffic doesn't need that help -- it just fails less
predictably, which is worse, not better.

## The fix: make check-and-commit a single atomic step

The fix isn't "add more locking" -- it's collapsing the three separate
critical sections (read / decide / write) into exactly one, so no other
caller can observe or act on an intermediate state.

### In-process: a CAS loop (`internal/budget/atomic.go`)

```go
// simplified from internal/budget/atomic.go
func (t *InMemoryAtomicTracker) TrySpend(ctx context.Context, campaignID string, amount int64) (bool, int64, error) {
    counter := t.budgets[campaignID] // *atomic.Int64
    for {
        current := counter.Load()
        if current < amount {
            return false, current, nil
        }
        newBalance := current - amount
        if counter.CompareAndSwap(current, newBalance) {
            return true, newBalance, nil // nobody else touched it between Load and CompareAndSwap
        }
        // someone else won; retry against the fresh value
    }
}
```

`CompareAndSwap` is a single hardware-level atomic instruction: "set this
memory location to `newBalance`, but only if it still equals `current`."
If another goroutine spent from the same counter in between our `Load` and
our `CompareAndSwap`, the CAS fails, we loop, and we re-read the *actual*
current balance before deciding again. There is no window where a decision
can be made against data that's already stale.

`TestInMemoryAtomicTracker_NeverDoubleSpends` runs the exact same
20-goroutines-against-a-$10-budget workload and asserts *exactly* 10
successes and a final balance of *exactly* $0 -- never negative, never
more than the budget allowed.

### Distributed: the same idea via a Redis Lua script (`internal/budget/redis.go`)

A single process's CAS loop doesn't help once AdSelector runs as multiple
stateless replicas behind a load balancer -- each replica has its own
memory, so two replicas could each run a "correct" CAS loop against their
*own*, independently wrong, copy of the balance. The fix has to live in
whatever shared store all replicas talk to.

Redis gives us the same "read+check+write is atomic" property for free, at
the cost of a network round trip, via Lua scripting: Redis executes an
entire script to completion without interleaving any other client's
command in the middle of it.

```lua
-- internal/budget/redis.go: spendScript
local remaining = redis.call('GET', KEYS[1])
if remaining == false then
  return -1
end
remaining = tonumber(remaining)
local amount = tonumber(ARGV[1])
if remaining < amount then
  return -2
end
local newRemaining = remaining - amount
redis.call('SET', KEYS[1], newRemaining, 'KEEPTTL')
return newRemaining
```

Every AdSelector replica calls `EVAL` with the same script; Redis
serializes execution of all Lua scripts against a given key, so this is the
distributed equivalent of the CAS loop: whichever replica's `EVAL` runs
first sees the true current balance, decides, and commits, atomically, and
the next replica's `EVAL` sees the updated value. No distributed lock to
acquire, hold, or fail to release under a GC pause or network blip -- the
atomicity comes from Redis's own single-threaded script execution
guarantee.

## Decision

- `budget.NaiveTracker` stays in the codebase, clearly documented as
  intentionally broken, purely to make this bug and its fix demoable and
  testable. It must never be reachable from `cmd/server/main.go`.
- `budget.InMemoryAtomicTracker` (CAS-loop) is the default enforcer for a
  single-replica deployment.
- `budget.RedisAtomicTracker` (Lua script) is available behind the same
  `BudgetEnforcer` interface for a multi-replica deployment, selected via
  the `BUDGET_BACKEND=redis` environment variable.
- Both real implementations are proven, in the same test file, against the
  same concurrent workload that reliably breaks the naive one.

## Consequences

- Callers (`internal/selector`) depend only on the `BudgetEnforcer`
  interface and never need to know or care which implementation is active.
- Switching from single-replica to multi-replica deployment is a
  one-line config change (`BUDGET_BACKEND=redis`), not a rewrite.
- The Redis implementation adds a network round trip per spend attempt
  that the in-memory CAS loop doesn't have -- an explicit latency/
  correctness-at-scale trade-off, not a free upgrade. This is exactly the
  kind of trade-off worth surfacing in a system-design interview: "atomic
  in-process is nearly free; atomic across replicas costs a round trip, and
  that cost is the price of correctness once you can't rely on shared
  memory anymore."
