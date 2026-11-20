# ADR 0004: Budget seeding across replicas

- **Status:** Accepted
- **Date:** 2026-11-19
- **Context:** Multiple API pods share one Redis budget key per campaign.
  What should happen when a pod starts and tries to seed that key?

## Context

`RedisAtomicTracker.SetBudget` writes the full budget with a 26h TTL. That is
correct for a single process or an explicit daily reset: the process owns
the value it just wrote. It is wrong at startup for a Deployment with more
than one replica:

1. Pod A starts and writes `budget:camp-1 = 500000000`.
2. Traffic spends 100000000, leaving 400000000.
3. A rolling update starts pod B. If B calls `SetBudget`, it resets the key
   to 500000000 and the campaign can spend its budget twice.

The race is not in the spend path. The Lua script is still atomic. The bug
is that a second process overwrites the shared starting value.

## Decision

Add `SeedIfAbsent` to `RedisAtomicTracker` using `SET NX` with the same 26h
TTL, and select it with `BUDGET_SEED`:

- `BUDGET_SEED=reset` (binary default): call `SetBudget`, preserving the
  original single-replica behavior. Existing local commands are unchanged.
- `BUDGET_SEED=init` (chart default): call `SeedIfAbsent`. The first pod
  creates the key; every later pod is a no-op. Spend already applied by
  peers is never overwritten.

`SET NX` is a single Redis command, so the check-and-create is atomic across
pods. A pod that loses the race simply leaves the existing value alone.

The chart sets `BUDGET_BACKEND=redis`, so all replicas use the same Lua
spend script against the same key. The in-memory backend remains available
for single-replica local runs and is still the binary default; it is not
replica-safe and the chart does not use it.

An explicit reset is still possible with
`BUDGET_SEED=reset` or by deleting the `budget:*` keys. The load-test runner
does the latter before each run so the invariant check starts from a known
seed.

## Consequences

- A scale-up or rolling update cannot reset spend.
- A first deploy creates the keys; a campaign added later is seeded on the
  next pod start (or by a reset job), because no pod watches for new
  campaigns at runtime. This matches the current static campaign store.
- The 26h TTL still exists. If no pod starts for longer than that, the keys
  expire and spending fails until a restart or a reset job. A scheduled
  daily reset is the production fix; it is out of scope here.
- `BUDGET_SEED` is read once at startup. Changing it requires a rollout,
  which is the intended operational boundary.
- `scripts/k8s-loadtest.sh` proves the outcome: after traffic, every
  `budget:*` value must be in `[0, seed]`, with at least one campaign spent.
