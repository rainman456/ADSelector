package budget

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// spendScript is the Redis-side equivalent of InMemoryAtomicTracker's CAS
// loop. Redis executes an entire Lua script as a single atomic operation
// (no other command from any other client can interleave with it), so the
// read-check-decrement sequence below is race-free across every AdSelector
// replica talking to this Redis instance -- exactly the same guarantee the
// in-memory CAS loop gives within one process, generalized to N stateless
// replicas.
//
// KEYS[1] = budget key, e.g. "budget:<campaignID>"
// ARGV[1] = amount to spend, in micros
//
// Returns:
//
//	-1                    the key doesn't exist (no budget set for this campaign/day)
//	-2                    remaining < amount (insufficient budget; rejected)
//	<new remaining value> on success (always >= 0)
const spendScript = `
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
`

// RedisAtomicTracker implements BudgetEnforcer against a shared Redis
// instance using the Lua script above, generalizing the in-memory
// CAS-loop fix to N stateless AdSelector replicas.
//
// It depends on redis.Cmdable (the interface satisfied by both
// *redis.Client and *redis.ClusterClient) rather than a concrete client
// type, so it's easy to unit test against a fake/mock and to swap a single
// node for a cluster later without changing this code.
type RedisAtomicTracker struct {
	rdb       redis.Cmdable
	keyPrefix string
	// TTL applied by SetBudget so a stalled daily-reset job can't leave a
	// stale counter alive forever. Defaults to 26h (see
	// NewRedisAtomicTracker) -- a little more than a day, so a healthy
	// daily reset always wins the race against expiry.
	ttl time.Duration
}

// NewRedisAtomicTracker builds a Redis-backed BudgetEnforcer. keyPrefix is
// typically "budget", producing keys like "budget:<campaignID>"; callers
// are expected to use day-scoped campaign IDs consistently (or run a daily
// reset job) if budgets should reset every 24h.
func NewRedisAtomicTracker(rdb redis.Cmdable, keyPrefix string) *RedisAtomicTracker {
	if keyPrefix == "" {
		keyPrefix = "budget"
	}
	return &RedisAtomicTracker{rdb: rdb, keyPrefix: keyPrefix, ttl: 26 * time.Hour}
}

var _ BudgetEnforcer = (*RedisAtomicTracker)(nil)

func (t *RedisAtomicTracker) key(campaignID string) string {
	return fmt.Sprintf("%s:%s", t.keyPrefix, campaignID)
}

// SetBudget seeds (or resets) the remaining budget for campaignID. In
// production this would typically be invoked by a daily cron/job at
// midnight in the campaign's billing timezone.
func (t *RedisAtomicTracker) SetBudget(ctx context.Context, campaignID string, amountMicros int64) error {
	return t.rdb.Set(ctx, t.key(campaignID), amountMicros, t.ttl).Err()
}

func (t *RedisAtomicTracker) Remaining(ctx context.Context, campaignID string) (int64, error) {
	v, err := t.rdb.Get(ctx, t.key(campaignID)).Int64()
	if err == redis.Nil {
		return 0, fmt.Errorf("budget: RedisAtomicTracker: unknown campaign %q", campaignID)
	}
	if err != nil {
		return 0, fmt.Errorf("budget: RedisAtomicTracker: GET %s: %w", t.key(campaignID), err)
	}
	return v, nil
}

func (t *RedisAtomicTracker) TrySpend(ctx context.Context, campaignID string, amountMicros int64) (bool, int64, error) {
	res, err := t.rdb.Eval(ctx, spendScript, []string{t.key(campaignID)}, amountMicros).Result()
	if err != nil {
		return false, 0, fmt.Errorf("budget: RedisAtomicTracker: EVAL spend script for %s: %w", campaignID, err)
	}

	newRemaining, ok := res.(int64)
	if !ok {
		return false, 0, fmt.Errorf("budget: RedisAtomicTracker: unexpected script result type %T", res)
	}

	switch {
	case newRemaining == -1:
		return false, 0, fmt.Errorf("budget: RedisAtomicTracker: unknown campaign %q (no budget set)", campaignID)
	case newRemaining == -2:
		return false, 0, nil // insufficient budget, correctly rejected -- not an error
	default:
		return true, newRemaining, nil
	}
}
