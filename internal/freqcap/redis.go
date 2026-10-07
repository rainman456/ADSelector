package freqcap

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// incrAndCapScript atomically increments today's impression counter for a
// (user, campaign) pair and enforces maxPerDay in a single round trip, so a
// burst of concurrent requests for the same user can't all read
// "count=2, cap=3" and all decide they're allowed through.
//
// KEYS[1] = frequency cap key, e.g. "freqcap:<campaignID>:<userID>:<YYYYMMDD>"
// ARGV[1] = max allowed impressions per day
// ARGV[2] = key TTL in seconds
//
// Returns 1 if this call was allowed (and counted), 0 if the cap was
// already reached (in which case the increment is rolled back).
const incrAndCapScript = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
end
local maxPerDay = tonumber(ARGV[1])
if count > maxPerDay then
  redis.call('DECR', KEYS[1])
  return 0
end
return 1
`

// RedisFrequencyCapper implements FrequencyCapper on top of Redis using
// INCR + EXPIRE, wrapped in the Lua script above so the
// "increment, check, roll back if over" sequence is atomic.
type RedisFrequencyCapper struct {
	rdb       redis.Cmdable
	keyPrefix string
	// ttl is how long a per-day counter key lives. In production this
	// should ideally be "seconds remaining until midnight" in the relevant
	// timezone; a fixed ~25h window is a reasonable default for a
	// portfolio-scale demo (see NewRedisFrequencyCapper).
	ttl time.Duration
}

// NewRedisFrequencyCapper builds a Redis-backed FrequencyCapper. keyPrefix
// is typically "freqcap".
func NewRedisFrequencyCapper(rdb redis.Cmdable, keyPrefix string) *RedisFrequencyCapper {
	if keyPrefix == "" {
		keyPrefix = "freqcap"
	}
	return &RedisFrequencyCapper{
		rdb:       rdb,
		keyPrefix: keyPrefix,
		ttl:       25 * time.Hour,
	}
}

var _ FrequencyCapper = (*RedisFrequencyCapper)(nil)

func (c *RedisFrequencyCapper) key(userID, campaignID string) string {
	day := time.Now().UTC().Format("20060102")
	return fmt.Sprintf("%s:%s:%s:%s", c.keyPrefix, campaignID, userID, day)
}

func (c *RedisFrequencyCapper) Allow(ctx context.Context, userID, campaignID string, maxPerDay int) (bool, error) {
	key := c.key(userID, campaignID)
	res, err := c.rdb.Eval(ctx, incrAndCapScript, []string{key}, maxPerDay, int(c.ttl.Seconds())).Result()
	if err != nil {
		return false, fmt.Errorf("freqcap: RedisFrequencyCapper: EVAL for %s: %w", key, err)
	}
	allowed, ok := res.(int64)
	if !ok {
		return false, fmt.Errorf("freqcap: RedisFrequencyCapper: unexpected script result type %T", res)
	}
	return allowed == 1, nil
}
