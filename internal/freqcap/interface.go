package freqcap

import "context"

// FrequencyCapper enforces "show this user at most N impressions of this
// campaign per day". Like budget.BudgetEnforcer, Allow must be atomic: two
// concurrent requests for the same user+campaign must not both be allowed
// through when only one slot remains under the cap.
type FrequencyCapper interface {
	// Allow atomically checks the user's current impression count for
	// campaignID against maxPerDay and, if under the cap, increments the
	// count. It returns allowed=true iff the increment happened (i.e. this
	// call itself consumed one of the user's allowed impressions for today).
	Allow(ctx context.Context, userID, campaignID string, maxPerDay int) (allowed bool, err error)
}
