package campaign

import "time"

// Targeting describes the audience-matching rules for a campaign. An empty
// slice for a dimension means "match everything" on that dimension.
type Targeting struct {
	Geos    []string // ISO country/region codes this campaign targets, e.g. "US", "DE".
	Devices []string // device classes this campaign targets, e.g. "mobile", "desktop", "ctv".
}

// Campaign is a single advertiser campaign eligible for selection.
//
// Note what is deliberately NOT here: there is no "remaining budget" field.
// Budget accounting is owned entirely by the budget package (see
// budget.BudgetEnforcer) so that the two concerns -- "which campaigns are
// even eligible" vs. "does this campaign still have money" -- can be
// reasoned about, tested, and scaled independently. DailyBudgetMicros here
// is just the campaign's configured cap, used to seed the enforcer.
type Campaign struct {
	ID         string
	Advertiser string
	CreativeID string
	Targeting  Targeting

	// BidPriceMicros is the campaign's bid price in micros of the account's
	// currency (1,000,000 micros == 1.00 currency unit). Integer micros
	// avoid floating point rounding issues in money math -- never use
	// float64 for currency in a real ad-serving system.
	BidPriceMicros int64

	// DailyBudgetMicros is the campaign's total daily spend cap, used to
	// seed a budget.BudgetEnforcer at startup / at the start of each day.
	DailyBudgetMicros int64

	// FrequencyCapPerDay limits how many times a single user may be shown
	// this campaign's creative within a rolling day. Zero means "use the
	// selector's default cap".
	FrequencyCapPerDay int

	CreatedAt time.Time
}

// Matches reports whether the campaign's targeting rules match the given
// geo/device signals.
func (c Campaign) Matches(geo, device string) bool {
	if !matchesDimension(c.Targeting.Geos, geo) {
		return false
	}
	if !matchesDimension(c.Targeting.Devices, device) {
		return false
	}
	return true
}

func matchesDimension(allowed []string, value string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, a := range allowed {
		if a == value {
			return true
		}
	}
	return false
}
