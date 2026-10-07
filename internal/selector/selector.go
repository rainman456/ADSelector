package selector

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/michaelstone897/adselector/internal/budget"
	"github.com/michaelstone897/adselector/internal/campaign"
	"github.com/michaelstone897/adselector/internal/events"
	"github.com/michaelstone897/adselector/internal/freqcap"
	"github.com/michaelstone897/adselector/internal/metrics"
)

// Request is the input to Select: the impression request signals needed to
// find and pay for a winning ad candidate.
type Request struct {
	UserID   string
	AdSlotID string
	Geo      string
	Device   string
}

// Decision is the outcome of Select.
type Decision struct {
	Filled         bool
	CampaignID     string
	CreativeID     string
	BidPriceMicros int64
	// Reason explains a no-fill outcome (e.g. "no_targeting_match",
	// "budget_and_freqcap_exhausted"). Empty when Filled is true.
	Reason string
}

// Selector ties together targeting, budget enforcement, frequency capping,
// and event publishing to answer one impression request. It depends only
// on interfaces (campaign.Store, budget.BudgetEnforcer,
// freqcap.FrequencyCapper, events.Publisher) so every piece can be swapped
// or mocked independently -- e.g. swap InMemoryAtomicTracker for
// RedisAtomicTracker without touching this file at all.
type Selector struct {
	Store   campaign.Store
	Budget  budget.BudgetEnforcer
	FreqCap freqcap.FrequencyCapper
	Events  events.Publisher
	Metrics *metrics.Metrics

	// MaxFreq is the default per-day frequency cap used when a campaign
	// doesn't set its own FrequencyCapPerDay.
	MaxFreq int
}

// New builds a Selector from its dependencies.
func New(store campaign.Store, enforcer budget.BudgetEnforcer, capper freqcap.FrequencyCapper, publisher events.Publisher, m *metrics.Metrics) *Selector {
	return &Selector{
		Store:   store,
		Budget:  enforcer,
		FreqCap: capper,
		Events:  publisher,
		Metrics: m,
		MaxFreq: 10,
	}
}

// Select runs the full decisioning pipeline:
//  1. targeting filter (campaign.Store.SelectCandidates)
//  2. rank eligible candidates by bid price, highest first
//  3. walk the ranked list, skipping any candidate whose frequency cap is
//     already hit for this user, or whose budget enforcer rejects the spend
//  4. on the first candidate that clears both checks, publish an
//     "impression_served" event (fire-and-forget) and return it as the
//     winner
//
// If no candidate clears both checks, Select returns a no-fill Decision.
func (s *Selector) Select(ctx context.Context, req Request) (Decision, error) {
	start := time.Now()
	decision, err := s.selectOnce(ctx, req)

	outcome := "filled"
	switch {
	case err != nil:
		outcome = "error"
	case !decision.Filled:
		outcome = "no_fill"
	}
	if s.Metrics != nil {
		s.Metrics.RequestDuration.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
	}
	return decision, err
}

func (s *Selector) selectOnce(ctx context.Context, req Request) (Decision, error) {
	candidates, err := s.Store.SelectCandidates(ctx, campaign.TargetingRequest{
		AdSlotID: req.AdSlotID,
		Geo:      req.Geo,
		Device:   req.Device,
	})
	if err != nil {
		return Decision{}, fmt.Errorf("selector: SelectCandidates: %w", err)
	}
	if len(candidates) == 0 {
		if s.Metrics != nil {
			s.Metrics.NoFillTotal.Inc()
		}
		return Decision{Filled: false, Reason: "no_targeting_match"}, nil
	}

	// Rank by bid price, highest first -- this is the entire "ranking
	// model" for v1. A real system would layer predicted CTR / eCPM here;
	// building that is explicitly out of scope for this project.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].BidPriceMicros > candidates[j].BidPriceMicros
	})

	for _, c := range candidates {
		maxFreq := c.FrequencyCapPerDay
		if maxFreq <= 0 {
			maxFreq = s.MaxFreq
		}

		allowed, err := s.FreqCap.Allow(ctx, req.UserID, c.ID, maxFreq)
		if err != nil {
			return Decision{}, fmt.Errorf("selector: FreqCap.Allow(%s): %w", c.ID, err)
		}
		if !allowed {
			continue
		}

		ok, _, err := s.Budget.TrySpend(ctx, c.ID, c.BidPriceMicros)
		if err != nil {
			return Decision{}, fmt.Errorf("selector: Budget.TrySpend(%s): %w", c.ID, err)
		}
		if !ok {
			if s.Metrics != nil {
				s.Metrics.BudgetRejectedTotal.Inc()
			}
			// NOTE: we've already consumed one frequency-cap slot for this
			// user+campaign via FreqCap.Allow above, even though the spend
			// itself was rejected. This is a deliberate v1 simplification
			// (see docs/architecture.md) -- a stricter implementation would
			// only charge the frequency cap once budget is confirmed, which
			// needs a two-phase reserve/commit protocol across both Redis
			// keys. Called out explicitly here rather than hidden.
			continue
		}

		decision := Decision{
			Filled:         true,
			CampaignID:     c.ID,
			CreativeID:     c.CreativeID,
			BidPriceMicros: c.BidPriceMicros,
		}

		if s.Events != nil {
			evt := events.ImpressionServed{
				EventID:        fmt.Sprintf("%s-%d", c.ID, time.Now().UnixNano()),
				ServedAt:       time.Now().UTC(),
				UserID:         req.UserID,
				AdSlotID:       req.AdSlotID,
				CampaignID:     c.ID,
				CreativeID:     c.CreativeID,
				BidPriceMicros: c.BidPriceMicros,
				Geo:            req.Geo,
				Device:         req.Device,
			}
			// Fire-and-forget: publish in the background so a slow/down
			// Kafka broker can never add latency to the response path.
			go func() {
				publishCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if pubErr := s.Events.PublishImpressionServed(publishCtx, evt); pubErr != nil {
					// In production this would go to structured logs plus a
					// publish-failure metric; kept minimal here since event
					// publishing is explicitly not the focus of this
					// project (see internal/events doc comment).
					_ = pubErr
				}
			}()
		}

		return decision, nil
	}

	if s.Metrics != nil {
		s.Metrics.NoFillTotal.Inc()
	}
	return Decision{Filled: false, Reason: "budget_and_freqcap_exhausted"}, nil
}
