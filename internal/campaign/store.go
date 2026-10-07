package campaign

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// TargetingRequest carries the signals used to filter eligible campaigns.
type TargetingRequest struct {
	AdSlotID string
	Geo      string
	Device   string
}

// Store is the read-side interface for campaign targeting data.
//
// The v1 implementation below (InMemoryStore) is a plain map guarded by a
// mutex. It is defined behind this interface specifically so a
// Postgres-backed (or Redis-backed) implementation could be swapped in
// later without touching callers -- the selector package, and every
// handler above it, only ever depends on this interface, never on
// InMemoryStore directly.
type Store interface {
	// SelectCandidates returns every campaign whose targeting rules match
	// the given request, in no particular ranking order. Budget and
	// frequency-cap filtering is NOT done here -- see the budget and
	// freqcap packages.
	SelectCandidates(ctx context.Context, req TargetingRequest) ([]Campaign, error)

	// Get returns a single campaign by ID.
	Get(ctx context.Context, id string) (Campaign, error)

	// All returns every campaign in the store, sorted by ID. Mostly useful
	// for startup seeding of the budget enforcer and for admin/debug tooling.
	All(ctx context.Context) ([]Campaign, error)

	// Upsert inserts or replaces a campaign definition.
	Upsert(ctx context.Context, c Campaign) error
}

// InMemoryStore is a concurrency-safe, map-backed Store implementation.
type InMemoryStore struct {
	mu        sync.RWMutex
	campaigns map[string]Campaign
}

// NewInMemoryStore builds an empty in-memory campaign store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		campaigns: make(map[string]Campaign),
	}
}

var _ Store = (*InMemoryStore)(nil)

func (s *InMemoryStore) Upsert(_ context.Context, c Campaign) error {
	if c.ID == "" {
		return fmt.Errorf("campaign: Upsert: campaign ID must not be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.campaigns[c.ID] = c
	return nil
}

func (s *InMemoryStore) Get(_ context.Context, id string) (Campaign, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.campaigns[id]
	if !ok {
		return Campaign{}, fmt.Errorf("campaign: Get: campaign %q not found", id)
	}
	return c, nil
}

func (s *InMemoryStore) All(_ context.Context) ([]Campaign, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Campaign, 0, len(s.campaigns))
	for _, c := range s.campaigns {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *InMemoryStore) SelectCandidates(_ context.Context, req TargetingRequest) ([]Campaign, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	candidates := make([]Campaign, 0, len(s.campaigns))
	for _, c := range s.campaigns {
		if c.Matches(req.Geo, req.Device) {
			candidates = append(candidates, c)
		}
	}
	// Deterministic ordering makes demo/test output reproducible; actual
	// ranking by bid happens in the selector package.
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })
	return candidates, nil
}
