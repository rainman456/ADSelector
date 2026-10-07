// Command server runs the AdSelector HTTP API.
//
// NOTE: this binary has not been built or run as part of scaffolding this
// project (see repo README's constraints -- no build/run/test command has
// been executed). It is written to compile and start cleanly once
// `go mod tidy` has been run and Redis/Kafka are reachable (e.g. via
// `docker compose up`), but none of that has been verified yet.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/michaelstone897/adselector/internal/api"
	"github.com/michaelstone897/adselector/internal/budget"
	"github.com/michaelstone897/adselector/internal/campaign"
	"github.com/michaelstone897/adselector/internal/events"
	"github.com/michaelstone897/adselector/internal/freqcap"
	"github.com/michaelstone897/adselector/internal/metrics"
	"github.com/michaelstone897/adselector/internal/selector"
)

type config struct {
	ListenAddr string
	RedisAddr  string

	KafkaBrokers []string
	KafkaTopic   string

	// BudgetBackend selects which budget.BudgetEnforcer implementation
	// serves real traffic: "memory" (default, InMemoryAtomicTracker) or
	// "redis" (RedisAtomicTracker). budget.NaiveTracker is NEVER selectable
	// here on purpose -- it only exists for the test/demo described in
	// docs/adr/0001-naive-vs-atomic-budget-enforcement.md.
	BudgetBackend string

	// FreqCapBackend selects the freqcap.FrequencyCapper implementation:
	// "redis" (default) or "noop" (for local dev without Redis running).
	FreqCapBackend string
}

func loadConfigFromEnv() config {
	return config{
		ListenAddr:     getEnv("LISTEN_ADDR", ":8080"),
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:   []string{getEnv("KAFKA_BROKER", "localhost:9092")},
		KafkaTopic:     getEnv("KAFKA_TOPIC", "impression_served"),
		BudgetBackend:  getEnv("BUDGET_BACKEND", "memory"),
		FreqCapBackend: getEnv("FREQCAP_BACKEND", "redis"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := loadConfigFromEnv()
	ctx := context.Background()

	// --- Campaign store (in-memory for v1; see internal/campaign.Store's
	// doc comment for how a Postgres-backed implementation would slot in
	// behind the same interface later).
	store := campaign.NewInMemoryStore()
	seedCampaigns(store)
	allCampaigns, err := store.All(ctx)
	if err != nil {
		logger.Error("failed to list seeded campaigns", "err", err)
		os.Exit(1)
	}

	// Lazily construct a shared Redis client only if something needs it,
	// so pure in-memory + noop-freqcap local runs don't require Redis at
	// all.
	var rdb *redis.Client
	needsRedis := cfg.BudgetBackend == "redis" || cfg.FreqCapBackend == "redis"
	if needsRedis {
		rdb = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	}

	// --- Budget enforcement. InMemoryAtomicTracker is the default and the
	// recommended option for a single-replica deployment; RedisAtomicTracker
	// generalizes the same correctness guarantee to N replicas. Either way,
	// budget.NaiveTracker is NEVER wired up here -- see its doc comment.
	var budgetEnforcer budget.BudgetEnforcer
	switch cfg.BudgetBackend {
	case "redis":
		tracker := budget.NewRedisAtomicTracker(rdb, "budget")
		for _, c := range allCampaigns {
			if err := tracker.SetBudget(ctx, c.ID, c.DailyBudgetMicros); err != nil {
				logger.Error("failed to seed redis budget", "campaign_id", c.ID, "err", err)
			}
		}
		budgetEnforcer = tracker
	default:
		tracker := budget.NewInMemoryAtomicTracker()
		for _, c := range allCampaigns {
			if err := tracker.SetBudget(ctx, c.ID, c.DailyBudgetMicros); err != nil {
				logger.Error("failed to seed in-memory budget", "campaign_id", c.ID, "err", err)
			}
		}
		budgetEnforcer = tracker
	}

	// --- Frequency capping.
	var capper freqcap.FrequencyCapper
	switch cfg.FreqCapBackend {
	case "noop":
		capper = freqcap.NoopCapper{}
	default:
		if rdb == nil {
			rdb = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
		}
		capper = freqcap.NewRedisFrequencyCapper(rdb, "freqcap")
	}

	// --- Event publishing (fire-and-forget; see internal/events doc comment).
	publisher := events.NewKafkaPublisher(events.KafkaConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
	}, logger)
	defer publisher.Close()

	// --- Metrics.
	m := metrics.New(nil) // nil -> prometheus.DefaultRegisterer

	sel := selector.New(store, budgetEnforcer, capper, publisher, m)
	handler := api.NewHandler(sel, logger)
	router := api.NewRouter(handler)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("adselector listening",
			"addr", cfg.ListenAddr, "budget_backend", cfg.BudgetBackend, "freqcap_backend", cfg.FreqCapBackend)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
	}
}

// seedCampaigns populates the in-memory store with a handful of sample
// campaigns so the API is demoable without a separate admin/ingestion path
// (which is out of scope for v1).
func seedCampaigns(store *campaign.InMemoryStore) {
	ctx := context.Background()
	now := time.Now().UTC()

	samples := []campaign.Campaign{
		{
			ID:                 "camp-us-mobile-1",
			Advertiser:         "Acme Shoes",
			CreativeID:         "cr-acme-shoes-300x250",
			Targeting:          campaign.Targeting{Geos: []string{"US", "CA"}, Devices: []string{"mobile"}},
			BidPriceMicros:     2_500_000, // $2.50 per win
			DailyBudgetMicros:  500_000_000,
			FrequencyCapPerDay: 3,
			CreatedAt:          now,
		},
		{
			ID:                 "camp-global-desktop-1",
			Advertiser:         "Globex Travel",
			CreativeID:         "cr-globex-travel-728x90",
			Targeting:          campaign.Targeting{Devices: []string{"desktop"}},
			BidPriceMicros:     1_800_000,
			DailyBudgetMicros:  1_000_000_000,
			FrequencyCapPerDay: 5,
			CreatedAt:          now,
		},
		{
			ID:                 "camp-eu-any-device-1",
			Advertiser:         "Initech Insurance",
			CreativeID:         "cr-initech-insurance-320x50",
			Targeting:          campaign.Targeting{Geos: []string{"DE", "FR", "NL"}},
			BidPriceMicros:     3_200_000,
			DailyBudgetMicros:  250_000_000,
			FrequencyCapPerDay: 2,
			CreatedAt:          now,
		},
		{
			ID:                 "camp-catch-all-low-bid",
			Advertiser:         "Umbrella Corp",
			CreativeID:         "cr-umbrella-house-ad",
			Targeting:          campaign.Targeting{}, // no restrictions -- always eligible
			BidPriceMicros:     100_000,
			DailyBudgetMicros:  2_000_000_000,
			FrequencyCapPerDay: 20,
			CreatedAt:          now,
		},
	}

	for _, c := range samples {
		_ = store.Upsert(ctx, c)
	}
}
