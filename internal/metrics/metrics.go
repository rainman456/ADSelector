package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics bundles the Prometheus collectors AdSelector exposes on
// /metrics. Constructed once at startup and threaded through the selector.
type Metrics struct {
	// RequestDuration measures end-to-end selection latency, labeled by
	// outcome so p50/p99 can be sliced by "filled" vs "no_fill" vs "error".
	// This is the number the load-test harness (cmd/loadtest) is meant to
	// corroborate from the client side.
	RequestDuration *prometheus.HistogramVec

	// BudgetRejectedTotal counts how many times a candidate campaign was
	// rejected by the BudgetEnforcer (i.e. the campaign ran out of budget,
	// possibly due to a concurrent request winning the race first),
	// forcing the selector to fall through to the next candidate or to
	// no-fill.
	BudgetRejectedTotal prometheus.Counter

	// NoFillTotal counts requests where no eligible campaign had both
	// matching targeting and remaining budget/frequency-cap headroom.
	NoFillTotal prometheus.Counter
}

// New registers and returns AdSelector's metrics against reg. Pass nil to
// use prometheus.DefaultRegisterer (the normal case in main.go); pass an
// isolated *prometheus.Registry in tests that construct multiple Metrics
// instances in the same process.
func New(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	factory := promauto.With(reg)
	return &Metrics{
		RequestDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "adselector",
			Subsystem: "http",
			Name:      "select_request_duration_seconds",
			Help:      "Latency of POST /v1/select, labeled by outcome.",
			Buckets:   prometheus.DefBuckets, // 5ms..10s; retune once real latency data exists
		}, []string{"outcome"}),

		BudgetRejectedTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "adselector",
			Subsystem: "budget",
			Name:      "rejected_total",
			Help:      "Number of times a candidate campaign was skipped because TrySpend rejected it (budget exhausted).",
		}),

		NoFillTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "adselector",
			Subsystem: "select",
			Name:      "no_fill_total",
			Help:      "Number of /v1/select requests that returned no eligible candidate.",
		}),
	}
}

// Handler returns the HTTP handler to mount at /metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}
