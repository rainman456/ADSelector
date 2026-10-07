package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/michaelstone897/adselector/internal/metrics"
)

// NewRouter builds the top-level chi router for the AdSelector API.
func NewRouter(h *Handler) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(2 * time.Second))

	r.Get("/healthz", h.HandleHealthz)
	r.Post("/v1/select", h.HandleSelect)
	r.Handle("/metrics", metrics.Handler())

	return r
}
