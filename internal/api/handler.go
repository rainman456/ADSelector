package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/michaelstone897/adselector/internal/selector"
)

// Handler holds the dependencies needed to serve the AdSelector HTTP API.
type Handler struct {
	Selector *selector.Selector
	Logger   *slog.Logger
}

// NewHandler builds a Handler.
func NewHandler(sel *selector.Selector, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{Selector: sel, Logger: logger}
}

// HandleSelect implements POST /v1/select: takes an impression request
// (user + context signals + ad slot) and returns a selected ad candidate,
// or a "no fill" response when no eligible campaign has remaining
// budget/frequency-cap headroom.
func (h *Handler) HandleSelect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req SelectRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if err := validateSelectRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	decision, err := h.Selector.Select(r.Context(), selector.Request{
		UserID:   req.UserID,
		AdSlotID: req.AdSlotID,
		Geo:      req.Geo,
		Device:   req.Device,
	})
	if err != nil {
		h.Logger.Error("select failed", "err", err, "user_id", req.UserID, "ad_slot_id", req.AdSlotID)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, SelectResponse{
		Filled:         decision.Filled,
		CampaignID:     decision.CampaignID,
		CreativeID:     decision.CreativeID,
		BidPriceMicros: decision.BidPriceMicros,
		Reason:         decision.Reason,
	})
}

// HandleHealthz implements GET /healthz, a trivial liveness probe.
func (h *Handler) HandleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func validateSelectRequest(req SelectRequest) error {
	if req.UserID == "" {
		return errors.New("user_id is required")
	}
	if req.AdSlotID == "" {
		return errors.New("ad_slot_id is required")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}
