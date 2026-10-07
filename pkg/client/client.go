// Package client is a minimal, dependency-free Go SDK for AdSelector's
// HTTP API. It lives under pkg/ (rather than internal/) because it is
// meant to be importable by other services -- and by this repo's own
// cmd/loadtest harness -- without pulling in adselector's internal
// wiring (chi, redis, kafka-go, etc).
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// SelectRequest mirrors internal/api.SelectRequest. It is redefined here,
// rather than imported, because Go's internal/ visibility rules mean no
// external module could ever import internal/api anyway -- this package IS
// the public contract external callers should depend on.
type SelectRequest struct {
	UserID   string `json:"user_id"`
	AdSlotID string `json:"ad_slot_id"`
	Geo      string `json:"geo"`
	Device   string `json:"device"`
}

// SelectResponse mirrors internal/api.SelectResponse.
type SelectResponse struct {
	Filled         bool   `json:"filled"`
	CampaignID     string `json:"campaign_id,omitempty"`
	CreativeID     string `json:"creative_id,omitempty"`
	BidPriceMicros int64  `json:"bid_price_micros,omitempty"`
	Reason         string `json:"reason,omitempty"`
}

// Client is a minimal SDK for AdSelector's HTTP API.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

// New builds a Client with a sane default timeout. Override
// c.HTTPClient.Timeout (or swap HTTPClient entirely) for load-testing
// scenarios that want a different deadline than typical caller code.
func New(baseURL string) *Client {
	return &Client{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: 2 * time.Second},
	}
}

// Select calls POST /v1/select and decodes the response.
func (c *Client) Select(ctx context.Context, req SelectRequest) (SelectResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return SelectResponse{}, fmt.Errorf("client: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/select", bytes.NewReader(body))
	if err != nil {
		return SelectResponse{}, fmt.Errorf("client: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return SelectResponse{}, fmt.Errorf("client: do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return SelectResponse{}, fmt.Errorf("client: unexpected status %d", resp.StatusCode)
	}

	var out SelectResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return SelectResponse{}, fmt.Errorf("client: decode response: %w", err)
	}
	return out, nil
}
