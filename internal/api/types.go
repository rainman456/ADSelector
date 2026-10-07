package api

// SelectRequest is the JSON body for POST /v1/select.
type SelectRequest struct {
	UserID   string `json:"user_id"`
	AdSlotID string `json:"ad_slot_id"`
	Geo      string `json:"geo"`
	Device   string `json:"device"`
}

// SelectResponse is the JSON body returned by POST /v1/select.
type SelectResponse struct {
	Filled         bool   `json:"filled"`
	CampaignID     string `json:"campaign_id,omitempty"`
	CreativeID     string `json:"creative_id,omitempty"`
	BidPriceMicros int64  `json:"bid_price_micros,omitempty"`
	Reason         string `json:"reason,omitempty"`
}

// ErrorResponse is the JSON body returned for 4xx/5xx responses.
type ErrorResponse struct {
	Error string `json:"error"`
}
