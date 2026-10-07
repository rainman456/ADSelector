package events

import "time"

// ImpressionServed is published (fire-and-forget) every time AdSelector
// successfully picks a winning candidate for an impression request. It is
// intentionally a thin, denormalized record -- building a real downstream
// analytics/processing pipeline on top of these events is explicitly out
// of scope for this project (that belongs to a separate portfolio project);
// this is just the publish side of the event.
type ImpressionServed struct {
	EventID        string    `json:"event_id"`
	ServedAt       time.Time `json:"served_at"`
	UserID         string    `json:"user_id"`
	AdSlotID       string    `json:"ad_slot_id"`
	CampaignID     string    `json:"campaign_id"`
	CreativeID     string    `json:"creative_id"`
	BidPriceMicros int64     `json:"bid_price_micros"`
	Geo            string    `json:"geo"`
	Device         string    `json:"device"`
}
