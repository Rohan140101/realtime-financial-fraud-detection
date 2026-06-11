package models

import "time"

type FraudAlert struct {
	AlertId          string    `json:"alert_id"`
	AccountId        string    `json:"account_id"`
	EventId          string    `json:"event_id"`
	EventType        string    `json:"event_type"`
	TransactionCount int       `json:"transaction_count"`
	DetectedAt       time.Time `json:"detected_at"`
}
