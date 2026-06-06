package models

import (
	"time"
)

type EventSummary struct {
	TotalEvents int32            `json:"total_events"`
	ByType      map[string]int32 `json:"by_type"`
	LatestEvent time.Time        `json:"latest_event"`
}
