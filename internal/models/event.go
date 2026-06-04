package models

import "time"

type Event struct {
	Id        string                 `json:"id"`
	Type      string                 `json:"type" binding:"required"`
	Timestamp time.Time              `json:"timestamp" binding:"required"`
	Payload   map[string]interface{} `json:"payload" binding:"required"`
}
