package main

import (
	"context"
	"event-platform/internal/models"
	"time"

	"testing"

	"github.com/redis/go-redis/v9"
)

func TestExtractAccountId(t *testing.T) {
	tests := []struct {
		name      string
		payload   map[string]interface{}
		wantId    string
		wantFound bool
	}{
		{
			"payment with accountId",
			map[string]interface{}{"accountId": "ACC-001"},
			"ACC-001",
			true,
		},
		{
			"payment with sender",
			map[string]interface{}{"sender": "ACC-002", "receiver": "ACC-003"},
			"ACC-002",
			true,
		},
		{
			"empty payload",
			map[string]interface{}{},
			"",
			false,
		},
		{
			"payload with neither field",
			map[string]interface{}{"amount": 100},
			"",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotId, gotFound := extractAccountId(tt.payload)
			if gotId != tt.wantId || gotFound != tt.wantFound {
				t.Errorf("extractAccountId = (%s, %v), want = (%s, %v)\n", gotId, gotFound, tt.wantId, tt.wantFound)
			}
		})
	}
}

func TestFraudAccount(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	expireTime := 10
	threshold := 5

	tests := []struct {
		name       string
		events     []*models.Event
		wantBool   bool
		wantNumber int
	}{
		{
			"4 events",
			[]*models.Event{
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-001"},
					Timestamp: time.Now(),
					Type:      "payment",
				},
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-001"},
					Timestamp: time.Now(),
					Type:      "invest",
				},
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-001"},
					Timestamp: time.Now(),
					Type:      "deposit",
				},
				&models.Event{
					Payload:   map[string]interface{}{"sender": "ACC-001", "receiver": "ACC-002"},
					Timestamp: time.Now(),
					Type:      "transfer",
				},
			},
			false,
			4,
		},
		{
			"5 events",
			[]*models.Event{
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-003"},
					Timestamp: time.Now(),
					Type:      "payment",
				},
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-003"},
					Timestamp: time.Now(),
					Type:      "invest",
				},
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-003"},
					Timestamp: time.Now(),
					Type:      "deposit",
				},
				&models.Event{
					Payload:   map[string]interface{}{"sender": "ACC-003", "receiver": "ACC-004"},
					Timestamp: time.Now(),
					Type:      "transfer",
				},
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-003"},
					Timestamp: time.Now(),
					Type:      "withdrawal",
				},
			},
			true,
			5,
		},
		{
			"6 events",
			[]*models.Event{
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-005"},
					Timestamp: time.Now(),
					Type:      "payment",
				},
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-005"},
					Timestamp: time.Now(),
					Type:      "invest",
				},
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-005"},
					Timestamp: time.Now(),
					Type:      "deposit",
				},
				&models.Event{
					Payload:   map[string]interface{}{"sender": "ACC-005", "receiver": "ACC-006"},
					Timestamp: time.Now(),
					Type:      "transfer",
				},
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-005"},
					Timestamp: time.Now(),
					Type:      "withdrawal",
				},
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-005"},
					Timestamp: time.Now(),
					Type:      "payment",
				},
			},
			true,
			6,
		},
		{
			"5 events - different accounts no frauds",
			[]*models.Event{
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-007"},
					Timestamp: time.Now(),
					Type:      "payment",
				},
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-008"},
					Timestamp: time.Now(),
					Type:      "invest",
				},
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-007"},
					Timestamp: time.Now(),
					Type:      "deposit",
				},
				&models.Event{
					Payload:   map[string]interface{}{"sender": "ACC-008", "receiver": "ACC-009"},
					Timestamp: time.Now(),
					Type:      "transfer",
				},
				&models.Event{
					Payload:   map[string]interface{}{"accountId": "ACC-007"},
					Timestamp: time.Now(),
					Type:      "withdrawal",
				},
			},
			false,
			3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdb.FlushDB(context.Background())

			var gotBool bool
			var gotNumber int

			for _, event := range tt.events {
				gotBool, _, gotNumber = detectFraud(rdb, event, threshold, expireTime)
			}

			if gotBool != tt.wantBool {
				t.Errorf("%s: detectFraud: gotBool= %v, wantBool= %v\n", tt.name, gotBool, tt.wantBool)
			}

			if gotNumber != tt.wantNumber {
				t.Errorf("%s: detectFraud: gotNumber= %d, wantNumber= %d\n", tt.name, gotNumber, tt.wantNumber)

			}
		})
	}

}
