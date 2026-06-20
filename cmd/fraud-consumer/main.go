package main

import (
	"context"
	"encoding/json"
	kafkaConfig "event-platform/internal/kafka"
	"event-platform/internal/models"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/joho/godotenv"
)

var logger *slog.Logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

func insertFraudAlert(pool *pgxpool.Pool, fraudAlert *models.FraudAlert) error {
	logger.Info("Inserting Fraud Alert into DB",
		"fraudAlertId", fraudAlert.AlertId)
	_, err := pool.Exec(context.Background(), "INSERT INTO FRAUD_ALERTS (alert_id, account_id, event_id, event_type, transaction_count, detected_at) values ($1, $2, $3, $4, $5, $6) ON CONFLICT (alert_id) DO NOTHING", fraudAlert.AlertId, fraudAlert.AccountId, fraudAlert.EventId, fraudAlert.EventType, fraudAlert.TransactionCount, fraudAlert.DetectedAt)
	if err != nil {
		logger.Error("Consumed Fraud DB Insertion Failed",
			"error", err.Error())
		return err
	}
	return nil

}

func main() {

	fraudTopic := "fraud-alerts"

	// Loading Env
	err := godotenv.Load()
	if err != nil {
		logger.Error("Error Loading .env file",
			"error", err.Error())
	}

	// Initializing Postgres DB Pool
	pool, err := pgxpool.New(context.Background(), os.Getenv("DB_URL"))
	if err != nil {
		logger.Error("Failed to Initialize Postgres Connection Pool",
			"error", err.Error())
		os.Exit(1)
	}
	defer pool.Close()

	// Consumer
	fraudConsumerConfig := kafkaConfig.GetFraudConsumerConfig()
	cons, err := kafka.NewConsumer(&fraudConsumerConfig)

	if err != nil {
		logger.Error("Failed to Create Fraud Consumer",
			"error", err.Error())
		os.Exit(1)
	}

	err = cons.SubscribeTopics([]string{fraudTopic}, nil)
	if err != nil {
		logger.Error("Failed to Create Fraud Consumer",
			"topic", fraudTopic,
			"error", err.Error())
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	run := true
	for run {
		select {
		case sig := <-sigChan:
			logger.Error("Caught Signal. Terminating",
				"signal", sig)
			run = false
		default:
			ev, err := cons.ReadMessage(100 * time.Millisecond)
			if err != nil {
				continue
			}

			logger.Info("New Fraud Alert Event",
				"topic", ev.TopicPartition.Topic,
				"partition", ev.TopicPartition.Partition,
				"key", string(ev.Key),
				"value", string(ev.Value))

			var fraudAlert models.FraudAlert
			err = json.Unmarshal(ev.Value, &fraudAlert)
			if err != nil {
				logger.Info("Fraud Alert Unmarshalling Failed",
					"error", err.Error())
			}

			insertFraudAlert(pool, &fraudAlert)

		}

	}

	cons.Close()
}
