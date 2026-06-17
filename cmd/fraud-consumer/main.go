package main

import (
	kafkaConfig "event-platform/internal/kafka"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/joho/godotenv"
)

var logger *slog.Logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

func main() {

	fraudTopic := "fraud-alerts"

	// Loading Env
	err := godotenv.Load()
	if err != nil {
		logger.Error("Error Loading .env file",
			"error", err.Error())
	}

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

		}

	}

	cons.Close()
}
