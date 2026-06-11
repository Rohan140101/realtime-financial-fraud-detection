package main

import (
	kafkaConfig "event-platform/internal/kafka"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/joho/godotenv"
)

func main() {

	fraudTopic := "fraud-alerts"

	// Loading Env
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Consumer
	fraudConsumerConfig := kafkaConfig.GetFraudConsumerConfig()
	cons, err := kafka.NewConsumer(&fraudConsumerConfig)

	if err != nil {
		log.Fatalf("Failed to Create Fraud Consumer: %s\n", err)
		os.Exit(1)
	}

	err = cons.SubscribeTopics([]string{fraudTopic}, nil)
	if err != nil {
		fmt.Printf("Consumer Error in Subscribing to Topic: %s: %v\n", fraudTopic, err)
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	run := true
	for run {
		select {
		case sig := <-sigChan:
			fmt.Printf("Caught Signal %v, terminating\n", sig)
			run = false
		default:
			ev, err := cons.ReadMessage(100 * time.Millisecond)
			if err != nil {
				continue
			}
			fmt.Printf("New Fraud Alert Event from topic: %s, key: %s, value: %s\n",
				*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))

		}

	}

	cons.Close()
}
