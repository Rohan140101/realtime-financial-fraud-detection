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

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	topic := "events"
	consumerConfig := kafkaConfig.GetConsumerConfig()

	// Consumer
	cons, err := kafka.NewConsumer(&consumerConfig)

	if err != nil {
		log.Fatalf("Failed to Create Consumer: %s\n", err)
		os.Exit(1)
	}

	err = cons.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("Consumer Error in Subscribing to Topics: %v\n", err)
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
			fmt.Printf("Consumed Event from topic: %s, key: %s, value: %sn",
				*ev.TopicPartition.Topic, ev.Key, ev.Value)

		}

	}

	cons.Close()
}
