package main

import (
	"context"
	"encoding/json"
	kafkaConfig "event-platform/internal/kafka"
	"event-platform/internal/models"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	topic := "events"

	// Initializing DB Connection
	conn, err := pgx.Connect(context.Background(), os.Getenv("DB_URL"))
	if err != nil {
		fmt.Printf("Failed to Connect to DB\n")
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	// Consumer
	consumerConfig := kafkaConfig.GetConsumerConfig()
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
			fmt.Printf("Consumed Event from topic: %s, key: %s, value: %s\n",
				*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))

			var eventJson models.Event
			err = json.Unmarshal(ev.Value, &eventJson)
			if err != nil {
				log.Printf("Error unmarshaling JSON: %v", err)
			}

			_, err = conn.Exec(context.Background(), "INSERT INTO EVENTS (id, type, timestamp, payload) values ($1, $2, $3, $4::jsonb)", eventJson.Id, eventJson.Type, eventJson.Timestamp, eventJson.Payload)
			if err != nil {
				fmt.Printf("Error while Inserting Consumed Event in DB: %v", err)
			}

		}

	}

	cons.Close()
}
