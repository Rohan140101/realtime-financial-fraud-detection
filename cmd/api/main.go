package main

import (
	"context"
	"encoding/json"
	"event-platform/internal/models"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	kafkaConfig "event-platform/internal/kafka"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func publishEvent(producer *kafka.Producer, topic string, event *models.Event) {
	eventJsonBytes, err := json.Marshal(event)
	if err != nil {
		fmt.Printf("Error while marshalling event json: %s\n", err)
	}

	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(event.Id),
		Value:          eventJsonBytes,
	}, nil)
}
func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Initializing DB Pool
	pool, err := pgxpool.New(context.Background(), os.Getenv("DB_URL"))
	if err != nil {
		fmt.Printf("Failed to Initialize Pool\n")
		os.Exit(1)
	}
	defer pool.Close()

	producerConfig := kafkaConfig.GetProducerConfig()
	producer, err := kafka.NewProducer(&producerConfig)

	if err != nil {
		log.Fatalf("Failed to Create Producer: %s\n", err)
		os.Exit(1)
	}

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	r := gin.Default()
	topic := "events"

	// POST Endpoints
	// Post Events Endpoint
	r.POST("/events", func(c *gin.Context) {
		var eventJson models.Event

		if err := c.ShouldBindJSON(&eventJson); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		new_uuid := uuid.New().String()
		eventJson.Id = new_uuid
		publishEvent(producer, topic, &eventJson)

		c.JSON(http.StatusAccepted, gin.H{"uuid": new_uuid})

	})

	// GET Endpoints
	// events/summary

	r.GET("/events/summary", func(c *gin.Context) {
		fmt.Println("Entering Event Summary API")
		var eventSummary models.EventSummary
		// Getting Types and Counts
		rows, err := pool.Query(context.Background(), `SELECT type, COUNT(id) count FROM EVENTS GROUP BY type order BY COUNT DESC;`)
		if err != nil {
			fmt.Printf("Error While Computing Summary of Events: %v\n", err)
		}
		var txnType string
		var count int32
		eventSummary.ByType = map[string]int32{}
		_, err = pgx.ForEachRow(rows, []any{&txnType, &count}, func() error {
			eventSummary.ByType[txnType] = count
			return nil
		})

		if err != nil {
			fmt.Printf("Error While Fetching Rows of Events: %v\n", err)
		}

		// Getting Max Timestamps and overall number of events

		rows, err = pool.Query(context.Background(), `SELECT MAX(timestamp) as maxTs, COUNT(id) as count FROM EVENTS;`)
		if err != nil {
			fmt.Printf("Error While Computing Summary of Events: %v\n", err)
		}
		_, err = pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (int32, error) {

			var totalEvents int32
			var latestEvent time.Time

			err = row.Scan(&latestEvent, &totalEvents)
			eventSummary.TotalEvents = totalEvents
			eventSummary.LatestEvent = latestEvent
			return c.GetInt32(1), err
		})

		c.JSON(http.StatusAccepted, eventSummary)

	})

	r.Run()
}
