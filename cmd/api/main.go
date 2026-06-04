package main

import (
	"encoding/json"
	"event-platform/internal/models"
	"fmt"
	"log"
	"net/http"
	"os"

	kafkaConfig "event-platform/internal/kafka"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

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

	r.POST("/events", func(c *gin.Context) {
		var eventJson models.Event

		if err := c.ShouldBindJSON(&eventJson); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		new_uuid := uuid.New().String()
		eventJson.Id = new_uuid
		eventJsonBytes, err := json.Marshal(eventJson)
		if err != nil {
			fmt.Printf("Error while marshalling event json: %s\n", err)
		}

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(new_uuid),
			Value:          eventJsonBytes,
		}, nil)
		c.JSON(http.StatusAccepted, gin.H{"uuid": new_uuid})

	})

	r.Run()
}
