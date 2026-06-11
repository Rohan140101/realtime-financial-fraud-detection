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
	"github.com/jackc/pgx/v5/pgxpool"

	"strconv"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func detectFraud(rdb *redis.Client, event *models.Event, threshold int, expireTime int) (bool, string, int) {
	accountId, exists := extractAccountId(event.Payload)
	if exists {
		key := fmt.Sprintf("fraud:account:%s", accountId)
		val := rdb.Incr(context.Background(), key).Val()
		if val == int64(1) {
			rdb.Expire(context.Background(), key, time.Duration(expireTime)*time.Second)
		}
		return val >= int64(threshold), accountId, int(val)
	}
	return false, accountId, 0

}

func publishFraudAlert(producer *kafka.Producer, event *models.Event, accountId string, count int) {
	fraudTopic := "fraud-alerts"
	fmt.Printf("Fraud Account Found: account_id: %s, Count: %d, Event ID: %s\n",
		accountId, count, event.Id)

	// Generating Fraud Alert Message
	new_uuid := uuid.New().String()
	var fraudAlert models.FraudAlert = models.FraudAlert{
		AlertId:          new_uuid,
		AccountId:        accountId,
		EventId:          event.Id,
		EventType:        event.Type,
		TransactionCount: count,
		DetectedAt:       time.Now(),
	}
	fraudAlertBytes, err := json.Marshal(fraudAlert)
	if err != nil {
		fmt.Printf("Error while marshalling event json: %s\n", err)
	}

	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &fraudTopic, Partition: kafka.PartitionAny},
		Key:            []byte(fraudAlert.AccountId),
		Value:          fraudAlertBytes,
	}, nil)

}

func extractAccountId(payload map[string]interface{}) (string, bool) {
	accountId, exists := payload["sender"]
	if exists {
		return accountId.(string), exists
	}
	accountId, exists = payload["accountId"]
	return accountId.(string), exists
}

func insertEvent(pool *pgxpool.Pool, event *models.Event) {
	_, err := pool.Exec(context.Background(), "INSERT INTO EVENTS (id, type, timestamp, payload) values ($1, $2, $3, $4::jsonb) ON CONFLICT (id) DO NOTHING", event.Id, event.Type, event.Timestamp, event.Payload)
	if err != nil {
		fmt.Printf("Error while Inserting Consumed Event in DB: %v", err)
	}
}

func main() {

	topic := "events"

	// Loading Env
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Initializing Postgres DB Pool
	pool, err := pgxpool.New(context.Background(), os.Getenv("DB_URL"))
	if err != nil {
		fmt.Printf("Failed to Initialize Pool\n")
		os.Exit(1)
	}
	defer pool.Close()

	// Initializing Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_URL"),
		Password: "",
		DB:       0,
	})

	_, err = rdb.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v\n", err)
	}

	// Initializing Producer
	producerConfig := kafkaConfig.GetProducerConfig()
	producer, err := kafka.NewProducer(&producerConfig)

	// Consumer
	consumerConfig := kafkaConfig.GetConsumerConfig()
	cons, err := kafka.NewConsumer(&consumerConfig)

	if err != nil {
		log.Fatalf("Failed to Create Consumer: %s\n", err)
		os.Exit(1)
	}

	expireTime, _ := strconv.Atoi(os.Getenv("FRAUD_WINDOW_SECONDS"))
	threshold, _ := strconv.Atoi(os.Getenv("FRAUD_THRESHOLD"))

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

			ifFraud, accountId, count := detectFraud(rdb, &eventJson, threshold, expireTime)

			if ifFraud {
				publishFraudAlert(producer, &eventJson, accountId, count)
			}

			insertEvent(pool, &eventJson)

		}

	}

	cons.Close()
}
