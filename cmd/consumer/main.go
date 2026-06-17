// Consumer Group Behavior (observed W3D1 2026-06-15):
// - 3 partitions across 2 consumer instances: ~2:1 split (one instance owns 2 partitions)
// - Rebalancing on instance death: surviving instance picks up all partitions within seconds
// - No data loss observed: idempotency keys prevent duplicate DB writes on rebalance
// - No explicit rebalancing logs from confluent-kafka-go: handled internally
package main

import (
	"context"
	"encoding/json"
	kafkaConfig "event-platform/internal/kafka"
	"event-platform/internal/models"
	"fmt"
	"log"
	"math"
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

// func getProducer() (*kafka.Producer, error) {
// 	producerConfig := kafkaConfig.GetProducerConfig()
// 	producer, err := kafka.NewProducer(&producerConfig)
// 	// if err != nil {
// 	// 	fmt.Println("Failed to Intialize Producer")
// 	// }
// 	return producer, err
// }

func insertEvent(pool *pgxpool.Pool, event *models.Event, rdb *redis.Client) error {
	_, err := pool.Exec(context.Background(), "INSERT INTO EVENTS (id, type, timestamp, payload) values ($1, $2, $3, $4::jsonb) ON CONFLICT (id) DO NOTHING", event.Id, event.Type, event.Timestamp, event.Payload)
	if err != nil {
		fmt.Printf("Error while Inserting Consumed Event in DB: %v", err)
		return err
	} else {
		// Insertion was successful so we invalidate cache
		// redis_cache_key := "cache:events:summary"
		// Deleting Cache
		// _, err = rdb.Del(context.Background(), redis_cache_key).Result()
		// if err != nil {
		// 	fmt.Printf("Error While Deleting Cache:%s\n", err)
		// }
		// fmt.Printf("Cache invalidated for key: %s\n", redis_cache_key)
		return nil
	}

}

func insertEventWithRetry(pool *pgxpool.Pool, event *models.Event, rdb *redis.Client, maxRetries int, producer *kafka.Producer) {
	err := insertEvent(pool, event, rdb)
	if err != nil {
		// Retry Logic with Exponential Backoff
		fmt.Printf("[Consumer] Failure while inserting event into Postgres : %s. Will Retry Insert %d times\n", err, maxRetries)

		for i := 1; i <= maxRetries; i++ {
			fmt.Printf("Retry Attempt: %d\n", i)
			time.Sleep(time.Duration(math.Pow(2, float64(i))) * time.Second)
			err = insertEvent(pool, event, rdb)
			if err != nil {
				fmt.Printf("Insert Retry Attempt %d failed: %s\n", i, err)
				continue
			} else {
				fmt.Printf("Insert Retry Attempt %d Success: %s\n", i, err)
				return
			}
		}
		// Call Publish DLQ
		publishDLQ(event, producer)
	}

}

func publishDLQ(event *models.Event, producer *kafka.Producer) {
	topic := "events-dlq"
	fmt.Printf("Entered publishDLQ for Event ID: %s\n", event.Id)
	eventBytes, err := json.Marshal(event)
	if err != nil {
		fmt.Printf("Failed to Marshal Event Object for DLQ, Event ID: %s\n", event.Id)
	}
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(event.Id),
		Value:          eventBytes,
	}, nil)
	fmt.Printf("Successfully completed publishDLQ for Event ID: %s\n", event.Id)

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
	// producer, err := getProducer()
	if err != nil {
		log.Fatalf("Failed to Create Producer: %s\n", err)
		os.Exit(1)
	}

	// Consumer
	consumerConfig := kafkaConfig.GetConsumerConfig()
	cons, err := kafka.NewConsumer(&consumerConfig)

	if err != nil {
		log.Fatalf("Failed to Create Consumer: %s\n", err)
		os.Exit(1)
	}

	expireTime, _ := strconv.Atoi(os.Getenv("FRAUD_WINDOW_SECONDS"))
	threshold, _ := strconv.Atoi(os.Getenv("FRAUD_THRESHOLD"))
	maxRetries, _ := strconv.Atoi(os.Getenv("MAX_RETRIES"))

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
			fmt.Printf("Consumed Event from partition: %d, topic: %s, key: %s, value: %s\n",
				ev.TopicPartition.Partition, *ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))

			var eventJson models.Event
			err = json.Unmarshal(ev.Value, &eventJson)
			if err != nil {
				log.Printf("Error unmarshaling JSON: %v", err)
			}

			ifFraud, accountId, count := detectFraud(rdb, &eventJson, threshold, expireTime)

			if ifFraud {
				publishFraudAlert(producer, &eventJson, accountId, count)
			}

			insertEventWithRetry(pool, &eventJson, rdb, maxRetries, producer)

		}

	}

	cons.Close()
}
