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
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v5/pgxpool"

	"strconv"

	"log/slog"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/getsentry/sentry-go"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

var logger *slog.Logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

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

	logger.Warn("Fraud Account Found",
		"accountId", accountId,
		"count", count,
		"eventId", event.Id)

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
		logger.Error("Event JSON Marshalling Failed",
			"error", err.Error())
		sentry.CaptureException(err)
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
	if exists {
		return accountId.(string), exists
	}
	return "", false
}

func insertEvent(pool *pgxpool.Pool, event *models.Event, rdb *redis.Client) error {
	_, err := pool.Exec(context.Background(), "INSERT INTO EVENTS (id, type, timestamp, payload) values ($1, $2, $3, $4::jsonb) ON CONFLICT (id) DO NOTHING", event.Id, event.Type, event.Timestamp, event.Payload)
	if err != nil {
		logger.Error("Consumed Event DB Insertion Failed",
			"error", err.Error())
		sentry.CaptureException(err)
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
		logger.Error("Failure while inserting event into Postgres",
			"error", err.Error(),
			"maxRetries", maxRetries)
		sentry.CaptureException(err)

		for i := 1; i <= maxRetries; i++ {
			logger.Info("Retry Attempt",
				"retry", i)
			time.Sleep(time.Duration(math.Pow(2, float64(i))) * time.Second)
			err = insertEvent(pool, event, rdb)
			if err != nil {
				logger.Error("Failed Insert",
					"error", err.Error(),
					"retryCount", i)
				sentry.CaptureException(err)

				continue
			} else {
				logger.Info("Successful Insert",
					"retryCount", i)
				return
			}
		}
		// Call Publish DLQ
		publishDLQ(event, producer)
	}

}

func publishDLQ(event *models.Event, producer *kafka.Producer) {
	topic := "events-dlq"
	logger.Info("Entered publishDLQ",
		"eventId", event.Id)
	eventBytes, err := json.Marshal(event)
	if err != nil {
		logger.Error("Event JSON Marshal failed in publishDLQ",
			"error", err.Error(),
			"eventId", event.Id,
		)
		sentry.CaptureException(err)
	}
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(event.Id),
		Value:          eventBytes,
	}, nil)
	logger.Info("Successfully completed publishDLQ",
		"eventId", event.Id)

}

func main() {
	topic := "events"

	// Loading Env
	err := godotenv.Load()
	if err != nil {
		logger.Error("Error Loading .env file",
			"error", err.Error())
	}

	// Loading Sentry
	err = sentry.Init(sentry.ClientOptions{
		Dsn:              os.Getenv("SENTRY_DSN"),
		TracesSampleRate: 1.0,
	})
	if err != nil {
		logger.Error("sentry init failed", "error", err.Error())
	}
	defer sentry.Flush(2 * time.Second)

	// Loading Datadog
	tracer.Start(
		tracer.WithService("consumer"),
		tracer.WithEnv("dev"),
		tracer.WithAgentAddr("datadog-agent:8126"),
	)
	defer tracer.Stop()

	// Initializing Postgres DB Pool
	pool, err := pgxpool.New(context.Background(), os.Getenv("DB_URL"))
	if err != nil {
		logger.Error("Failed to Initialize Postgres Connection Pool",
			"error", err.Error())
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
		logger.Error("Failed to connect to Redis",
			"error", err.Error())
		sentry.CaptureException(err)
	}

	// Initializing Producer
	producerConfig := kafkaConfig.GetProducerConfig()
	producer, err := kafka.NewProducer(&producerConfig)
	// producer, err := getProducer()
	if err != nil {
		logger.Error("Failed to Create Producer",
			"error", err.Error())
		sentry.CaptureException(err)
		os.Exit(1)
	}

	// Consumer
	consumerConfig := kafkaConfig.GetConsumerConfig()
	cons, err := kafka.NewConsumer(&consumerConfig)

	if err != nil {
		logger.Error("Failed to Create Consumer",
			"error", err.Error())
		sentry.CaptureException(err)
		os.Exit(1)
	}

	expireTime, _ := strconv.Atoi(os.Getenv("FRAUD_WINDOW_SECONDS"))
	threshold, _ := strconv.Atoi(os.Getenv("FRAUD_THRESHOLD"))
	maxRetries, _ := strconv.Atoi(os.Getenv("MAX_RETRIES"))

	err = cons.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		logger.Error("Consumer Failure in Subscribing to Topics",
			"error", err.Error())
		sentry.CaptureException(err)
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	run := true

	for run {
		select {
		case sig := <-sigChan:
			logger.Error("Caught Signal, terminating",
				"signal", sig)
			run = false
		default:
			ev, err := cons.ReadMessage(100 * time.Millisecond)
			if err != nil {
				continue
			}
			logger.Info("Event Consumed",
				"partition", ev.TopicPartition.Partition,
				"topic", ev.TopicPartition.Topic,
				"key", string(ev.Key),
				"value", string(ev.Value))

			var eventJson models.Event
			err = json.Unmarshal(ev.Value, &eventJson)
			if err != nil {
				logger.Error("JSON Unmarshalling Failed",
					"error", err.Error())
				sentry.CaptureException(err)
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
