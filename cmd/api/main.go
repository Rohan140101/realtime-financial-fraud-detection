package main

import (
	"context"
	"encoding/json"
	"event-platform/internal/models"

	"log/slog"
	"net/http"
	"os"
	"time"

	kafkaConfig "event-platform/internal/kafka"

	gintrace "github.com/DataDog/dd-trace-go/contrib/gin-gonic/gin/v2"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/getsentry/sentry-go"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

var logger *slog.Logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

func publishEvent(producer *kafka.Producer, topic string, event *models.Event) {
	eventJsonBytes, err := json.Marshal(event)
	if err != nil {
		logger.Error("Event JSON Unmarshalling Failed",
			"error", err.Error())
		sentry.CaptureException(err)
	}

	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(event.Id),
		Value:          eventJsonBytes,
	}, nil)
}
func main() {

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
		tracer.WithAgentAddr("datadog-agent:8126"),
		tracer.WithService("api"),
		tracer.WithEnv("dev"),
	)
	defer tracer.Stop()

	// Initializing DB Pool
	pool, err := pgxpool.New(context.Background(), os.Getenv("DB_URL"))
	if err != nil {
		logger.Error("Failed to Initialize Postgres Pool",
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

	if err != nil {
		logger.Error("Failed to Create Producer",
			"error", err.Error())
		sentry.CaptureException(err)
		os.Exit(1)
	}

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					logger.Error("Failed to deliver message",
						"partition", ev.TopicPartition.Partition,
						"error", ev.TopicPartition.Error)

				} else {
					logger.Info("Produced Event",
						"topic", *ev.TopicPartition.Topic,
						"key", string(ev.Key),
						"value", string(ev.Value),
					)
				}
			}
		}
	}()

	r := gin.Default()
	r.Use(gintrace.Middleware("api"))
	topic := "events"

	// POST Endpoints
	// Post Events Endpoint
	r.POST("/events", func(c *gin.Context) {
		var eventJson models.Event

		if err := c.ShouldBindJSON(&eventJson); err != nil {
			logger.Error("Error in Binding Input Event",
				"error", err.Error())
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
		logger.Info("Entering Event Summary API")
		var eventSummary models.EventSummary
		// First Trying Redis Cache
		redis_cache_key := "cache:events:summary"
		res, err := rdb.Get(context.Background(), redis_cache_key).Result()
		if err != nil {
			logger.Info("Cache Miss, Retrieving from DB")
			// Getting Types and Counts
			rows, err := pool.Query(context.Background(), `SELECT type, COUNT(id) count FROM EVENTS GROUP BY type order BY COUNT DESC;`)
			if err != nil {
				logger.Error("Event Summary Query Failed",
					"error", err.Error())
				sentry.CaptureException(err)
			}
			var txnType string
			var count int32
			eventSummary.ByType = map[string]int32{}
			_, err = pgx.ForEachRow(rows, []any{&txnType, &count}, func() error {
				eventSummary.ByType[txnType] = count
				return nil
			})

			if err != nil {
				logger.Error("Error While Fetching Rows of Events",
					"error", err.Error())
				sentry.CaptureException(err)
			}

			// Getting Max Timestamps and overall number of events

			rows, err = pool.Query(context.Background(), `SELECT MAX(timestamp) as maxTs, COUNT(id) as count FROM EVENTS;`)
			if err != nil {
				logger.Error("Error While Computing Summary of Events",
					"error", err.Error())
				sentry.CaptureException(err)
			}
			_, err = pgx.CollectOneRow(rows, func(row pgx.CollectableRow) (int32, error) {

				var totalEvents int32
				var latestEvent time.Time

				err = row.Scan(&latestEvent, &totalEvents)
				eventSummary.TotalEvents = totalEvents
				eventSummary.LatestEvent = latestEvent
				return totalEvents, err
			})

			// Storing Event Summary in Cache
			eventJsonBytes, err := json.Marshal(eventSummary)
			if err != nil {
				logger.Error("Event JSON Marshalling Failed",
					"error", err.Error())
				sentry.CaptureException(err)
			}
			err = rdb.Set(context.Background(), redis_cache_key, eventJsonBytes, 30*time.Second).Err()
			if err != nil {
				logger.Error("Setting Cache Data Failed",
					"error", err.Error())
				sentry.CaptureException(err)
			}
		} else {
			logger.Info("Cache Hit")
			err = json.Unmarshal([]byte(res), &eventSummary)
			if err != nil {
				logger.Error("Event JSON Unmarshalling Failed",
					"error", err.Error())
			}
		}

		c.JSON(http.StatusOK, eventSummary)

	})

	r.Run()
}
