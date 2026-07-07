package kafka

import (
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func GetConsumerConfig() kafka.ConfigMap {

	consumer_config := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		"group.id":          "event-consumer-group",
		"auto.offset.reset": "earliest",
	}

	return *consumer_config
}
