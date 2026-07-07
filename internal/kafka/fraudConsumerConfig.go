package kafka

import (
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func GetFraudConsumerConfig() kafka.ConfigMap {

	consumer_config := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		"group.id":          "fraud-alert-processor",
		"auto.offset.reset": "earliest",
	}

	return *consumer_config
}
