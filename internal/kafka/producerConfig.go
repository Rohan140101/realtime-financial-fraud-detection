package kafka

import (
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func GetProducerConfig() kafka.ConfigMap {

	producer_config := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		"acks":              "all",
	}

	return *producer_config
}
