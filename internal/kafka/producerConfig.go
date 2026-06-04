package kafka

import (
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func GetProducerConfig() kafka.ConfigMap {

	producer_config := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		"sasl.username":     os.Getenv("KAFKA_API_KEY"),
		"sasl.password":     os.Getenv("KAFKA_API_SECRET"),
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"acks":              "all",
	}

	return *producer_config
}
