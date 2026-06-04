package kafka

import (
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func GetConsumerConfig() kafka.ConfigMap {

	consumer_config := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		"sasl.username":     os.Getenv("KAFKA_API_KEY"),
		"sasl.password":     os.Getenv("KAFKA_API_SECRET"),
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"group.id":          "event-consumer-group",
		"auto.offset.reset": "earliest",
	}

	return *consumer_config
}
