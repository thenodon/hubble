package env

import (
	"os"
)

var (
	KafkaTopic      = os.Getenv("KAFKA_TOPIC")
	KafkaUser       = os.Getenv("KAFKA_USER")
	KafkaPassword   = os.Getenv("KAFKA_PASSWORD")
	KafkaBrokers    = os.Getenv("KAFKA_BROKERS")
	KafkaVersion    = getenv("KAFKA_VERSION", "3.2.0")
	KafkaMessageKey = getenv("KAFKA_MESSAGE_KEY", "hubble")
)

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}
