package config

import (
	"errors"
	"os"
	"strings"
)

type Config struct {
	HTTPAddr      string
	DatabaseURL   string
	KafkaBrokers  []string
	CommandsTopic string
	EventsTopic   string
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr: value("HTTP_ADDR", ":8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		CommandsTopic: value("KAFKA_COMMANDS_TOPIC", "payment.commands"),
		EventsTopic: value("KAFKA_EVENTS_TOPIC", "payment.events"),
	}
	for _, broker := range strings.Split(value("KAFKA_BROKERS", "localhost:9092"), ",") {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			return Config{}, errors.New("KAFKA_BROKERS contains an empty broker address")
		}
		c.KafkaBrokers = append(c.KafkaBrokers, broker)
	}
	if c.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if c.CommandsTopic == c.EventsTopic {
		return Config{}, errors.New("commands and events must use different Kafka topics")
	}
	return c, nil
}

func value(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

