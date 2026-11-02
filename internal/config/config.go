package config

import (
	"errors"
	"net/url"
	"os"
	"strings"
)

type Config struct {
	HTTPAddr       string
	DatabaseURL    string
	KafkaBrokers   []string
	CommandsTopic  string
	EventsTopic    string
	ProviderURL    string
	CallbackURL    string
	CallbackSecret string
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:       value("HTTP_ADDR", ":8080"),
		DatabaseURL:    os.Getenv("DATABASE_URL"),
		CommandsTopic:  value("KAFKA_COMMANDS_TOPIC", "payment.commands"),
		EventsTopic:    value("KAFKA_EVENTS_TOPIC", "payment.events"),
		ProviderURL:    value("PROVIDER_URL", "http://localhost:8081"),
		CallbackURL:    value("CALLBACK_URL", "http://localhost:8080/callbacks/mock"),
		CallbackSecret: os.Getenv("CALLBACK_SECRET"),
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
	if len(c.CallbackSecret) < 32 {
		return Config{}, errors.New("CALLBACK_SECRET must contain at least 32 characters")
	}
	if c.CommandsTopic == c.EventsTopic {
		return Config{}, errors.New("commands and events must use different Kafka topics")
	}
	for _, address := range []string{c.ProviderURL, c.CallbackURL} {
		u, err := url.Parse(address)
		if err != nil || u.Hostname() == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
			return Config{}, errors.New("PROVIDER_URL and CALLBACK_URL must be HTTP URLs without credentials")
		}
	}
	return c, nil
}

func value(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
