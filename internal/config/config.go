package config

import (
	"errors"
	"net/url"
	"os"
	"strings"
)

type Backend struct {
	DatabaseURL  string
	KafkaBrokers []string
	EventsTopic  string
}

type Config struct {
	Backend
	HTTPAddr       string
	CommandsTopic  string
	ProviderURL    string
	CallbackURL    string
	CallbackSecret string
}

func LoadBackend() (Backend, error) {
	c := Backend{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		EventsTopic: value("KAFKA_EVENTS_TOPIC", "payment.events"),
	}
	for _, broker := range strings.Split(value("KAFKA_BROKERS", "localhost:9092"), ",") {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			return Backend{}, errors.New("KAFKA_BROKERS contains an empty broker address")
		}
		c.KafkaBrokers = append(c.KafkaBrokers, broker)
	}
	if c.DatabaseURL == "" {
		return Backend{}, errors.New("DATABASE_URL is required")
	}
	return c, nil
}

func Load() (Config, error) {
	backend, err := LoadBackend()
	if err != nil {
		return Config{}, err
	}
	c := Config{
		Backend:        backend,
		HTTPAddr:       value("HTTP_ADDR", ":8080"),
		CommandsTopic:  value("KAFKA_COMMANDS_TOPIC", "payment.commands"),
		ProviderURL:    value("PROVIDER_URL", "http://localhost:8081"),
		CallbackURL:    value("CALLBACK_URL", "http://localhost:8080/callbacks/mock"),
		CallbackSecret: os.Getenv("CALLBACK_SECRET"),
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
