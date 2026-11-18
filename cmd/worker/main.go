package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"payway/internal/broker"
	"payway/internal/config"
	"payway/internal/payment"
	"payway/internal/provider"
	"payway/internal/service"
	"payway/internal/storage"
	"payway/internal/telemetry"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "worker"))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	shutdownTelemetry, err := telemetry.Setup(ctx, "payway-worker")
	if err != nil {
		return err
	}
	defer shutdownTelemetry()
	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return errors.New("cannot initialize PostgreSQL pool; check DATABASE_URL")
	}
	defer db.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = db.Ping(pingCtx)
	cancel()
	if err != nil {
		return errors.New("PostgreSQL connection unavailable")
	}
	events := broker.NewWriter(cfg.KafkaBrokers, cfg.EventsTopic)
	defer events.Close()
	commands := &service.Commands{
		Store:    &storage.Store{DB: db},
		Provider: provider.NewClient(cfg.ProviderURL, cfg.CallbackSecret),
		PublishEvent: func(ctx context.Context, event payment.Event) error {
			return broker.Publish(ctx, events, event.Payment.ID, event)
		},
	}
	slog.Info("command consumer started")
	err = broker.Consume(ctx, cfg.KafkaBrokers, cfg.CommandsTopic, "payment-workers", func(ctx context.Context, message kafka.Message) error {
		var command payment.Command
		if err := json.Unmarshal(message.Value, &command); err != nil {
			return err
		}
		if string(message.Key) != command.PaymentID {
			return errors.New("invalid payment command")
		}
		return commands.Handle(ctx, command)
	})
	if ctx.Err() != nil {
		return nil
	}
	return err
}
