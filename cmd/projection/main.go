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
	"payway/internal/storage"
	"payway/internal/telemetry"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "projection"))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("projection stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.LoadBackend()
	if err != nil {
		return err
	}
	shutdownTelemetry, err := telemetry.Setup(ctx, "payway-projection")
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
	store := &storage.Store{DB: db}
	slog.Info("event consumer started")
	err = broker.Consume(ctx, cfg.KafkaBrokers, cfg.EventsTopic, "projections", func(ctx context.Context, message kafka.Message) error {
		var event payment.Event
		if err := json.Unmarshal(message.Value, &event); err != nil {
			return err
		}
		if string(message.Key) != event.Payment.ID {
			return errors.New("event key does not match payment ID")
		}
		if err := store.Apply(ctx, event); err != nil {
			return err
		}
		slog.Info("payment event stored", "payment_id", event.Payment.ID, "event_id", event.ID, "trace_id", telemetry.TraceID(ctx))
		return nil
	})
	if ctx.Err() != nil {
		return nil
	}
	return err
}
