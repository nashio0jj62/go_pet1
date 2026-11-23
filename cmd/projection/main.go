package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"payway/internal/service"
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
	projection := &service.Projection{Store: store}
	slog.Info("event consumer started")
	err = broker.Consume(ctx, cfg.KafkaBrokers, cfg.EventsTopic, "projections", func(ctx context.Context, message kafka.Message) error {
		var event payment.Event
		if err := json.Unmarshal(message.Value, &event); err != nil {
			return fmt.Errorf("%w: %v", payment.ErrInvalidMessage, err)
		}
		if string(message.Key) != event.Payment.ID {
			return fmt.Errorf("%w: event key does not match payment ID", payment.ErrInvalidMessage)
		}
		return projection.Apply(ctx, event)
	}, store.SaveRejection)
	if ctx.Err() != nil {
		return nil
	}
	return err
}
