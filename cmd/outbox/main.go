package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"payway/internal/broker"
	"payway/internal/config"
	"payway/internal/storage"
	"payway/internal/telemetry"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "outbox"))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("outbox stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.LoadBackend()
	if err != nil {
		return err
	}
	shutdownTelemetry, err := telemetry.Setup(ctx, "payway-outbox")
	if err != nil {
		return err
	}
	defer shutdownTelemetry()
	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return errors.New("cannot initialize PostgreSQL pool; check DATABASE_URL")
	}
	defer db.Close()
	store := &storage.Store{DB: db}
	writer := broker.NewWriter(cfg.KafkaBrokers, cfg.CommandsTopic)
	defer writer.Close()
	publish := func(ctx context.Context, command storage.OutboxCommand) error {
		return broker.Publish(telemetry.Parent(ctx, command.TraceParent), writer, command.PaymentID, command.Payload)
	}
	backoff := time.Second
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, 8*time.Second)
		sent, err := store.DispatchCommand(attempt, publish)
		cancel()
		if ctx.Err() != nil {
			break
		}
		delay := 250 * time.Millisecond
		if err != nil {
			slog.Warn("outbox delivery failed", "retry_after", backoff, "error", err)
			telemetry.Error(ctx, "outbox", "dispatch", err)
			delay = backoff
			backoff = min(backoff*2, 30*time.Second)
		} else {
			backoff = time.Second
			if sent {
				continue
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return nil
}
