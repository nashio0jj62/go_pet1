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
	"payway/internal/provider"
	"payway/internal/storage"
	"payway/internal/telemetry"
	"payway/internal/worker"
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
	w := &worker.Worker{
		Events: events, Store: &storage.Store{DB: db},
		Provider: provider.NewClient(cfg.ProviderURL, cfg.CallbackSecret),
	}
	slog.Info("command consumer started")
	err = broker.Consume(ctx, cfg.KafkaBrokers, cfg.CommandsTopic, "payment-workers", w.HandleCommand)
	if ctx.Err() != nil {
		return nil
	}
	return err
}
