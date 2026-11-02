package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"payway/internal/config"
	"payway/internal/provider"
	"payway/internal/telemetry"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "mock-psp"))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("mock provider stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	shutdownTelemetry, err := telemetry.Setup(ctx, "payway-mock-psp")
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
	mock := &provider.Mock{
		DB: db, Secret: cfg.CallbackSecret, CallbackURL: cfg.CallbackURL,
		HTTP: &http.Client{Timeout: 10 * time.Second},
	}
	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	done := make(chan struct{})
	go func() {
		defer close(done)
		mock.Run(workerCtx)
	}()
	server := &http.Server{
		Addr: cfg.HTTPAddr, Handler: mock.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: time.Minute,
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	select {
	case err = <-serverErr:
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = server.Shutdown(shutdownCtx)
		cancel()
		if err != nil {
			server.Close()
		}
	}
	cancelWorker()
	<-done
	return err
}
