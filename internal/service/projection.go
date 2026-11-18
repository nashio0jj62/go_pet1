package service

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"payway/internal/payment"
	"payway/internal/storage"
	"payway/internal/telemetry"
)

type Projection struct {
	Store *storage.Store
}

func (s *Projection) Apply(ctx context.Context, event payment.Event) (retErr error) {
	ctx, span := telemetry.Tracer.Start(ctx, "payment.project", trace.WithAttributes(
		attribute.String("payment.id", event.Payment.ID), attribute.Int("payment.version", event.Payment.Version)))
	defer func() {
		if retErr != nil {
			telemetry.Error(ctx, "projection", "apply", retErr)
		}
		span.End()
	}()
	if err := event.Validate(); err != nil {
		return err
	}
	applied, err := s.Store.Project(ctx, event)
	if err != nil {
		return err
	}
	for _, p := range applied {
		telemetry.PaymentApplied(ctx, p)
	}
	slog.Info("payment event stored", "payment_id", event.Payment.ID, "event_id", event.ID, "trace_id", telemetry.TraceID(ctx))
	return nil
}
