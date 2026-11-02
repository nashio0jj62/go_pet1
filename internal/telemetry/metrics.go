package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"payway/internal/payment"
)

var (
	meter = otel.Meter("payway")
	created, _ = meter.Int64Counter("payway_payments_created")
	finished, _ = meter.Int64Counter("payway_payments_finished")
	paymentDuration, _ = meter.Float64Histogram("payway_payment_duration_seconds")
	httpRequests, _ = meter.Int64Counter("payway_http_requests")
	httpDuration, _ = meter.Float64Histogram("payway_http_request_duration_seconds")
	providerDuration, _ = meter.Float64Histogram("payway_provider_request_duration_seconds")
	errorsTotal, _ = meter.Int64Counter("payway_errors")
	retries, _ = meter.Int64Counter("payway_message_retries")
	callbackRetries, _ = meter.Int64Counter("payway_callback_retries")
)

func PaymentApplied(ctx context.Context, p payment.Payment) {
	if p.Status == payment.New {
		created.Add(ctx, 1)
	}
	if p.Status.Terminal() {
		labels := metric.WithAttributes(attribute.String("status", string(p.Status)))
		finished.Add(ctx, 1, labels)
		paymentDuration.Record(ctx, p.UpdatedAt.Sub(p.CreatedAt).Seconds(), labels)
	}
}

func ProviderRequest(ctx context.Context, operation string, started time.Time) {
	providerDuration.Record(ctx, time.Since(started).Seconds(),
		metric.WithAttributes(attribute.String("operation", operation)))
}

func Retry(ctx context.Context, topic string) {
	retries.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic)))
}

func CallbackRetry(ctx context.Context) {
	callbackRetries.Add(ctx, 1)
}

func Error(ctx context.Context, component, kind string, err error) {
	errorsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("component", component), attribute.String("type", kind),
	))
	span := trace.SpanFromContext(ctx)
	span.RecordError(err)
	span.SetStatus(codes.Error, kind)
}
