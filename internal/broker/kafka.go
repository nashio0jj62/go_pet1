package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"payway/internal/telemetry"
)

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(brokers...), Topic: topic,
		Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
		MaxAttempts: 3,
	}
}

func Publish(ctx context.Context, writer *kafka.Writer, paymentID string, value any) (retErr error) {
	ctx, span := telemetry.Tracer.Start(ctx, "kafka.publish", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("messaging.destination.name", writer.Topic), attribute.String("payment.id", paymentID)))
	defer func() {
		if retErr != nil {
			telemetry.Error(ctx, "kafka", "publish", retErr)
		}
		span.End()
	}()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	message := kafka.Message{Key: []byte(paymentID), Value: data}
	for key, value := range carrier {
		message.Headers = append(message.Headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return writer.WriteMessages(ctx, message)
}

func Consume(ctx context.Context, brokers []string, topic, group string, handle func(context.Context, kafka.Message) error) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: group,
		MinBytes: 1, MaxBytes: 10e6, MaxWait: time.Second,
		CommitInterval: 0, StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() == nil {
				telemetry.Error(ctx, "kafka", "fetch", err)
			}
			return fmt.Errorf("fetch %s: %w", topic, err)
		}
		carrier := propagation.MapCarrier{}
		for _, header := range message.Headers {
			carrier.Set(header.Key, string(header.Value))
		}
		messageCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)
		delay := time.Second
		for {
			attemptCtx, cancel := context.WithTimeout(messageCtx, 15*time.Second)
			attemptCtx, span := telemetry.Tracer.Start(attemptCtx, "kafka.consume", trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(attribute.String("messaging.destination.name", topic),
					attribute.String("payment.id", string(message.Key)), attribute.Int64("messaging.offset", message.Offset)))
			err = handle(attemptCtx, message)
			if err != nil && ctx.Err() == nil {
				telemetry.Error(attemptCtx, "kafka", "process", err)
				telemetry.Retry(attemptCtx, topic)
			}
			span.End()
			cancel()
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("message processing failed; retrying",
				"topic", topic, "partition", message.Partition, "offset", message.Offset,
				"retry_after", delay.String(), "error", err, "trace_id", telemetry.TraceID(messageCtx))
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			delay = min(delay*2, 30*time.Second)
		}
		commitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = reader.CommitMessages(commitCtx, message)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				telemetry.Error(messageCtx, "kafka", "commit", err)
			}
			return fmt.Errorf("commit %s: %w", topic, err)
		}
	}
}

