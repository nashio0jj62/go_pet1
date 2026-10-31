package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
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

func Publish(ctx context.Context, writer *kafka.Writer, paymentID string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return writer.WriteMessages(ctx, kafka.Message{Key: []byte(paymentID), Value: data})
}

// Process one message at a time: committing a later offset also commits earlier
// messages in that partition. Never move on while the current message has failed.
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
			return fmt.Errorf("fetch %s: %w", topic, err)
		}
		delay := time.Second
		for {
			attemptCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			err = handle(attemptCtx, message)
			cancel()
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("message processing failed; retrying",
				"topic", topic, "partition", message.Partition, "offset", message.Offset,
				"retry_after", delay.String(), "error", err)
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
			return fmt.Errorf("commit %s: %w", topic, err)
		}
	}
}

