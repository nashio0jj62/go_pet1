package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/segmentio/kafka-go"

	"payway/internal/broker"
	"payway/internal/payment"
	"payway/internal/storage"
)

type Worker struct {
	Events *kafka.Writer
	Store  *storage.Store
}

func (w *Worker) HandleCommand(ctx context.Context, message kafka.Message) error {
	var command payment.Command
	if err := json.Unmarshal(message.Value, &command); err != nil {
		return err
	}
	event, err := payment.CreatedEvent(command)
	if err != nil {
		return err
	}
	if string(message.Key) != command.PaymentID {
		return errors.New("command key does not match payment ID")
	}
	if err := broker.Publish(ctx, w.Events, command.PaymentID, event); err != nil {
		return err
	}
	slog.Info("payment event published", "payment_id", command.PaymentID, "event_id", event.ID)
	return nil
}

func (w *Worker) HandleEvent(ctx context.Context, message kafka.Message) error {
	var event payment.Event
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return err
	}
	if string(message.Key) != event.Payment.ID {
		return errors.New("event key does not match payment ID")
	}
	if err := w.Store.ApplyCreated(ctx, event); err != nil {
		return err
	}
	slog.Info("payment projection applied", "payment_id", event.Payment.ID, "event_id", event.ID)
	return nil
}

