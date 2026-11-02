package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"payway/internal/broker"
	"payway/internal/payment"
	"payway/internal/provider"
	"payway/internal/storage"
)

type Worker struct {
	Events   *kafka.Writer
	Store    *storage.Store
	Provider *provider.Client
}

func (w *Worker) HandleCommand(ctx context.Context, message kafka.Message) error {
	var command payment.Command
	if err := json.Unmarshal(message.Value, &command); err != nil {
		return err
	}
	if command.SchemaVersion != 1 || !payment.ValidID(command.PaymentID) || command.CreatedAt.IsZero() ||
		command.ID == "" || len(command.ID) > 200 || string(message.Key) != command.PaymentID {
		return errors.New("invalid payment command")
	}
	switch command.Type {
	case payment.CreatePayment:
		return w.create(ctx, command)
	case payment.CancelPayment:
		if command.ID != "cancel_"+command.PaymentID {
			return errors.New("invalid cancellation command ID")
		}
		return w.cancel(ctx, command)
	case payment.ProviderCallback:
		return w.callback(ctx, command)
	default:
		return errors.New("unknown payment command")
	}
}

func (w *Worker) create(ctx context.Context, command payment.Command) error {
	event, err := payment.CreatedEvent(command)
	if err != nil {
		return err
	}
	p, err := w.Store.Get(ctx, command.PaymentID)
	if errors.Is(err, storage.ErrNotFound) {
		p, err = w.publish(ctx, event)
	}
	if err != nil {
		return err
	}
	if p.MerchantID != command.Data.MerchantID || p.Amount != command.Data.Amount || p.Currency != command.Data.Currency {
		return errors.New("creation command conflicts with existing payment")
	}
	if p.Status.Terminal() {
		return nil
	}
	result, err := w.Provider.Start(ctx, p)
	if err != nil {
		return err
	}
	if p.ProviderID != "" && p.ProviderID != result.ProviderID {
		return errors.New("provider changed payment reference")
	}
	if p.Status == payment.New {
		start := command
		start.ID = "start_" + p.ID
		start.CreatedAt = command.CreatedAt
		event, err := payment.ChangeEvent(start, p, payment.Processing, result.ProviderID, "")
		if err != nil {
			return err
		}
		p, err = w.publish(ctx, event)
		if err != nil {
			return err
		}
	}
	if result.Status.Terminal() && !p.Status.Terminal() {
		command.ID = "result_" + p.ID + "_" + string(result.Status)
		command.CreatedAt = result.UpdatedAt
		return w.finish(ctx, command, p, result)
	}
	return nil
}

func (w *Worker) cancel(ctx context.Context, command payment.Command) error {
	p, err := w.Store.Get(ctx, command.PaymentID)
	if err != nil {
		return err
	}
	if p.Status.Terminal() {
		return nil
	}
	result := provider.Result{
		PaymentID: p.ID, ProviderID: p.ProviderID,
		Status: payment.Canceled, UpdatedAt: command.CreatedAt,
	}
	if p.Status == payment.Processing {
		result, err = w.Provider.Cancel(ctx, p.ID)
		if err != nil {
			return err
		}
		if !result.Status.Terminal() {
			return errors.New("provider has not confirmed cancellation")
		}
		if result.ProviderID != p.ProviderID {
			return errors.New("provider returned a different reference")
		}
	}
	command.CreatedAt = result.UpdatedAt
	return w.finish(ctx, command, p, result)
}

func (w *Worker) callback(ctx context.Context, command payment.Command) error {
	result := provider.Result{
		PaymentID: command.PaymentID, ProviderID: command.ProviderID,
		Status: command.Result, FailureReason: command.FailureReason, UpdatedAt: command.CreatedAt,
	}
	if err := result.Validate(); err != nil {
		return err
	}
	if !result.Status.Terminal() {
		return errors.New("callback status must be terminal")
	}
	p, err := w.Store.Get(ctx, command.PaymentID)
	if err != nil {
		return err
	}
	if p.Status.Terminal() {
		if p.Status != command.Result {
			slog.Warn("late callback ignored", "payment_id", p.ID, "status", p.Status, "callback_status", command.Result)
		}
		return nil
	}
	if p.Status != payment.Processing || p.ProviderID != command.ProviderID {
		return errors.New("callback does not match the active provider payment")
	}
	return w.finish(ctx, command, p, result)
}

func (w *Worker) finish(ctx context.Context, command payment.Command, p payment.Payment, result provider.Result) error {
	event, err := payment.ChangeEvent(command, p, result.Status, result.ProviderID, result.FailureReason)
	if err != nil {
		return err
	}
	_, err = w.publish(ctx, event)
	return err
}

func (w *Worker) publish(ctx context.Context, event payment.Event) (payment.Payment, error) {
	if err := broker.Publish(ctx, w.Events, event.Payment.ID, event); err != nil {
		return payment.Payment{}, err
	}
	slog.Info("payment event published", "payment_id", event.Payment.ID, "event_id", event.ID)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		p, err := w.Store.Get(ctx, event.Payment.ID)
		if err == nil && p.Version >= event.Payment.Version {
			return p, nil
		}
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return payment.Payment{}, err
		}
		select {
		case <-ctx.Done():
			return payment.Payment{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) HandleEvent(ctx context.Context, message kafka.Message) error {
	var event payment.Event
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return err
	}
	if string(message.Key) != event.Payment.ID {
		return errors.New("event key does not match payment ID")
	}
	if err := w.Store.Apply(ctx, event); err != nil {
		return err
	}
	slog.Info("payment event stored", "payment_id", event.Payment.ID, "event_id", event.ID)
	return nil
}
