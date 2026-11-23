package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"payway/internal/payment"
	"payway/internal/provider"
	"payway/internal/storage"
	"payway/internal/telemetry"
)

type Commands struct {
	Store        *storage.Store
	Provider     *provider.Client
	PublishEvent func(context.Context, payment.Event) error
}

func (s *Commands) Handle(ctx context.Context, command payment.Command) error {
	if command.SchemaVersion != 1 || !payment.ValidID(command.PaymentID) || command.CreatedAt.IsZero() ||
		command.ID == "" || len(command.ID) > 200 {
		return fmt.Errorf("%w: invalid command envelope", payment.ErrInvalidMessage)
	}
	command.CreatedAt = command.CreatedAt.UTC()
	payload, err := json.Marshal(command)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	fingerprint := hex.EncodeToString(sum[:])
	completed, err := s.Store.CommandCompleted(ctx, command.ID, fingerprint)
	if err != nil {
		return err
	}
	if completed {
		slog.Info("duplicate payment command skipped", "payment_id", command.PaymentID, "command_id", command.ID, "trace_id", telemetry.TraceID(ctx))
		return nil
	}

	switch command.Type {
	case payment.CreatePayment:
		err = s.create(ctx, command)
	case payment.CancelPayment:
		if command.ID != "cancel_"+command.PaymentID {
			return fmt.Errorf("%w: invalid cancellation command ID", payment.ErrInvalidMessage)
		}
		err = s.cancel(ctx, command)
	case payment.ProviderCallback:
		err = s.callback(ctx, command)
	default:
		return fmt.Errorf("%w: unknown command type", payment.ErrInvalidMessage)
	}
	if err != nil {
		return err
	}
	return s.Store.MarkCommandCompleted(ctx, command.ID, command.PaymentID, fingerprint)
}

func (s *Commands) create(ctx context.Context, command payment.Command) error {
	event, err := payment.CreatedEvent(command)
	if err != nil {
		return fmt.Errorf("%w: %v", payment.ErrInvalidMessage, err)
	}
	p, err := s.Store.Get(ctx, command.PaymentID)
	if errors.Is(err, storage.ErrNotFound) {
		p, err = s.publish(ctx, event)
	}
	if err != nil {
		return err
	}
	if p.MerchantID != command.Data.MerchantID || p.Amount != command.Data.Amount || p.Currency != command.Data.Currency {
		return fmt.Errorf("%w: creation command conflicts with existing payment", payment.ErrInvalidMessage)
	}
	if p.Status.Terminal() {
		return nil
	}
	result, err := s.Provider.Start(ctx, p)
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
		p, err = s.publish(ctx, event)
		if err != nil {
			return err
		}
	}
	if result.Status.Terminal() && !p.Status.Terminal() {
		command.ID = "result_" + p.ID + "_" + string(result.Status)
		command.CreatedAt = result.UpdatedAt
		return s.finish(ctx, command, p, result)
	}
	return nil
}

func (s *Commands) cancel(ctx context.Context, command payment.Command) error {
	p, err := s.Store.Get(ctx, command.PaymentID)
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
		result, err = s.Provider.Cancel(ctx, p.ID)
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
	return s.finish(ctx, command, p, result)
}

func (s *Commands) callback(ctx context.Context, command payment.Command) error {
	result := provider.Result{
		PaymentID: command.PaymentID, ProviderID: command.ProviderID,
		Status: command.Result, FailureReason: command.FailureReason, UpdatedAt: command.CreatedAt,
	}
	if err := result.Validate(); err != nil {
		return fmt.Errorf("%w: %v", payment.ErrInvalidMessage, err)
	}
	if !result.Status.Terminal() {
		return fmt.Errorf("%w: callback status must be terminal", payment.ErrInvalidMessage)
	}
	p, err := s.Store.Get(ctx, command.PaymentID)
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
	return s.finish(ctx, command, p, result)
}

func (s *Commands) finish(ctx context.Context, command payment.Command, p payment.Payment, result provider.Result) error {
	event, err := payment.ChangeEvent(command, p, result.Status, result.ProviderID, result.FailureReason)
	if err != nil {
		return err
	}
	_, err = s.publish(ctx, event)
	return err
}

func (s *Commands) publish(ctx context.Context, event payment.Event) (payment.Payment, error) {
	if err := s.PublishEvent(ctx, event); err != nil {
		return payment.Payment{}, err
	}
	slog.Info("payment event published", "payment_id", event.Payment.ID, "event_id", event.ID, "trace_id", telemetry.TraceID(ctx))
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		p, err := s.Store.Get(ctx, event.Payment.ID)
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
