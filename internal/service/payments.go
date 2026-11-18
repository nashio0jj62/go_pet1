package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"

	"payway/internal/payment"
	"payway/internal/provider"
	"payway/internal/storage"
	"payway/internal/telemetry"
)

type Payments struct {
	Store          *storage.Store
	PublishCommand func(context.Context, payment.Command) error
}

type ErrorKind int

const (
	Invalid ErrorKind = iota + 1
	NotFound
	Conflict
	Unavailable
)

type Error struct {
	Kind    ErrorKind
	Message string
	Cause   error
}

func (e *Error) Error() string {
	return e.Message
}

func (e *Error) Unwrap() error {
	return e.Cause
}

func (s *Payments) Create(ctx context.Context, key string, request payment.CreateRequest) (string, error) {
	if err := request.Validate(); err != nil {
		return "", &Error{Kind: Invalid, Message: err.Error()}
	}
	id := payment.ID(request.MerchantID, key)
	createdAt, err := s.Store.ReserveRequest(ctx, id, request.Fingerprint())
	if errors.Is(err, storage.ErrConflict) {
		return "", &Error{Kind: Conflict, Message: err.Error()}
	}
	if err != nil {
		return "", &Error{Kind: Unavailable, Message: "request storage unavailable; retry with the same Idempotency-Key", Cause: err}
	}
	ctx = s.tracePayment(ctx, id)
	command := payment.NewCommand(id, request, createdAt)
	if err := s.PublishCommand(ctx, command); err != nil {
		return "", &Error{Kind: Unavailable, Message: "publication not confirmed; retry with the same Idempotency-Key", Cause: err}
	}
	slog.Info("payment command accepted", "payment_id", id, "command_id", command.ID, "trace_id", telemetry.TraceID(ctx))
	return id, nil
}

func (s *Payments) Get(ctx context.Context, id string) (payment.Payment, error) {
	return s.get(s.tracePayment(ctx, id), id)
}

func (s *Payments) History(ctx context.Context, id string, after, limit int) ([]payment.Event, error) {
	ctx = s.tracePayment(ctx, id)
	if _, err := s.get(ctx, id); err != nil {
		return nil, err
	}
	events, err := s.Store.History(ctx, id, after, limit)
	if err != nil {
		return nil, &Error{Kind: Unavailable, Message: "payment storage unavailable", Cause: err}
	}
	return events, nil
}

func (s *Payments) Cancel(ctx context.Context, id string) (string, error) {
	ctx = s.tracePayment(ctx, id)
	p, err := s.get(ctx, id)
	if err != nil {
		return "", err
	}
	if p.Status == payment.Completed || p.Status == payment.Failed {
		return "", &Error{Kind: Conflict, Message: "payment is already finished"}
	}
	commandID := "cancel_" + id
	createdAt, err := s.Store.ReserveRequest(ctx, commandID, commandID)
	if err != nil {
		return "", &Error{Kind: Unavailable, Message: "request storage unavailable", Cause: err}
	}
	return s.publish(ctx, payment.Command{
		ID: commandID, Type: payment.CancelPayment, SchemaVersion: 1,
		PaymentID: id, CreatedAt: createdAt,
	})
}

func (s *Payments) Callback(ctx context.Context, callback provider.Callback) (string, error) {
	if err := callback.Result.Validate(); err != nil {
		return "", &Error{Kind: Invalid, Message: err.Error()}
	}
	if payment.ValidateKey(callback.EventID) != nil || !callback.Status.Terminal() {
		return "", &Error{Kind: Invalid, Message: "invalid callback event ID or status"}
	}
	ctx = s.tracePayment(ctx, callback.PaymentID)
	p, err := s.get(ctx, callback.PaymentID)
	if err != nil {
		return "", err
	}
	if p.ProviderID != "" && p.ProviderID != callback.ProviderID {
		return "", &Error{Kind: Conflict, Message: "provider reference does not match payment"}
	}
	idSum := sha256.Sum256([]byte(callback.EventID))
	commandID := "callback_" + hex.EncodeToString(idSum[:])
	canonical, err := json.Marshal(callback)
	if err != nil {
		return "", &Error{Kind: Invalid, Message: "invalid callback data"}
	}
	bodySum := sha256.Sum256(canonical)
	_, err = s.Store.ReserveRequest(ctx, commandID, hex.EncodeToString(bodySum[:]))
	if errors.Is(err, storage.ErrConflict) {
		return "", &Error{Kind: Conflict, Message: "callback event ID reused with different data"}
	}
	if err != nil {
		return "", &Error{Kind: Unavailable, Message: "request storage unavailable", Cause: err}
	}
	return s.publish(ctx, payment.Command{
		ID: commandID, Type: payment.ProviderCallback, SchemaVersion: 1,
		PaymentID: callback.PaymentID, CreatedAt: callback.UpdatedAt.UTC(),
		ProviderID: callback.ProviderID, Result: callback.Status, FailureReason: callback.FailureReason,
	})
}

func (s *Payments) get(ctx context.Context, id string) (payment.Payment, error) {
	p, err := s.Store.Get(ctx, id)
	if errors.Is(err, storage.ErrNotFound) {
		return payment.Payment{}, &Error{Kind: NotFound, Message: "payment not found or not yet projected"}
	}
	if err != nil {
		return payment.Payment{}, &Error{Kind: Unavailable, Message: "payment storage unavailable", Cause: err}
	}
	return p, nil
}

func (s *Payments) publish(ctx context.Context, command payment.Command) (string, error) {
	if err := s.PublishCommand(ctx, command); err != nil {
		return "", &Error{Kind: Unavailable, Message: "publication not confirmed; retry the same request", Cause: err}
	}
	return command.ID, nil
}

func (s *Payments) tracePayment(ctx context.Context, id string) context.Context {
	parent, err := s.Store.TraceParent(ctx, id)
	if err != nil {
		slog.Warn("read payment trace", "payment_id", id, "error", err)
	}
	if parent != "" {
		storedCtx := telemetry.Parent(ctx, parent)
		if telemetry.TraceID(storedCtx) != telemetry.TraceID(ctx) {
			ctx = storedCtx
		}
	}
	ctx = telemetry.StartPaymentRequest(ctx, id)
	if parent == "" && telemetry.TraceParent(ctx) != "" {
		stored, err := s.Store.RememberTrace(ctx, id, telemetry.TraceParent(ctx))
		if err != nil {
			slog.Warn("save payment trace", "payment_id", id, "error", err)
		} else if stored != telemetry.TraceParent(ctx) {
			ctx = telemetry.StartPaymentRequest(telemetry.Parent(ctx, stored), id)
		}
	}
	return ctx
}
