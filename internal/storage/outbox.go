package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"payway/internal/payment"
)

type OutboxCommand struct {
	ID          string
	PaymentID   string
	Payload     json.RawMessage
	TraceParent string
}

func (s *Store) EnqueueCommand(ctx context.Context, requestID, fingerprint, traceParent string, command payment.Command) (string, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var createdAt time.Time
	var enqueued bool
	var parent string
	err = tx.QueryRow(ctx, `
		INSERT INTO payment_requests (payment_id, fingerprint, traceparent)
		VALUES ($1, $2, $3)
		ON CONFLICT (payment_id) DO UPDATE SET traceparent =
			COALESCE(NULLIF(payment_requests.traceparent, ''), EXCLUDED.traceparent)
		WHERE payment_requests.fingerprint = EXCLUDED.fingerprint
		RETURNING created_at, command_enqueued, traceparent`, requestID, fingerprint, traceParent).
		Scan(&createdAt, &enqueued, &parent)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrConflict
	}
	if err != nil {
		return "", err
	}
	if !enqueued {
		if command.CreatedAt.IsZero() {
			command.CreatedAt = createdAt.UTC()
		}
		payload, err := json.Marshal(command)
		if err != nil {
			return "", err
		}
		_, err = tx.Exec(ctx, `INSERT INTO command_outbox (command_id, payment_id, payload, traceparent)
			VALUES ($1, $2, $3::jsonb, $4)`, command.ID, command.PaymentID, string(payload), parent)
		if err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `UPDATE payment_requests SET command_enqueued = TRUE WHERE payment_id = $1`, requestID); err != nil {
			return "", err
		}
	}
	return parent, tx.Commit(ctx)
}

func (s *Store) DispatchCommand(ctx context.Context, publish func(context.Context, OutboxCommand) error) (bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var position int64
	var command OutboxCommand
	err = tx.QueryRow(ctx, `SELECT position, command_id, payment_id, payload, traceparent
		FROM command_outbox ORDER BY position LIMIT 1 FOR UPDATE`).Scan(
		&position, &command.ID, &command.PaymentID, &command.Payload, &command.TraceParent)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := publish(ctx, command); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM command_outbox WHERE position = $1`, position); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
