package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"payway/internal/payment"
	"payway/internal/telemetry"
)

var (
	ErrNotFound = errors.New("payment not found")
	ErrConflict = errors.New("idempotency key already used with different data")
)

type Store struct {
	DB *pgxpool.Pool
}

func (s *Store) Reserve(ctx context.Context, id string, data payment.CreateRequest) (time.Time, error) {
	return s.ReserveRequest(ctx, id, data.Fingerprint())
}

func (s *Store) ReserveRequest(ctx context.Context, id, fingerprint string) (time.Time, error) {
	var createdAt time.Time
	err := s.DB.QueryRow(ctx, `
		INSERT INTO payment_requests (payment_id, fingerprint)
		VALUES ($1, $2)
		ON CONFLICT (payment_id) DO UPDATE
		SET fingerprint = payment_requests.fingerprint
		WHERE payment_requests.fingerprint = EXCLUDED.fingerprint
		RETURNING created_at`, id, fingerprint).Scan(&createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrConflict
	}
	return createdAt.UTC(), err
}

func scanPayment(row pgx.Row) (payment.Payment, error) {
	var p payment.Payment
	err := row.Scan(&p.ID, &p.MerchantID, &p.Amount, &p.Currency, &p.Status,
		&p.Version, &p.CreatedAt, &p.UpdatedAt, &p.ProviderID, &p.FailureReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, ErrNotFound
	}
	p.CreatedAt = p.CreatedAt.UTC()
	p.UpdatedAt = p.UpdatedAt.UTC()
	return p, err
}

const paymentColumns = `id, merchant_id, amount, currency, status, version,
	created_at, updated_at, provider_id, failure_reason`

func (s *Store) Get(ctx context.Context, id string) (payment.Payment, error) {
	return scanPayment(s.DB.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payments WHERE id = $1`, id))
}

func (s *Store) History(ctx context.Context, id string, after, limit int) ([]payment.Event, error) {
	rows, err := s.DB.Query(ctx, `SELECT payload FROM payment_events
		WHERE payment_id = $1 AND version > $2 ORDER BY version LIMIT $3`, id, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]payment.Event, 0)
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var event payment.Event
		if err := json.Unmarshal(data, &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) Apply(ctx context.Context, event payment.Event) (retErr error) {
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
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	id := event.Payment.ID
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, id); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `
		INSERT INTO payment_events (id, payment_id, command_id, type, version, payload)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		ON CONFLICT (id) DO NOTHING`, event.ID, id, event.CommandID,
		event.Type, event.Payment.Version, string(payload))
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		var same bool
		if err := tx.QueryRow(ctx, `SELECT payload = $2::jsonb FROM payment_events WHERE id = $1`,
			event.ID, string(payload)).Scan(&same); err != nil {
			return err
		}
		if !same {
			return errors.New("event ID reused with different data")
		}
	}

	current, err := scanPayment(tx.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payments WHERE id = $1`, id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	var applied []payment.Payment
	for {
		var data []byte
		err := tx.QueryRow(ctx, `SELECT payload FROM payment_events WHERE payment_id = $1 AND version = $2`,
			id, current.Version+1).Scan(&data)
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		var next payment.Event
		if err := json.Unmarshal(data, &next); err != nil {
			return err
		}
		p := next.Payment
		if err := payment.ValidateTransition(current.Status, p.Status); err != nil {
			return err
		}
		if current.Version > 0 && (p.MerchantID != current.MerchantID || p.Amount != current.Amount ||
			p.Currency != current.Currency || !p.CreatedAt.Equal(current.CreatedAt) ||
			p.UpdatedAt.Before(current.UpdatedAt) || (current.ProviderID != "" && p.ProviderID != current.ProviderID)) {
			return errors.New("event changes immutable payment data or moves time backwards")
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO payments (`+paymentColumns+`)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, version = EXCLUDED.version,
			updated_at = EXCLUDED.updated_at, provider_id = EXCLUDED.provider_id,
			failure_reason = EXCLUDED.failure_reason`, p.ID, p.MerchantID, p.Amount, p.Currency,
			p.Status, p.Version, p.CreatedAt, p.UpdatedAt, p.ProviderID, p.FailureReason)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO processed_commands (id, payment_id) VALUES ($1, $2)
			ON CONFLICT (id) DO NOTHING`, next.CommandID, id)
		if err != nil {
			return err
		}
		current = p
		applied = append(applied, p)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	for _, p := range applied {
		telemetry.PaymentApplied(ctx, p)
	}
	return nil
}

func (s *Store) TraceParent(ctx context.Context, id string) (string, error) {
	var parent string
	err := s.DB.QueryRow(ctx, `SELECT traceparent FROM payment_requests WHERE payment_id = $1`, id).Scan(&parent)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return parent, err
}

func (s *Store) RememberTrace(ctx context.Context, id, parent string) (string, error) {
	var stored string
	err := s.DB.QueryRow(ctx, `UPDATE payment_requests
		SET traceparent = CASE WHEN traceparent = '' THEN $2 ELSE traceparent END
		WHERE payment_id = $1 RETURNING traceparent`, id, parent).Scan(&stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return parent, nil
	}
	return stored, err
}
