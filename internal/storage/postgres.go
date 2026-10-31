package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"payway/internal/payment"
)

var (
	ErrNotFound = errors.New("payment not found")
	ErrConflict = errors.New("idempotency key already used with different payment data")
)

type Store struct {
	DB *pgxpool.Pool
}

// Reserve stores request metadata only. The API never writes payment state.
// A reservation survives a failed Kafka publish so the client can retry safely.
func (s *Store) Reserve(ctx context.Context, id string, data payment.CreateRequest) (time.Time, error) {
	var createdAt time.Time
	err := s.DB.QueryRow(ctx, `
		INSERT INTO payment_requests (payment_id, fingerprint)
		VALUES ($1, $2)
		ON CONFLICT (payment_id) DO UPDATE
		SET fingerprint = payment_requests.fingerprint
		WHERE payment_requests.fingerprint = EXCLUDED.fingerprint
		RETURNING created_at`, id, data.Fingerprint()).Scan(&createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrConflict
	}
	return createdAt, err
}

func (s *Store) Get(ctx context.Context, id string) (payment.Payment, error) {
	var p payment.Payment
	err := s.DB.QueryRow(ctx, `
		SELECT id, merchant_id, amount, currency, status, version, created_at, updated_at
		FROM payments WHERE id = $1`, id).Scan(
		&p.ID, &p.MerchantID, &p.Amount, &p.Currency, &p.Status,
		&p.Version, &p.CreatedAt, &p.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, ErrNotFound
	}
	return p, err
}

func (s *Store) ApplyCreated(ctx context.Context, event payment.Event) error {
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

	p := event.Payment
	result, err := tx.Exec(ctx, `
		INSERT INTO payment_events (id, payment_id, command_id, type, version, payload)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		ON CONFLICT (id) DO NOTHING`,
		event.ID, p.ID, event.CommandID, event.Type, p.Version, string(payload))
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
		return tx.Commit(ctx)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO payments (id, merchant_id, amount, currency, status, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		p.ID, p.MerchantID, p.Amount, p.Currency, p.Status, p.Version, p.CreatedAt, p.UpdatedAt)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO processed_commands (id, payment_id) VALUES ($1, $2)`,
		event.CommandID, p.ID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

