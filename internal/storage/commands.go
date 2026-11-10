package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

var errCommandConflict = errors.New("command ID reused with different data")

func (s *Store) CommandCompleted(ctx context.Context, id, fingerprint string) (bool, error) {
	var same bool
	err := s.DB.QueryRow(ctx, `SELECT fingerprint = $2 FROM processed_commands
		WHERE id = $1 AND fingerprint IS NOT NULL`, id, fingerprint).Scan(&same)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !same {
		return false, errCommandConflict
	}
	return true, nil
}

func (s *Store) MarkCommandCompleted(ctx context.Context, id, paymentID, fingerprint string) error {
	result, err := s.DB.Exec(ctx, `INSERT INTO processed_commands (id, payment_id, fingerprint)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET fingerprint = EXCLUDED.fingerprint,
			processed_at = CASE WHEN processed_commands.fingerprint IS NULL
				THEN EXCLUDED.processed_at ELSE processed_commands.processed_at END
		WHERE processed_commands.payment_id = EXCLUDED.payment_id
			AND (processed_commands.fingerprint IS NULL OR processed_commands.fingerprint = EXCLUDED.fingerprint)`,
		id, paymentID, fingerprint)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return errCommandConflict
	}
	return nil
}
