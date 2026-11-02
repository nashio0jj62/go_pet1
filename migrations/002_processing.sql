BEGIN;

ALTER TABLE payments ADD COLUMN IF NOT EXISTS provider_id TEXT NOT NULL DEFAULT '';
ALTER TABLE payments ADD COLUMN IF NOT EXISTS failure_reason TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS mock_provider_payments (
    payment_id TEXT PRIMARY KEY,
    merchant_id TEXT NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PROCESSING', 'COMPLETED', 'FAILED', 'CANCELED')),
    failure_reason TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ready_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '8 seconds',
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    callback_sent_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS mock_provider_callbacks_idx
    ON mock_provider_payments (next_attempt_at) WHERE callback_sent_at IS NULL;

COMMIT;
