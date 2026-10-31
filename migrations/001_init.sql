BEGIN;

-- Technical request reservations; these do not represent payment state.
CREATE TABLE payment_requests (
    payment_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE payments (
    id TEXT PRIMARY KEY,
    merchant_id TEXT NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL CHECK (currency IN ('KZT', 'RUB', 'USD', 'EUR')),
    status TEXT NOT NULL CHECK (status IN ('NEW', 'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELED')),
    version INTEGER NOT NULL CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX payments_merchant_created_idx ON payments (merchant_id, created_at DESC);

CREATE TABLE payment_events (
    id TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    type TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    payload JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (payment_id, version)
);

CREATE TABLE processed_commands (
    id TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL REFERENCES payments (id),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
