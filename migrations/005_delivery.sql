BEGIN;

ALTER TABLE payment_requests ADD COLUMN IF NOT EXISTS command_enqueued BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS command_outbox (
    position BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    command_id TEXT NOT NULL UNIQUE,
    payment_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    traceparent TEXT NOT NULL,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rejected_messages (
    consumer_group TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    message_offset BIGINT NOT NULL,
    message_key BYTEA,
    payload BYTEA,
    reason TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition, message_offset)
);

COMMIT;
