BEGIN;

-- Legacy rows prove only that an event was applied, so leave their fingerprints NULL.
ALTER TABLE processed_commands ADD COLUMN IF NOT EXISTS fingerprint TEXT;

COMMIT;
