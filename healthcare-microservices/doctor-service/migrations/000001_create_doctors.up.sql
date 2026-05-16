CREATE TABLE IF NOT EXISTS doctors (
    id             TEXT PRIMARY KEY,
    full_name      TEXT NOT NULL,
    specialization TEXT NOT NULL DEFAULT '',
    email          TEXT NOT NULL UNIQUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_doctors_specialization ON doctors (specialization);
