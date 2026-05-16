CREATE TABLE IF NOT EXISTS appointments (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    doctor_id   TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'new',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_appointments_doctor_id ON appointments (doctor_id);
CREATE INDEX IF NOT EXISTS idx_appointments_status ON appointments (status);
