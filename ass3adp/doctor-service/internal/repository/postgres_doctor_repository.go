package repository

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"doctor-service/internal/model"

	"github.com/jackc/pgx/v5/pgconn"
)

// PostgresDoctorRepository implements DoctorRepository on top of a
// *sql.DB managed by the application bootstrap layer.
type PostgresDoctorRepository struct {
	db *sql.DB
}

// NewPostgresDoctorRepository wires the repository over an already-opened
// *sql.DB. Connection lifetime is the application's responsibility.
func NewPostgresDoctorRepository(db *sql.DB) *PostgresDoctorRepository {
	return &PostgresDoctorRepository{db: db}
}

const createDoctorQuery = `
INSERT INTO doctors (id, full_name, specialization, email, created_at)
VALUES ($1, $2, $3, $4, $5)
`

// Create persists a new doctor. Unique-constraint violations on email are
// translated into the domain-level model.ErrEmailExists.
func (r *PostgresDoctorRepository) Create(ctx context.Context, d *model.Doctor) error {
	_, err := r.db.ExecContext(ctx, createDoctorQuery,
		d.ID, d.FullName, d.Specialization, d.Email, d.CreatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		// SQLSTATE 23505 = unique_violation
		if errors.As(err, &pgErr) && pgErr.Code == "23505" &&
			strings.Contains(pgErr.ConstraintName, "email") {
			return model.ErrEmailExists
		}
		return err
	}
	return nil
}

const getDoctorByIDQuery = `
SELECT id, full_name, specialization, email, created_at
FROM doctors
WHERE id = $1
`

func (r *PostgresDoctorRepository) GetByID(ctx context.Context, id string) (*model.Doctor, error) {
	row := r.db.QueryRowContext(ctx, getDoctorByIDQuery, id)
	var d model.Doctor
	if err := row.Scan(&d.ID, &d.FullName, &d.Specialization, &d.Email, &d.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, model.ErrDoctorNotFound
		}
		return nil, err
	}
	return &d, nil
}

const listDoctorsQuery = `
SELECT id, full_name, specialization, email, created_at
FROM doctors
ORDER BY created_at ASC
`

func (r *PostgresDoctorRepository) List(ctx context.Context) ([]*model.Doctor, error) {
	rows, err := r.db.QueryContext(ctx, listDoctorsQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var doctors []*model.Doctor
	for rows.Next() {
		var d model.Doctor
		if err := rows.Scan(&d.ID, &d.FullName, &d.Specialization, &d.Email, &d.CreatedAt); err != nil {
			return nil, err
		}
		doctors = append(doctors, &d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return doctors, nil
}
