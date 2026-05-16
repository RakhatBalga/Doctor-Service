package repository

import (
	"context"
	"database/sql"
	"errors"

	"appointment-service/internal/model"
)

// PostgresAppointmentRepository implements AppointmentRepository on a
// *sql.DB. UpdateStatus uses a transaction so the read-modify-write cycle
// is atomic.
type PostgresAppointmentRepository struct {
	db *sql.DB
}

func NewPostgresAppointmentRepository(db *sql.DB) *PostgresAppointmentRepository {
	return &PostgresAppointmentRepository{db: db}
}

const createAppointmentQuery = `
INSERT INTO appointments (id, title, description, doctor_id, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
`

func (r *PostgresAppointmentRepository) Create(ctx context.Context, a *model.Appointment) error {
	_, err := r.db.ExecContext(ctx, createAppointmentQuery,
		a.ID, a.Title, a.Description, a.DoctorID, string(a.Status), a.CreatedAt, a.UpdatedAt,
	)
	return err
}

const getAppointmentQuery = `
SELECT id, title, description, doctor_id, status, created_at, updated_at
FROM appointments
WHERE id = $1
`

func (r *PostgresAppointmentRepository) GetByID(ctx context.Context, id string) (*model.Appointment, error) {
	row := r.db.QueryRowContext(ctx, getAppointmentQuery, id)
	a, err := scanAppointment(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, model.ErrAppointmentNotFound
		}
		return nil, err
	}
	return a, nil
}

// UpdateStatus uses a serializable transaction so the read-modify-write
// cycle cannot interleave with another concurrent update. The previous
// status is returned to the use case so it can be embedded in the
// "appointments.status_updated" event.
func (r *PostgresAppointmentRepository) UpdateStatus(ctx context.Context, id string, status model.Status) (model.Status, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()

	var current string
	err = tx.QueryRowContext(ctx,
		`SELECT status FROM appointments WHERE id = $1 FOR UPDATE`, id).Scan(&current)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", model.ErrAppointmentNotFound
		}
		return "", err
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE appointments SET status = $1, updated_at = now() WHERE id = $2`,
		string(status), id); err != nil {
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", err
	}
	return model.Status(current), nil
}

const listAppointmentsBaseQuery = `
SELECT id, title, description, doctor_id, status, created_at, updated_at
FROM appointments
`

func (r *PostgresAppointmentRepository) List(ctx context.Context, doctorID string) ([]*model.Appointment, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if doctorID == "" {
		rows, err = r.db.QueryContext(ctx, listAppointmentsBaseQuery+`ORDER BY created_at ASC`)
	} else {
		rows, err = r.db.QueryContext(ctx, listAppointmentsBaseQuery+`WHERE doctor_id = $1 ORDER BY created_at ASC`, doctorID)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*model.Appointment
	for rows.Next() {
		a, err := scanAppointment(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// scanAppointment hides the column ordering in one place so additions to
// the SELECT lists above don't drift across functions.
func scanAppointment(scan func(...any) error) (*model.Appointment, error) {
	var (
		a      model.Appointment
		status string
	)
	if err := scan(&a.ID, &a.Title, &a.Description, &a.DoctorID, &status, &a.CreatedAt, &a.UpdatedAt); err != nil {
		return nil, err
	}
	a.Status = model.Status(status)
	return &a, nil
}
