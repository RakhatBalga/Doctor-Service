package repository

import (
	"context"

	"appointment-service/internal/model"
)

// AppointmentRepository is the persistence port. Its signature is identical
// to Assignment 2; only the implementation switched from in-memory to
// PostgreSQL.
type AppointmentRepository interface {
	Create(ctx context.Context, a *model.Appointment) error
	GetByID(ctx context.Context, id string) (*model.Appointment, error)
	UpdateStatus(ctx context.Context, id string, status model.Status) (oldStatus model.Status, err error)
	List(ctx context.Context, doctorID string) ([]*model.Appointment, error)
}
