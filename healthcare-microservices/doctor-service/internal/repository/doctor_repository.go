package repository

import (
	"context"

	"doctor-service/internal/model"
)

// DoctorRepository is the persistence-port consumed by the use-case layer.
// It is unchanged from Assignment 2; only its implementation is swapped from
// an in-memory map to a PostgreSQL-backed implementation.
type DoctorRepository interface {
	Create(ctx context.Context, d *model.Doctor) error
	GetByID(ctx context.Context, id string) (*model.Doctor, error)
	List(ctx context.Context) ([]*model.Doctor, error)
}
