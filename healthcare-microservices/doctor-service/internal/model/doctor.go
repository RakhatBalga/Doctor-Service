package model

import (
	"errors"
	"strings"
	"time"
)

// Doctor is the core domain entity. It does not depend on the transport
// layer (gRPC) or the persistence layer (PostgreSQL).
type Doctor struct {
	ID             string
	FullName       string
	Specialization string
	Email          string
	CreatedAt      time.Time
}

// Domain errors carry semantic meaning that the use-case layer translates
// into transport-specific errors (e.g. gRPC codes).
var (
	ErrInvalidInput   = errors.New("invalid doctor input")
	ErrDoctorNotFound = errors.New("doctor not found")
	ErrEmailExists    = errors.New("doctor with this email already exists")
)

// Validate enforces invariants shared by every layer of the application.
func (d *Doctor) Validate() error {
	if strings.TrimSpace(d.FullName) == "" {
		return errors.Join(ErrInvalidInput, errors.New("full_name is required"))
	}
	if strings.TrimSpace(d.Email) == "" {
		return errors.Join(ErrInvalidInput, errors.New("email is required"))
	}
	if !strings.Contains(d.Email, "@") {
		return errors.Join(ErrInvalidInput, errors.New("email is malformed"))
	}
	return nil
}
