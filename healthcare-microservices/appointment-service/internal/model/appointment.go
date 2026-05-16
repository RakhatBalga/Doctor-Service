package model

import (
	"errors"
	"strings"
	"time"
)

// Status is the finite state machine each appointment lives inside.
type Status string

const (
	StatusNew        Status = "new"
	StatusInProgress Status = "in_progress"
	StatusDone       Status = "done"
	StatusCancelled  Status = "cancelled"
)

// IsValid reports whether s is one of the allowed statuses.
func (s Status) IsValid() bool {
	switch s {
	case StatusNew, StatusInProgress, StatusDone, StatusCancelled:
		return true
	}
	return false
}

// Appointment is the core domain entity for the appointment context.
type Appointment struct {
	ID          string
	Title       string
	Description string
	DoctorID    string
	Status      Status
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

var (
	ErrInvalidInput        = errors.New("invalid appointment input")
	ErrAppointmentNotFound = errors.New("appointment not found")
	ErrInvalidStatus       = errors.New("invalid appointment status")
	ErrDoctorUnavailable   = errors.New("doctor service is unreachable")
	ErrDoctorNotFound      = errors.New("doctor not found")
)

// Validate enforces invariants required to persist a new appointment.
func (a *Appointment) Validate() error {
	if strings.TrimSpace(a.Title) == "" {
		return errors.Join(ErrInvalidInput, errors.New("title is required"))
	}
	if strings.TrimSpace(a.DoctorID) == "" {
		return errors.Join(ErrInvalidInput, errors.New("doctor_id is required"))
	}
	if !a.Status.IsValid() {
		return ErrInvalidStatus
	}
	return nil
}
