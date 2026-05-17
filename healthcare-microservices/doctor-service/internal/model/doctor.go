package model

import (
	"errors"
	"strings"
	"time"
)

type Doctor struct {
	ID             string
	FullName       string
	Specialization string
	Email          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

var (
	ErrInvalidInput   = errors.New("invalid doctor input")
	ErrDoctorNotFound = errors.New("doctor not found")
	ErrEmailExists    = errors.New("doctor with this email already exists")
)

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
