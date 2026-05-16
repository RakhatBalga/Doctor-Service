package event

import "context"

// EventPublisher is the broker port used by the appointment use case.
// Concrete implementations live alongside (NATS, noop).
type EventPublisher interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Close() error
}

// Subjects published by the Appointment Service. Centralising them avoids
// typos that could silently break subscribers.
const (
	AppointmentCreatedSubject       = "appointments.created"
	AppointmentStatusUpdatedSubject = "appointments.status_updated"
)
