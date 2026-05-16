package event

import "context"

// EventPublisher abstracts the message broker so the use-case layer never
// depends on NATS or RabbitMQ directly. Swapping brokers is a matter of
// providing a different implementation.
type EventPublisher interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Close() error
}

// DoctorCreatedSubject is the canonical subject / routing key used for
// doctor creation events across the entire platform.
const DoctorCreatedSubject = "doctors.created"
