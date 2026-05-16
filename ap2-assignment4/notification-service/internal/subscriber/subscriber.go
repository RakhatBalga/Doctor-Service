package subscriber

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"

	"notification-service/internal/jobqueue"
	"notification-service/internal/logger"
)

var Subjects = []string{
	"doctors.created",
	"appointments.created",
	"appointments.status_updated",
}

type Subscriber struct {
	conn      *nats.Conn
	url       string
	logger    *slog.Logger
	evtLogger *logger.EventLogger
	queue     *jobqueue.JobQueue
	subs      []*nats.Subscription
}

func New(url string, logger *slog.Logger, evtLogger *logger.EventLogger, queue *jobqueue.JobQueue) *Subscriber {
	if logger == nil {
		logger = slog.Default()
	}
	return &Subscriber{
		url:       url,
		logger:    logger,
		evtLogger: evtLogger,
		queue:     queue,
	}
}

func (s *Subscriber) ConnectWithBackoff(ctx context.Context, maxRetries int) error {
	var err error
	for i := 0; i <= maxRetries; i++ {
		s.conn, err = nats.Connect(s.url,
			nats.Name("notification-service"),
			nats.MaxReconnects(-1),
		)
		if err == nil {
			s.logger.Info("connected to NATS", slog.String("url", s.url))
			return nil
		}
		s.logger.Warn("NATS connection failed, retrying",
			slog.Int("attempt", i+1), slog.String("error", err.Error()))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second * time.Duration(1<<i)): // exponential backoff
		}
	}
	return fmt.Errorf("exhausted %d retries: %w", maxRetries, err)
}

func (s *Subscriber) Subscribe() error {
	for _, subject := range Subjects {
		sub, err := s.conn.Subscribe(subject, s.handleMessage)
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", subject, err)
		}
		s.subs = append(s.subs, sub)
	}
	return nil
}

func (s *Subscriber) Shutdown() error {
	for _, sub := range s.subs {
		_ = sub.Unsubscribe()
	}
	if s.conn != nil {
		s.conn.Close()
	}
	return nil
}

func (s *Subscriber) handleMessage(msg *nats.Msg) {
	// 1. Log event
	s.evtLogger.LogEvent(msg.Subject, msg.Data)

	// 2. Queue job if it's appointments.status_updated with new_status = "done"
	if msg.Subject == "appointments.status_updated" {
		var payload struct {
			EventType  string `json:"event_type"`
			OccurredAt string `json:"occurred_at"`
			ID         string `json:"id"`
			DoctorID   string `json:"doctor_id"`
			OldStatus  string `json:"old_status"`
			NewStatus  string `json:"new_status"`
		}
		if err := json.Unmarshal(msg.Data, &payload); err == nil {
			if payload.NewStatus == "done" {
				// Create idempotency key: SHA-256 hex of event_type + id + occurred_at
				hashStr := payload.EventType + payload.ID + payload.OccurredAt
				hashBytes := sha256.Sum256([]byte(hashStr))
				idempotencyKey := hex.EncodeToString(hashBytes[:])

				job := jobqueue.Job{
					IdempotencyKey: idempotencyKey,
					AppointmentID:  payload.ID,
					DoctorID:       payload.DoctorID,
					OccurredAt:     payload.OccurredAt,
					Channel:        "email",
					Recipient:      "patient@clinic.kz",
					Message:        fmt.Sprintf("Your appointment %s with doctor %s is complete.", payload.ID, payload.DoctorID),
				}
				s.queue.Enqueue(job)
			}
		}
	}
}
