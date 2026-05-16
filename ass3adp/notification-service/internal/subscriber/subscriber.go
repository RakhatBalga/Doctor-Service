package subscriber

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Subjects this service is interested in. They mirror the contract from
// Section 6 of the assignment exactly.
var Subjects = []string{
	"doctors.created",
	"appointments.created",
	"appointments.status_updated",
}

// NATSSubscriber is the entire notification-service runtime: it connects
// to NATS, subscribes to the three known subjects, and prints one
// structured JSON log line per received event to stdout.
type NATSSubscriber struct {
	url    string
	logger *slog.Logger
	conn   *nats.Conn
	subs   []*nats.Subscription
	mu     sync.Mutex
}

func New(url string, logger *slog.Logger) *NATSSubscriber {
	if logger == nil {
		logger = slog.Default()
	}
	return &NATSSubscriber{url: url, logger: logger}
}

// ConnectWithBackoff dials NATS using an exponential backoff (1s, 2s, 4s, …)
// up to maxRetries. After the last failed attempt it returns the wrapped
// error so main can exit with a non-zero status code, as required by the
// assignment's error-handling table.
func (s *NATSSubscriber) ConnectWithBackoff(ctx context.Context, maxRetries int) error {
	if maxRetries <= 0 {
		maxRetries = 6
	}
	var lastErr error
	wait := time.Second
	for attempt := 1; attempt <= maxRetries; attempt++ {
		conn, err := nats.Connect(s.url,
			nats.Name("notification-service"),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(2*time.Second),
		)
		if err == nil {
			s.conn = conn
			s.logger.Info("connected to NATS",
				slog.String("url", s.url),
				slog.Int("attempt", attempt))
			return nil
		}
		lastErr = err
		s.logger.Warn("nats connect failed, will retry",
			slog.Int("attempt", attempt),
			slog.Int("max_retries", maxRetries),
			slog.String("error", err.Error()),
			slog.Duration("backoff", wait))

		select {
		case <-ctx.Done():
			return errors.Join(lastErr, ctx.Err())
		case <-time.After(wait):
		}
		wait *= 2
	}
	return fmt.Errorf("nats connect failed after %d attempts: %w", maxRetries, lastErr)
}

// Subscribe binds a handler to every subject in Subjects. NATS core does
// not require explicit acks — receiving the message is the ack — but we
// still log a clear warning if a JSON payload cannot be decoded so no
// message is silently dropped (per Section 7.3).
func (s *NATSSubscriber) Subscribe() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil {
		return errors.New("subscriber: not connected")
	}
	for _, subject := range Subjects {
		subjectCopy := subject
		sub, err := s.conn.Subscribe(subjectCopy, func(msg *nats.Msg) {
			s.handle(subjectCopy, msg.Data)
		})
		if err != nil {
			return fmt.Errorf("subscribe %q: %w", subject, err)
		}
		s.subs = append(s.subs, sub)
		s.logger.Info("subscribed", slog.String("subject", subject))
	}
	return nil
}

// handle is the per-message hot path. It deserialises the JSON payload
// into a generic map and prints exactly one structured log line — time,
// subject, full event — to stdout as required by Section 7.2.
func (s *NATSSubscriber) handle(subject string, data []byte) {
	var event map[string]any
	if err := json.Unmarshal(data, &event); err != nil {
		s.logger.Error("invalid event payload",
			slog.String("subject", subject),
			slog.String("error", err.Error()),
			slog.String("raw", string(data)))
		return
	}
	logLine := struct {
		Time    string         `json:"time"`
		Subject string         `json:"subject"`
		Event   map[string]any `json:"event"`
	}{
		Time:    time.Now().UTC().Format(time.RFC3339),
		Subject: subject,
		Event:   event,
	}
	out, err := json.Marshal(logLine)
	if err != nil {
		s.logger.Error("marshal log line",
			slog.String("subject", subject),
			slog.String("error", err.Error()))
		return
	}
	// Use fmt.Println directly so the user sees the canonical format
	// described in Section 7.2 — not the slog JSON wrapper.
	fmt.Println(string(out))
}

// Shutdown drains in-flight messages and closes the connection. It is
// idempotent: calling it twice is harmless.
func (s *NATSSubscriber) Shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return nil
	}
	for _, sub := range s.subs {
		_ = sub.Unsubscribe()
	}
	if err := s.conn.Drain(); err != nil {
		s.logger.Warn("nats drain failed", slog.String("error", err.Error()))
	}
	s.conn.Close()
	s.conn = nil
	return nil
}
