package event

import (
	"context"
	"errors"
	"log/slog"

	"github.com/nats-io/nats.go"
)

// NATSPublisher is a thin EventPublisher implementation over core NATS
// (no JetStream). It is fire-and-forget by design.
type NATSPublisher struct {
	conn   *nats.Conn
	logger *slog.Logger
}

// NewNATSPublisher dials the broker and returns a ready-to-use publisher.
// The caller owns Close().
func NewNATSPublisher(url string, logger *slog.Logger) (*NATSPublisher, error) {
	if logger == nil {
		logger = slog.Default()
	}
	conn, err := nats.Connect(url,
		nats.Name("doctor-service"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(nats.DefaultReconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				logger.Warn("nats disconnected", slog.String("error", err.Error()))
			}
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			logger.Info("nats reconnected", slog.String("url", c.ConnectedUrl()))
		}),
	)
	if err != nil {
		return nil, err
	}
	return &NATSPublisher{conn: conn, logger: logger}, nil
}

// Publish forwards the payload to NATS. Failures are returned to the caller
// so the use-case layer can log them, but per the assignment they must NOT
// be allowed to bubble up to the gRPC response.
func (p *NATSPublisher) Publish(_ context.Context, subject string, payload []byte) error {
	if p == nil || p.conn == nil {
		return errors.New("nats publisher not connected")
	}
	return p.conn.Publish(subject, payload)
}

// Close drains and closes the underlying NATS connection.
func (p *NATSPublisher) Close() error {
	if p == nil || p.conn == nil {
		return nil
	}
	if err := p.conn.Drain(); err != nil {
		p.logger.Warn("nats drain failed", slog.String("error", err.Error()))
	}
	p.conn.Close()
	return nil
}
