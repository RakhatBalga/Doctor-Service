package event

import (
	"context"
	"errors"
	"log/slog"

	"github.com/nats-io/nats.go"
)

// NATSPublisher is a fire-and-forget core NATS publisher. JetStream is
// intentionally not used per the assignment's NATS option.
type NATSPublisher struct {
	conn   *nats.Conn
	logger *slog.Logger
}

func NewNATSPublisher(url string, logger *slog.Logger) (*NATSPublisher, error) {
	if logger == nil {
		logger = slog.Default()
	}
	conn, err := nats.Connect(url,
		nats.Name("appointment-service"),
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

func (p *NATSPublisher) Publish(_ context.Context, subject string, payload []byte) error {
	if p == nil || p.conn == nil {
		return errors.New("nats publisher not connected")
	}
	return p.conn.Publish(subject, payload)
}

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
