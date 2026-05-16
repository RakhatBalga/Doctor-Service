package logger

import (
	"encoding/json"
	"log/slog"
)

// EventLogger logs events to stdout.
type EventLogger struct {
	logger *slog.Logger
}

func NewEventLogger(logger *slog.Logger) *EventLogger {
	if logger == nil {
		logger = slog.Default()
	}
	return &EventLogger{logger: logger}
}

func (l *EventLogger) LogEvent(subject string, payload []byte) {
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		l.logger.Error("failed to decode event", slog.String("subject", subject), slog.String("error", err.Error()))
		return
	}
	
	// Print exactly one structured JSON log line per event
	l.logger.Info("event received", 
		slog.String("subject", subject),
		slog.Any("payload", data),
	)
}
