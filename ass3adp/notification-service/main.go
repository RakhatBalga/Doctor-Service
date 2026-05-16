package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"notification-service/internal/subscriber"
)

const defaultMaxRetries = 6

// main wires the entire notification-service. It has no gRPC server, no
// HTTP server, no database — only a NATS subscriber and a stdout logger.
// On a clean SIGINT/SIGTERM it drains in-flight messages and exits 0.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		logger.Error("NATS_URL is required")
		os.Exit(1)
	}

	sub := subscriber.New(natsURL, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := sub.ConnectWithBackoff(ctx, defaultMaxRetries); err != nil {
		logger.Error("could not connect to NATS, exiting", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if err := sub.Subscribe(); err != nil {
		logger.Error("subscribe failed", slog.String("error", err.Error()))
		_ = sub.Shutdown()
		os.Exit(1)
	}

	logger.Info("notification-service ready",
		slog.Any("subjects", subscriber.Subjects))

	<-ctx.Done()
	logger.Info("shutdown signal received, draining...")
	if err := sub.Shutdown(); err != nil {
		logger.Error("shutdown error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
