package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"notification-service/internal/jobqueue"
	"notification-service/internal/logger"
	"notification-service/internal/subscriber"
)

const defaultMaxRetries = 6

func main() {
	logg := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logg)

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		logg.Error("NATS_URL is required")
		os.Exit(1)
	}

	redisURL := getEnv("REDIS_URL", "redis://localhost:6379")
	gatewayURL := getEnv("GATEWAY_URL", "http://localhost:8080/notify")
	poolSizeStr := getEnv("WORKER_POOL_SIZE", "3")
	poolSize, _ := strconv.Atoi(poolSizeStr)

	evtLogger := logger.NewEventLogger(logg)
	queue, err := jobqueue.NewJobQueue(redisURL, gatewayURL, poolSize, logg)
	if err != nil {
		logg.Error("failed to init job queue", slog.String("error", err.Error()))
		os.Exit(1)
	}

	sub := subscriber.New(natsURL, logg, evtLogger, queue)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := sub.ConnectWithBackoff(ctx, defaultMaxRetries); err != nil {
		logg.Error("could not connect to NATS, exiting", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if err := sub.Subscribe(); err != nil {
		logg.Error("subscribe failed", slog.String("error", err.Error()))
		_ = sub.Shutdown()
		os.Exit(1)
	}

	logg.Info("notification-service ready",
		slog.Any("subjects", subscriber.Subjects))

	<-ctx.Done()
	logg.Info("shutdown signal received, draining...")
	queue.Shutdown()
	if err := sub.Shutdown(); err != nil {
		logg.Error("shutdown error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

