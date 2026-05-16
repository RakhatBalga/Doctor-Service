package jobqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type JobQueue struct {
	client     *redis.Client
	gatewayURL string
	jobs       chan Job
	wg         sync.WaitGroup
	logger     *slog.Logger
	errLogger  *slog.Logger
	httpClient *http.Client
}

func NewJobQueue(redisURL, gatewayURL string, poolSize int, logger *slog.Logger) (*JobQueue, error) {
	if logger == nil {
		logger = slog.Default()
	}
	errLogger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	client := redis.NewClient(opts)

	jq := &JobQueue{
		client:     client,
		gatewayURL: gatewayURL,
		jobs:       make(chan Job, 100), // buffered channel
		logger:     logger,
		errLogger:  errLogger,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	for i := 0; i < poolSize; i++ {
		jq.wg.Add(1)
		go jq.worker()
	}

	return jq, nil
}

func (jq *JobQueue) Enqueue(job Job) {
	jq.logger.Info("job state",
		slog.String("time", time.Now().UTC().Format(time.RFC3339)),
		slog.String("level", "info"),
		slog.String("job_id", job.IdempotencyKey),
		slog.Int("attempt", 0),
		slog.String("status", "enqueued"),
	)
	jq.jobs <- job
}

func (jq *JobQueue) Shutdown() {
	close(jq.jobs)
	jq.wg.Wait()
	jq.client.Close()
}

func (jq *JobQueue) worker() {
	defer jq.wg.Done()
	for job := range jq.jobs {
		jq.process(job)
	}
}

func (jq *JobQueue) process(job Job) {
	ctx := context.Background()

	// Check idempotency key
	val, err := jq.client.Get(ctx, job.IdempotencyKey).Result()
	if err == nil && val == "done" {
		jq.logger.Info("duplicate job dropped", slog.String("job_id", job.IdempotencyKey))
		return // Silently drop
	}

	maxAttempts := 3
	backoffs := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		jq.logger.Info("job state",
			slog.String("time", time.Now().UTC().Format(time.RFC3339)),
			slog.String("level", "info"),
			slog.String("job_id", job.IdempotencyKey),
			slog.Int("attempt", attempt),
			slog.String("status", "processing"),
		)

		err := jq.callGateway(ctx, job)
		if err == nil {
			// Success
			jq.client.Set(ctx, job.IdempotencyKey, "done", 24*time.Hour)
			jq.logger.Info("job state",
				slog.String("time", time.Now().UTC().Format(time.RFC3339)),
				slog.String("level", "info"),
				slog.String("job_id", job.IdempotencyKey),
				slog.Int("attempt", attempt),
				slog.String("status", "success"),
			)
			return
		}

		if attempt < maxAttempts {
			jq.logger.Warn("job state",
				slog.String("time", time.Now().UTC().Format(time.RFC3339)),
				slog.String("level", "warn"),
				slog.String("job_id", job.IdempotencyKey),
				slog.Int("attempt", attempt),
				slog.String("status", "retry"),
				slog.String("error", err.Error()),
			)
			time.Sleep(backoffs[attempt-1])
		} else {
			// Dead letter
			jq.errLogger.Error("job state",
				slog.String("time", time.Now().UTC().Format(time.RFC3339)),
				slog.String("level", "error"),
				slog.String("job_id", job.IdempotencyKey),
				slog.Int("attempt", attempt),
				slog.String("status", "dead_letter"),
				slog.String("error", err.Error()),
			)
		}
	}
}

func (jq *JobQueue) callGateway(ctx context.Context, job Job) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", jq.gatewayURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := jq.httpClient.Do(req)
	if err != nil {
		return err // Network error
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}
