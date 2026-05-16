package main

import (
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"
)

type NotifyRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
	Channel        string `json:"channel"`
	Recipient      string `json:"recipient"`
	Message        string `json:"message"`
}

var (
	processedKeys = make(map[string]bool)
	mu            sync.Mutex
	logger        *slog.Logger
)

func main() {
	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	rand.Seed(time.Now().UnixNano())

	http.HandleFunc("/notify", notifyHandler)

	logger.Info("mock gateway listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		logger.Error("server failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func notifyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req NotifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Error("failed to decode request", slog.String("error", err.Error()))
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	// Log received request
	logger.Info("request received",
		slog.String("idempotency_key", req.IdempotencyKey),
		slog.String("channel", req.Channel),
		slog.String("recipient", req.Recipient),
		slog.String("message", req.Message),
	)

	//demo failure
	simulateFailure := false

	if simulateFailure {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "service_unavailable"})
		return
	}

	mu.Lock()
	defer mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if processedKeys[req.IdempotencyKey] {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "duplicate"})
		return
	}

	processedKeys[req.IdempotencyKey] = true
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}
