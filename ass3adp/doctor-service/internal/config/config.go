package config

import (
	"errors"
	"os"
	"strings"
)

// Config groups all environment-driven settings for the Doctor Service.
type Config struct {
	GRPCPort    string
	DatabaseURL string
	NATSURL     string
}

// Load reads configuration from environment variables. DATABASE_URL has a
// fallback name (DB_DSN) per Section 13 of the assignment.
func Load() (Config, error) {
	cfg := Config{
		GRPCPort:    getEnv("DOCTOR_SERVICE_GRPC_PORT", ":50051"),
		DatabaseURL: firstNonEmpty(os.Getenv("DATABASE_URL"), os.Getenv("DB_DSN")),
		NATSURL:     os.Getenv("NATS_URL"),
	}
	if !strings.HasPrefix(cfg.GRPCPort, ":") {
		cfg.GRPCPort = ":" + cfg.GRPCPort
	}
	if cfg.DatabaseURL == "" {
		return cfg, errors.New("DATABASE_URL (or DB_DSN) is required")
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
