package config

import (
	"errors"
	"os"
	"strings"
)

// Config groups all environment-driven settings for the Appointment Service.
type Config struct {
	GRPCPort         string
	DatabaseURL      string
	NATSURL          string
	DoctorServiceURL string
}

func Load() (Config, error) {
	cfg := Config{
		GRPCPort:         getEnv("APPOINTMENT_SERVICE_GRPC_PORT", ":50052"),
		DatabaseURL:      firstNonEmpty(os.Getenv("DATABASE_URL"), os.Getenv("DB_DSN")),
		NATSURL:          os.Getenv("NATS_URL"),
		DoctorServiceURL: getEnv("DOCTOR_SERVICE_URL", "localhost:50051"),
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
