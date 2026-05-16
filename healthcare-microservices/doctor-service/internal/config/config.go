package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

// Config groups all environment-driven settings for the Doctor Service.
type Config struct {
	GRPCPort        string
	DatabaseURL     string
	NATSURL         string
	RedisURL        string
	CacheTTLSeconds int
	RateLimitRPM    int
}

// Load reads configuration from environment variables. DATABASE_URL has a
// fallback name (DB_DSN) per Section 13 of the assignment.
func Load() (Config, error) {
	cacheTTLStr := getEnv("CACHE_TTL_SECONDS", "60")
	cacheTTL, _ := strconv.Atoi(cacheTTLStr)
	
	rateLimitStr := getEnv("RATE_LIMIT_RPM", "100")
	rateLimit, _ := strconv.Atoi(rateLimitStr)

	cfg := Config{
		GRPCPort:        getEnv("DOCTOR_SERVICE_GRPC_PORT", ":50051"),
		DatabaseURL:     firstNonEmpty(os.Getenv("DATABASE_URL"), os.Getenv("DB_DSN")),
		NATSURL:         os.Getenv("NATS_URL"),
		RedisURL:        getEnv("REDIS_URL", "redis://localhost:6379"),
		CacheTTLSeconds: cacheTTL,
		RateLimitRPM:    rateLimit,
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
