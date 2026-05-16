package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type CacheRepository interface {
	Get(ctx context.Context, key string, dest any) error
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

type RedisCache struct {
	client *redis.Client
	logger *slog.Logger
}

var ErrCacheMiss = errors.New("cache miss")

func NewRedisCache(url string, logger *slog.Logger) (*RedisCache, error) {
	if logger == nil {
		logger = slog.Default()
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		logger.Warn("redis unavailable on startup (caching is best-effort)", slog.String("error", err.Error()))
	} else {
		logger.Info("connected to redis", slog.String("url", url))
	}
	return &RedisCache{client: client, logger: logger}, nil
}

func (c *RedisCache) Get(ctx context.Context, key string, dest any) error {
	val, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrCacheMiss
		}
		c.logger.Warn("cache get failed", slog.String("key", key), slog.String("error", err.Error()))
		return err
	}
	if err := json.Unmarshal(val, dest); err != nil {
		c.logger.Warn("cache unmarshal failed", slog.String("key", key), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (c *RedisCache) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	val, err := json.Marshal(value)
	if err != nil {
		c.logger.Warn("cache marshal failed", slog.String("key", key), slog.String("error", err.Error()))
		return err
	}
	if err := c.client.Set(ctx, key, val, ttl).Err(); err != nil {
		c.logger.Warn("cache set failed", slog.String("key", key), slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (c *RedisCache) Delete(ctx context.Context, key string) error {
	if err := c.client.Del(ctx, key).Err(); err != nil {
		c.logger.Warn("cache delete failed", slog.String("key", key), slog.String("error", err.Error()))
		return err
	}
	return nil
}
