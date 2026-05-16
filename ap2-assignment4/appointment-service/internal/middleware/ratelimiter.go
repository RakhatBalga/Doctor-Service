package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type RateLimiter struct {
	client *redis.Client
	limit  int
	window time.Duration
	logger *slog.Logger
}

func NewRateLimiter(redisURL string, limit int, logger *slog.Logger) (*RateLimiter, error) {
	if logger == nil {
		logger = slog.Default()
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	client := redis.NewClient(opts)
	return &RateLimiter{
		client: client,
		limit:  limit,
		window: time.Minute,
		logger: logger,
	}, nil
}

func (rl *RateLimiter) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		ip := "unknown"
		if p, ok := peer.FromContext(ctx); ok {
			addrStr := p.Addr.String()
			if host, _, err := net.SplitHostPort(addrStr); err == nil {
				ip = host
			} else {
				ip = addrStr
			}
		}

		key := fmt.Sprintf("ratelimit:%s", ip)
		now := time.Now().UnixNano()
		windowStart := now - rl.window.Nanoseconds()

		pipe := rl.client.Pipeline()
		pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%d", windowStart))
		pipe.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: now})
		countCmd := pipe.ZCard(ctx, key)
		pipe.Expire(ctx, key, rl.window)

		if _, err := pipe.Exec(ctx); err != nil {
			rl.logger.Warn("rate limiter redis error, failing open", slog.String("error", err.Error()))
			return handler(ctx, req)
		}

		if countCmd.Val() > int64(rl.limit) {
			rl.logger.Warn("rate limit exceeded", slog.String("ip", ip))
			return nil, status.Errorf(codes.ResourceExhausted, "rate limit exceeded: retry after %v", rl.window)
		}

		return handler(ctx, req)
	}
}
