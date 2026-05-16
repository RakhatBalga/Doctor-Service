package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratepostgres "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"appointment-service/internal/cache"
	"appointment-service/internal/client"
	"appointment-service/internal/config"
	"appointment-service/internal/event"
	"appointment-service/internal/middleware"
	"appointment-service/internal/repository"
	grpcserver "appointment-service/internal/transport/grpc"
	"appointment-service/internal/usecase"
	pb "appointment-service/proto/appointmentpb"
)

// Run is the composition root. The wiring order is intentional:
//  1. database (must be reachable, otherwise we exit)
//  2. migrations (block until applied)
//  3. broker (best-effort — we keep going even if it's down)
//  4. gRPC client to Doctor Service (lazy — actual dial happens on first call)
//  5. gRPC server (start last, after everything is wired)
func Run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	db, err := openDatabase(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	if err := runMigrations(db, logger); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	publisher := buildPublisher(cfg.NATSURL, logger)
	defer func() { _ = publisher.Close() }()

	doctorClient, err := client.NewDoctorGRPCClient(cfg.DoctorServiceURL)
	if err != nil {
		return fmt.Errorf("connect to doctor-service: %w", err)
	}
	defer func() { _ = doctorClient.Close() }()

	redisCache, err := cache.NewRedisCache(cfg.RedisURL, logger)
	if err != nil {
		logger.Warn("redis cache init failed, using nil (caching will fail but app continues)", slog.String("error", err.Error()))
	}
	
	rateLimiter, err := middleware.NewRateLimiter(cfg.RedisURL, cfg.RateLimitRPM, logger)
	if err != nil {
		logger.Warn("rate limiter init failed (failing open)", slog.String("error", err.Error()))
	}

	repo := repository.NewPostgresAppointmentRepository(db)
	
	cacheTTL := time.Duration(cfg.CacheTTLSeconds) * time.Second
	uc := usecase.NewAppointmentUseCase(repo, redisCache, cacheTTL, doctorClient, publisher, logger)
	server := grpcserver.NewAppointmentServer(uc)

	var grpcServer *grpc.Server
	if rateLimiter != nil {
		grpcServer = grpc.NewServer(
			grpc.UnaryInterceptor(rateLimiter.UnaryInterceptor()),
		)
	} else {
		grpcServer = grpc.NewServer()
	}
	
	pb.RegisterAppointmentServiceServer(grpcServer, server)
	reflection.Register(grpcServer)

	listener, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.GRPCPort, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("appointment-service grpc listening",
			slog.String("addr", cfg.GRPCPort),
			slog.String("doctor_service", cfg.DoctorServiceURL))
		errCh <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
		grpcServer.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}

func openDatabase(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func runMigrations(db *sql.DB, logger *slog.Logger) error {
	driver, err := migratepostgres.WithInstance(db, &migratepostgres.Config{})
	if err != nil {
		return err
	}
	abs, err := filepath.Abs("migrations")
	if err != nil {
		return err
	}
	m, err := migrate.NewWithDatabaseInstance("file://"+abs, "postgres", driver)
	if err != nil {
		return err
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	version, dirty, vErr := m.Version()
	if vErr == nil {
		logger.Info("migrations applied",
			slog.Uint64("version", uint64(version)),
			slog.Bool("dirty", dirty))
	}
	return nil
}

// buildPublisher mirrors doctor-service's policy: any broker failure is
// degraded to a NoopPublisher rather than aborting startup, because
// publishing is best-effort.
func buildPublisher(natsURL string, logger *slog.Logger) event.EventPublisher {
	if natsURL == "" {
		logger.Warn("NATS_URL is empty, broker publishing disabled (events will be dropped)")
		return event.NoopPublisher{}
	}
	pub, err := event.NewNATSPublisher(natsURL, logger)
	if err != nil {
		logger.Warn("connect to NATS failed, falling back to noop publisher",
			slog.String("nats_url", natsURL), slog.String("error", err.Error()))
		return event.NoopPublisher{}
	}
	logger.Info("connected to NATS", slog.String("nats_url", natsURL))
	return pub
}
