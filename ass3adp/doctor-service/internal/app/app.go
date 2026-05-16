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

	"doctor-service/internal/config"
	"doctor-service/internal/event"
	"doctor-service/internal/repository"
	grpcserver "doctor-service/internal/transport/grpc"
	"doctor-service/internal/usecase"
	pb "doctor-service/proto/doctorpb"
)

// Run is the composition root: it wires every layer, runs migrations,
// starts the gRPC server, and blocks until SIGINT/SIGTERM.
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

	repo := repository.NewPostgresDoctorRepository(db)
	uc := usecase.NewDoctorUseCase(repo, publisher, logger)
	server := grpcserver.NewDoctorServer(uc)

	grpcServer := grpc.NewServer()
	pb.RegisterDoctorServiceServer(grpcServer, server)
	reflection.Register(grpcServer)

	listener, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.GRPCPort, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("doctor-service grpc listening", slog.String("addr", cfg.GRPCPort))
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

// runMigrations applies all pending migrations from ./migrations. It treats
// "no change" as success and returns any other error so main can exit.
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

// buildPublisher follows the assignment rule that broker outages must not
// stop the service from starting: a NoopPublisher is returned on failure
// so RPC handlers continue to work.
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
