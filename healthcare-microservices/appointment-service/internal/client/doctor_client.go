package client

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"appointment-service/internal/model"
	pb "appointment-service/proto/doctorpb"
)

// DoctorChecker is the appointment-service-internal port that hides the
// gRPC client behind a small interface. The use case depends on the
// interface, never on the generated client, so unit tests can plug in a
// fake.
type DoctorChecker interface {
	EnsureDoctorExists(ctx context.Context, doctorID string) error
	Close() error
}

// DoctorGRPCClient is the gRPC implementation of DoctorChecker. It dials
// the Doctor Service lazily on construction and reuses the connection.
type DoctorGRPCClient struct {
	conn   *grpc.ClientConn
	client pb.DoctorServiceClient
}

// NewDoctorGRPCClient dials addr and returns a ready-to-use client.
// Network errors are returned to the caller so the application can decide
// whether to retry or exit.
func NewDoctorGRPCClient(addr string) (*DoctorGRPCClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &DoctorGRPCClient{
		conn:   conn,
		client: pb.NewDoctorServiceClient(conn),
	}, nil
}

// EnsureDoctorExists is the single business question this client answers:
// "does this doctor exist?". It maps gRPC errors to domain errors so the
// use case never depends on grpc/codes.
func (c *DoctorGRPCClient) EnsureDoctorExists(ctx context.Context, doctorID string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := c.client.GetDoctor(ctx, &pb.GetDoctorRequest{Id: doctorID})
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		return errors.Join(model.ErrDoctorUnavailable, err)
	}
	switch st.Code() {
	case codes.NotFound:
		return model.ErrDoctorNotFound
	case codes.Unavailable, codes.DeadlineExceeded:
		return errors.Join(model.ErrDoctorUnavailable, err)
	default:
		return err
	}
}

func (c *DoctorGRPCClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
