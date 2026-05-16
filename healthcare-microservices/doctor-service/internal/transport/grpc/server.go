package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"doctor-service/internal/model"
	"doctor-service/internal/usecase"
	pb "doctor-service/proto/doctorpb"
)

// DoctorServer is the thin gRPC adapter — it only translates between the
// generated protobuf types and the use-case-level types. It contains no
// business logic.
type DoctorServer struct {
	pb.UnimplementedDoctorServiceServer
	uc *usecase.DoctorUseCase
}

func NewDoctorServer(uc *usecase.DoctorUseCase) *DoctorServer {
	return &DoctorServer{uc: uc}
}

func (s *DoctorServer) CreateDoctor(ctx context.Context, req *pb.CreateDoctorRequest) (*pb.DoctorResponse, error) {
	d, err := s.uc.CreateDoctor(ctx, usecase.CreateDoctorInput{
		FullName:       req.GetFullName(),
		Specialization: req.GetSpecialization(),
		Email:          req.GetEmail(),
	})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.DoctorResponse{Doctor: toProto(d)}, nil
}

func (s *DoctorServer) GetDoctor(ctx context.Context, req *pb.GetDoctorRequest) (*pb.DoctorResponse, error) {
	d, err := s.uc.GetDoctor(ctx, req.GetId())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.DoctorResponse{Doctor: toProto(d)}, nil
}

func (s *DoctorServer) ListDoctors(ctx context.Context, _ *pb.ListDoctorsRequest) (*pb.ListDoctorsResponse, error) {
	doctors, err := s.uc.ListDoctors(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}
	resp := &pb.ListDoctorsResponse{Doctors: make([]*pb.Doctor, 0, len(doctors))}
	for _, d := range doctors {
		resp.Doctors = append(resp.Doctors, toProto(d))
	}
	return resp, nil
}

func toProto(d *model.Doctor) *pb.Doctor {
	if d == nil {
		return nil
	}
	return &pb.Doctor{
		Id:             d.ID,
		FullName:       d.FullName,
		Specialization: d.Specialization,
		Email:          d.Email,
	}
}

// toGRPCError maps domain errors to the gRPC status codes documented in
// Section 10 of the assignment. Anything unknown is reported as Internal.
func toGRPCError(err error) error {
	switch {
	case errors.Is(err, model.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, model.ErrDoctorNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, model.ErrEmailExists):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
