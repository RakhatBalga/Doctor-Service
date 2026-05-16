package grpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"appointment-service/internal/model"
	"appointment-service/internal/usecase"
	pb "appointment-service/proto/appointmentpb"
)

// AppointmentServer is a thin protobuf <-> use-case translator.
type AppointmentServer struct {
	pb.UnimplementedAppointmentServiceServer
	uc *usecase.AppointmentUseCase
}

func NewAppointmentServer(uc *usecase.AppointmentUseCase) *AppointmentServer {
	return &AppointmentServer{uc: uc}
}

func (s *AppointmentServer) CreateAppointment(ctx context.Context, req *pb.CreateAppointmentRequest) (*pb.AppointmentResponse, error) {
	a, err := s.uc.CreateAppointment(ctx, usecase.CreateAppointmentInput{
		Title:       req.GetTitle(),
		Description: req.GetDescription(),
		DoctorID:    req.GetDoctorId(),
	})
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.AppointmentResponse{Appointment: toProto(a)}, nil
}

func (s *AppointmentServer) GetAppointment(ctx context.Context, req *pb.GetAppointmentRequest) (*pb.AppointmentResponse, error) {
	a, err := s.uc.GetAppointment(ctx, req.GetId())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.AppointmentResponse{Appointment: toProto(a)}, nil
}

func (s *AppointmentServer) UpdateAppointmentStatus(ctx context.Context, req *pb.UpdateAppointmentStatusRequest) (*pb.AppointmentResponse, error) {
	a, err := s.uc.UpdateStatus(ctx, req.GetId(), req.GetStatus())
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &pb.AppointmentResponse{Appointment: toProto(a)}, nil
}

func (s *AppointmentServer) ListAppointments(ctx context.Context, req *pb.ListAppointmentsRequest) (*pb.ListAppointmentsResponse, error) {
	appts, err := s.uc.ListAppointments(ctx, req.GetDoctorId())
	if err != nil {
		return nil, toGRPCError(err)
	}
	resp := &pb.ListAppointmentsResponse{Appointments: make([]*pb.Appointment, 0, len(appts))}
	for _, a := range appts {
		resp.Appointments = append(resp.Appointments, toProto(a))
	}
	return resp, nil
}

func toProto(a *model.Appointment) *pb.Appointment {
	if a == nil {
		return nil
	}
	return &pb.Appointment{
		Id:          a.ID,
		Title:       a.Title,
		Description: a.Description,
		DoctorId:    a.DoctorID,
		Status:      string(a.Status),
	}
}

// toGRPCError encodes the table from Section 10. ErrDoctorUnavailable maps
// to FailedPrecondition because the doctor service is reachable through
// the system; if it cannot be contacted, the request cannot be fulfilled.
func toGRPCError(err error) error {
	switch {
	case errors.Is(err, model.ErrInvalidInput),
		errors.Is(err, model.ErrInvalidStatus):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, model.ErrAppointmentNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, model.ErrDoctorNotFound):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, model.ErrDoctorUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
