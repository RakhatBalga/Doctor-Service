package usecase

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"doctor-service/internal/event"
	"doctor-service/internal/model"
	"doctor-service/internal/repository"
)

// DoctorUseCase encapsulates the business rules around doctors and is
// completely free of transport- or persistence-specific types.
type DoctorUseCase struct {
	repo      repository.DoctorRepository
	publisher event.EventPublisher
	logger    *slog.Logger
	now       func() time.Time
}

// NewDoctorUseCase wires the use-case with its dependencies. The publisher
// is injected via the same EventPublisher interface used in tests, which
// means broker outages can be simulated by passing a NoopPublisher.
func NewDoctorUseCase(repo repository.DoctorRepository, publisher event.EventPublisher, logger *slog.Logger) *DoctorUseCase {
	if publisher == nil {
		publisher = event.NoopPublisher{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &DoctorUseCase{repo: repo, publisher: publisher, logger: logger, now: time.Now}
}

// CreateDoctorInput is the use-case-level request type. It contains only
// raw domain data — no protobuf, no JSON tags, no SQL.
type CreateDoctorInput struct {
	FullName       string
	Specialization string
	Email          string
}

// CreateDoctor validates the input, persists the doctor, and best-effort
// publishes a "doctors.created" domain event.
func (uc *DoctorUseCase) CreateDoctor(ctx context.Context, in CreateDoctorInput) (*model.Doctor, error) {
	d := &model.Doctor{
		ID:             uuid.NewString(),
		FullName:       strings.TrimSpace(in.FullName),
		Specialization: strings.TrimSpace(in.Specialization),
		Email:          strings.TrimSpace(strings.ToLower(in.Email)),
		CreatedAt:      uc.now().UTC(),
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	if err := uc.repo.Create(ctx, d); err != nil {
		return nil, err
	}
	uc.publishCreated(ctx, d)
	return d, nil
}

func (uc *DoctorUseCase) GetDoctor(ctx context.Context, id string) (*model.Doctor, error) {
	if strings.TrimSpace(id) == "" {
		return nil, model.ErrInvalidInput
	}
	return uc.repo.GetByID(ctx, id)
}

func (uc *DoctorUseCase) ListDoctors(ctx context.Context) ([]*model.Doctor, error) {
	return uc.repo.List(ctx)
}

// doctorCreatedEvent matches the JSON contract documented in Section 6 of
// the assignment. The fields are explicit because external services depend
// on this exact structure.
type doctorCreatedEvent struct {
	EventType      string `json:"event_type"`
	OccurredAt     string `json:"occurred_at"`
	ID             string `json:"id"`
	FullName       string `json:"full_name"`
	Specialization string `json:"specialization"`
	Email          string `json:"email"`
}

// publishCreated builds the JSON payload and forwards it to the broker.
// Errors are logged but never returned, because broker delivery is
// best-effort and must not affect the RPC outcome.
func (uc *DoctorUseCase) publishCreated(ctx context.Context, d *model.Doctor) {
	evt := doctorCreatedEvent{
		EventType:      event.DoctorCreatedSubject,
		OccurredAt:     uc.now().UTC().Format(time.RFC3339),
		ID:             d.ID,
		FullName:       d.FullName,
		Specialization: d.Specialization,
		Email:          d.Email,
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		uc.logger.Error("marshal doctor.created event",
			slog.String("doctor_id", d.ID), slog.String("error", err.Error()))
		return
	}
	if err := uc.publisher.Publish(ctx, event.DoctorCreatedSubject, payload); err != nil {
		uc.logger.Warn("publish doctor.created failed",
			slog.String("doctor_id", d.ID),
			slog.String("subject", event.DoctorCreatedSubject),
			slog.String("error", err.Error()))
	}
}
