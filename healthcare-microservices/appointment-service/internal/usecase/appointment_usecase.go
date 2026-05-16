package usecase

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"appointment-service/internal/cache"
	"appointment-service/internal/client"
	"appointment-service/internal/event"
	"appointment-service/internal/model"
	"appointment-service/internal/repository"
)

// AppointmentUseCase orchestrates appointment business rules. It depends
// only on three ports — repository, doctor checker, and event publisher —
// none of which are infrastructure-specific.
type AppointmentUseCase struct {
	repo      repository.AppointmentRepository
	cacheRepo cache.CacheRepository
	cacheTTL  time.Duration
	doctors   client.DoctorChecker
	publisher event.EventPublisher
	logger    *slog.Logger
	now       func() time.Time
}

func NewAppointmentUseCase(
	repo repository.AppointmentRepository,
	cacheRepo cache.CacheRepository,
	cacheTTL time.Duration,
	doctors client.DoctorChecker,
	publisher event.EventPublisher,
	logger *slog.Logger,
) *AppointmentUseCase {
	if publisher == nil {
		publisher = event.NoopPublisher{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &AppointmentUseCase{
		repo:      repo,
		cacheRepo: cacheRepo,
		cacheTTL:  cacheTTL,
		doctors:   doctors,
		publisher: publisher,
		logger:    logger,
		now:       time.Now,
	}
}

type CreateAppointmentInput struct {
	Title       string
	Description string
	DoctorID    string
}

// CreateAppointment validates input, calls the Doctor Service to confirm
// the doctor exists, persists the appointment, and best-effort publishes
// the "appointments.created" event.
//
// The Doctor Service call is intentionally synchronous because the
// assignment requires the gRPC error path "if the Doctor Service is
// unreachable, the Appointment Service must still return a descriptive
// gRPC error".
func (uc *AppointmentUseCase) CreateAppointment(ctx context.Context, in CreateAppointmentInput) (*model.Appointment, error) {
	a := &model.Appointment{
		ID:          uuid.NewString(),
		Title:       strings.TrimSpace(in.Title),
		Description: strings.TrimSpace(in.Description),
		DoctorID:    strings.TrimSpace(in.DoctorID),
		Status:      model.StatusNew,
		CreatedAt:   uc.now().UTC(),
		UpdatedAt:   uc.now().UTC(),
	}
	if err := a.Validate(); err != nil {
		return nil, err
	}

	if err := uc.doctors.EnsureDoctorExists(ctx, a.DoctorID); err != nil {
		return nil, err
	}

	if err := uc.repo.Create(ctx, a); err != nil {
		return nil, err
	}

	// Write-Around: invalidate list key
	_ = uc.cacheRepo.Delete(ctx, "appointments:list")

	uc.publishCreated(ctx, a)
	return a, nil
}

func (uc *AppointmentUseCase) GetAppointment(ctx context.Context, id string) (*model.Appointment, error) {
	if strings.TrimSpace(id) == "" {
		return nil, model.ErrInvalidInput
	}
	
	cacheKey := "appointment:" + id
	var a model.Appointment
	err := uc.cacheRepo.Get(ctx, cacheKey, &a)
	if err == nil {
		return &a, nil // Cache hit
	}
	
	app, err := uc.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	
	_ = uc.cacheRepo.Set(ctx, cacheKey, app, uc.cacheTTL)
	
	return app, nil
}

// UpdateStatus changes the appointment's status, persisting the update in
// a transaction (see PostgresAppointmentRepository.UpdateStatus). On
// success it emits "appointments.status_updated" with both the old and
// new status, as required by the event contract.
func (uc *AppointmentUseCase) UpdateStatus(ctx context.Context, id, status string) (*model.Appointment, error) {
	if strings.TrimSpace(id) == "" {
		return nil, model.ErrInvalidInput
	}
	newStatus := model.Status(strings.ToLower(strings.TrimSpace(status)))
	if !newStatus.IsValid() {
		return nil, model.ErrInvalidStatus
	}

	old, err := uc.repo.UpdateStatus(ctx, id, newStatus)
	if err != nil {
		return nil, err
	}
	updated, err := uc.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	
	// Write-Through: update specific key, invalidate list
	_ = uc.cacheRepo.Set(ctx, "appointment:"+id, updated, uc.cacheTTL)
	_ = uc.cacheRepo.Delete(ctx, "appointments:list")
	
	uc.publishStatusUpdated(ctx, updated, old)
	return updated, nil
}

func (uc *AppointmentUseCase) ListAppointments(ctx context.Context, doctorID string) ([]*model.Appointment, error) {
	cacheKey := "appointments:list"
	if doctorID != "" {
		cacheKey += ":doctor:" + doctorID
	}
	
	var apps []*model.Appointment
	err := uc.cacheRepo.Get(ctx, cacheKey, &apps)
	if err == nil {
		return apps, nil // Cache hit
	}
	
	apps, err = uc.repo.List(ctx, strings.TrimSpace(doctorID))
	if err != nil {
		return nil, err
	}
	
	_ = uc.cacheRepo.Set(ctx, cacheKey, apps, uc.cacheTTL)
	return apps, nil
}

// appointmentCreatedEvent matches the JSON contract for
// "appointments.created" exactly. Field names are explicit to lock the
// wire format down at compile time.
type appointmentCreatedEvent struct {
	EventType  string `json:"event_type"`
	OccurredAt string `json:"occurred_at"`
	ID         string `json:"id"`
	Title      string `json:"title"`
	DoctorID   string `json:"doctor_id"`
	Status     string `json:"status"`
}

type appointmentStatusUpdatedEvent struct {
	EventType  string `json:"event_type"`
	OccurredAt string `json:"occurred_at"`
	ID         string `json:"id"`
	DoctorID   string `json:"doctor_id"`
	OldStatus  string `json:"old_status"`
	NewStatus  string `json:"new_status"`
}

func (uc *AppointmentUseCase) publishCreated(ctx context.Context, a *model.Appointment) {
	evt := appointmentCreatedEvent{
		EventType:  event.AppointmentCreatedSubject,
		OccurredAt: uc.now().UTC().Format(time.RFC3339),
		ID:         a.ID,
		Title:      a.Title,
		DoctorID:   a.DoctorID,
		Status:     string(a.Status),
	}
	uc.publish(ctx, event.AppointmentCreatedSubject, evt, a.ID)
}

func (uc *AppointmentUseCase) publishStatusUpdated(ctx context.Context, a *model.Appointment, old model.Status) {
	evt := appointmentStatusUpdatedEvent{
		EventType:  event.AppointmentStatusUpdatedSubject,
		OccurredAt: uc.now().UTC().Format(time.RFC3339),
		ID:         a.ID,
		DoctorID:   a.DoctorID,
		OldStatus:  string(old),
		NewStatus:  string(a.Status),
	}
	uc.publish(ctx, event.AppointmentStatusUpdatedSubject, evt, a.ID)
}

func (uc *AppointmentUseCase) publish(ctx context.Context, subject string, payload any, entityID string) {
	body, err := json.Marshal(payload)
	if err != nil {
		uc.logger.Error("marshal event",
			slog.String("subject", subject),
			slog.String("entity_id", entityID),
			slog.String("error", err.Error()))
		return
	}
	if err := uc.publisher.Publish(ctx, subject, body); err != nil {
		uc.logger.Warn("publish event failed",
			slog.String("subject", subject),
			slog.String("entity_id", entityID),
			slog.String("error", err.Error()))
	}
}
