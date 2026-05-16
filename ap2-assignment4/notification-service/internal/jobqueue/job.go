package jobqueue

type Job struct {
	IdempotencyKey string `json:"idempotency_key"`
	AppointmentID  string `json:"-"`
	DoctorID       string `json:"-"`
	OccurredAt     string `json:"-"`
	Channel        string `json:"channel"`
	Recipient      string `json:"recipient"`
	Message        string `json:"message"`
}
