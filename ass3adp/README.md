# AP2 — Assignment 3: Message Queue & Database Migrations

Medical Scheduling Platform — three Go services communicating over **gRPC** and **NATS**, backed by **PostgreSQL** with **golang-migrate**-managed schemas.

This repository extends Assignment 2 in two directions:

1. **Asynchronous, event-driven communication** via a message broker. Every successful write publishes a domain event, and a brand-new third service — the **Notification Service** — consumes those events.
2. **Real PostgreSQL persistence**, with the schema managed exclusively through versioned migration files.

The domain logic, the gRPC contracts, and the Clean-Architecture layering from Assignment 2 are preserved. Only the infrastructure layer changed.

---

## 1. What Changed Compared to Assignment 2

| Concern | Assignment 2 | Assignment 3 |
|---|---|---|
| Persistence | In-memory `map` per service | **PostgreSQL** via `database/sql` + `pgx/v5/stdlib` driver |
| Schema | Hardcoded Go structs only | **Migrations** (`golang-migrate`) — `up` / `down` SQL files |
| Inter-service traffic | Synchronous gRPC only | **Synchronous gRPC + asynchronous NATS Pub/Sub** |
| Event publishing | None | After every write, an event is published (best-effort) |
| Notification handling | Did not exist | **`notification-service`** subscribes and logs |
| Multi-step writes | Single in-memory mutation | Wrapped in a **DB transaction** (`UpdateAppointmentStatus`) |
| Repository interfaces | `DoctorRepository`, `AppointmentRepository` | **Unchanged** — only implementations swapped |
| gRPC contracts | `.proto` files in `proto/` | **Unchanged** |

---

## 2. Broker Choice — NATS (core)

I chose **core NATS** (no JetStream) for this assignment.

**Reasons**

- **Simpler local setup.** A single static binary (`nats-server`) with zero configuration. No Docker required for development.
- **Fits the use case.** The notification service only logs — losing a message during a broker outage is acceptable, so the lack of persistence is not a problem.
- **Idiomatic Go client.** `github.com/nats-io/nats.go` is small, well-documented, and reconnects automatically.
- **Lower operational footprint.** No exchanges, no queues, no bindings to model — just subjects.

**What would change if I switched to RabbitMQ?**

- All three services would import `github.com/rabbitmq/amqp091-go` instead of `nats.go`.
- Each publisher would `ExchangeDeclare("ap2.events", "fanout", durable=true)` and `Publish` with that exchange + a routing key matching the subject.
- `notification-service` would `QueueDeclare` an exclusive queue, `QueueBind` it to `ap2.events` for each routing key, and consume with explicit `ack`s (NATS core needs no ack).
- Connection setup is heavier: `Dial` returns a `Connection`, each goroutine needs its own `Channel`, and graceful shutdown must close both in order.
- **Where durable delivery becomes mandatory in production:** anything that affects user-visible state outside this system — e.g. sending a real email, charging a card, calling an external EHR. Either RabbitMQ with `durable=true` queues + publisher confirms, **or** NATS JetStream with persisted streams + consumer acks would be required there. The Outbox pattern would still be the safest belt-and-braces solution, regardless of broker.

---

## 3. Architecture

```
                          ┌──────────────────────────┐
                          │   notification-service   │
                          │  (subscriber-only,       │
                          │   prints JSON to stdout) │
                          └────────────▲─────────────┘
                                       │ subscribes to
                                       │  • doctors.created
                                       │  • appointments.created
                                       │  • appointments.status_updated
                                       │
                              ┌────────┴─────────┐
                              │   NATS broker    │
                              │   (core, Pub/Sub)│
                              └────▲────────▲────┘
                                   │        │
                       publishes   │        │   publishes
                doctors.created    │        │   appointments.created
                                   │        │   appointments.status_updated
                                   │        │
        gRPC                 ┌─────┴──┐  ┌──┴────────────────┐
   client ──── CreateDoctor──►        │  │                   │
   (grpcurl)                 │ doctor │  │ appointment       │
                             │service │◄─┤ service           │◄── gRPC client
                             │  :50051│  │  :50052           │      (grpcurl)
                             └────┬───┘  └─────────┬─────────┘
                                  │                │
                        ┌─────────▼──┐    ┌────────▼──────────┐
                        │ Postgres   │    │ Postgres          │
                        │ ap2_doctor │    │ ap2_appointment   │
                        │ doctors    │    │ appointments      │
                        └────────────┘    └───────────────────┘
```

The Appointment Service still calls the Doctor Service synchronously over gRPC (`GetDoctor`) before persisting an appointment, exactly as in Assignment 2. The new event flow is **additional** — never a replacement for the gRPC validation call.

---

## 4. Repository Layout

```
ass3adp/
├── doctor-service/
│   ├── main.go                        ← entry point (`go run .`)
│   ├── go.mod
│   ├── internal/
│   │   ├── app/                       ← composition root
│   │   ├── config/                    ← env-driven config loader
│   │   ├── model/                     ← domain entity + invariants
│   │   ├── repository/                ← interface + Postgres impl
│   │   ├── usecase/                   ← business rules
│   │   ├── transport/grpc/            ← gRPC adapter
│   │   └── event/                     ← EventPublisher interface + NATS impl + Noop
│   ├── migrations/
│   │   ├── 000001_create_doctors.up.sql
│   │   └── 000001_create_doctors.down.sql
│   └── proto/doctorpb/
│       ├── doctor.proto
│       ├── doctor.pb.go               ← generated
│       └── doctor_grpc.pb.go          ← generated
├── appointment-service/
│   ├── main.go
│   ├── go.mod
│   ├── internal/
│   │   ├── app/
│   │   ├── config/
│   │   ├── model/
│   │   ├── repository/                ← Postgres impl, transactional UpdateStatus
│   │   ├── usecase/
│   │   ├── transport/grpc/
│   │   ├── client/                    ← gRPC client to doctor-service
│   │   └── event/
│   ├── migrations/
│   │   ├── 000001_create_appointments.up.sql
│   │   └── 000001_create_appointments.down.sql
│   └── proto/
│       ├── appointmentpb/             ← AppointmentService stubs
│       └── doctorpb/                  ← DoctorService stubs (consumed as client)
├── notification-service/
│   ├── main.go
│   ├── go.mod
│   └── internal/subscriber/           ← NATS subscriber + JSON logger
├── go.work
└── README.md
```

---

## 5. Environment Variables

| Variable | Used by | Required | Default | Purpose |
|---|---|---|---|---|
| `DATABASE_URL` (or `DB_DSN`) | `doctor-service`, `appointment-service` | **yes** | — | Postgres connection string. Each service must point to **its own** database. |
| `NATS_URL` | all three | **yes for `notification-service`**, optional for the others | — | NATS broker URL, e.g. `nats://localhost:4222`. If empty in `doctor-service`/`appointment-service`, broker publishing is silently dropped (degraded mode). |
| `DOCTOR_SERVICE_GRPC_PORT` | `doctor-service` | no | `:50051` | gRPC listen address. |
| `APPOINTMENT_SERVICE_GRPC_PORT` | `appointment-service` | no | `:50052` | gRPC listen address. |
| `DOCTOR_SERVICE_URL` | `appointment-service` | no | `localhost:50051` | Address for the gRPC client to the Doctor Service. |

> **No URL is hardcoded in source code.** All of them flow through `internal/config`.

---

## 6. Infrastructure Setup

### 6.1 PostgreSQL — two databases

You need **two separate databases**, one per service. Either two distinct databases on the same Postgres instance or two different instances — both satisfy the "no shared tables" rule.

#### Option A — local Postgres (Homebrew on macOS)

```bash
# install + start
brew install postgresql@16
brew services start postgresql@16

# create one database per service
createdb ap2_doctor
createdb ap2_appointment
```

#### Option B — Docker

```bash
docker run --name ap2-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -p 5432:5432 -d postgres:16

docker exec -it ap2-postgres psql -U postgres -c 'CREATE DATABASE ap2_doctor;'
docker exec -it ap2-postgres psql -U postgres -c 'CREATE DATABASE ap2_appointment;'
```

Resulting DSNs:

```
postgres://postgres:postgres@localhost:5432/ap2_doctor?sslmode=disable
postgres://postgres:postgres@localhost:5432/ap2_appointment?sslmode=disable
```

### 6.2 NATS broker

#### Option A — local binary

```bash
brew install nats-server
nats-server -p 4222    # listens on nats://localhost:4222
```

#### Option B — Docker

```bash
docker run --name ap2-nats -p 4222:4222 -d nats:2
```

The default URL used everywhere in this README is **`nats://localhost:4222`**.

---

## 7. Migrations

Migrations are **applied automatically** at service startup via `golang-migrate`'s in-process API (`internal/app/app.go::runMigrations`). No manual step is required for the normal run.

### Manual control with the `migrate` CLI (optional)

For testing `down` migrations or operating in production, install the CLI:

```bash
brew install golang-migrate          # macOS
# or: go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```

Apply / roll back:

```bash
# from inside doctor-service/
migrate -path migrations \
        -database "postgres://postgres:postgres@localhost:5432/ap2_doctor?sslmode=disable" \
        up         # or: down 1, force <version>, version

# from inside appointment-service/
migrate -path migrations \
        -database "postgres://postgres:postgres@localhost:5432/ap2_appointment?sslmode=disable" \
        up
```

Down migrations are tested:

```bash
migrate -path migrations \
        -database "postgres://postgres:postgres@localhost:5432/ap2_doctor?sslmode=disable" \
        down 1
```

There is **no raw DDL anywhere in application code** — `grep -ri 'CREATE TABLE\|ALTER TABLE\|DROP TABLE' --include='*.go'` yields zero hits.

---

## 8. Service Startup Order

Recommended order:

1. **PostgreSQL** (must be ready before any Go service starts — they fail fast on a missing DB).
2. **NATS broker** (`nats-server -p 4222`).
3. **`notification-service`** (start *before* the producers so it doesn't miss any test events; it retries the broker connection with backoff anyway).
4. **`doctor-service`**.
5. **`appointment-service`** (it dials `doctor-service` lazily, so it tolerates `doctor-service` starting at the same time, but the doctor must be reachable before the first `CreateAppointment` call).

### Exact `go run .` commands

Open four terminals.

```bash
# Terminal 1 — notification-service
cd notification-service
NATS_URL=nats://localhost:4222 go run .
```

```bash
# Terminal 2 — doctor-service
cd doctor-service
DATABASE_URL='postgres://postgres:postgres@localhost:5432/ap2_doctor?sslmode=disable' \
NATS_URL='nats://localhost:4222' \
go run .
```

```bash
# Terminal 3 — appointment-service
cd appointment-service
DATABASE_URL='postgres://postgres:postgres@localhost:5432/ap2_appointment?sslmode=disable' \
NATS_URL='nats://localhost:4222' \
DOCTOR_SERVICE_URL='localhost:50051' \
go run .
```

```bash
# Terminal 4 — grpcurl test client (see Section 11)
```

---

## 9. Event Contract (Section 6 of the spec)

All three events are JSON-serialised. Every payload contains `event_type` and `occurred_at` (RFC3339), plus the entity-specific fields below.

| Subject (NATS) / routing key | Publisher | Trigger | Payload fields |
|---|---|---|---|
| `doctors.created` | `doctor-service` | `CreateDoctor` returned successfully | `event_type`, `occurred_at`, `id`, `full_name`, `specialization`, `email` |
| `appointments.created` | `appointment-service` | `CreateAppointment` returned successfully | `event_type`, `occurred_at`, `id`, `title`, `doctor_id`, `status` |
| `appointments.status_updated` | `appointment-service` | `UpdateAppointmentStatus` returned successfully | `event_type`, `occurred_at`, `id`, `old_status`, `new_status` |

### Example payloads

```json
{
  "event_type": "doctors.created",
  "occurred_at": "2026-05-01T10:23:44Z",
  "id": "f6d4...",
  "full_name": "Dr. Aisha Seitkali",
  "specialization": "Cardiology",
  "email": "a.seitkali@clinic.kz"
}
```

```json
{
  "event_type": "appointments.created",
  "occurred_at": "2026-05-01T10:24:01Z",
  "id": "appt-1",
  "title": "Initial cardiac consultation",
  "doctor_id": "f6d4...",
  "status": "new"
}
```

```json
{
  "event_type": "appointments.status_updated",
  "occurred_at": "2026-05-01T10:25:10Z",
  "id": "appt-1",
  "old_status": "new",
  "new_status": "in_progress"
}
```

The publisher type lives behind an `EventPublisher` interface (`internal/event/publisher.go`) in each service. Concrete implementations: `NATSPublisher` (real broker) and `NoopPublisher` (used when `NATS_URL` is empty or the initial dial failed). **Failures inside `Publish` are logged and never propagate to the gRPC response.**

---

## 10. Notification Service

Single binary, single responsibility: receive events, log JSON.

### What it does

- On startup it reads `NATS_URL` and dials the broker with **exponential backoff** (1s, 2s, 4s, 8s, 16s, 32s — `defaultMaxRetries = 6`). After the last failed attempt it exits with a non-zero status code and a descriptive error (`internal/subscriber/subscriber.go::ConnectWithBackoff`).
- It subscribes to all three subjects (`doctors.created`, `appointments.created`, `appointments.status_updated`).
- For every received message it deserialises the JSON payload and prints **one structured line to stdout**:

```json
{"time":"2026-05-01T10:23:44Z","subject":"doctors.created","event":{"event_type":"doctors.created","occurred_at":"2026-05-01T10:23:44Z","id":"...","full_name":"...","specialization":"...","email":"..."}}
```

- Invalid payloads are not silently dropped — they are logged via `slog` to **stderr** with the raw bytes attached.
- On `SIGINT` / `SIGTERM` it `Drain`s the connection (delivering any in-flight messages) and exits 0.

### What it does **not** do

- ❌ No gRPC server, no HTTP server, no DB connection. It exposes no port.

### How to verify it during a demo

1. Start `notification-service`. Wait for `notification-service ready`.
2. From another terminal, run a `grpcurl CreateDoctor` (Section 11). Within a second you should see a `doctors.created` JSON line in `notification-service`'s stdout.
3. Run `grpcurl CreateAppointment` → `appointments.created` line.
4. Run `grpcurl UpdateAppointmentStatus` → `appointments.status_updated` line.

---

## 11. Test Plan — `grpcurl` + Notification Service Output

Install `grpcurl`: `brew install grpcurl`. Reflection is enabled on both gRPC services.

### Step 1 — Create a doctor

```bash
grpcurl -plaintext -d '{
  "full_name": "Dr. Aisha Seitkali",
  "specialization": "Cardiology",
  "email": "a.seitkali@clinic.kz"
}' localhost:50051 doctor.v1.DoctorService/CreateDoctor
```

Expected gRPC response (the `id` is a fresh UUID):

```json
{ "doctor": { "id": "<uuid>", "fullName": "Dr. Aisha Seitkali", "specialization": "Cardiology", "email": "a.seitkali@clinic.kz" } }
```

Expected `notification-service` stdout (single line):

```json
{"time":"2026-05-03T19:23:44Z","subject":"doctors.created","event":{"event_type":"doctors.created","occurred_at":"2026-05-03T19:23:44Z","id":"<uuid>","full_name":"Dr. Aisha Seitkali","specialization":"Cardiology","email":"a.seitkali@clinic.kz"}}
```

### Step 2 — Create an appointment

```bash
grpcurl -plaintext -d '{
  "title": "Initial cardiac consultation",
  "description": "Routine check, ECG planned",
  "doctor_id": "<uuid-from-step-1>"
}' localhost:50052 appointment.v1.AppointmentService/CreateAppointment
```

Expected `notification-service` stdout:

```json
{"time":"2026-05-03T19:24:01Z","subject":"appointments.created","event":{"event_type":"appointments.created","occurred_at":"2026-05-03T19:24:01Z","id":"<uuid>","title":"Initial cardiac consultation","doctor_id":"<uuid-from-step-1>","status":"new"}}
```

### Step 3 — Move the appointment to `in_progress`

```bash
grpcurl -plaintext -d '{
  "id": "<uuid-from-step-2>",
  "status": "in_progress"
}' localhost:50052 appointment.v1.AppointmentService/UpdateAppointmentStatus
```

Expected `notification-service` stdout:

```json
{"time":"2026-05-03T19:25:10Z","subject":"appointments.status_updated","event":{"event_type":"appointments.status_updated","occurred_at":"2026-05-03T19:25:10Z","id":"<uuid-from-step-2>","old_status":"new","new_status":"in_progress"}}
```

### Step 4 — Negative cases

```bash
# duplicate email -> AlreadyExists
grpcurl -plaintext -d '{"full_name":"X","email":"a.seitkali@clinic.kz"}' \
  localhost:50051 doctor.v1.DoctorService/CreateDoctor
# => ERROR: Code: AlreadyExists, Message: doctor with this email already exists

# unknown doctor -> FailedPrecondition
grpcurl -plaintext -d '{"title":"x","doctor_id":"does-not-exist"}' \
  localhost:50052 appointment.v1.AppointmentService/CreateAppointment
# => ERROR: Code: FailedPrecondition, Message: doctor not found

# invalid status -> InvalidArgument
grpcurl -plaintext -d '{"id":"<uuid>","status":"wrong"}' \
  localhost:50052 appointment.v1.AppointmentService/UpdateAppointmentStatus
# => ERROR: Code: InvalidArgument, Message: invalid appointment status
```

### Step 5 — Doctor Service unreachable (preserved from Assignment 2)

Stop `doctor-service` (Ctrl-C). Run a `CreateAppointment` again:

```
ERROR: Code: Unavailable, Message: doctor service is unreachable: ...
```

The Appointment Service still returns a descriptive gRPC error — exactly as required.

---

## 12. Consistency Trade-offs (be ready to discuss)

The current "publish after commit" pattern is a deliberate trade-off:

```
1. begin tx
2. INSERT into doctors / appointments
3. commit            ← state is now durable
4. publish event     ← best-effort, no durability guarantee
```

**Failure modes:**

| Where it fails | Result |
|---|---|
| Step 2 fails | `tx.Rollback()` runs, RPC returns `Internal`/`InvalidArgument`. **No event is published.** Consistent. |
| Step 3 fails | DB rollback, RPC returns error. **No event published.** Consistent. |
| Step 4 fails (broker down, network partition, process crash *between 3 and 4*) | DB has the row, but the event was never delivered. Notification service never sees it. **Eventually inconsistent.** |

Today this is acceptable because the consumer only writes a log line. In production, with real downstream effects (email, audit log, billing), three remedies — in increasing order of robustness — would apply:

1. **Publisher confirms** (RabbitMQ) or **JetStream `PublishAsync` with ack** (NATS): the publisher only returns success once the broker has durably accepted the message. Closes most of the window but **still** loses messages if the process dies between step 3 and the broker call.
2. **Outbox pattern**: in step 2 we additionally `INSERT` into an `outbox` table inside the same transaction. A separate worker reads from `outbox` and publishes to the broker, marking rows as sent. The DB commit and the "intent to publish" become **atomic**, eliminating the lost-event window entirely. This is the production-grade fix.
3. **Idempotent consumers**: even with an outbox, retries can re-deliver the same message. Each event carries a unique `id`, and consumers must dedupe on it before acting.

---

## 13. Broker Comparison — NATS (core) vs RabbitMQ

Two concrete differences (more in §2):

1. **Persistence.** Core NATS has none — messages are dropped if no subscriber is alive when they're published. RabbitMQ keeps persistent messages in durable queues until each consumer acks them. ⇒ **Pick RabbitMQ when at-least-once delivery is non-negotiable, even across restarts.**
2. **Routing model.** NATS uses subject-based wildcards (`*`, `>`) — publishers don't know who is listening. RabbitMQ requires explicit topology (exchanges + queues + bindings) before any message moves. ⇒ **Pick NATS for fast, low-ceremony fan-out; pick RabbitMQ when you want operational visibility into queue depth, dead-letter queues, and per-consumer flow control.**

**Rule of thumb used here:** core NATS for telemetry-style notifications; RabbitMQ (or NATS JetStream) when a missed event would corrupt user-visible state.

---

## 14. Error-Handling Matrix

| Situation | Response |
|---|---|
| DB unreachable on startup | Service exits non-zero with a descriptive log (`open database`, `ping` errors). |
| DB query fails at runtime | RPC returns `codes.Internal` with the underlying message. |
| Broker unreachable on `doctor-service` / `appointment-service` startup | Service starts normally with a `NoopPublisher`, RPCs succeed, a `WARN` is logged. |
| Broker `Publish` fails during an RPC | Logged (`subject`, `entity_id`, `error`); RPC response unchanged. |
| Broker unreachable on `notification-service` startup | Exponential backoff (1s, 2s, 4s, 8s, 16s, 32s); after 6 attempts the process exits with code 1. |
| Duplicate email in DB | `codes.AlreadyExists` (Postgres SQLSTATE 23505 mapped in `repository/postgres_doctor_repository.go`). |
| Row not found in DB | `codes.NotFound`. |
| Invalid status transition / empty fields | `codes.InvalidArgument`. |
| Doctor Service unreachable from Appointment Service | `codes.Unavailable, Message: doctor service is unreachable: ...` (preserved from Assignment 2). |

---

## 15. Regenerating the gRPC Stubs

Stubs are committed to the repo, so a clean clone compiles immediately. To regenerate from the `.proto` files:

```bash
# install plugins (once)
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# regenerate doctor-service
cd doctor-service
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       --proto_path=. proto/doctorpb/doctor.proto

# regenerate appointment-service
cd ../appointment-service
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       --proto_path=. proto/appointmentpb/appointment.proto proto/doctorpb/doctor.proto
```

---

## 16. Quick Smoke Test (no DB / no broker required)

The following commands compile every service end-to-end:

```bash
cd doctor-service        && go build ./... && cd ..
cd appointment-service   && go build ./... && cd ..
cd notification-service  && go build ./... && cd ..
```

All three should exit 0 with no output.
