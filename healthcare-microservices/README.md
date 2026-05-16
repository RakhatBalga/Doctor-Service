# AP2 Assignment 4 - Caching & Background Jobs

## Overview
This project adds Redis caching, rate limiting, and a background job queue to the microservices architecture from Assignment 3. The objective is to demonstrate performance optimization via cache-aside/write-through strategies, protect endpoints with a rate limiter, and robustly handle background jobs with exponential backoff and idempotency.

## Architecture
- **Doctor Service**: Manages doctors, emits `doctors.created`. Exposes gRPC API with sliding-window rate limiting. Uses Redis for caching.
- **Appointment Service**: Manages appointments, emits `appointments.created` and `appointments.status_updated`. Calls Doctor Service over gRPC to validate doctors. Uses sliding-window rate limiting and Redis caching.
- **Notification Service**: Subscribes to events. When `appointments.status_updated` is `done`, it enqueues a background job to an internal worker pool. The workers call the Mock Gateway over HTTP, handling retries, idempotency via Redis, and dead-letter logging.
- **Mock Gateway**: Simulates an external notification API (email/sms). Returns HTTP 200 (accepted/duplicate) or randomly HTTP 503 to test retry logic.

## Cache Strategy
- **GetDoctor / GetAppointment (Cache-Aside)**: We check Redis first. On a miss, we fetch from PostgreSQL and write to Redis. This is ideal for read-heavy operations where stale data is acceptable within the TTL window.
- **CreateDoctor (Write-Through)**: Writes to DB and immediately invalidates the `doctors:list` cache. This ensures the next `ListDoctors` call fetches fresh data.
- **CreateAppointment (Write-Around)**: Writes to DB and invalidates the `appointments:list` cache. We don't cache the specific appointment immediately since it might not be read immediately.
- **UpdateAppointmentStatus (Write-Through)**: Updates DB, then updates the specific `appointment:<id>` cache and invalidates `appointments:list`. Ensures immediate consistency for the updated appointment.

## Rate Limiting Algorithm
We implemented a **Sliding Window Log / Sorted Set** algorithm using Redis.
- We use a Redis Sorted Set (`ZADD`, `ZREMRANGEBYSCORE`, `ZCARD`) for each client IP.
- The score is the timestamp. We remove items older than the window (1 minute) and count the remaining elements.
- Why: Provides very accurate rate limiting without the edge-case bursts that fixed-window counters suffer from.

## Cache Invalidation
- Invalidation happens immediately after a successful database write but before returning the gRPC response.
- The stale-read window exists only for unchanged entities where the TTL hasn't expired, but since we invalidate lists on creation and update individual entities on status change, the stale window is practically eliminated for our core operations.

## Job Queue Design
- **Worker Pool**: A configurable number of goroutines (`WORKER_POOL_SIZE`, default 3) reading from a buffered Go channel (`size 100`).
- **Backpressure**: If the channel fills up, the subscriber will block on `jobs <- job`, effectively applying backpressure to the NATS subscription (NATS will buffer or drop depending on consumer config, though we use standard core NATS which will slow down the consumer).
- **Idempotency**: The idempotency key is `SHA256(event_type + id + occurred_at)`. It is stored in Redis for 24 hours upon successful gateway processing. If a job is picked up and the key exists with value `done`, it is silently dropped.
- **Dead-Letter**: If the mock gateway returns an error (or 503) 3 times with exponential backoff (1s, 2s, 4s), the job is logged to `stderr` with status `dead_letter` as a JSON struct. In production, this would be routed to a real Dead Letter Queue (DLQ) in the broker for manual inspection and alerting.

## Infrastructure Setup & Startup Order

1. Start Infrastructure:
   ```bash
   docker-compose up -d
   ```
2. Start Mock Gateway:
   ```bash
   cd mock-gateway && go run main.go
   ```
3. Start Notification Service:
   ```bash
   cd notification-service
   NATS_URL="nats://localhost:4222" REDIS_URL="redis://localhost:6379" GATEWAY_URL="http://localhost:8080/notify" go run main.go
   ```
4. Start Doctor Service:
   ```bash
   cd doctor-service
   DATABASE_URL="postgres://postgres:password@localhost:5432/doctor_db?sslmode=disable" REDIS_URL="redis://localhost:6379" NATS_URL="nats://localhost:4222" go run main.go
   ```
5. Start Appointment Service:
   ```bash
   cd appointment-service
   DATABASE_URL="postgres://postgres:password@localhost:5432/appointment_db?sslmode=disable" REDIS_URL="redis://localhost:6379" NATS_URL="nats://localhost:4222" DOCTOR_SERVICE_URL="localhost:50051" go run main.go
   ```

## Trade-offs

### Cache Consistency Trade-offs
- If Redis is unavailable, the services fall back to the DB transparently (best-effort). This means reads become slower but the system remains available.
- In a distributed cache (e.g. Redis Cluster), network partitions could lead to temporary split-brain scenarios where stale data is served. Our write-through invalidation mitigates this but depends on Redis acknowledging the delete/set.

### Rate-Limiting Trade-offs
- Per-instance rate limiting (e.g., using an in-memory map in Go) fails in horizontally scaled environments because a client could hit 5 instances and get 5x the allowed limit.
- A centralized Redis-backed counter solves this by providing a single source of truth for the client's request count across all instances.
- Limitation: Redis becomes a single point of failure and a bottleneck. If Redis is down, we "fail open" to ensure availability, but this means rate limiting is temporarily disabled.
