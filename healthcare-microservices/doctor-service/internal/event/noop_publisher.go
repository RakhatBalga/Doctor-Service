package event

import "context"

// NoopPublisher is used when the broker URL is not configured or the
// initial connection failed. Publishing is silently dropped so the gRPC
// handler is never blocked by broker outages, satisfying the
// "broker publishing is best-effort" requirement.
type NoopPublisher struct{}

func (NoopPublisher) Publish(_ context.Context, _ string, _ []byte) error { return nil }
func (NoopPublisher) Close() error                                        { return nil }
