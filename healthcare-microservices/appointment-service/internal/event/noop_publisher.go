package event

import "context"

// NoopPublisher is a stand-in used when the broker URL is missing or the
// initial connection failed. Its Publish never errors, satisfying the
// assignment's requirement that broker outages must not affect RPCs.
type NoopPublisher struct{}

func (NoopPublisher) Publish(_ context.Context, _ string, _ []byte) error { return nil }
func (NoopPublisher) Close() error                                        { return nil }
