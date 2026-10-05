// Package observability is a thin shim delegating to the canonical
// chora-common/observability OTLP wiring (OTLP/gRPC via
// OTEL_EXPORTER_OTLP_ENDPOINT; stdout fallback when unset). Kept minimal +
// optional: InitAsync runs in its own goroutine with its own deadline and
// degrades to a no-op shutdown on timeout/error so the rest of bootstrap keeps
// its budget.
package observability

import (
	"context"

	"github.com/apollo-chora/chora-common/bootstrap"
	commonobs "github.com/apollo-chora/chora-common/observability"
)

// version is bumped per-release.
const version = "0.1.0"

// InitAsync wires OTLP for the named binary (chora-user-event-stream-server /
// chora-user-event-stream-fanin) and returns a handle the caller defers to flush spans.
func InitAsync(ctx context.Context, serviceName string) *bootstrap.OTLPHandle {
	return commonobs.InitOTLPAsync(ctx, serviceName, version)
}
