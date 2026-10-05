// Package observability — smoke coverage for the OTLP async-init shim.
//
// InitAsync delegates to libs/chora-go-common's fail-soft OTLP wiring: init
// runs in its own goroutine with its own deadline and degrades to a no-op
// shutdown on timeout/error. The unit here asserts the shim returns a usable
// handle and that the handle's WaitContext+Shutdown cycle terminates without
// panic even when no OTLP endpoint is configured (dev stdout fallback).
package observability

import (
	"context"
	"testing"
	"time"
)

func TestInitAsync_ReturnsUsableHandle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	h := InitAsync(ctx, "chora-user-event-stream-test")
	if h == nil {
		t.Fatalf("InitAsync returned a nil handle")
	}

	// Wait for the async init to settle (bounded), then shut down. No OTLP
	// endpoint is configured here, so the exporter constructor falls back to
	// the dev stdout exporter — never a real network dial.
	sc, scCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scCancel()

	res := h.WaitContext(sc)
	if res.Shutdown == nil {
		t.Fatalf("WaitContext returned a result without a Shutdown func (fail-soft contract)")
	}
	if err := res.Shutdown(sc); err != nil {
		// Fail-soft: a shutdown error is tolerated by bootstrap (never fatal),
		// so the test only asserts the call returned without panicking/hanging.
		t.Logf("shutdown err (fail-soft, tolerated): %v", err)
	}
}
