// Package httpadapter serves chora-realtime's HTTP surface: the learner SSE
// stream (ADR-183) + health/readiness probes. The stream is the connection
// tier — it terminates the EventSource, validates the one-time ticket, SUBSCRIBEs
// the learner's backplane channel, and relays frames with a 15s keepalive.
package httpadapter

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/apollo-chora/chora-user-event-stream/internal/adapter/ticket"
	"github.com/apollo-chora/chora-user-event-stream/internal/domain/realtime"
	"github.com/apollo-chora/chora-user-event-stream/internal/port"
)

// defaultKeepalive is the SSE heartbeat interval (`:keepalive\n\n`) — keeps
// proxy/LB connections from idling out under the dedicated 3600s HTTPRoute.
const defaultKeepalive = 15 * time.Second

// StreamHandler serves GET /api/v1/realtime/stream?ticket=…
type StreamHandler struct {
	validator port.TicketValidator
	bus       port.UserBus
	keepalive time.Duration
}

// NewStreamHandler wires the handler. A nil validator/bus makes the endpoint
// respond 503 (dev / backplane not configured) instead of panicking.
func NewStreamHandler(v port.TicketValidator, bus port.UserBus, keepalive time.Duration) *StreamHandler {
	if keepalive <= 0 {
		keepalive = defaultKeepalive
	}
	return &StreamHandler{validator: v, bus: bus, keepalive: keepalive}
}

// ServeHTTP implements the SSE handler.
func (h *StreamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS first — every response (200 stream, 401s, 503) must carry the
	// echo or a cross-origin EventSource dies browser-side after the 200.
	applyCORS(w, r)
	if r.Method != http.MethodGet {
		writeRealtimeError(w, http.StatusMethodNotAllowed, "REALTIME_TICKET_INVALID", "GET only")
		return
	}
	if h.validator == nil || h.bus == nil {
		writeRealtimeError(w, http.StatusServiceUnavailable, "REALTIME_UNAVAILABLE", "realtime backplane not configured")
		return
	}

	raw := strings.TrimSpace(r.URL.Query().Get("ticket"))
	if raw == "" {
		writeRealtimeError(w, http.StatusUnauthorized, "REALTIME_TICKET_MISSING", "missing ticket query parameter")
		return
	}
	gcid, tenant, err := h.validator.Validate(r.Context(), raw)
	if err != nil {
		writeRealtimeError(w, http.StatusUnauthorized, mapTicketError(err), "invalid ticket")
		return
	}

	sink, release, err := h.bus.Subscribe(r.Context(), gcid)
	if err != nil {
		writeRealtimeError(w, http.StatusInternalServerError, "REALTIME_SUBSCRIBE_FAILED", "subscribe failed")
		return
	}
	defer release()

	// SSE headers — Cache-Control + X-Accel-Buffering mirror the payments SSE
	// blueprint so no proxy buffers the stream.
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}

	hb := time.NewTicker(h.keepalive)
	defer hb.Stop()

	var seq int64 // per-connection monotonic — the SSE `id:` for client gap detection
	for {
		select {
		case <-r.Context().Done():
			return
		case <-hb.C:
			if _, err := io.WriteString(w, ":keepalive\n\n"); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		case frameBytes, ok := <-sink:
			if !ok {
				return
			}
			var env realtime.Envelope
			if err := json.Unmarshal(frameBytes, &env); err != nil {
				continue // skip a malformed backplane frame
			}
			// Defence-in-depth: drop any frame whose tenant_id ≠ the ticket's
			// tenant (the channel is per-gcid, but a gcid is portable across
			// tenants — never relay another tenant's frame).
			if tenant != "" && env.TenantID != "" && env.TenantID != tenant {
				continue
			}
			data, err := env.MarshalFrame()
			if err != nil {
				continue
			}
			seq++
			if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", seq, env.Topic, data); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

// mapTicketError classifies a validator error onto a RealtimeError code.
func mapTicketError(err error) string {
	switch {
	case errors.Is(err, ticket.ErrExpired):
		return "REALTIME_TICKET_EXPIRED"
	case errors.Is(err, ticket.ErrReplayed):
		return "REALTIME_TICKET_REPLAYED"
	default:
		return "REALTIME_TICKET_INVALID"
	}
}

// writeRealtimeError emits the RealtimeError JSON shape {error:{code,message}}.
func writeRealtimeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"code": code, "message": msg},
	})
}
