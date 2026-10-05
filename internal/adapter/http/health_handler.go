// health_handler.go — liveness/readiness probes + the service mux.
//
// /healthz is liveness (always ok while the process is up). /readyz is
// readiness: it flips to 503 the moment SIGTERM arrives so the load balancer
// stops routing NEW streams before the pod drains its held connections (the
// connection tier runs at fixed replicas — graceful drain matters).
package httpadapter

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

// Health tracks readiness state shared between the probe handler and the
// shutdown path.
type Health struct {
	ready atomic.Bool
}

// NewHealth returns a Health that starts Ready.
func NewHealth() *Health {
	h := &Health{}
	h.ready.Store(true)
	return h
}

// SetReady flips readiness (false on SIGTERM → /readyz 503).
func (h *Health) SetReady(ready bool) { h.ready.Store(ready) }

// Healthz is the liveness probe.
func (h *Health) Healthz(w http.ResponseWriter, _ *http.Request) {
	writeStatus(w, http.StatusOK, "ok")
}

// Readyz is the readiness probe.
func (h *Health) Readyz(w http.ResponseWriter, _ *http.Request) {
	if h.ready.Load() {
		writeStatus(w, http.StatusOK, "ok")
		return
	}
	writeStatus(w, http.StatusServiceUnavailable, "draining")
}

// NewMux builds the service mux. stream may be nil (the fan-in tier serves
// only health probes).
func NewMux(stream *StreamHandler, health *Health) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", health.Healthz)
	mux.HandleFunc("/readyz", health.Readyz)
	if stream != nil {
		mux.Handle("/api/v1/realtime/stream", stream)
	}
	return mux
}

// writeStatus emits the HealthResponse JSON shape {status}.
func writeStatus(w http.ResponseWriter, status int, s string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": s})
}
