package httpadapter

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func decodeStatus(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type = %q", ct)
	}
	var m map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	return m["status"]
}

func TestHealthz_AlwaysOK(t *testing.T) {
	h := NewHealth()
	rec := httptest.NewRecorder()
	h.Healthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || decodeStatus(t, rec) != "ok" {
		t.Fatalf("healthz = %d %s", rec.Code, rec.Body.String())
	}

	// Liveness stays ok even while draining.
	h.SetReady(false)
	rec = httptest.NewRecorder()
	h.Healthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz while draining = %d, want 200", rec.Code)
	}
}

func TestReadyz_FlipsWithSetReady(t *testing.T) {
	h := NewHealth()

	rec := httptest.NewRecorder()
	h.Readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK || decodeStatus(t, rec) != "ok" {
		t.Fatalf("initial readyz = %d %s, want 200 ok", rec.Code, rec.Body.String())
	}

	h.SetReady(false) // SIGTERM path
	rec = httptest.NewRecorder()
	h.Readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable || decodeStatus(t, rec) != "draining" {
		t.Fatalf("draining readyz = %d %s, want 503 draining", rec.Code, rec.Body.String())
	}

	h.SetReady(true)
	rec = httptest.NewRecorder()
	h.Readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("re-ready readyz = %d, want 200", rec.Code)
	}
}

func TestNewMux_NilStream_HealthRoutesOnly(t *testing.T) {
	mux := NewMux(nil, NewHealth())

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/readyz = %d", rec.Code)
	}

	// The fan-in tier has no stream route at all.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/realtime/stream", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("stream route with nil handler = %d, want 404", rec.Code)
	}
}

func TestNewMux_WiresStreamRoute(t *testing.T) {
	// A StreamHandler with nil deps answers 503 — proving the route is wired
	// without needing a backplane.
	mux := NewMux(NewStreamHandler(nil, nil, 0), NewHealth())
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/realtime/stream?ticket=x", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("stream route = %d, want 503 (nil deps)", rec.Code)
	}
}
