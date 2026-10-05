// cors_test.go — strict TDD RED for the stream's CORS surface (rt lane,
// walk-caught 2026-06-12): the SSE stream BYPASSES the BFF (and its CORS
// middleware), so chora-realtime must emit Access-Control-Allow-Origin
// itself or every browser EventSource self-aborts after the 200 — observed
// live as a mint→504(masked)→reconnect loop while curl saw a healthy stream
// (LB: 200 client_disconnected_after_partial_response).
package httpadapter

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func corsProbe(t *testing.T, origin string) *httptest.ResponseRecorder {
	t.Helper()
	// nil validator/bus → 503 path; CORS headers must ride EVERY response.
	h := NewStreamHandler(nil, nil, 0)
	r := httptest.NewRequest(http.MethodGet, "/api/v1/realtime/stream", nil)
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestStreamCORS_EchoesAllowlistedOrigin(t *testing.T) {
	for _, origin := range []string{
		"https://chora.site",
		"https://rplus.chora.site",
		"http://localhost:4200",
	} {
		w := corsProbe(t, origin)
		if got := w.Header().Get("Access-Control-Allow-Origin"); got != origin {
			t.Errorf("origin %s: ACAO = %q, want echo", origin, got)
		}
		if got := w.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
			t.Errorf("origin %s: ACA-Credentials = %q, want true (FE EventSource uses withCredentials)", origin, got)
		}
		if got := w.Header().Get("Vary"); got != "Origin" {
			t.Errorf("origin %s: Vary = %q, want Origin (per-origin echo must not be cached cross-origin)", origin, got)
		}
	}
}

func TestStreamCORS_UnknownOriginGetsNoACAO(t *testing.T) {
	w := corsProbe(t, "https://evil.example.com")
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("ACAO = %q for non-allowlisted origin, want empty", got)
	}
}

func TestStreamCORS_NoOriginHeaderNoACAO(t *testing.T) {
	// Same-origin / curl traffic carries no Origin — no CORS headers needed.
	w := corsProbe(t, "")
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("ACAO = %q without an Origin header, want empty", got)
	}
}

func TestStreamCORS_EnvOverridesAllowlist(t *testing.T) {
	t.Setenv(EnvCORSAllowedOrigins, "https://only.example.com")
	resetCORSOriginsForTest()
	t.Cleanup(resetCORSOriginsForTest)

	if w := corsProbe(t, "https://only.example.com"); w.Header().Get("Access-Control-Allow-Origin") != "https://only.example.com" {
		t.Errorf("env-listed origin not echoed")
	}
	if w := corsProbe(t, "https://chora.site"); w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("default origin must NOT be allowed when env overrides the list")
	}
}
