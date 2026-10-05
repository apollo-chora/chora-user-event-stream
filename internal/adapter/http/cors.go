// cors.go — CORS for the learner SSE stream (rt lane, walk-caught
// 2026-06-12). The stream is served on api.chora.site but BYPASSES the
// chora-gateway BFF pod (dedicated gateway path rule, ADR-183), so the BFF's
// CORS middleware never touches it. A cross-origin EventSource whose 200
// lacks Access-Control-Allow-Origin is killed by the BROWSER after the
// headers land — observed live as a healthy-at-the-LB stream
// (`client_disconnected_after_partial_response`) that the FE saw as an
// instant error and reconnect-looped on.
//
// The allowlist mirrors chora-gateway's cors.go exactly (same env var, same
// default set) so the two halves of api.chora.site never drift: explicit
// origins, no wildcard — the FE EventSource runs withCredentials, and CORS
// credentials mode forbids `*`.
package httpadapter

import (
	"net/http"
	"os"
	"strings"
	"sync"
)

// EnvCORSAllowedOrigins overrides the default chora-web origin set
// (comma-separated, scheme-qualified, no trailing slash).
const EnvCORSAllowedOrigins = "CHORA_CORS_ALLOWED_ORIGINS"

// defaultCORSAllowedOrigins is the canonical chora-web origin set —
// production chora.site + the four surface subdomains + localhost dev.
var defaultCORSAllowedOrigins = map[string]struct{}{
	"https://chora.site":       {},
	"https://cplus.chora.site": {},
	"https://hplus.chora.site": {},
	"https://oplus.chora.site": {},
	"https://rplus.chora.site": {},
	"http://localhost:4200":    {},
	"http://localhost:4201":    {},
	"http://localhost:6006":    {},
}

var (
	corsOriginsOnce sync.Once
	corsOrigins     map[string]struct{}
)

// allowedCORSOrigins memoises the effective set: env-CSV when set, otherwise
// the defaults.
func allowedCORSOrigins() map[string]struct{} {
	corsOriginsOnce.Do(func() {
		csv := strings.TrimSpace(os.Getenv(EnvCORSAllowedOrigins))
		if csv == "" {
			corsOrigins = defaultCORSAllowedOrigins
			return
		}
		set := make(map[string]struct{})
		for _, o := range strings.Split(csv, ",") {
			if o = strings.TrimSpace(o); o != "" {
				set[o] = struct{}{}
			}
		}
		corsOrigins = set
	})
	return corsOrigins
}

// resetCORSOriginsForTest clears the memoised set so env-driven tests can
// re-evaluate it.
func resetCORSOriginsForTest() {
	corsOriginsOnce = sync.Once{}
	corsOrigins = nil
}

// applyCORS echoes an allowlisted Origin (+credentials +Vary) onto the
// response. No Origin header (same-origin, curl, probes) or a non-allowlisted
// origin → no CORS headers, and the browser enforces from there.
func applyCORS(w http.ResponseWriter, r *http.Request) {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return
	}
	if _, ok := allowedCORSOrigins()[origin]; !ok {
		return
	}
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Access-Control-Allow-Credentials", "true")
	h.Set("Vary", "Origin")
}
