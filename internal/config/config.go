// Package config loads chora-user-event-stream's env-driven configuration. Per
// feedback_no_inline_config every URL/secret is sourced from env — never
// hard-coded. Secret values resolve from a *_SECRET_ID env via the shared
// env-backed secrets helper, falling back to a literal env for local dev.
//
// Fail-loud contract (CHO-2036): when a *_SECRET_ID is configured the operator
// INTENDS secret resolution. Load resolves it with a bounded retry-backoff
// (cgcsecrets.FetchSecretWithBackoff). If a configured secret STILL cannot be
// resolved after the retry budget, Load returns an error so the boot path fails
// LOUD (exit non-zero → crashloop → never Ready). It MUST NOT boot with a
// silently-empty secret: an empty Redis password connects NOAUTH, the fan-in
// tier logs "fan-in DISABLED" and serves health-only, and every push silently
// drops with no signal. Per feedback_no_stubs_real_wiring +
// feedback_resilience_priority.
package config

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	cgcsecrets "github.com/apollo-chora/chora-common/secrets"
)

// Config is the resolved runtime configuration shared by both binaries.
type Config struct {
	Port             string // HTTP port (PORT, default 8080)
	NATSURL          string // NATS_URL (fan-in only; "" → fan-in disabled)
	SourceProject    string // CHORA_SOURCE_PROJECT (default chora-local)
	RedisAddr        string // CHORA_REDIS_ADDR host:port ("" → backplane disabled)
	RedisPassword    string // resolved AUTH string ("" → no auth)
	RedisCACertPEM   string // resolved server-CA PEM ("" → plaintext)
	TicketSigner     string // resolved HS256 ticket-signer key ("" → ticket validation disabled)
	KeepaliveSeconds int    // SSE heartbeat (CHORA_REALTIME_KEEPALIVE_SECONDS, default 15)
}

// Keepalive returns the SSE heartbeat interval.
func (c Config) Keepalive() time.Duration {
	if c.KeepaliveSeconds <= 0 {
		return 15 * time.Second
	}
	return time.Duration(c.KeepaliveSeconds) * time.Second
}

// secretIDEnvs enumerates every *_SECRET_ID env Load may resolve — used to
// decide whether a secret resolver is needed at all (pure-literal dev boots
// skip it entirely).
var secretIDEnvs = []string{
	"CHORA_REDIS_PASSWORD_SECRET_ID",
	"CHORA_REDIS_CA_CERT_SECRET_ID",
	"CHORA_REALTIME_TICKET_SIGNER_SECRET_ID",
}

// newFetcher constructs an env-backed secret fetcher for the given project. It
// is a package var so tests can inject a programmable fetcher (the
// SecretFetcher seam) without touching the process environment. Returns the
// fetcher plus a close func the caller MUST defer.
var newFetcher = func(ctx context.Context, project string) (cgcsecrets.SecretFetcher, func() error, error) {
	c, err := cgcsecrets.NewClient(ctx, project)
	if err != nil {
		return nil, nil, err
	}
	return c, c.Close, nil
}

// Load reads + resolves configuration. Returns an error (fail loud) when a
// configured *_SECRET_ID cannot be resolved after the bounded retry-backoff —
// never boots with a silently-empty secret. See the package doc.
func Load(ctx context.Context) (Config, error) {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8080"
	}
	keepalive, _ := strconv.Atoi(os.Getenv("CHORA_REALTIME_KEEPALIVE_SECONDS"))
	sourceProject := strings.TrimSpace(os.Getenv("CHORA_SOURCE_PROJECT"))
	if sourceProject == "" {
		sourceProject = "chora-local"
	}

	// Build ONE secret fetcher, shared across every secret, iff the operator
	// configured at least one *_SECRET_ID. A configured SECRET_ID signals intent
	// to resolve a secret, so a client build failure here is fatal (fail loud),
	// not a silent degrade to empty.
	var fetcher cgcsecrets.SecretFetcher
	if firstNonEmptyEnv(secretIDEnvs...) != "" {
		f, closeFn, err := newFetcher(ctx, sourceProject)
		if err != nil {
			return Config{}, fmt.Errorf("realtime config: secret resolver (project=%s): %w", sourceProject, err)
		}
		if closeFn != nil {
			defer func() { _ = closeFn() }()
		}
		fetcher = f
	}

	redisPassword, err := resolveSecret(ctx, fetcher, "CHORA_REDIS_PASSWORD_SECRET_ID", "CHORA_REDIS_PASSWORD")
	if err != nil {
		return Config{}, err
	}
	redisCACert, err := resolveSecret(ctx, fetcher, "CHORA_REDIS_CA_CERT_SECRET_ID", "CHORA_REDIS_CA_CERT")
	if err != nil {
		return Config{}, err
	}
	ticketSigner, err := resolveSecret(ctx, fetcher, "CHORA_REALTIME_TICKET_SIGNER_SECRET_ID", "CHORA_REALTIME_TICKET_SIGNER_SECRET")
	if err != nil {
		return Config{}, err
	}

	return Config{
		Port:             port,
		NATSURL:          strings.TrimSpace(os.Getenv("NATS_URL")),
		SourceProject:    sourceProject,
		RedisAddr:        strings.TrimSpace(os.Getenv("CHORA_REDIS_ADDR")),
		RedisPassword:    redisPassword,
		RedisCACertPEM:   redisCACert,
		TicketSigner:     ticketSigner,
		KeepaliveSeconds: keepalive,
	}, nil
}

// firstNonEmptyEnv returns the first non-empty env value among keys.
func firstNonEmptyEnv(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

// resolveSecret resolves one secret value:
//
//   - No *_SECRET_ID set → the literal env (local dev), no error.
//   - *_SECRET_ID set but fetcher == nil (no resolvable project) → the literal
//     env; a dev box may set a SECRET_ID without a project and must still boot.
//   - *_SECRET_ID set with a fetcher → resolve it with bounded retry-backoff. A
//     persistent failure returns an error (fail loud) — it NEVER silently falls
//     back to the literal env or "".
func resolveSecret(ctx context.Context, fetcher cgcsecrets.SecretFetcher, secretIDEnv, literalEnv string) (string, error) {
	id := strings.TrimSpace(os.Getenv(secretIDEnv))
	if id == "" || fetcher == nil {
		return strings.TrimSpace(os.Getenv(literalEnv)), nil
	}
	v, err := cgcsecrets.FetchSecretWithBackoff(ctx, fetcher, id)
	if err != nil {
		return "", fmt.Errorf("realtime config: secret %q (%s) resolve failed: %w", id, secretIDEnv, err)
	}
	return strings.TrimSpace(v), nil
}
