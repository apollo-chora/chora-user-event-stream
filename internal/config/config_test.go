package config

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	cgcsecrets "github.com/apollo-chora/chora-common/secrets"
)

// allEnvKeys is every env var Load consults — pinned (possibly to empty) in
// each test so ambient developer/CI environment never bleeds in.
var allEnvKeys = []string{
	"PORT",
	"CHORA_REALTIME_KEEPALIVE_SECONDS",
	"NATS_URL",
	"CHORA_SOURCE_PROJECT",
	"CHORA_REDIS_ADDR",
	"CHORA_REDIS_PASSWORD_SECRET_ID",
	"CHORA_REDIS_PASSWORD",
	"CHORA_REDIS_CA_CERT_SECRET_ID",
	"CHORA_REDIS_CA_CERT",
	"CHORA_REALTIME_TICKET_SIGNER_SECRET_ID",
	"CHORA_REALTIME_TICKET_SIGNER_SECRET",
}

func pinEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	for _, k := range allEnvKeys {
		t.Setenv(k, "")
	}
	for k, v := range overrides {
		t.Setenv(k, v)
	}
}

// stubFetcher is a programmable cgcsecrets.SecretFetcher for exercising the
// boot-time retry-backoff + fail-loud contract without touching the process
// environment. Injected via the newFetcher seam (installStubFetcher).
type stubFetcher struct {
	mu          sync.Mutex
	calls       int
	failFirstN  int  // return a RETRIABLE error for the first N calls
	failForever bool // always return a NON-retriable error (fail fast, no sleeps)
	value       string
}

func (s *stubFetcher) GetSecret(_ context.Context, _ string) (string, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	if s.failForever {
		// ErrSecretNotFound is non-retriable → FetchSecretWithBackoff fails
		// fast (no backoff sleeps) modelling a misconfigured/absent secret.
		return "", cgcsecrets.ErrSecretNotFound
	}
	if n <= s.failFirstN {
		// A plain error is treated as retriable → the helper backs off + retries.
		return "", errors.New("stub transient resolve failure")
	}
	return s.value, nil
}

func (s *stubFetcher) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// installStubFetcher points the newFetcher seam at f for the duration of the
// test, restoring the real constructor afterwards.
func installStubFetcher(t *testing.T, f cgcsecrets.SecretFetcher) {
	t.Helper()
	prev := newFetcher
	newFetcher = func(context.Context, string) (cgcsecrets.SecretFetcher, func() error, error) {
		return f, func() error { return nil }, nil
	}
	t.Cleanup(func() { newFetcher = prev })
}

func TestLoad_Defaults(t *testing.T) {
	pinEnv(t, nil)
	c, err := Load(context.Background())
	if err != nil {
		t.Fatalf("Load erred on defaults: %v", err)
	}
	if c.Port != "8080" {
		t.Fatalf("Port = %q, want 8080", c.Port)
	}
	if c.NATSURL != "" || c.RedisAddr != "" || c.RedisPassword != "" || c.RedisCACertPEM != "" || c.TicketSigner != "" {
		t.Fatalf("defaults must be empty: %+v", c)
	}
	if c.SourceProject != "chora-local" {
		t.Fatalf("SourceProject = %q, want chora-local", c.SourceProject)
	}
	if c.KeepaliveSeconds != 0 {
		t.Fatalf("KeepaliveSeconds = %d, want 0", c.KeepaliveSeconds)
	}
	if got := c.Keepalive(); got != 15*time.Second {
		t.Fatalf("Keepalive() = %v, want 15s", got)
	}
}

func TestLoad_WhitespacePortDefaults(t *testing.T) {
	pinEnv(t, map[string]string{"PORT": "   "})
	c, err := Load(context.Background())
	if err != nil {
		t.Fatalf("Load erred: %v", err)
	}
	if c.Port != "8080" {
		t.Fatalf("Port = %q, want 8080", c.Port)
	}
}

func TestLoad_LiteralEnvValues(t *testing.T) {
	pinEnv(t, map[string]string{
		"PORT":                                "9090",
		"CHORA_REALTIME_KEEPALIVE_SECONDS":    "30",
		"NATS_URL":                            "  nats://127.0.0.1:4222  ",
		"CHORA_SOURCE_PROJECT":                "  chora-local  ",
		"CHORA_REDIS_ADDR":                    " 10.0.0.5:6379 ",
		"CHORA_REDIS_PASSWORD":                " hunter2 ",
		"CHORA_REDIS_CA_CERT":                 " ---PEM--- ",
		"CHORA_REALTIME_TICKET_SIGNER_SECRET": " signer-key ",
	})
	c, err := Load(context.Background())
	if err != nil {
		t.Fatalf("Load erred: %v", err)
	}
	if c.Port != "9090" {
		t.Fatalf("Port = %q", c.Port)
	}
	if c.KeepaliveSeconds != 30 || c.Keepalive() != 30*time.Second {
		t.Fatalf("keepalive = %d / %v, want 30 / 30s", c.KeepaliveSeconds, c.Keepalive())
	}
	if c.NATSURL != "nats://127.0.0.1:4222" {
		t.Fatalf("NATSURL = %q (want trimmed)", c.NATSURL)
	}
	if c.SourceProject != "chora-local" {
		t.Fatalf("SourceProject = %q (want trimmed)", c.SourceProject)
	}
	if c.RedisAddr != "10.0.0.5:6379" {
		t.Fatalf("RedisAddr = %q (want trimmed)", c.RedisAddr)
	}
	if c.RedisPassword != "hunter2" || c.RedisCACertPEM != "---PEM---" || c.TicketSigner != "signer-key" {
		t.Fatalf("literal secrets not resolved/trimmed: %+v", c)
	}
}

func TestLoad_NonNumericKeepaliveFallsBack(t *testing.T) {
	pinEnv(t, map[string]string{"CHORA_REALTIME_KEEPALIVE_SECONDS": "abc"})
	c, err := Load(context.Background())
	if err != nil {
		t.Fatalf("Load erred: %v", err)
	}
	if c.KeepaliveSeconds != 0 || c.Keepalive() != 15*time.Second {
		t.Fatalf("keepalive = %d / %v, want 0 / 15s", c.KeepaliveSeconds, c.Keepalive())
	}
}

func TestKeepalive_NegativeDefaults(t *testing.T) {
	if got := (Config{KeepaliveSeconds: -5}).Keepalive(); got != 15*time.Second {
		t.Fatalf("Keepalive(-5) = %v, want 15s", got)
	}
	if got := (Config{KeepaliveSeconds: 7}).Keepalive(); got != 7*time.Second {
		t.Fatalf("Keepalive(7) = %v, want 7s", got)
	}
}

func TestFirstNonEmptyEnv(t *testing.T) {
	keys := []string{"CHORA_REDIS_PASSWORD_SECRET_ID", "CHORA_REDIS_CA_CERT_SECRET_ID"}

	t.Run("none set", func(t *testing.T) {
		pinEnv(t, nil)
		if got := firstNonEmptyEnv(keys...); got != "" {
			t.Fatalf("firstNonEmptyEnv = %q, want empty", got)
		}
	})
	t.Run("later key wins when earlier empty", func(t *testing.T) {
		pinEnv(t, map[string]string{"CHORA_REDIS_CA_CERT_SECRET_ID": " secret-b "})
		if got := firstNonEmptyEnv(keys...); got != "secret-b" {
			t.Fatalf("firstNonEmptyEnv = %q, want secret-b (trimmed)", got)
		}
	})
	t.Run("earlier key takes precedence", func(t *testing.T) {
		pinEnv(t, map[string]string{
			"CHORA_REDIS_PASSWORD_SECRET_ID": "secret-a",
			"CHORA_REDIS_CA_CERT_SECRET_ID":  "secret-b",
		})
		if got := firstNonEmptyEnv(keys...); got != "secret-a" {
			t.Fatalf("firstNonEmptyEnv = %q, want secret-a", got)
		}
	})
}

// --- Boot-time secret resolve (CHO-2036) ---------------------------------
//
// The fail-silent bug: a *_SECRET_ID configured but unresolvable used to
// degrade to "" — downstream the empty Redis password connects NOAUTH, the
// fan-in tier logs "fan-in DISABLED" and boots health-only, and every push
// silently drops. The fix retries transient failures with bounded backoff
// and FAILS LOUD (Load returns an error) on persistent failure.

// TestLoad_SecretTransientFailureRetriesThenResolves — (a): a *_SECRET_ID is
// set and the resolver fails transiently once, then succeeds. Load MUST retry
// and boot with the secret resolved (fan-in enabled), NOT degrade to empty.
func TestLoad_SecretTransientFailureRetriesThenResolves(t *testing.T) {
	pinEnv(t, map[string]string{
		"CHORA_REALTIME_TICKET_SIGNER_SECRET_ID": "realtime-ticket-signer",
	})
	f := &stubFetcher{failFirstN: 1, value: "resolved-signer-key"}
	installStubFetcher(t, f)

	c, err := Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error on transient-then-success: %v", err)
	}
	if c.TicketSigner != "resolved-signer-key" {
		t.Fatalf("TicketSigner = %q, want resolved-signer-key (retry then resolve)", c.TicketSigner)
	}
	if f.callCount() < 2 {
		t.Fatalf("expected >= 2 resolver calls (1 transient + 1 success); got %d", f.callCount())
	}
}

// TestLoad_SecretPermanentFailureFailsLoud — (b): a *_SECRET_ID is set (the
// operator INTENDS secret resolution) but resolution permanently fails. Load
// MUST fail loud (return an error) so boot exits non-zero / stays NotReady —
// it MUST NOT silently return an empty secret NOR leak the literal env
// fallback.
func TestLoad_SecretPermanentFailureFailsLoud(t *testing.T) {
	pinEnv(t, map[string]string{
		"CHORA_REDIS_PASSWORD_SECRET_ID": "redis-pass",
		"CHORA_REDIS_PASSWORD":           "literal-must-not-leak",
	})
	f := &stubFetcher{failForever: true}
	installStubFetcher(t, f)

	c, err := Load(context.Background())
	if err == nil {
		t.Fatalf("Load must FAIL LOUD on permanent resolve failure; got nil err, cfg=%+v", c)
	}
	if c.RedisPassword == "literal-must-not-leak" {
		t.Fatalf("Load leaked literal fallback on SECRET_ID resolve failure")
	}
	if !strings.Contains(err.Error(), "redis-pass") {
		t.Errorf("error should name the failing secret; got %q", err.Error())
	}
}

// TestLoad_SecretPersistentRetriableExhaustsBudgetFailsLoud — the resolver
// keeps returning a retriable error. With a short caller deadline the backoff
// bails within budget and Load fails loud — never boots with fan-in silently
// disabled.
func TestLoad_SecretPersistentRetriableExhaustsBudgetFailsLoud(t *testing.T) {
	pinEnv(t, map[string]string{
		"CHORA_REDIS_PASSWORD_SECRET_ID": "redis-pass",
	})
	f := &stubFetcher{failFirstN: 1 << 30} // effectively always retriable-fails
	installStubFetcher(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	c, err := Load(ctx)
	if err == nil {
		t.Fatalf("Load must fail loud when a retriable resolve never clears; got nil err, cfg=%+v", c)
	}
	if f.callCount() < 1 {
		t.Errorf("resolver should have been attempted at least once; got %d", f.callCount())
	}
}

// TestLoad_SecretResolverBuildFailureFailsLoud — a *_SECRET_ID is set but the
// resolver cannot be constructed. Load MUST fail loud, NOT boot with a
// silently-empty secret.
func TestLoad_SecretResolverBuildFailureFailsLoud(t *testing.T) {
	pinEnv(t, map[string]string{
		"CHORA_REDIS_PASSWORD_SECRET_ID": "redis-pass",
		"CHORA_REDIS_PASSWORD":           "literal-must-not-leak",
	})
	prev := newFetcher
	newFetcher = func(context.Context, string) (cgcsecrets.SecretFetcher, func() error, error) {
		return nil, nil, errors.New("resolver unavailable")
	}
	t.Cleanup(func() { newFetcher = prev })

	c, err := Load(context.Background())
	if err == nil {
		t.Fatalf("Load must fail loud when the resolver cannot build; got nil err, cfg=%+v", c)
	}
	if c.RedisPassword == "literal-must-not-leak" {
		t.Fatalf("Load leaked literal fallback on resolver build failure")
	}
}

// TestLoad_SecretIDWithoutResolverFallsBackToLiteral — a *_SECRET_ID with NO
// resolver (the newFetcher seam returns nil, nil, nil) degrades to the literal
// env (local-dev box) rather than failing boot.
func TestLoad_SecretIDWithoutResolverFallsBackToLiteral(t *testing.T) {
	pinEnv(t, map[string]string{
		"CHORA_REDIS_PASSWORD_SECRET_ID": "redis-pass",
		"CHORA_REDIS_PASSWORD":           "literal-fallback",
	})
	prev := newFetcher
	newFetcher = func(context.Context, string) (cgcsecrets.SecretFetcher, func() error, error) {
		return nil, func() error { return nil }, nil
	}
	t.Cleanup(func() { newFetcher = prev })

	c, err := Load(context.Background())
	if err != nil {
		t.Fatalf("Load erred without a resolver: %v", err)
	}
	if c.RedisPassword != "literal-fallback" {
		t.Fatalf("RedisPassword = %q, want literal-fallback", c.RedisPassword)
	}
}

// TestResolveSecret_NoSecretIDUsesLiteral — no *_SECRET_ID set → literal env,
// no fetcher touched.
func TestResolveSecret_NoSecretIDUsesLiteral(t *testing.T) {
	pinEnv(t, map[string]string{"CHORA_REDIS_PASSWORD": " direct-literal "})
	got, err := resolveSecret(context.Background(), nil, "CHORA_REDIS_PASSWORD_SECRET_ID", "CHORA_REDIS_PASSWORD")
	if err != nil {
		t.Fatalf("resolveSecret erred: %v", err)
	}
	if got != "direct-literal" {
		t.Fatalf("resolveSecret = %q, want direct-literal (trimmed)", got)
	}
}

// TestResolveSecret_SecretIDButNilFetcherUsesLiteral — SECRET_ID set but no
// fetcher (no resolver) → literal fallback, no error.
func TestResolveSecret_SecretIDButNilFetcherUsesLiteral(t *testing.T) {
	pinEnv(t, map[string]string{
		"CHORA_REDIS_PASSWORD_SECRET_ID": "redis-pass",
		"CHORA_REDIS_PASSWORD":           "nil-fetcher-literal",
	})
	got, err := resolveSecret(context.Background(), nil, "CHORA_REDIS_PASSWORD_SECRET_ID", "CHORA_REDIS_PASSWORD")
	if err != nil {
		t.Fatalf("resolveSecret erred: %v", err)
	}
	if got != "nil-fetcher-literal" {
		t.Fatalf("resolveSecret = %q, want nil-fetcher-literal", got)
	}
}

// TestResolveSecret_TransientRetriesThenResolves — direct unit: an injected
// fetcher fails transiently once, then succeeds; resolveSecret returns the
// resolved (trimmed) value with no error.
func TestResolveSecret_TransientRetriesThenResolves(t *testing.T) {
	pinEnv(t, map[string]string{"CHORA_REDIS_PASSWORD_SECRET_ID": "redis-pass"})
	f := &stubFetcher{failFirstN: 1, value: "  resolved-pass  "}
	got, err := resolveSecret(context.Background(), f, "CHORA_REDIS_PASSWORD_SECRET_ID", "CHORA_REDIS_PASSWORD")
	if err != nil {
		t.Fatalf("resolveSecret erred on transient-then-success: %v", err)
	}
	if got != "resolved-pass" {
		t.Fatalf("resolveSecret = %q, want resolved-pass (trimmed)", got)
	}
	if f.callCount() < 2 {
		t.Fatalf("expected >= 2 calls (1 transient + success); got %d", f.callCount())
	}
}

// TestResolveSecret_PermanentFailureReturnsError — direct unit: a persistent
// resolve failure returns a wrapped error (fail loud), NOT "".
func TestResolveSecret_PermanentFailureReturnsError(t *testing.T) {
	pinEnv(t, map[string]string{"CHORA_REDIS_PASSWORD_SECRET_ID": "redis-pass"})
	f := &stubFetcher{failForever: true}
	got, err := resolveSecret(context.Background(), f, "CHORA_REDIS_PASSWORD_SECRET_ID", "CHORA_REDIS_PASSWORD")
	if err == nil {
		t.Fatalf("resolveSecret must return an error on permanent failure; got %q", got)
	}
	if got != "" {
		t.Fatalf("resolveSecret must not return a value alongside the error; got %q", got)
	}
}

// byKeyFetcher resolves per secret-ID so Load can fail on a LATER secret
// after EARLIER ones have already resolved — exercising the fail-loud return
// arms of the CA-cert and ticket-signer resolutions (config.go resolveSecret
// error returns), which the call-order-based stubFetcher cannot reach.
type byKeyFetcher struct {
	behaviors map[string]func() (string, error)
}

func (f *byKeyFetcher) GetSecret(_ context.Context, secretID string) (string, error) {
	b, ok := f.behaviors[secretID]
	if !ok {
		return "", errors.New("byKeyFetcher: unprogrammed secret " + secretID)
	}
	return b()
}

// TestLoad_LaterSecretResolveFailureFailsLoud — each *_SECRET_ID resolve is
// independently fail-loud: the CA-cert secret (resolved SECOND) and the
// ticket signer (resolved THIRD) must each abort Load once they fail, even
// though the earlier secrets resolved fine. Non-retriable
// ErrSecretNotFound keeps the failure immediate (no backoff sleeps).
func TestLoad_LaterSecretResolveFailureFailsLoud(t *testing.T) {
	t.Run("ca cert resolves after password succeeds", func(t *testing.T) {
		pinEnv(t, map[string]string{
			"CHORA_REDIS_PASSWORD_SECRET_ID": "redis-pass",
			"CHORA_REDIS_CA_CERT_SECRET_ID":  "redis-ca",
		})
		f := &byKeyFetcher{behaviors: map[string]func() (string, error){
			"redis-pass": func() (string, error) { return "resolved-pass", nil },
			"redis-ca":   func() (string, error) { return "", cgcsecrets.ErrSecretNotFound },
		}}
		installStubFetcher(t, f)

		_, err := Load(context.Background())
		if err == nil {
			t.Fatalf("Load must fail loud when the CA-cert secret cannot resolve")
		}
		if !strings.Contains(err.Error(), "redis-ca") {
			t.Errorf("error should name the failing secret; got %q", err.Error())
		}
	})

	t.Run("ticket signer resolves after earlier secrets succeed", func(t *testing.T) {
		pinEnv(t, map[string]string{
			"CHORA_REDIS_PASSWORD_SECRET_ID":         "redis-pass",
			"CHORA_REDIS_CA_CERT_SECRET_ID":          "redis-ca",
			"CHORA_REALTIME_TICKET_SIGNER_SECRET_ID": "realtime-ticket-signer",
		})
		f := &byKeyFetcher{behaviors: map[string]func() (string, error){
			"redis-pass":             func() (string, error) { return "resolved-pass", nil },
			"redis-ca":               func() (string, error) { return "resolved-ca", nil },
			"realtime-ticket-signer": func() (string, error) { return "", cgcsecrets.ErrSecretNotFound },
		}}
		installStubFetcher(t, f)

		_, err := Load(context.Background())
		if err == nil {
			t.Fatalf("Load must fail loud when the ticket signer secret cannot resolve")
		}
		if !strings.Contains(err.Error(), "realtime-ticket-signer") {
			t.Errorf("error should name the failing secret; got %q", err.Error())
		}
	})
}
