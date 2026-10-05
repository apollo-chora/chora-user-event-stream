// Package ticket validates the short-lived (~60s) one-time HMAC stream ticket
// minted by chora-gateway on the Bearer-authed GET /api/v1/realtime/ticket path
// (ADR-183). EventSource cannot send an Authorization header, so the ticket
// rides the stream's ?ticket= query param; keeping it ~60s + single-use bounds
// the exposure.
//
// The ticket is an HS256 JWT (aud "chora-realtime") signed with the
// chora-realtime-ticket-signer secret — hand-rolled HMAC verification matching
// the codebase convention (chora-common/auth/chorasession +
// chora-gateway's signSessionJWT intentionally avoid github.com/golang-jwt to
// save a dependency). Validation order: structural + signature + aud + exp
// (cheap, local) FIRST, then a single-use Redis claim (SET NX) — so a garbage
// token never reaches the backplane.
package ticket

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-user-event-stream/internal/port"
)

// Audience is the required `aud` claim.
//
// KEPT-CONTRACT (service renamed chora-realtime → chora-user-event-stream
// 2026-06-20): this audience string is a SHARED auth contract — chora-gateway
// mints stream tickets with aud="chora-realtime" (routes_realtime_ticket.go)
// and this tier validates it. It is an OPAQUE token audience, NOT the service's
// deployable name. Do NOT "fix" it to the new service name in isolation: minter
// and validator must change in lockstep (with dual-accept) or EVERY ticket is
// rejected → total SSE-auth outage. Intentionally stable.
const Audience = "chora-realtime"

// minSignerBytes is the HS256 minimum per RFC 7518 §3.2 (≥ HMAC-SHA-256 output
// size). Enforced at construction to fail loud on an undersized secret.
const minSignerBytes = 32

// defaultSkew tolerates minor clock drift between the gateway minter and the
// realtime validator on exp. Kept small (the ticket lives ~60s).
const defaultSkew = 5 * time.Second

// jtiClaimTTL is the single-use claim retention. It must comfortably exceed the
// ticket lifetime so a replayed ticket within its own validity window is still
// caught (ticket ~60s ⇒ claim 70s, matching ADR-183 `EX 70`).
const jtiClaimTTL = 70 * time.Second

// Error sentinels — the HTTP handler maps each to a RealtimeError code (401).
var (
	ErrMalformed      = errors.New("ticket: malformed")
	ErrBadSignature   = errors.New("ticket: invalid signature")
	ErrUnsupportedAlg = errors.New("ticket: unsupported alg (HS256 only)")
	ErrWrongAudience  = errors.New("ticket: wrong audience")
	ErrExpired        = errors.New("ticket: expired")
	ErrMissingClaim   = errors.New("ticket: missing required claim")
	ErrReplayed       = errors.New("ticket: replayed (jti already used)")
	ErrInvalidConfig  = errors.New("ticket: invalid validator config")
)

// Claims is the ticket claim set. This is the EXACT shape the chora-gateway
// minter must produce (HS256, header {"alg":"HS256","typ":"JWT"}):
//
//	{
//	  "aud":       "chora-realtime",
//	  "gcid":      "<learner GCID from the ChoraSession>",
//	  "tenant_id": "<active tenant from the ChoraSession>",
//	  "jti":       "<UUIDv7 — single-use nonce>",
//	  "iat":       <unix seconds>,
//	  "exp":       <unix seconds, ~iat+60>
//	}
type Claims struct {
	Aud      string `json:"aud"`
	GCID     string `json:"gcid"`
	TenantID string `json:"tenant_id"`
	JTI      string `json:"jti"`
	IssuedAt int64  `json:"iat"`
	Expires  int64  `json:"exp"`
}

// HMACValidator validates tickets + enforces single-use via a JTIChecker.
type HMACValidator struct {
	signer []byte
	jti    port.JTIChecker
	skew   time.Duration
	now    func() time.Time
}

// NewHMACValidator constructs a validator. Fails loud when the signer is
// shorter than 32 bytes or the JTIChecker is nil. The signer slice is copied.
func NewHMACValidator(signer []byte, jti port.JTIChecker) (*HMACValidator, error) {
	if len(signer) < minSignerBytes {
		return nil, fmt.Errorf("%w: signer must be ≥%d bytes (got %d)", ErrInvalidConfig, minSignerBytes, len(signer))
	}
	if jti == nil {
		return nil, fmt.Errorf("%w: nil JTIChecker", ErrInvalidConfig)
	}
	cp := make([]byte, len(signer))
	copy(cp, signer)
	return &HMACValidator{
		signer: cp,
		jti:    jti,
		skew:   defaultSkew,
		now:    func() time.Time { return time.Now().UTC() },
	}, nil
}

// Validate verifies signature + aud + exp, then claims the jti for single-use.
// Returns (gcid, tenant) on success; one of the package sentinels otherwise.
func (v *HMACValidator) Validate(ctx context.Context, raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("%w: empty ticket", ErrMalformed)
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return "", "", fmt.Errorf("%w: expected 3 segments, got %d", ErrMalformed, len(parts))
	}

	// 1. Header — alg must be HS256.
	hdrJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", "", fmt.Errorf("%w: decode header: %v", ErrMalformed, err)
	}
	var hdr struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(hdrJSON, &hdr); err != nil {
		return "", "", fmt.Errorf("%w: parse header: %v", ErrMalformed, err)
	}
	if hdr.Alg != "HS256" {
		return "", "", fmt.Errorf("%w: got %q", ErrUnsupportedAlg, hdr.Alg)
	}

	// 2. HS256 MAC over header.claims (constant-time compare).
	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", "", fmt.Errorf("%w: decode signature: %v", ErrMalformed, err)
	}
	mac := hmac.New(sha256.New, v.signer)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(mac.Sum(nil), sigBytes) {
		return "", "", fmt.Errorf("%w: HMAC mismatch", ErrBadSignature)
	}

	// 3. Claims.
	clmJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("%w: decode claims: %v", ErrMalformed, err)
	}
	var c Claims
	if err := json.Unmarshal(clmJSON, &c); err != nil {
		return "", "", fmt.Errorf("%w: parse claims: %v", ErrMalformed, err)
	}

	// 4. Audience.
	if c.Aud != Audience {
		return "", "", fmt.Errorf("%w: got %q want %q", ErrWrongAudience, c.Aud, Audience)
	}

	// 5. Required identity claims.
	if strings.TrimSpace(c.GCID) == "" {
		return "", "", fmt.Errorf("%w: gcid", ErrMissingClaim)
	}
	if c.Expires == 0 {
		return "", "", fmt.Errorf("%w: exp", ErrMissingClaim)
	}
	if strings.TrimSpace(c.JTI) == "" {
		return "", "", fmt.Errorf("%w: jti", ErrMissingClaim)
	}

	// 6. Expiry (with small skew).
	exp := time.Unix(c.Expires, 0).UTC()
	if exp.Add(v.skew).Before(v.now()) {
		return "", "", fmt.Errorf("%w: exp=%s now=%s", ErrExpired, exp, v.now())
	}

	// 7. Single-use: claim the jti LAST (after the token is otherwise valid) so
	//    a garbage/expired/forged ticket never burns a backplane write.
	claimed, err := v.jti.Claim(ctx, c.JTI, jtiClaimTTL)
	if err != nil {
		return "", "", fmt.Errorf("ticket: jti claim: %w", err)
	}
	if !claimed {
		return "", "", fmt.Errorf("%w: jti=%s", ErrReplayed, c.JTI)
	}

	return strings.TrimSpace(c.GCID), strings.TrimSpace(c.TenantID), nil
}

// Compile-time assertion.
var _ port.TicketValidator = (*HMACValidator)(nil)
