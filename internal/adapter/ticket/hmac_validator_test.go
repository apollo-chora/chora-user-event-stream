package ticket

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// mint reproduces the chora-gateway minter (HS256, hand-rolled — matching
// signSessionJWT) so the test exercises the exact wire shape the validator
// expects.
func mint(signer []byte, c Claims) string {
	hb, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	cb, _ := json.Marshal(c)
	signing := base64.RawURLEncoding.EncodeToString(hb) + "." + base64.RawURLEncoding.EncodeToString(cb)
	mac := hmac.New(sha256.New, signer)
	mac.Write([]byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// mintRaw signs ARBITRARY header/claims segments so tests can exercise the
// decode arms behind a structurally valid signature.
func mintRaw(signer []byte, hdrSeg, clmSeg string) string {
	signing := hdrSeg + "." + clmSeg
	mac := hmac.New(sha256.New, signer)
	mac.Write([]byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// stubJTI is an in-memory JTIChecker: fresh on first claim, replay after.
type stubJTI struct {
	seen        map[string]bool
	forceReplay bool
	err         error
}

func (s *stubJTI) Claim(_ context.Context, jti string, _ time.Duration) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	if s.forceReplay {
		return false, nil
	}
	if s.seen == nil {
		s.seen = map[string]bool{}
	}
	if s.seen[jti] {
		return false, nil
	}
	s.seen[jti] = true
	return true, nil
}

var testSigner = []byte("0123456789abcdef0123456789abcdef") // 32 bytes

func validClaims() Claims {
	now := time.Now().UTC()
	return Claims{
		Aud:      Audience,
		GCID:     "learner-1",
		TenantID: "tenant-A",
		JTI:      "jti-001",
		IssuedAt: now.Unix(),
		Expires:  now.Add(60 * time.Second).Unix(),
	}
}

func newV(t *testing.T, jti *stubJTI) *HMACValidator {
	t.Helper()
	v, err := NewHMACValidator(testSigner, jti)
	if err != nil {
		t.Fatalf("NewHMACValidator: %v", err)
	}
	return v
}

func TestValidate_Valid(t *testing.T) {
	v := newV(t, &stubJTI{})
	gcid, tenant, err := v.Validate(context.Background(), mint(testSigner, validClaims()))
	if err != nil {
		t.Fatalf("Validate valid: %v", err)
	}
	if gcid != "learner-1" || tenant != "tenant-A" {
		t.Fatalf("got gcid=%q tenant=%q", gcid, tenant)
	}
}

func TestValidate_WrongAudience(t *testing.T) {
	c := validClaims()
	c.Aud = "chora-gateway"
	v := newV(t, &stubJTI{})
	if _, _, err := v.Validate(context.Background(), mint(testSigner, c)); !errors.Is(err, ErrWrongAudience) {
		t.Fatalf("err = %v; want ErrWrongAudience", err)
	}
}

func TestValidate_Expired(t *testing.T) {
	c := validClaims()
	c.Expires = time.Now().UTC().Add(-1 * time.Minute).Unix()
	v := newV(t, &stubJTI{})
	if _, _, err := v.Validate(context.Background(), mint(testSigner, c)); !errors.Is(err, ErrExpired) {
		t.Fatalf("err = %v; want ErrExpired", err)
	}
}

func TestValidate_BadSignature(t *testing.T) {
	wrong := []byte("ffffffffffffffffffffffffffffffff") // 32 bytes, different key
	v := newV(t, &stubJTI{})
	if _, _, err := v.Validate(context.Background(), mint(wrong, validClaims())); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("err = %v; want ErrBadSignature", err)
	}
}

func TestValidate_Replayed(t *testing.T) {
	// First use succeeds; the second (same jti) is caught by the stub.
	jti := &stubJTI{}
	v := newV(t, jti)
	tok := mint(testSigner, validClaims())
	if _, _, err := v.Validate(context.Background(), tok); err != nil {
		t.Fatalf("first use: %v", err)
	}
	if _, _, err := v.Validate(context.Background(), tok); !errors.Is(err, ErrReplayed) {
		t.Fatalf("replay err = %v; want ErrReplayed", err)
	}
}

func TestValidate_MissingGCID(t *testing.T) {
	c := validClaims()
	c.GCID = ""
	v := newV(t, &stubJTI{})
	if _, _, err := v.Validate(context.Background(), mint(testSigner, c)); !errors.Is(err, ErrMissingClaim) {
		t.Fatalf("err = %v; want ErrMissingClaim", err)
	}
}

func TestValidate_MalformedSegments(t *testing.T) {
	v := newV(t, &stubJTI{})
	if _, _, err := v.Validate(context.Background(), "not-a-jwt"); !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v; want ErrMalformed", err)
	}
}

func TestNewHMACValidator_RejectsShortSigner(t *testing.T) {
	if _, err := NewHMACValidator([]byte("short"), &stubJTI{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("err = %v; want ErrInvalidConfig", err)
	}
}

func TestNewHMACValidator_RejectsNilJTIChecker(t *testing.T) {
	if _, err := NewHMACValidator(testSigner, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("err = %v; want ErrInvalidConfig", err)
	}
}

func TestNewHMACValidator_CopiesSigner(t *testing.T) {
	key := make([]byte, len(testSigner))
	copy(key, testSigner)
	v, err := NewHMACValidator(key, &stubJTI{})
	if err != nil {
		t.Fatalf("NewHMACValidator: %v", err)
	}
	key[0] ^= 0xFF // mutate the caller's slice AFTER construction
	if _, _, err := v.Validate(context.Background(), mint(testSigner, validClaims())); err != nil {
		t.Fatalf("validator must hold its own signer copy: %v", err)
	}
}

func TestValidate_EmptyTicket(t *testing.T) {
	v := newV(t, &stubJTI{})
	if _, _, err := v.Validate(context.Background(), "   "); !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v; want ErrMalformed", err)
	}
}

// hdrSeg / clmSeg helpers for the structural table below.
func b64seg(v any) string {
	b, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(b)
}

func TestValidate_StructuralArms(t *testing.T) {
	goodHdr := b64seg(map[string]string{"alg": "HS256", "typ": "JWT"})
	goodClm := b64seg(validClaims())

	tests := []struct {
		name string
		tok  string
		want error
	}{
		{"two segments", "a.b", ErrMalformed},
		{"four segments", "a.b.c.d", ErrMalformed},
		{"bad base64 header", mintRaw(testSigner, "%%%not-b64%%%", goodClm), ErrMalformed},
		{"header not JSON", mintRaw(testSigner, base64.RawURLEncoding.EncodeToString([]byte("not-json")), goodClm), ErrMalformed},
		{"non-HS256 alg", mintRaw(testSigner, b64seg(map[string]string{"alg": "none", "typ": "JWT"}), goodClm), ErrUnsupportedAlg},
		{"RS256 alg rejected", mintRaw(testSigner, b64seg(map[string]string{"alg": "RS256", "typ": "JWT"}), goodClm), ErrUnsupportedAlg},
		{"bad base64 signature", goodHdr + "." + goodClm + "." + "!!!not-b64!!!", ErrMalformed},
		{"bad base64 claims", mintRaw(testSigner, goodHdr, "%%%not-b64%%%"), ErrMalformed},
		{"claims not JSON", mintRaw(testSigner, goodHdr, base64.RawURLEncoding.EncodeToString([]byte("not-json"))), ErrMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newV(t, &stubJTI{})
			if _, _, err := v.Validate(context.Background(), tt.tok); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v; want %v", err, tt.want)
			}
		})
	}
}

func TestValidate_MissingClaimArms(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(c Claims) Claims
	}{
		{"missing exp", func(c Claims) Claims { c.Expires = 0; return c }},
		{"missing jti", func(c Claims) Claims { c.JTI = "  "; return c }},
		{"whitespace gcid", func(c Claims) Claims { c.GCID = "   "; return c }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newV(t, &stubJTI{})
			if _, _, err := v.Validate(context.Background(), mint(testSigner, tt.mutate(validClaims()))); !errors.Is(err, ErrMissingClaim) {
				t.Fatalf("err = %v; want ErrMissingClaim", err)
			}
		})
	}
}

func TestValidate_ExpiredWithinSkewStillValid(t *testing.T) {
	// exp 3s in the past is inside the 5s skew → valid (clock drift between
	// the gateway minter and this validator).
	c := validClaims()
	c.Expires = time.Now().UTC().Add(-3 * time.Second).Unix()
	v := newV(t, &stubJTI{})
	if _, _, err := v.Validate(context.Background(), mint(testSigner, c)); err != nil {
		t.Fatalf("within-skew exp must validate: %v", err)
	}
}

func TestValidate_JTICheckerError(t *testing.T) {
	checkErr := errors.New("redis down")
	v := newV(t, &stubJTI{err: checkErr})
	_, _, err := v.Validate(context.Background(), mint(testSigner, validClaims()))
	if !errors.Is(err, checkErr) {
		t.Fatalf("err = %v; want wrapped checker error", err)
	}
	if errors.Is(err, ErrReplayed) {
		t.Fatalf("checker ERROR must not classify as replay: %v", err)
	}
}

func TestValidate_TrimsIdentityOutputs(t *testing.T) {
	c := validClaims()
	c.GCID = " learner-2 "
	c.TenantID = " tenant-B "
	v := newV(t, &stubJTI{})
	gcid, tenant, err := v.Validate(context.Background(), mint(testSigner, c))
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if gcid != "learner-2" || tenant != "tenant-B" {
		t.Fatalf("got gcid=%q tenant=%q; want trimmed", gcid, tenant)
	}
}
