// redis_test.go covers the dependency-free surface of the adapter: the
// TLS chain-verification (pure crypto/x509), Store construction,
// and the fail-loud error arms against a guaranteed-dead loopback endpoint.
// Happy-path PUBLISH / SET NX / pubsub fan-out need a live Redis and are
// covered by the service's integration battery, not unit tests (miniredis was
// deliberately NOT added — it would churn the workspace go.work.sum).
package redis

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

// deadAddr is a loopback endpoint nothing listens on — dials fail fast.
func deadAddr(t *testing.T) string {
	t.Helper()
	// Reserve a port then close the listener so the port is provably dead.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  string
}

func newTestCA(t *testing.T, cn string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	return &testCA{
		cert: cert,
		key:  key,
		pem:  string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
	}
}

// issue signs a child certificate (leaf or intermediate) off parent.
func issue(t *testing.T, parent *testCA, cn string, isCA bool) (*x509.Certificate, *testCA) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  isCA,
		BasicConstraintsValid: true,
	}
	if isCA {
		tmpl.KeyUsage = x509.KeyUsageCertSign
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent.cert, &key.PublicKey, parent.key)
	if err != nil {
		t.Fatalf("issue %s: %v", cn, err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse %s: %v", cn, err)
	}
	return cert, &testCA{cert: cert, key: key}
}

func TestBuildTLS_EmptyPEMDisablesTLS(t *testing.T) {
	cfg, err := buildTLS("   ")
	if err != nil || cfg != nil {
		t.Fatalf("buildTLS(empty) = %v, %v; want nil, nil (plaintext)", cfg, err)
	}
}

func TestBuildTLS_InvalidPEM(t *testing.T) {
	if _, err := buildTLS("not-a-pem"); err == nil {
		t.Fatalf("buildTLS(garbage): want error")
	}
}

func TestBuildTLS_VerifyPeerCertificate(t *testing.T) {
	ca := newTestCA(t, "chora-redis-instance-ca")
	cfg, err := buildTLS(ca.pem)
	if err != nil {
		t.Fatalf("buildTLS: %v", err)
	}
	if cfg.MinVersion != 0x0303 { // tls.VersionTLS12
		t.Fatalf("MinVersion = %x, want TLS1.2", cfg.MinVersion)
	}
	if cfg.VerifyPeerCertificate == nil {
		t.Fatalf("VerifyPeerCertificate must be set (hostname-skip mitigation)")
	}

	leaf, _ := issue(t, ca, "10.0.0.5", false)

	t.Run("ca-signed leaf verifies", func(t *testing.T) {
		if err := cfg.VerifyPeerCertificate([][]byte{leaf.Raw}, nil); err != nil {
			t.Fatalf("verify: %v", err)
		}
	})

	t.Run("chain through intermediate verifies", func(t *testing.T) {
		_, interCA := issue(t, ca, "chora-redis-intermediate", true)
		viaInter, _ := issue(t, interCA, "10.0.0.6", false)
		if err := cfg.VerifyPeerCertificate([][]byte{viaInter.Raw, interCA.cert.Raw}, nil); err != nil {
			t.Fatalf("verify with intermediate: %v", err)
		}
	})

	t.Run("untrusted issuer rejected", func(t *testing.T) {
		other := newTestCA(t, "evil-ca")
		evilLeaf, _ := issue(t, other, "10.0.0.5", false)
		if err := cfg.VerifyPeerCertificate([][]byte{evilLeaf.Raw}, nil); err == nil {
			t.Fatalf("untrusted chain must fail verification")
		}
	})

	t.Run("no certificate rejected", func(t *testing.T) {
		if err := cfg.VerifyPeerCertificate(nil, nil); err == nil {
			t.Fatalf("empty chain must fail")
		}
	})

	t.Run("unparseable certificate rejected", func(t *testing.T) {
		if err := cfg.VerifyPeerCertificate([][]byte{[]byte("junk")}, nil); err == nil {
			t.Fatalf("garbage DER must fail")
		}
	})
}

func TestNewStore_BadCAPEM(t *testing.T) {
	if _, err := NewStore(Config{Addr: "127.0.0.1:1", CACertPEM: "junk"}); err == nil {
		t.Fatalf("NewStore with bad CA PEM: want error")
	}
}

func TestStore_FailLoudOnDeadEndpoint(t *testing.T) {
	addr := deadAddr(t)
	s, err := NewStore(Config{Addr: addr, Password: "pw"})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Ping(ctx); err == nil {
		t.Fatalf("Ping(dead endpoint): want error")
	}
	if err := s.Publish(ctx, "rt:user:g", []byte("x")); err == nil {
		t.Fatalf("Publish(dead endpoint): want error")
	}
	if _, err := s.Claim(ctx, "jti-1", time.Minute); err == nil {
		t.Fatalf("Claim(dead endpoint): want error")
	}
	if !strings.Contains(ticketJTIPrefix, "rt:") {
		t.Fatalf("jti keys must live under the rt: namespace, got %q", ticketJTIPrefix)
	}
}

func TestUserBus_SubscribeErrorOnDeadEndpoint(t *testing.T) {
	s, err := NewStore(Config{Addr: deadAddr(t)})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer func() { _ = s.Close() }()

	bus := s.NewUserBus()
	defer func() { _ = bus.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := bus.Subscribe(ctx, "gcid-1"); err == nil {
		t.Fatalf("Subscribe(dead endpoint): want error (fail-loud, no half-registered sink)")
	}
}
