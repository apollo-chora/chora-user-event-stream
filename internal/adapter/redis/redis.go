// Package redis is the Redis 7.x adapter for chora-realtime's
// per-GCID backplane (ADR-183, reusing the ADR-168 instance). One Store wraps
// a *redis.Client and serves both tiers:
//
//   - fan-in tier  → Backplane.Publish (PUBLISH rt:user:{gcid} <frame>)
//   - connection tier → JTIChecker.Claim (SET NX rt:ticket:jti:{jti}) +
//     NewUserBus (dynamic SUBSCRIBE/UNSUBSCRIBE multiplexer, see userbus.go)
//
// Connection config is env/secret-sourced (feedback_no_inline_config) — never
// hard-coded. The TLS handling mirrors chora-delivery's classroom-realtime
// adapter: a TLS server endpoint presents a per-instance-CA-signed
// cert for a private IP the cert is NOT issued for, so we chain-verify against
// the instance CA ourselves and skip only the hostname check (MITM-resistant
// while tolerating the IP/CN mismatch).
package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/apollo-chora/chora-user-event-stream/internal/domain/realtime"
	"github.com/apollo-chora/chora-user-event-stream/internal/port"
)

// ticketJTIPrefix namespaces single-use ticket nonce keys.
const ticketJTIPrefix = realtime.ChannelPrefix + "ticket:jti:"

// Config is the env/secret-sourced connection config (resolved at the wiring
// layer; no inline config).
type Config struct {
	Addr      string // host:port (CHORA_REDIS_ADDR)
	Password  string // AUTH string; "" → no auth
	CACertPEM string // server-CA PEM; "" → plaintext (no TLS)
}

// Store wraps one go-redis client. The caller owns Close.
type Store struct {
	c *goredis.Client
}

// buildTLS builds the *tls.Config for a TLS server endpoint from the instance
// server-CA PEM. (nil, nil) when caPEM is empty.
func buildTLS(caPEM string) (*tls.Config, error) {
	if strings.TrimSpace(caPEM) == "" {
		return nil, nil
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(caPEM)) {
		return nil, errors.New("redis: CHORA_REDIS_CA_CERT is not a valid PEM certificate")
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		RootCAs:            pool,
		InsecureSkipVerify: true, //nolint:gosec // chain verified in VerifyPeerCertificate below
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			certs := make([]*x509.Certificate, 0, len(rawCerts))
			for _, raw := range rawCerts {
				c, err := x509.ParseCertificate(raw)
				if err != nil {
					return err
				}
				certs = append(certs, c)
			}
			if len(certs) == 0 {
				return errors.New("redis: server presented no certificate")
			}
			inter := x509.NewCertPool()
			for _, c := range certs[1:] {
				inter.AddCert(c)
			}
			_, err := certs[0].Verify(x509.VerifyOptions{Roots: pool, Intermediates: inter})
			return err
		},
	}, nil
}

// NewStore dials cfg.Addr with optional AUTH + TLS.
func NewStore(cfg Config) (*Store, error) {
	tlsCfg, err := buildTLS(cfg.CACertPEM)
	if err != nil {
		return nil, err
	}
	return &Store{c: goredis.NewClient(&goredis.Options{
		Addr:      cfg.Addr,
		Password:  cfg.Password,
		TLSConfig: tlsCfg,
	})}, nil
}

// Ping verifies connectivity (boot-time fail-loud on a bad endpoint).
func (s *Store) Ping(ctx context.Context) error { return s.c.Ping(ctx).Err() }

// Close releases the underlying client.
func (s *Store) Close() error { return s.c.Close() }

// Publish broadcasts payload on channel (Backplane port — Redis PUBLISH).
func (s *Store) Publish(ctx context.Context, channel string, payload []byte) error {
	return s.c.Publish(ctx, channel, payload).Err()
}

// Claim atomically claims a one-time ticket jti (JTIChecker port — SET NX EX).
// Returns true on the first claim (fresh), false when already claimed (replay).
func (s *Store) Claim(ctx context.Context, jti string, ttl time.Duration) (bool, error) {
	return s.c.SetNX(ctx, ticketJTIPrefix+jti, 1, ttl).Result()
}

// NewUserBus returns the pod-shared per-gcid subscription multiplexer over this
// Store's client. The connection tier creates exactly one.
func (s *Store) NewUserBus() *UserBus { return newUserBus(s.c) }

// Compile-time assertions: Store satisfies the backplane + jti ports.
var (
	_ port.Backplane  = (*Store)(nil)
	_ port.JTIChecker = (*Store)(nil)
)
