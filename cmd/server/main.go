// Command chora-user-event-stream-server is the CONNECTION tier of chora-realtime
// (ADR-183): it terminates the learner SSE stream, validates the one-time HMAC
// ticket, SUBSCRIBEs only its locally-connected users' backplane channels, and
// relays frames. It does ZERO event-bus work (that is the fan-in tier's job).
//
// Runs at FIXED replicas (idle SSE ≈ 0 CPU → CPU-HPA would collapse + drop
// connections). Graceful SIGTERM: flip /readyz NotReady, cancel the server base
// context to unblock held SSE handlers, drain, then close Redis.
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	httpadapter "github.com/apollo-chora/chora-user-event-stream/internal/adapter/http"
	redisadapter "github.com/apollo-chora/chora-user-event-stream/internal/adapter/redis"
	ticketadapter "github.com/apollo-chora/chora-user-event-stream/internal/adapter/ticket"
	"github.com/apollo-chora/chora-user-event-stream/internal/config"
	"github.com/apollo-chora/chora-user-event-stream/internal/observability"
	"github.com/apollo-chora/chora-user-event-stream/internal/port"
)

const serviceName = "chora-user-event-stream-server"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(ctx)
	if err != nil {
		// Fail loud: a configured secret could not be resolved after
		// retry-backoff. Exiting non-zero (never Ready) beats booting with
		// ticket validation / backplane silently disabled (CHO-2036).
		log.Fatalf("%s: config load failed (fail-loud): %v", serviceName, err)
	}

	// OTLP — fail-soft, own goroutine + deadline.
	otlp := observability.InitAsync(ctx, serviceName)
	defer func() {
		sc, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otlp.WaitContext(sc).Shutdown(sc); err != nil {
			log.Printf("%s: trace shutdown error: %v", serviceName, err)
		}
	}()

	// --- Redis backplane (required for streaming; dev-tolerant) --------------
	var store *redisadapter.Store
	var userBus *redisadapter.UserBus
	if cfg.RedisAddr != "" {
		s, err := redisadapter.NewStore(redisadapter.Config{
			Addr:      cfg.RedisAddr,
			Password:  cfg.RedisPassword,
			CACertPEM: cfg.RedisCACertPEM,
		})
		if err != nil {
			log.Printf("%s: WARNING redis client build failed (%v) — streaming DISABLED", serviceName, err)
		} else if err := s.Ping(ctx); err != nil {
			log.Printf("%s: WARNING redis ping failed (%v) — streaming DISABLED", serviceName, err)
			_ = s.Close()
		} else {
			store = s
			userBus = store.NewUserBus()
			log.Printf("%s: backplane wired to Redis %s (tls=%v)", serviceName, cfg.RedisAddr, cfg.RedisCACertPEM != "")
		}
	} else {
		log.Printf("%s: WARNING CHORA_REDIS_ADDR unset — streaming DISABLED (health only)", serviceName)
	}

	// --- Ticket validator (HS256 + single-use jti via Redis) ----------------
	var validator port.TicketValidator
	if store != nil && cfg.TicketSigner != "" {
		v, err := ticketadapter.NewHMACValidator([]byte(cfg.TicketSigner), store)
		if err != nil {
			// Set-but-invalid signer is a misconfiguration — fail loud.
			log.Fatalf("%s: ticket validator init failed (fail-loud): %v", serviceName, err)
		}
		validator = v
		log.Printf("%s: ticket validator wired (aud=%s)", serviceName, ticketadapter.Audience)
	} else {
		log.Printf("%s: WARNING ticket validation DISABLED (need Redis + CHORA_REALTIME_TICKET_SIGNER_SECRET[_ID])", serviceName)
	}

	// nil concrete → nil interface so the handler's nil-check 503s cleanly.
	var busPort port.UserBus
	if userBus != nil {
		busPort = userBus
	}

	health := httpadapter.NewHealth()
	stream := httpadapter.NewStreamHandler(validator, busPort, cfg.Keepalive())
	mux := httpadapter.NewMux(stream, health)

	// BaseContext lets shutdown cancel every in-flight SSE handler (their
	// r.Context() derives from it) so srv.Shutdown does not block on held
	// streams forever.
	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
		ReadHeaderTimeout: 10 * time.Second,
		// NO WriteTimeout/IdleTimeout — SSE connections are long-lived.
	}

	go func() {
		log.Printf("%s listening on %s (SSE /api/v1/realtime/stream, keepalive=%s)", serviceName, srv.Addr, cfg.Keepalive())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("%s: server error: %v", serviceName, err)
		}
	}()

	<-ctx.Done()
	log.Printf("%s: SIGTERM — draining", serviceName)
	health.SetReady(false) // LB stops routing new streams
	cancelBase()           // unblock held SSE handlers

	drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(drainCtx); err != nil {
		log.Printf("%s: shutdown error: %v", serviceName, err)
	}
	if userBus != nil {
		_ = userBus.Close()
	}
	if store != nil {
		_ = store.Close()
	}
}
