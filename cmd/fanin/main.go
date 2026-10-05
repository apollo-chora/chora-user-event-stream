// Command chora-user-event-stream-fanin is the BRIDGE tier of chora-realtime (ADR-183):
// it consumes the domain-event firehose on shared durable JetStream consumers,
// maps each event to a recipient gcid via the domain Registry, and PUBLISHes a
// per-learner frame to rt:user:{recipientGCID} on the Redis backplane. It holds
// NO client connections (that is the connection tier's job) and serves only
// health probes.
//
// Concentrating ingest here keeps the connection pods at O(local-connections)
// while ingest stays O(firehose / fan-in-replicas).
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"

	faninevents "github.com/apollo-chora/chora-user-event-stream/internal/adapter/events"
	httpadapter "github.com/apollo-chora/chora-user-event-stream/internal/adapter/http"
	"github.com/apollo-chora/chora-user-event-stream/internal/adapter/protofield"
	redisadapter "github.com/apollo-chora/chora-user-event-stream/internal/adapter/redis"
	"github.com/apollo-chora/chora-user-event-stream/internal/config"
	"github.com/apollo-chora/chora-user-event-stream/internal/domain/realtime"
	"github.com/apollo-chora/chora-user-event-stream/internal/observability"
)

const serviceName = "chora-user-event-stream-fanin"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(ctx)
	if err != nil {
		// Fail loud: a configured secret could not be resolved after
		// retry-backoff. Exiting non-zero (never Ready) beats booting a
		// health-only pod with fan-in silently DISABLED (CHO-2036).
		log.Fatalf("%s: config load failed (fail-loud): %v", serviceName, err)
	}

	otlp := observability.InitAsync(ctx, serviceName)
	defer func() {
		sc, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otlp.WaitContext(sc).Shutdown(sc); err != nil {
			log.Printf("%s: trace shutdown error: %v", serviceName, err)
		}
	}()

	// --- Redis backplane (publish target) -----------------------------------
	var store *redisadapter.Store
	if cfg.RedisAddr != "" {
		s, err := redisadapter.NewStore(redisadapter.Config{
			Addr:      cfg.RedisAddr,
			Password:  cfg.RedisPassword,
			CACertPEM: cfg.RedisCACertPEM,
		})
		if err != nil {
			log.Printf("%s: WARNING redis client build failed (%v) — fan-in DISABLED", serviceName, err)
		} else if err := s.Ping(ctx); err != nil {
			log.Printf("%s: WARNING redis ping failed (%v) — fan-in DISABLED", serviceName, err)
			_ = s.Close()
		} else {
			store = s
			defer func() { _ = store.Close() }()
			log.Printf("%s: backplane wired to Redis %s (tls=%v)", serviceName, cfg.RedisAddr, cfg.RedisCACertPEM != "")
		}
	} else {
		log.Printf("%s: WARNING CHORA_REDIS_ADDR unset — fan-in DISABLED", serviceName)
	}

	// --- Event bus (subscribe source) ---------------------------------------
	var bus eventbus.Bus
	if cfg.NATSURL != "" {
		b, err := eventbus.NewJetStream(eventbus.JetStreamConfig{URL: cfg.NATSURL})
		if err != nil {
			log.Printf("%s: WARNING eventbus init failed (%v) — fan-in DISABLED", serviceName, err)
		} else {
			bus = b
			defer func() { _ = bus.Close() }()
			log.Printf("%s: eventbus wired to NATS %s (project=%s)", serviceName, cfg.NATSURL, cfg.SourceProject)
		}
	} else {
		log.Printf("%s: WARNING NATS_URL unset — fan-in DISABLED", serviceName)
	}

	// --- Fan-in subscribers (one durable consumer per registered subject) ---
	if store != nil && bus != nil {
		reg, err := realtime.DefaultRegistry(protofield.New())
		if err != nil {
			log.Fatalf("%s: registry build failed (fail-loud): %v", serviceName, err)
		}
		faninSub := faninevents.NewFaninSubscriber(reg, store, idempotent.NewMemoryStore())

		for _, subject := range reg.Topics() {
			subject := subject
			consumerName := faninevents.SubscriptionName(subject)
			cfg := eventbus.ConsumerConfig{
				Name:       consumerName,
				Subject:    subject,
				MaxDeliver: 5,
				AckWait:    30 * time.Second,
				Backoff:    []time.Duration{1 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second},
				DLQSubject: eventbus.DLQSubject(subject),
			}
			go func() {
				log.Printf("%s: fan-in consumer binding %s → %s", serviceName, consumerName, subject)
				if err := bus.Subscribe(ctx, cfg, faninSub.HandlerFor(subject)); err != nil &&
					!errors.Is(err, context.Canceled) &&
					!errors.Is(err, context.DeadlineExceeded) {
					log.Printf("%s: fan-in consumer %s exited: %v", serviceName, consumerName, err)
				}
			}()
		}
		log.Printf("%s: fan-in consumers started for %d subjects", serviceName, len(reg.Topics()))
	} else {
		log.Printf("%s: fan-in consumers NOT started (store/eventbus unset)", serviceName)
	}

	// --- Health server ------------------------------------------------------
	health := httpadapter.NewHealth()
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           httpadapter.NewMux(nil, health),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("%s listening on %s (health only)", serviceName, srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("%s: health server error: %v", serviceName, err)
		}
	}()

	<-ctx.Done()
	log.Printf("%s: SIGTERM — draining", serviceName)
	health.SetReady(false)
	drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(drainCtx); err != nil {
		log.Printf("%s: shutdown error: %v", serviceName, err)
	}
}
