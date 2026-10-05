// Package events is the inbound eventbus adapter for chora-realtime's fan-in
// bridge tier (ADR-183). For each registered domain-event subject a JetStream
// durable consumer's receive loop hands messages to FaninSubscriber.HandlerFor,
// which maps the event onto a per-learner frame via the domain Registry and
// PUBLISHes it to rt:user:{recipientGCID} on the Redis backplane.
//
// Mirrors chora-notifications' fanout_subscriber: the bound consumer supplies
// the KNOWN subject (NOT the message's own subject attribute, which some
// producers omit); dedup is idempotent; publish is ack-after-publish (handler
// error → Nack → broker retry → DLQ).
package events

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"

	"github.com/apollo-chora/chora-user-event-stream/internal/domain/realtime"
	"github.com/apollo-chora/chora-user-event-stream/internal/port"
)

// FaninInboxTTL is the dedupe-key retention window — covers the broker's
// redelivery window with margin.
const FaninInboxTTL = 48 * time.Hour

// FaninSubscriber maps inbound domain events to backplane frames.
type FaninSubscriber struct {
	reg   *realtime.Registry
	bp    port.Backplane
	dedup idempotent.Store // optional; nil → publish without dedup
	ttl   time.Duration
}

// NewFaninSubscriber wires the subscriber. dedup may be nil (dev).
func NewFaninSubscriber(reg *realtime.Registry, bp port.Backplane, dedup idempotent.Store) *FaninSubscriber {
	return &FaninSubscriber{reg: reg, bp: bp, dedup: dedup, ttl: FaninInboxTTL}
}

// HandlerFor returns an eventbus.Handler bound to a KNOWN inbound subject.
func (s *FaninSubscriber) HandlerFor(subject string) eventbus.Handler {
	return func(ctx context.Context, msg eventbus.Message) error {
		ev := realtime.InboundEvent{
			Topic:      subject,
			TenantID:   msg.Envelope.TenantID,
			GCID:       msg.Envelope.GCID,
			OccurredAt: msg.Envelope.OccurredAt,
			Payload:    msg.Payload,
		}
		out, recipient, ok, err := s.reg.Map(ev)
		if !ok {
			// Bound consumers are always registered; defensive ack-drop so a
			// misrouted message does not redeliver forever.
			log.Printf("realtime fanin: unregistered subject %q on bound consumer — ack-drop", subject)
			return nil
		}
		if err != nil {
			// Malformed event (recipient/projection failed) — Nack → broker
			// retry → DLQ (D6).
			log.Printf("realtime fanin: subject=%s tenant=%s map err=%v (nack)", subject, ev.TenantID, err)
			return err
		}

		frame, err := json.Marshal(out) // backplane frame: includes tenant_id + gcid for the connection-tier drop check
		if err != nil {
			return err
		}
		channel := realtime.UserChannel(recipient)
		publish := func() error { return s.bp.Publish(ctx, channel, frame) }

		if s.dedup != nil {
			// ack-after-publish: dedup.Process runs publish under the idempotency
			// claim, so a redelivery after a successful publish is a no-op.
			return s.dedup.Process(ctx, dedupKey(msg, subject), s.ttl, publish)
		}
		return publish()
	}
}

// dedupKey namespaces the inbound event's idempotency key (fallback event_id).
func dedupKey(msg eventbus.Message, subject string) string {
	k := strings.TrimSpace(msg.Envelope.IdempotencyKey)
	if k == "" {
		k = strings.TrimSpace(msg.Envelope.EventID)
	}
	if k == "" {
		// Last-resort: a content-free key still bounds duplicates within a subject.
		k = subject
	}
	return "rt-fanin:" + k
}

// SubscriptionName derives the canonical durable-consumer name for an inbound
// subject (chora.{domain}.{aggregate}.{event}.v{N} →
// chora-realtime.{domain}-{aggregate}-{event}), matching the chora-notifications
// convention.
//
// KEPT-CONTRACT (service renamed chora-realtime → chora-user-event-stream
// 2026-06-20): the "chora-realtime." prefix maps to the 33 EXISTING durable
// consumer names on the live subjects. Do NOT rename it to the new service
// name in isolation: it would orphan all 33 live consumers and the renamed
// service would subscribe under non-existent names. A prefix rename is a
// separate infra migration (create 33 new consumers + drain/delete 33 old).
// Intentionally stable. eventbus sanitises the dotted name into a NATS-legal
// durable before the consumer is created.
func SubscriptionName(topic string) string {
	t := strings.TrimPrefix(topic, "chora.")
	if i := strings.LastIndex(t, "."); i >= 0 && strings.HasPrefix(t[i+1:], "v") {
		t = t[:i] // strip trailing .v{N}
	}
	return "chora-realtime." + strings.ReplaceAll(t, ".", "-")
}
