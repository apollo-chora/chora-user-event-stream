// registry.go — the inbound-topic map (ADR-183). A Registry turns a domain
// event (chora.{domain}.{aggregate}.{event}.v{N}) into a per-learner
// outbound Envelope addressed to a recipient gcid, via a Spec describing:
//   - OutTopic   — the FE-facing SSE event name (the `event:` line)
//   - Recipient  — how to resolve the recipient gcid (envelope vs payload field)
//   - Projector  — how to build the FE-facing payload JSON
//
// Mirrors the chora-notifications fan-out registry pattern (EnvelopeRecipient
// vs PayloadFieldRecipient + an injected FieldExtractor) so the domain stays
// adapter-free: the concrete protobuf-wire extractor is injected by the wiring
// layer (cmd/fanin).
package realtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrFieldNotFound is the port-level sentinel a FieldExtractor returns when a
// requested top-level proto field is absent. proto3 omits zero-value scalars
// from the wire, so projector int reads treat "not found" as 0; recipient
// string reads treat it as a fatal empty-recipient.
var ErrFieldNotFound = errors.New("realtime: proto field not found")

// InboundEvent is the minimal view of a domain event the Registry
// needs. tenant_id / gcid / occurred_at come from the event ENVELOPE (the
// chora-go-common pubsub attributes); Payload is the raw binary proto.
type InboundEvent struct {
	Topic      string
	TenantID   string
	GCID       string
	OccurredAt time.Time
	Payload    []byte
}

// FieldExtractor reads a top-level proto field from binary payload bytes by
// number, WITHOUT generated bindings. Implemented by the protofield adapter.
type FieldExtractor interface {
	// String returns a top-level length-delimited (string/bytes) field, or
	// ErrFieldNotFound when absent.
	String(payload []byte, field int) (string, error)
	// Int64 returns a top-level varint field (proto int32/int64/enum all
	// encode as varint), or ErrFieldNotFound when absent.
	Int64(payload []byte, field int) (int64, error)
}

// -----------------------------------------------------------------------------
// Recipient resolution
// -----------------------------------------------------------------------------

// RecipientResolver resolves the recipient gcid for an inbound event.
type RecipientResolver interface {
	Resolve(ev InboundEvent) (string, error)
}

// EnvelopeRecipient resolves recipient = envelope.gcid. Used when the event
// actor IS the recipient (mana wallet movements + Companion growth — the
// learner who acted is the learner who sees the frame).
type EnvelopeRecipient struct{}

// Resolve returns ev.GCID, erroring if empty.
func (EnvelopeRecipient) Resolve(ev InboundEvent) (string, error) {
	g := strings.TrimSpace(ev.GCID)
	if g == "" {
		return "", errors.New("realtime: envelope gcid is empty")
	}
	return g, nil
}

// PayloadFieldRecipient resolves recipient from a top-level proto string field
// in the binary payload. Used when the recipient is NOT the envelope actor —
// notifications (recipient_gcid, field 3) + payments (learner_gcid, field 3).
type PayloadFieldRecipient struct {
	Extractor FieldExtractor
	Field     int
}

// Resolve extracts the configured field as the recipient gcid.
func (r PayloadFieldRecipient) Resolve(ev InboundEvent) (string, error) {
	if r.Extractor == nil {
		return "", errors.New("realtime: PayloadFieldRecipient has nil extractor")
	}
	g, err := r.Extractor.String(ev.Payload, r.Field)
	if err != nil {
		return "", fmt.Errorf("realtime: extract recipient field %d: %w", r.Field, err)
	}
	if strings.TrimSpace(g) == "" {
		return "", fmt.Errorf("realtime: recipient field %d is empty", r.Field)
	}
	return strings.TrimSpace(g), nil
}

// -----------------------------------------------------------------------------
// Payload projection
// -----------------------------------------------------------------------------

// PayloadProjector builds the FE-facing payload JSON for an inbound event.
type PayloadProjector interface {
	Project(ev InboundEvent) (json.RawMessage, error)
}

// ManaProjector renders the SIGNAL-ONLY mana payload {reason, ledger_seq}
// (NEVER the balance — ADR-183). reason is fixed per inbound topic
// (credited/debited/refunded). ledger_seq is the event's occurred_at as
// epoch-microseconds: a monotonic-per-producer ordering token the FE uses
// ONLY to discard a stale signal that lands after a newer authoritative
// re-fetch of /api/v1/me/mana.
//
// NOTE: the upstream chora.identity.user_mana.* proto exposes entry_id
// (UUIDv7) + balance_after_units, neither a clean int "ledger sequence";
// occurred_at-micros is the chosen token. See the deviations note in the
// service README/handoff.
type ManaProjector struct{ Reason string }

// Project emits {reason, ledger_seq}.
func (p ManaProjector) Project(ev InboundEvent) (json.RawMessage, error) {
	return json.Marshal(map[string]any{
		"reason":     p.Reason,
		"ledger_seq": ev.OccurredAt.UnixMicro(),
	})
}

// CompanionStageUpProjector renders {familiar_id, from_stage, to_stage} from
// CompanionStageUp (stage_up.proto: companion_id=2, stage_from=4, stage_to=5).
// The payload key stays `familiar_id`: FE frame name kept; SPA contract
// (ADR-254 D9: topics renamed, frames unchanged).
//
// Buildout live-walk fix (2026-07-04): this spec previously bound the dead
// leveled_up.v1 topic (no publisher) with the CompanionLeveledUp field layout
// (5/6), so the stage-up ceremony push was undeliverable end-to-end. The growth
// service's real event is stage_up.v1; the FE frame name (familiar.leveled_up)
// is unchanged.
type CompanionStageUpProjector struct{ Ext FieldExtractor }

// Project emits the stage-up payload.
func (p CompanionStageUpProjector) Project(ev InboundEvent) (json.RawMessage, error) {
	if p.Ext == nil {
		return nil, errors.New("realtime: CompanionStageUpProjector has nil extractor")
	}
	fid, err := p.Ext.String(ev.Payload, 2)
	if err != nil {
		return nil, fmt.Errorf("realtime: stage_up companion_id: %w", err)
	}
	from, err := optInt(p.Ext, ev.Payload, 4)
	if err != nil {
		return nil, fmt.Errorf("realtime: stage_up stage_from: %w", err)
	}
	to, err := optInt(p.Ext, ev.Payload, 5)
	if err != nil {
		return nil, fmt.Errorf("realtime: stage_up stage_to: %w", err)
	}
	return json.Marshal(map[string]any{
		// FE frame name kept; SPA contract (ADR-254 D9: topics renamed, frames unchanged).
		"familiar_id": strings.TrimSpace(fid),
		"from_stage":  from,
		"to_stage":    to,
	})
}

// CompanionRefProjector renders {familiar_id} (proto field 2, companion_id)
// for the bonded / retired / skin_equipped events whose FE frame only needs
// the instance id (the FE re-fetches the roster on signal). The payload key
// stays `familiar_id`: FE frame name kept; SPA contract (ADR-254 D9: topics
// renamed, frames unchanged).
type CompanionRefProjector struct{ Ext FieldExtractor }

// Project emits {familiar_id}.
func (p CompanionRefProjector) Project(ev InboundEvent) (json.RawMessage, error) {
	if p.Ext == nil {
		return nil, errors.New("realtime: CompanionRefProjector has nil extractor")
	}
	fid, err := p.Ext.String(ev.Payload, 2)
	if err != nil {
		return nil, fmt.Errorf("realtime: companion_id: %w", err)
	}
	// FE frame name kept; SPA contract (ADR-254 D9: topics renamed, frames unchanged).
	return json.Marshal(map[string]any{"familiar_id": strings.TrimSpace(fid)})
}

// NotificationCreatedProjector renders {notification_id, title?} from
// InAppNotificationCreated (notif_id=2, title=5). title is optional (proto3
// omits empty); only notification_id is required by the contract.
type NotificationCreatedProjector struct{ Ext FieldExtractor }

// Project emits the notification-created payload.
func (p NotificationCreatedProjector) Project(ev InboundEvent) (json.RawMessage, error) {
	if p.Ext == nil {
		return nil, errors.New("realtime: NotificationCreatedProjector has nil extractor")
	}
	nid, err := p.Ext.String(ev.Payload, 2)
	if err != nil {
		return nil, fmt.Errorf("realtime: notif_id: %w", err)
	}
	out := map[string]any{"notification_id": strings.TrimSpace(nid)}
	if title, err := optStr(p.Ext, ev.Payload, 5); err != nil {
		return nil, fmt.Errorf("realtime: notif title: %w", err)
	} else if title != "" {
		out["title"] = title
	}
	return json.Marshal(out)
}

// NotificationRefProjector renders {notification_id} (field 2) for the read
// event.
type NotificationRefProjector struct{ Ext FieldExtractor }

// Project emits {notification_id}.
func (p NotificationRefProjector) Project(ev InboundEvent) (json.RawMessage, error) {
	if p.Ext == nil {
		return nil, errors.New("realtime: NotificationRefProjector has nil extractor")
	}
	nid, err := p.Ext.String(ev.Payload, 2)
	if err != nil {
		return nil, fmt.Errorf("realtime: notif_id: %w", err)
	}
	return json.Marshal(map[string]any{"notification_id": strings.TrimSpace(nid)})
}

// PaymentStateProjector renders {purchase_id, aggregate_type, state}.
// purchase_id is field 2 of every chora.payments.* event; aggregate_type +
// state are fixed per inbound topic (encoded in the topic name, NOT the
// payload).
type PaymentStateProjector struct {
	AggregateType string
	State         string
	Ext           FieldExtractor
}

// Project emits the payment-state payload.
func (p PaymentStateProjector) Project(ev InboundEvent) (json.RawMessage, error) {
	if p.Ext == nil {
		return nil, errors.New("realtime: PaymentStateProjector has nil extractor")
	}
	pid, err := p.Ext.String(ev.Payload, 2)
	if err != nil {
		return nil, fmt.Errorf("realtime: purchase_id: %w", err)
	}
	return json.Marshal(map[string]any{
		"purchase_id":    strings.TrimSpace(pid),
		"aggregate_type": p.AggregateType,
		"state":          p.State,
	})
}

// optInt reads a varint field tolerating absence (proto3 zero-value omission)
// as 0; any other extractor error propagates.
func optInt(ext FieldExtractor, payload []byte, field int) (int64, error) {
	v, err := ext.Int64(payload, field)
	if errors.Is(err, ErrFieldNotFound) {
		return 0, nil
	}
	return v, err
}

// optStr reads a string field tolerating absence as "".
func optStr(ext FieldExtractor, payload []byte, field int) (string, error) {
	v, err := ext.String(payload, field)
	if errors.Is(err, ErrFieldNotFound) {
		return "", nil
	}
	return strings.TrimSpace(v), err
}

// -----------------------------------------------------------------------------
// Spec + Registry
// -----------------------------------------------------------------------------

// Spec is one inbound-topic → outbound-frame mapping.
type Spec struct {
	// InTopic is the canonical inbound domain-event topic.
	InTopic string
	// OutTopic is the FE-facing SSE event name (the `event:` line).
	OutTopic string
	// Recipient resolves the recipient gcid.
	Recipient RecipientResolver
	// Projector builds the FE-facing payload JSON.
	Projector PayloadProjector
}

func (s Spec) validate() error {
	if strings.TrimSpace(s.InTopic) == "" {
		return errors.New("realtime: spec in-topic is required")
	}
	if strings.TrimSpace(s.OutTopic) == "" {
		return fmt.Errorf("realtime: spec %q has empty out-topic", s.InTopic)
	}
	if s.Recipient == nil {
		return fmt.Errorf("realtime: spec %q has nil recipient resolver", s.InTopic)
	}
	if s.Projector == nil {
		return fmt.Errorf("realtime: spec %q has nil projector", s.InTopic)
	}
	return nil
}

// Registry maps inbound topic → Spec.
type Registry struct {
	specs map[string]Spec
}

// NewRegistry validates + indexes specs by inbound topic. Fail-loud at
// construction (a bad spec / duplicate crashes boot, never a live event).
func NewRegistry(specs ...Spec) (*Registry, error) {
	m := make(map[string]Spec, len(specs))
	for _, s := range specs {
		if err := s.validate(); err != nil {
			return nil, err
		}
		if _, dup := m[s.InTopic]; dup {
			return nil, fmt.Errorf("realtime: duplicate inbound topic %q", s.InTopic)
		}
		m[s.InTopic] = s
	}
	return &Registry{specs: m}, nil
}

// Lookup returns the Spec for an inbound topic; ok=false if unregistered.
func (r *Registry) Lookup(topic string) (Spec, bool) {
	s, ok := r.specs[topic]
	return s, ok
}

// Topics returns the registered inbound topics (subscription wiring iterates
// these). Order is unspecified.
func (r *Registry) Topics() []string {
	out := make([]string, 0, len(r.specs))
	for t := range r.specs {
		out = append(out, t)
	}
	return out
}

// Map resolves an inbound event to an outbound Envelope addressed to a
// recipient gcid.
//
//   - ok=false (err=nil)  → the topic is unregistered; the caller ack-drops.
//   - ok=true,  err!=nil  → recipient/projection failed (malformed event);
//     the caller nacks → broker retry → DLQ.
//   - ok=true,  err=nil   → out is ready to PUBLISH to UserChannel(recipient).
//
// out.GCID is set to the RECIPIENT (which for notifications/payments may
// differ from ev.GCID, the actor). out.Seq is left 0 — the connection tier
// assigns the per-connection sequence at SSE-send time.
func (r *Registry) Map(ev InboundEvent) (out Envelope, recipient string, ok bool, err error) {
	s, found := r.specs[ev.Topic]
	if !found {
		return Envelope{}, "", false, nil
	}
	rcpt, err := s.Recipient.Resolve(ev)
	if err != nil {
		return Envelope{}, "", true, err
	}
	payload, err := s.Projector.Project(ev)
	if err != nil {
		return Envelope{}, "", true, err
	}
	occ := ev.OccurredAt
	if occ.IsZero() {
		occ = time.Now().UTC()
	}
	return Envelope{
		Topic:      s.OutTopic,
		OccurredAt: occ,
		Payload:    payload,
		TenantID:   ev.TenantID,
		GCID:       rcpt,
	}, rcpt, true, nil
}

// -----------------------------------------------------------------------------
// Default v1 registry
// -----------------------------------------------------------------------------

// paymentAggregates are the learner-facing Payments aggregates whose
// state-change events carry learner_gcid at field 3 (verified against
// chora-contracts/proto/events/payments/*). tenant_mana_topup (tenant-scoped)
// + dispute (admin) are intentionally excluded from the learner channel.
var paymentAggregates = []string{
	"course_purchase",
	"companion_egg_purchase",
	"user_mana_topup",
	"user_subscription",
	"application_payment",
	"identity_kyc_fee",
}

// paymentStateSuffixes maps the inbound topic event-suffix → the FE `state`
// enum value (captured/refunded/failed/expired per RealtimePaymentStateChanged).
var paymentStateSuffixes = map[string]string{
	"payment_captured": "captured",
	"payment_failed":   "failed",
	"refunded":         "refunded",
	"expired":          "expired",
}

// DefaultRegistry builds the v1 inbound-topic map. The FieldExtractor (proto
// wire reader) is injected so the domain stays adapter-free.
func DefaultRegistry(ext FieldExtractor) (*Registry, error) {
	specs := []Spec{
		// --- mana wallet (signal-only; recipient = envelope.gcid) -----------
		{InTopic: "chora.identity.user_mana.credited.v1", OutTopic: TopicManaBalanceChanged, Recipient: EnvelopeRecipient{}, Projector: ManaProjector{Reason: "credit"}},
		{InTopic: "chora.identity.user_mana.debited.v1", OutTopic: TopicManaBalanceChanged, Recipient: EnvelopeRecipient{}, Projector: ManaProjector{Reason: "debit"}},
		{InTopic: "chora.identity.user_mana.refunded.v1", OutTopic: TopicManaBalanceChanged, Recipient: EnvelopeRecipient{}, Projector: ManaProjector{Reason: "refund"}},

		// --- Companion growth (recipient = envelope.gcid) --------------------
		// stage_up.v1 is the growth service's real stage event; the OutTopic
		// keeps the shipped FE frame name (see CompanionStageUpProjector doc).
		{InTopic: "chora.consumption.companion.stage_up.v1", OutTopic: TopicCompanionLeveledUp, Recipient: EnvelopeRecipient{}, Projector: CompanionStageUpProjector{Ext: ext}},
		{InTopic: "chora.consumption.companion.bonded.v1", OutTopic: TopicCompanionBonded, Recipient: EnvelopeRecipient{}, Projector: CompanionRefProjector{Ext: ext}},
		{InTopic: "chora.consumption.companion.retired.v1", OutTopic: TopicCompanionRetired, Recipient: EnvelopeRecipient{}, Projector: CompanionRefProjector{Ext: ext}},
		{InTopic: "chora.consumption.companion.skin_equipped.v1", OutTopic: TopicCompanionSkinEquipped, Recipient: EnvelopeRecipient{}, Projector: CompanionRefProjector{Ext: ext}},

		// --- in-app notifications (recipient = payload recipient_gcid, f3) --
		{InTopic: "chora.notifications.in_app.created.v1", OutTopic: TopicNotificationCreated, Recipient: PayloadFieldRecipient{Extractor: ext, Field: 3}, Projector: NotificationCreatedProjector{Ext: ext}},
		{InTopic: "chora.notifications.in_app.read.v1", OutTopic: TopicNotificationRead, Recipient: PayloadFieldRecipient{Extractor: ext, Field: 3}, Projector: NotificationRefProjector{Ext: ext}},
	}
	// --- learner payment-state changes (recipient = payload learner_gcid, f3)
	for _, agg := range paymentAggregates {
		for suffix, state := range paymentStateSuffixes {
			specs = append(specs, Spec{
				InTopic:   "chora.payments." + agg + "." + suffix + ".v1",
				OutTopic:  TopicPaymentStateChanged,
				Recipient: PayloadFieldRecipient{Extractor: ext, Field: 3},
				Projector: PaymentStateProjector{AggregateType: agg, State: state, Ext: ext},
			})
		}
	}
	return NewRegistry(specs...)
}
