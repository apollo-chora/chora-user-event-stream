// Package realtime is the domain core of chora-realtime (ADR-183): the
// learner-scoped multiplexed SSE channel. It is infrastructure-free
// (hexagonal — the domain at the centre): the Envelope value object + the
// inbound-topic Registry that maps each domain event onto a
// per-learner frame. Redis, the event bus, HTTP + the HMAC ticket all live in
// adapters that depend on this package, never the reverse.
//
// Two wire shapes share one Envelope:
//
//   - The BACKPLANE frame (fan-in tier → connection tier) is the full
//     struct-tag marshalling of Envelope (`json.Marshal(env)`), carrying
//     tenant_id + gcid so the connection tier can drop a cross-tenant frame
//     (defence-in-depth) before relaying.
//   - The CLIENT frame (connection tier → browser EventSource) is
//     MarshalFrame(): exactly {topic, occurred_at, payload} per the
//     RealtimeEnvelope schema in chora-contracts/openapi/realtime.yaml.
package realtime

import (
	"encoding/json"
	"time"
)

// FE-facing SSE event names (the `event:` line) — v1 topic set per ADR-183
// + the realtime.yaml RealtimeEnvelope description. These are the OUTBOUND
// topics; the inbound domain topics they map from live in the Registry.
//
// The four Companion frames keep their pre-rename `familiar.*` names: FE frame
// name kept; SPA contract (ADR-254 D9: topics renamed, frames unchanged). The
// inbound topics they map from are chora.consumption.companion.<event>.v1.
const (
	TopicManaBalanceChanged    = "mana.balance.changed"
	TopicCompanionLeveledUp    = "familiar.leveled_up"    // FE frame name kept; SPA contract (ADR-254 D9: topics renamed, frames unchanged)
	TopicCompanionBonded       = "familiar.bonded"        // FE frame name kept; SPA contract (ADR-254 D9: topics renamed, frames unchanged)
	TopicCompanionRetired      = "familiar.retired"       // FE frame name kept; SPA contract (ADR-254 D9: topics renamed, frames unchanged)
	TopicCompanionSkinEquipped = "familiar.skin_equipped" // FE frame name kept; SPA contract (ADR-254 D9: topics renamed, frames unchanged)
	TopicNotificationCreated   = "notification.created"
	TopicNotificationRead      = "notification.read"
	TopicPaymentStateChanged   = "payment.state.changed"
)

// ChannelPrefix namespaces every chora-realtime Redis key + backplane channel
// (matches the ADR-168 classroom-realtime `rt:` convention).
const ChannelPrefix = "rt:"

// UserChannel is the per-learner backplane channel. The fan-in tier
// PUBLISHes a frame here; the connection tier SUBSCRIBEs to it only for its
// locally-connected users. This naming is the cross-tier contract — both
// tiers import it so they never drift.
func UserChannel(gcid string) string { return ChannelPrefix + "user:" + gcid }

// Envelope is one per-learner realtime frame.
//
// Struct-tag marshalling is the BACKPLANE shape (includes tenant_id + gcid).
// Seq is connection-assigned (a per-connection monotonic counter set at
// SSE-send time for client-side gap DETECTION only) and never rides the
// backplane wire — hence `json:"-"`.
type Envelope struct {
	Topic      string          `json:"topic"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
	TenantID   string          `json:"tenant_id,omitempty"`
	GCID       string          `json:"gcid,omitempty"`
	Seq        int64           `json:"-"`
}

// MarshalFrame renders the CLIENT-facing SSE `data:` JSON — exactly
// {topic, occurred_at, payload} per the RealtimeEnvelope contract. tenant_id,
// gcid + seq are deliberately omitted (the client already knows its own
// identity; seq rides the SSE `id:` line, not the data body). A nil/empty
// payload is normalised to `{}` so the frame always satisfies the
// "payload is an object" contract.
func (e Envelope) MarshalFrame() ([]byte, error) {
	payload := e.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}
	return json.Marshal(struct {
		Topic      string          `json:"topic"`
		OccurredAt time.Time       `json:"occurred_at"`
		Payload    json.RawMessage `json:"payload"`
	}{
		Topic:      e.Topic,
		OccurredAt: e.OccurredAt,
		Payload:    payload,
	})
}
