// registry_wire_test.go exercises the DefaultRegistry happy paths through the
// REAL protowire extractor (protofield.New()) over hand-encoded binary
// payloads — the exact wire shape the producer-side hand-rolled protowire
// encoders emit. External test package: protofield imports realtime, so an
// in-package test would cycle.
package realtime_test

import (
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/apollo-chora/chora-user-event-stream/internal/adapter/protofield"
	"github.com/apollo-chora/chora-user-event-stream/internal/domain/realtime"
)

func appendStr(b []byte, field int, v string) []byte {
	b = protowire.AppendTag(b, protowire.Number(field), protowire.BytesType)
	return protowire.AppendString(b, v)
}

func appendVarint(b []byte, field int, v int64) []byte {
	b = protowire.AppendTag(b, protowire.Number(field), protowire.VarintType)
	return protowire.AppendVarint(b, uint64(v))
}

func wireRegistry(t *testing.T) *realtime.Registry {
	t.Helper()
	r, err := realtime.DefaultRegistry(protofield.New())
	if err != nil {
		t.Fatalf("DefaultRegistry: %v", err)
	}
	return r
}

func decode(t *testing.T, p json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(p, &m); err != nil {
		t.Fatalf("decode payload %s: %v", p, err)
	}
	return m
}

func TestWire_NotificationCreated(t *testing.T) {
	// InAppNotificationCreated: notif_id=2, recipient_gcid=3, title=5.
	payload := appendStr(nil, 2, "notif-1")
	payload = appendStr(payload, 3, "recipient-5")
	payload = appendStr(payload, 5, "Cert earned")

	out, recipient, ok, err := wireRegistry(t).Map(realtime.InboundEvent{
		Topic:      "chora.notifications.in_app.created.v1",
		TenantID:   "tenant-W",
		GCID:       "actor-system",
		OccurredAt: time.Unix(1700000500, 0).UTC(),
		Payload:    payload,
	})
	if !ok || err != nil {
		t.Fatalf("Map ok=%v err=%v", ok, err)
	}
	if recipient != "recipient-5" || out.GCID != "recipient-5" {
		t.Fatalf("recipient = %q / %q, want recipient-5", recipient, out.GCID)
	}
	m := decode(t, out.Payload)
	if m["notification_id"] != "notif-1" || m["title"] != "Cert earned" {
		t.Fatalf("payload = %s", out.Payload)
	}
}

func TestWire_NotificationCreated_NoTitle(t *testing.T) {
	payload := appendStr(nil, 2, "notif-2")
	payload = appendStr(payload, 3, "recipient-6")

	out, _, ok, err := wireRegistry(t).Map(realtime.InboundEvent{
		Topic:   "chora.notifications.in_app.created.v1",
		Payload: payload,
	})
	if !ok || err != nil {
		t.Fatalf("Map ok=%v err=%v", ok, err)
	}
	if _, hasTitle := decode(t, out.Payload)["title"]; hasTitle {
		t.Fatalf("absent wire title must be omitted: %s", out.Payload)
	}
}

func TestWire_NotificationRead(t *testing.T) {
	payload := appendStr(nil, 2, "notif-3")
	payload = appendStr(payload, 3, "recipient-7")

	out, recipient, ok, err := wireRegistry(t).Map(realtime.InboundEvent{
		Topic:   "chora.notifications.in_app.read.v1",
		Payload: payload,
	})
	if !ok || err != nil {
		t.Fatalf("Map ok=%v err=%v", ok, err)
	}
	if recipient != "recipient-7" || out.Topic != realtime.TopicNotificationRead {
		t.Fatalf("recipient=%q topic=%q", recipient, out.Topic)
	}
	if decode(t, out.Payload)["notification_id"] != "notif-3" {
		t.Fatalf("payload = %s", out.Payload)
	}
}

func TestWire_CompanionStageUp(t *testing.T) {
	// CompanionStageUp (stage_up.proto): companion_id=2, stage_from=4, stage_to=5.
	payload := appendStr(nil, 2, "companion-9")
	payload = appendVarint(payload, 4, 3)
	payload = appendVarint(payload, 5, 4)

	out, recipient, ok, err := wireRegistry(t).Map(realtime.InboundEvent{
		Topic:   "chora.consumption.companion.stage_up.v1",
		GCID:    "owner-1",
		Payload: payload,
	})
	if !ok || err != nil {
		t.Fatalf("Map ok=%v err=%v", ok, err)
	}
	if recipient != "owner-1" {
		t.Fatalf("recipient = %q", recipient)
	}
	m := decode(t, out.Payload)
	if m["familiar_id"] != "companion-9" || m["from_stage"].(float64) != 3 || m["to_stage"].(float64) != 4 {
		t.Fatalf("payload = %s", out.Payload)
	}
}

func TestWire_CompanionBonded_RefOnly(t *testing.T) {
	payload := appendStr(nil, 2, "companion-2")
	out, _, ok, err := wireRegistry(t).Map(realtime.InboundEvent{
		Topic:   "chora.consumption.companion.bonded.v1",
		GCID:    "owner-3",
		Payload: payload,
	})
	if !ok || err != nil {
		t.Fatalf("Map ok=%v err=%v", ok, err)
	}
	if out.Topic != realtime.TopicCompanionBonded {
		t.Fatalf("topic = %q", out.Topic)
	}
	if decode(t, out.Payload)["familiar_id"] != "companion-2" {
		t.Fatalf("payload = %s", out.Payload)
	}
}

func TestWire_PaymentRefunded(t *testing.T) {
	// chora.payments.*: purchase_id=2, learner_gcid=3.
	payload := appendStr(nil, 2, "purchase-77")
	payload = appendStr(payload, 3, "buyer-4")

	out, recipient, ok, err := wireRegistry(t).Map(realtime.InboundEvent{
		Topic:   "chora.payments.companion_egg_purchase.refunded.v1",
		GCID:    "ops-actor",
		Payload: payload,
	})
	if !ok || err != nil {
		t.Fatalf("Map ok=%v err=%v", ok, err)
	}
	if recipient != "buyer-4" {
		t.Fatalf("recipient = %q, want buyer-4 (learner_gcid f3, NOT the actor)", recipient)
	}
	m := decode(t, out.Payload)
	if m["purchase_id"] != "purchase-77" || m["aggregate_type"] != "companion_egg_purchase" || m["state"] != "refunded" {
		t.Fatalf("payload = %s", out.Payload)
	}
}

func TestWire_MalformedPayload_Errors(t *testing.T) {
	// A truncated wire payload on a payload-recipient topic must surface a
	// hard error (nack → DLQ), not resolve to an empty recipient.
	_, _, ok, err := wireRegistry(t).Map(realtime.InboundEvent{
		Topic:   "chora.notifications.in_app.created.v1",
		GCID:    "actor",
		Payload: []byte{0x80}, // truncated varint tag
	})
	if !ok || err == nil {
		t.Fatalf("malformed wire: ok=%v err=%v; want true + error", ok, err)
	}
}
