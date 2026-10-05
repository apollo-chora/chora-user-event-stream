package realtime

import (
	"encoding/json"
	"testing"
	"time"
)

// fakeExtractor returns canned top-level proto field values keyed by number,
// standing in for the protowire adapter so the domain test stays pure.
type fakeExtractor struct {
	strs map[int]string
	ints map[int]int64
}

func (f *fakeExtractor) String(_ []byte, field int) (string, error) {
	if v, ok := f.strs[field]; ok {
		return v, nil
	}
	return "", ErrFieldNotFound
}

func (f *fakeExtractor) Int64(_ []byte, field int) (int64, error) {
	if v, ok := f.ints[field]; ok {
		return v, nil
	}
	return 0, ErrFieldNotFound
}

func mustRegistry(t *testing.T, ext FieldExtractor) *Registry {
	t.Helper()
	r, err := DefaultRegistry(ext)
	if err != nil {
		t.Fatalf("DefaultRegistry: %v", err)
	}
	return r
}

func decodePayload(t *testing.T, p json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(p, &m); err != nil {
		t.Fatalf("decode payload %s: %v", p, err)
	}
	return m
}

func TestMap_ManaDebit_SignalOnly(t *testing.T) {
	occ := time.Unix(1700000000, 0).UTC()
	r := mustRegistry(t, &fakeExtractor{})
	out, recipient, ok, err := r.Map(InboundEvent{
		Topic:      "chora.identity.user_mana.debited.v1",
		TenantID:   "tenant-A",
		GCID:       "learner-1",
		OccurredAt: occ,
	})
	if !ok || err != nil {
		t.Fatalf("Map ok=%v err=%v", ok, err)
	}
	if recipient != "learner-1" || out.GCID != "learner-1" {
		t.Fatalf("recipient=%q out.GCID=%q want learner-1", recipient, out.GCID)
	}
	if out.Topic != TopicManaBalanceChanged {
		t.Fatalf("out.Topic=%q", out.Topic)
	}
	if out.TenantID != "tenant-A" {
		t.Fatalf("out.TenantID=%q", out.TenantID)
	}
	m := decodePayload(t, out.Payload)
	if m["reason"] != "debit" {
		t.Fatalf("reason=%v want debit", m["reason"])
	}
	if _, hasBalance := m["balance"]; hasBalance {
		t.Fatalf("mana payload must be signal-only (no balance): %s", out.Payload)
	}
	if int64(m["ledger_seq"].(float64)) != occ.UnixMicro() {
		t.Fatalf("ledger_seq=%v want %d", m["ledger_seq"], occ.UnixMicro())
	}
}

func TestMap_NotificationCreated_PayloadRecipient(t *testing.T) {
	ext := &fakeExtractor{strs: map[int]string{2: "notif-abc", 3: "recipient-7", 5: "You earned a cert"}}
	r := mustRegistry(t, ext)
	out, recipient, ok, err := r.Map(InboundEvent{
		Topic:    "chora.notifications.in_app.created.v1",
		TenantID: "tenant-B",
		GCID:     "actor-system", // actor != recipient
		Payload:  []byte("ignored-by-fake"),
	})
	if !ok || err != nil {
		t.Fatalf("Map ok=%v err=%v", ok, err)
	}
	if recipient != "recipient-7" || out.GCID != "recipient-7" {
		t.Fatalf("recipient=%q out.GCID=%q want recipient-7", recipient, out.GCID)
	}
	if out.Topic != TopicNotificationCreated {
		t.Fatalf("out.Topic=%q", out.Topic)
	}
	m := decodePayload(t, out.Payload)
	if m["notification_id"] != "notif-abc" {
		t.Fatalf("notification_id=%v", m["notification_id"])
	}
	if m["title"] != "You earned a cert" {
		t.Fatalf("title=%v", m["title"])
	}
}

func TestMap_PaymentCaptured_TopicDerivedState(t *testing.T) {
	ext := &fakeExtractor{strs: map[int]string{2: "purchase-1", 3: "buyer-9"}}
	r := mustRegistry(t, ext)
	out, recipient, ok, err := r.Map(InboundEvent{
		Topic:    "chora.payments.course_purchase.payment_captured.v1",
		TenantID: "tenant-C",
		GCID:     "ops-actor",
		Payload:  []byte("x"),
	})
	if !ok || err != nil {
		t.Fatalf("Map ok=%v err=%v", ok, err)
	}
	if recipient != "buyer-9" {
		t.Fatalf("recipient=%q want buyer-9 (learner_gcid f3)", recipient)
	}
	if out.Topic != TopicPaymentStateChanged {
		t.Fatalf("out.Topic=%q", out.Topic)
	}
	m := decodePayload(t, out.Payload)
	if m["purchase_id"] != "purchase-1" || m["aggregate_type"] != "course_purchase" || m["state"] != "captured" {
		t.Fatalf("payment payload=%s", out.Payload)
	}
}

// TestMap_CompanionStageUp_StageInts pins the buildout live-walk fix
// (2026-07-04): the growth service publishes
// chora.consumption.companion.stage_up.v1 (stage_up.proto: companion_id=2,
// stage_from=4, stage_to=5); the registry previously bound the dead
// chora.consumption.companion.leveled_up.v1 topic (nothing publishes it) with
// field numbers 5/6 — so the stage-up ceremony push could NEVER be delivered.
// The FE frame name stays familiar.leveled_up (KEPT-CONTRACT with the shipped
// bundle's channel mapping; ADR-254 D9: topics renamed, frames unchanged).
func TestMap_CompanionStageUp_StageInts(t *testing.T) {
	ext := &fakeExtractor{
		strs: map[int]string{2: "companion-3"},
		ints: map[int]int64{4: 1, 5: 2}, // stage_from=1, stage_to=2
	}
	r := mustRegistry(t, ext)
	out, recipient, ok, err := r.Map(InboundEvent{
		Topic:      "chora.consumption.companion.stage_up.v1",
		TenantID:   "tenant-D",
		GCID:       "owner-2",
		OccurredAt: time.Now().UTC(),
		Payload:    []byte("x"),
	})
	if !ok || err != nil {
		t.Fatalf("Map ok=%v err=%v", ok, err)
	}
	if recipient != "owner-2" {
		t.Fatalf("recipient=%q want owner-2 (envelope gcid)", recipient)
	}
	if out.Topic != TopicCompanionLeveledUp {
		t.Fatalf("out topic=%q want %q (FE frame unchanged)", out.Topic, TopicCompanionLeveledUp)
	}
	m := decodePayload(t, out.Payload)
	if m["familiar_id"] != "companion-3" {
		t.Fatalf("familiar_id=%v (FE payload key kept, ADR-254 D9)", m["familiar_id"])
	}
	if int(m["from_stage"].(float64)) != 1 || int(m["to_stage"].(float64)) != 2 {
		t.Fatalf("stages from=%v to=%v want 1,2", m["from_stage"], m["to_stage"])
	}
}

// TestMap_DeadLeveledUpTopic_Unregistered proves the dead binding is gone —
// keeping it would resurrect the silent-drop path this fix closes.
func TestMap_DeadLeveledUpTopic_Unregistered(t *testing.T) {
	r := mustRegistry(t, &fakeExtractor{})
	_, _, ok, err := r.Map(InboundEvent{Topic: "chora.consumption.companion.leveled_up.v1", GCID: "g"})
	if ok || err != nil {
		t.Fatalf("dead leveled_up topic: ok=%v err=%v want false,nil", ok, err)
	}
}

// TestMap_PreRenameConsumptionTopics_Unregistered pins the ADR-254 D9 / D10
// cut: the consumer moves its subscription ids in ONE deploy window (no dual
// subscription), so the pre-rename chora.consumption.familiar.* topics must
// not be bound any more. A leftover binding would derive a subscription id
// (chora-realtime.consumption-familiar-*) that the estate no longer provisions.
func TestMap_PreRenameConsumptionTopics_Unregistered(t *testing.T) {
	r := mustRegistry(t, &fakeExtractor{})
	for _, topic := range []string{
		"chora.consumption.familiar.stage_up.v1",
		"chora.consumption.familiar.bonded.v1",
		"chora.consumption.familiar.retired.v1",
		"chora.consumption.familiar.skin_equipped.v1",
		"chora.payments.familiar_egg_purchase.refunded.v1",
	} {
		if _, ok := r.Lookup(topic); ok {
			t.Fatalf("pre-rename topic %q is still registered (ADR-254 D9 cut is one window, no dual subscription)", topic)
		}
	}
}

func TestMap_UnregisteredTopic_OkFalse(t *testing.T) {
	r := mustRegistry(t, &fakeExtractor{})
	_, _, ok, err := r.Map(InboundEvent{Topic: "chora.creation.atom.published.v1", GCID: "g"})
	if ok || err != nil {
		t.Fatalf("unregistered topic: ok=%v err=%v want false,nil", ok, err)
	}
}

func TestMap_MissingEnvelopeGCID_Errors(t *testing.T) {
	r := mustRegistry(t, &fakeExtractor{})
	_, _, ok, err := r.Map(InboundEvent{Topic: "chora.identity.user_mana.credited.v1", GCID: "  "})
	if !ok || err == nil {
		t.Fatalf("empty gcid: ok=%v err=%v want true,err", ok, err)
	}
}

func TestDefaultRegistry_RegistersExpectedTopics(t *testing.T) {
	r := mustRegistry(t, &fakeExtractor{})
	// 3 mana + 4 companion + 2 notification + (6 aggregates * 4 states) = 33.
	if got := len(r.Topics()); got != 33 {
		t.Fatalf("registered %d topics, want 33", got)
	}
	for _, topic := range []string{
		"chora.identity.user_mana.refunded.v1",
		"chora.consumption.companion.skin_equipped.v1",
		"chora.notifications.in_app.read.v1",
		"chora.payments.user_subscription.refunded.v1",
	} {
		if _, ok := r.Lookup(topic); !ok {
			t.Fatalf("expected topic %q registered", topic)
		}
	}
}
