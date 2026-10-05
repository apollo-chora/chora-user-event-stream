package realtime

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMarshalFrame_OnlyTopicOccurredAtPayload(t *testing.T) {
	env := Envelope{
		Topic:      TopicManaBalanceChanged,
		OccurredAt: time.Unix(1700000000, 0).UTC(),
		Payload:    json.RawMessage(`{"reason":"debit","ledger_seq":1188}`),
		TenantID:   "tenant-A",
		GCID:       "gcid-1",
		Seq:        42,
	}
	raw, err := env.MarshalFrame()
	if err != nil {
		t.Fatalf("MarshalFrame: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal frame: %v", err)
	}
	// Exactly the three contract fields — no tenant_id / gcid / seq leak.
	if len(m) != 3 {
		t.Fatalf("frame has %d keys, want 3: %s", len(m), raw)
	}
	for _, k := range []string{"topic", "occurred_at", "payload"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("frame missing %q: %s", k, raw)
		}
	}
	for _, k := range []string{"tenant_id", "gcid", "seq", "Seq"} {
		if _, ok := m[k]; ok {
			t.Fatalf("frame leaked %q: %s", k, raw)
		}
	}
	if string(m["topic"]) != `"mana.balance.changed"` {
		t.Fatalf("topic = %s", m["topic"])
	}
}

func TestMarshalFrame_EmptyPayloadDefaultsToObject(t *testing.T) {
	env := Envelope{Topic: TopicCompanionBonded, OccurredAt: time.Now().UTC()}
	raw, err := env.MarshalFrame()
	if err != nil {
		t.Fatalf("MarshalFrame: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(m["payload"]) != "{}" {
		t.Fatalf("empty payload = %s, want {}", m["payload"])
	}
}

func TestEnvelope_BackplaneRoundTrip(t *testing.T) {
	want := Envelope{
		Topic:      TopicNotificationCreated,
		OccurredAt: time.Unix(1700000123, 0).UTC(),
		Payload:    json.RawMessage(`{"notification_id":"n-1"}`),
		TenantID:   "tenant-X",
		GCID:       "recipient-9",
		Seq:        7, // must NOT survive the wire
	}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// The backplane frame carries tenant_id + gcid (for the connection-tier
	// drop check) but never seq.
	var probe map[string]json.RawMessage
	_ = json.Unmarshal(raw, &probe)
	if _, ok := probe["tenant_id"]; !ok {
		t.Fatalf("backplane frame missing tenant_id: %s", raw)
	}
	if _, ok := probe["seq"]; ok {
		t.Fatalf("backplane frame leaked seq: %s", raw)
	}

	var got Envelope
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.TenantID != want.TenantID || got.GCID != want.GCID || got.Topic != want.Topic {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, want)
	}
	if !got.OccurredAt.Equal(want.OccurredAt) {
		t.Fatalf("occurred_at mismatch: got %v want %v", got.OccurredAt, want.OccurredAt)
	}
	if got.Seq != 0 {
		t.Fatalf("seq must not survive the wire, got %d", got.Seq)
	}
}
