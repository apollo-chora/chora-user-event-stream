package events

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"

	"github.com/apollo-chora/chora-user-event-stream/internal/adapter/protofield"
	"github.com/apollo-chora/chora-user-event-stream/internal/domain/realtime"
)

// fakeBackplane records PUBLISHes (implements port.Backplane).
type fakeBackplane struct {
	mu    sync.Mutex
	chans []string
	bodys [][]byte
	err   error
}

func (f *fakeBackplane) Publish(_ context.Context, channel string, payload []byte) error {
	if f.err != nil {
		return f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.chans = append(f.chans, channel)
	f.bodys = append(f.bodys, append([]byte(nil), payload...))
	return nil
}

func (f *fakeBackplane) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.chans)
}

func wireReg(t *testing.T) *realtime.Registry {
	t.Helper()
	r, err := realtime.DefaultRegistry(protofield.New())
	if err != nil {
		t.Fatalf("DefaultRegistry: %v", err)
	}
	return r
}

func manaMsg(idemKey, eventID string) eventbus.Message {
	return eventbus.Message{
		Subject: "chora.identity.user_mana.credited.v1",
		Envelope: envelope.Envelope{
			EventID:        eventID,
			IdempotencyKey: idemKey,
			TenantID:       "tenant-A",
			GCID:           "learner-1",
			OccurredAt:     time.Unix(1700000000, 0).UTC(),
		},
	}
}

func TestSubscriptionName(t *testing.T) {
	tests := []struct{ topic, want string }{
		{"chora.identity.user_mana.credited.v1", "chora-realtime.identity-user_mana-credited"},
		{"chora.notifications.in_app.created.v2", "chora-realtime.notifications-in_app-created"},
		{"chora.payments.course_purchase.payment_captured.v1", "chora-realtime.payments-course_purchase-payment_captured"},
		// No trailing version segment — nothing stripped.
		{"chora.consumption.companion.bonded", "chora-realtime.consumption-companion-bonded"},
		// Non-chora prefix passes through (only the version strips).
		{"custom.topic.v9", "chora-realtime.custom-topic"},
	}
	for _, tt := range tests {
		if got := SubscriptionName(tt.topic); got != tt.want {
			t.Fatalf("SubscriptionName(%q) = %q, want %q", tt.topic, got, tt.want)
		}
	}
}

func TestHandlerFor_HappyPath_PublishesUserChannel(t *testing.T) {
	bp := &fakeBackplane{}
	sub := NewFaninSubscriber(wireReg(t), bp, nil)
	h := sub.HandlerFor("chora.identity.user_mana.credited.v1")

	if err := h(context.Background(), manaMsg("ik-1", "ev-1")); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if bp.count() != 1 {
		t.Fatalf("publishes = %d, want 1", bp.count())
	}
	if bp.chans[0] != realtime.UserChannel("learner-1") {
		t.Fatalf("channel = %q, want %q", bp.chans[0], realtime.UserChannel("learner-1"))
	}
	// The backplane frame is the FULL envelope (tenant_id + gcid ride for the
	// connection-tier drop check).
	var env realtime.Envelope
	if err := json.Unmarshal(bp.bodys[0], &env); err != nil {
		t.Fatalf("frame decode: %v", err)
	}
	if env.Topic != realtime.TopicManaBalanceChanged || env.TenantID != "tenant-A" || env.GCID != "learner-1" {
		t.Fatalf("frame = %+v", env)
	}
	var payload map[string]any
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	if payload["reason"] != "credit" {
		t.Fatalf("payload = %s", env.Payload)
	}
}

func TestHandlerFor_PayloadRecipient_RealWire(t *testing.T) {
	// notification created → recipient comes from wire field 3, NOT the actor.
	var wire []byte
	wire = protowire.AppendTag(wire, 2, protowire.BytesType)
	wire = protowire.AppendString(wire, "notif-1")
	wire = protowire.AppendTag(wire, 3, protowire.BytesType)
	wire = protowire.AppendString(wire, "recipient-9")

	bp := &fakeBackplane{}
	sub := NewFaninSubscriber(wireReg(t), bp, nil)
	h := sub.HandlerFor("chora.notifications.in_app.created.v1")
	msg := eventbus.Message{
		Envelope: envelope.Envelope{TenantID: "tenant-B", GCID: "actor-system", OccurredAt: time.Now().UTC()},
		Payload:  wire,
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if bp.chans[0] != realtime.UserChannel("recipient-9") {
		t.Fatalf("channel = %q, want recipient-9's channel", bp.chans[0])
	}
}

func TestHandlerFor_UnregisteredTopic_AckDrops(t *testing.T) {
	bp := &fakeBackplane{}
	sub := NewFaninSubscriber(wireReg(t), bp, nil)
	h := sub.HandlerFor("chora.creation.atom.published.v1") // not in registry
	if err := h(context.Background(), manaMsg("ik", "ev")); err != nil {
		t.Fatalf("unregistered topic must ACK (nil), got %v", err)
	}
	if bp.count() != 0 {
		t.Fatalf("publishes = %d, want 0", bp.count())
	}
}

func TestHandlerFor_MalformedEvent_Nacks(t *testing.T) {
	// in_app.created with NO payload → recipient field 3 unresolvable →
	// handler must return an error (Nack → retry → DLQ), never publish.
	bp := &fakeBackplane{}
	sub := NewFaninSubscriber(wireReg(t), bp, nil)
	h := sub.HandlerFor("chora.notifications.in_app.created.v1")
	msg := eventbus.Message{
		Envelope: envelope.Envelope{TenantID: "tenant-A", GCID: "actor", OccurredAt: time.Now().UTC()},
		Payload:  nil,
	}
	if err := h(context.Background(), msg); err == nil {
		t.Fatalf("malformed event: want error (nack)")
	}
	if bp.count() != 0 {
		t.Fatalf("publishes = %d, want 0", bp.count())
	}
}

func TestHandlerFor_PublishErrorPropagates(t *testing.T) {
	wantErr := errors.New("redis down")
	sub := NewFaninSubscriber(wireReg(t), &fakeBackplane{err: wantErr}, nil)
	h := sub.HandlerFor("chora.identity.user_mana.credited.v1")
	if err := h(context.Background(), manaMsg("ik", "ev")); !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want publish error (nack)", err)
	}
}

func TestHandlerFor_DedupSkipsRedelivery(t *testing.T) {
	bp := &fakeBackplane{}
	sub := NewFaninSubscriber(wireReg(t), bp, idempotent.NewMemoryStore())
	h := sub.HandlerFor("chora.identity.user_mana.credited.v1")

	if err := h(context.Background(), manaMsg("dup-key", "ev-1")); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if err := h(context.Background(), manaMsg("dup-key", "ev-1")); err != nil {
		t.Fatalf("redelivery must be idempotent-nil: %v", err)
	}
	if bp.count() != 1 {
		t.Fatalf("publishes = %d, want exactly 1 (dedup)", bp.count())
	}

	// A DIFFERENT key publishes again.
	if err := h(context.Background(), manaMsg("fresh-key", "ev-2")); err != nil {
		t.Fatalf("fresh key: %v", err)
	}
	if bp.count() != 2 {
		t.Fatalf("publishes = %d, want 2", bp.count())
	}
}

func TestHandlerFor_DedupDoesNotClaimOnPublishError(t *testing.T) {
	bp := &fakeBackplane{err: errors.New("transient")}
	sub := NewFaninSubscriber(wireReg(t), bp, idempotent.NewMemoryStore())
	h := sub.HandlerFor("chora.identity.user_mana.credited.v1")

	if err := h(context.Background(), manaMsg("retry-key", "ev")); err == nil {
		t.Fatalf("failed publish must error")
	}
	bp.err = nil // backplane heals → broker redelivery must now succeed
	if err := h(context.Background(), manaMsg("retry-key", "ev")); err != nil {
		t.Fatalf("retry after heal: %v", err)
	}
	if bp.count() != 1 {
		t.Fatalf("publishes = %d, want 1", bp.count())
	}
}

func TestDedupKey_Fallbacks(t *testing.T) {
	const topic = "chora.identity.user_mana.credited.v1"
	tests := []struct {
		name string
		msg  eventbus.Message
		want string
	}{
		{"idempotency key", manaMsg(" ik-7 ", "ev-7"), "rt-fanin:ik-7"},
		{"event id fallback", manaMsg("", " ev-7 "), "rt-fanin:ev-7"},
		{"topic last resort", manaMsg("", ""), "rt-fanin:" + topic},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := dedupKey(tt.msg, topic); got != tt.want {
				t.Fatalf("dedupKey = %q, want %q", got, tt.want)
			}
		})
	}
}

// Guard: every registry topic derives a non-empty, prefix-correct sub name.
func TestSubscriptionName_AllRegistryTopics(t *testing.T) {
	for _, topic := range wireReg(t).Topics() {
		name := SubscriptionName(topic)
		if !strings.HasPrefix(name, "chora-realtime.") || strings.Contains(name, ".v") {
			t.Fatalf("SubscriptionName(%q) = %q", topic, name)
		}
	}
}
