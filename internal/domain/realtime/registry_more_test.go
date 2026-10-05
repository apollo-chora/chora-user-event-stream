package realtime

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// errExtractor injects per-field values OR errors, standing in for a wire
// extractor that fails mid-payload (malformed wire → must propagate, never
// be swallowed as "absent").
type errExtractor struct {
	strs    map[int]string
	strErrs map[int]error
	ints    map[int]int64
	intErrs map[int]error
}

func (f *errExtractor) String(_ []byte, field int) (string, error) {
	if err, ok := f.strErrs[field]; ok {
		return "", err
	}
	if v, ok := f.strs[field]; ok {
		return v, nil
	}
	return "", ErrFieldNotFound
}

func (f *errExtractor) Int64(_ []byte, field int) (int64, error) {
	if err, ok := f.intErrs[field]; ok {
		return 0, err
	}
	if v, ok := f.ints[field]; ok {
		return v, nil
	}
	return 0, ErrFieldNotFound
}

var errWire = errors.New("boom: malformed wire")

func TestUserChannel(t *testing.T) {
	if got := UserChannel("gcid-9"); got != "rt:user:gcid-9" {
		t.Fatalf("UserChannel = %q, want rt:user:gcid-9", got)
	}
	if !strings.HasPrefix(UserChannel("x"), ChannelPrefix) {
		t.Fatalf("UserChannel must be namespaced under %q", ChannelPrefix)
	}
}

func TestEnvelopeRecipient_Trims(t *testing.T) {
	g, err := EnvelopeRecipient{}.Resolve(InboundEvent{GCID: "  learner-1  "})
	if err != nil || g != "learner-1" {
		t.Fatalf("Resolve = %q, %v; want learner-1, nil", g, err)
	}
}

func TestPayloadFieldRecipient_ErrorArms(t *testing.T) {
	tests := []struct {
		name string
		r    PayloadFieldRecipient
	}{
		{"nil extractor", PayloadFieldRecipient{Extractor: nil, Field: 3}},
		{"extractor error", PayloadFieldRecipient{Extractor: &errExtractor{strErrs: map[int]error{3: errWire}}, Field: 3}},
		{"absent field", PayloadFieldRecipient{Extractor: &errExtractor{}, Field: 3}},
		{"whitespace-only field", PayloadFieldRecipient{Extractor: &errExtractor{strs: map[int]string{3: "   "}}, Field: 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := tt.r.Resolve(InboundEvent{GCID: "actor"}); err == nil {
				t.Fatalf("Resolve: want error")
			}
		})
	}
}

func TestPayloadFieldRecipient_TrimsHappy(t *testing.T) {
	r := PayloadFieldRecipient{Extractor: &errExtractor{strs: map[int]string{3: " buyer-1 "}}, Field: 3}
	g, err := r.Resolve(InboundEvent{})
	if err != nil || g != "buyer-1" {
		t.Fatalf("Resolve = %q, %v; want buyer-1, nil", g, err)
	}
}

func TestProjectors_NilExtractorArms(t *testing.T) {
	ev := InboundEvent{Payload: []byte("x")}
	tests := []struct {
		name string
		p    PayloadProjector
	}{
		{"stage_up", CompanionStageUpProjector{}},
		{"companion_ref", CompanionRefProjector{}},
		{"notification_created", NotificationCreatedProjector{}},
		{"notification_ref", NotificationRefProjector{}},
		{"payment_state", PaymentStateProjector{AggregateType: "course_purchase", State: "captured"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := tt.p.Project(ev); err == nil {
				t.Fatalf("Project with nil extractor: want error")
			}
		})
	}
}

func TestProjectors_ExtractorErrorArms(t *testing.T) {
	ev := InboundEvent{Payload: []byte("x")}
	tests := []struct {
		name string
		p    PayloadProjector
	}{
		// Required string field errors.
		{"stage_up companion_id", CompanionStageUpProjector{Ext: &errExtractor{strErrs: map[int]error{2: errWire}}}},
		{"companion_ref companion_id", CompanionRefProjector{Ext: &errExtractor{strErrs: map[int]error{2: errWire}}}},
		{"notification_created notif_id", NotificationCreatedProjector{Ext: &errExtractor{strErrs: map[int]error{2: errWire}}}},
		{"notification_ref notif_id", NotificationRefProjector{Ext: &errExtractor{strErrs: map[int]error{2: errWire}}}},
		{"payment_state purchase_id", PaymentStateProjector{AggregateType: "a", State: "s", Ext: &errExtractor{strErrs: map[int]error{2: errWire}}}},
		// Optional fields: a NON-NotFound extractor error must propagate.
		{"stage_up stage_from", CompanionStageUpProjector{Ext: &errExtractor{strs: map[int]string{2: "f-1"}, intErrs: map[int]error{4: errWire}}}},
		{"stage_up stage_to", CompanionStageUpProjector{Ext: &errExtractor{strs: map[int]string{2: "f-1"}, ints: map[int]int64{4: 1}, intErrs: map[int]error{5: errWire}}}},
		{"notification_created title", NotificationCreatedProjector{Ext: &errExtractor{strs: map[int]string{2: "n-1"}, strErrs: map[int]error{5: errWire}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := tt.p.Project(ev); !errors.Is(err, errWire) {
				t.Fatalf("Project err = %v; want wrapped errWire", err)
			}
		})
	}
}

func TestCompanionStageUp_AbsentStagesDefaultZero(t *testing.T) {
	// proto3 omits zero-value varints: stage_from=0 simply isn't on the
	// wire — optInt must tolerate ErrFieldNotFound as 0.
	p := CompanionStageUpProjector{Ext: &errExtractor{strs: map[int]string{2: " f-7 "}}}
	raw, err := p.Project(InboundEvent{Payload: []byte("x")})
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m["familiar_id"] != "f-7" {
		t.Fatalf("familiar_id = %v (want trimmed f-7)", m["familiar_id"])
	}
	if m["from_stage"].(float64) != 0 || m["to_stage"].(float64) != 0 {
		t.Fatalf("absent stages must default 0: %s", raw)
	}
}

func TestNotificationCreated_AbsentTitleOmitted(t *testing.T) {
	p := NotificationCreatedProjector{Ext: &errExtractor{strs: map[int]string{2: "n-9"}}}
	raw, err := p.Project(InboundEvent{Payload: []byte("x")})
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := m["title"]; ok {
		t.Fatalf("absent title must be omitted: %s", raw)
	}
	if m["notification_id"] != "n-9" {
		t.Fatalf("notification_id = %v", m["notification_id"])
	}
}

func TestNotificationRef_Happy(t *testing.T) {
	p := NotificationRefProjector{Ext: &errExtractor{strs: map[int]string{2: " n-2 "}}}
	raw, err := p.Project(InboundEvent{Payload: []byte("x")})
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if string(raw) != `{"notification_id":"n-2"}` {
		t.Fatalf("payload = %s", raw)
	}
}

func TestCompanionRef_Happy(t *testing.T) {
	p := CompanionRefProjector{Ext: &errExtractor{strs: map[int]string{2: "f-3"}}}
	raw, err := p.Project(InboundEvent{Payload: []byte("x")})
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if string(raw) != `{"familiar_id":"f-3"}` {
		t.Fatalf("payload = %s", raw)
	}
}

func TestOptIntOptStr(t *testing.T) {
	// optInt: absent → 0, nil err; value passes through; error propagates.
	if v, err := optInt(&errExtractor{}, nil, 1); v != 0 || err != nil {
		t.Fatalf("optInt absent = %d, %v; want 0, nil", v, err)
	}
	if v, err := optInt(&errExtractor{ints: map[int]int64{1: 6}}, nil, 1); v != 6 || err != nil {
		t.Fatalf("optInt present = %d, %v; want 6, nil", v, err)
	}
	if _, err := optInt(&errExtractor{intErrs: map[int]error{1: errWire}}, nil, 1); !errors.Is(err, errWire) {
		t.Fatalf("optInt error = %v; want errWire", err)
	}
	// optStr: absent → "", nil err; value trimmed; error propagates.
	if v, err := optStr(&errExtractor{}, nil, 1); v != "" || err != nil {
		t.Fatalf("optStr absent = %q, %v; want empty, nil", v, err)
	}
	if v, err := optStr(&errExtractor{strs: map[int]string{1: " t "}}, nil, 1); v != "t" || err != nil {
		t.Fatalf("optStr present = %q, %v; want t, nil", v, err)
	}
	if _, err := optStr(&errExtractor{strErrs: map[int]error{1: errWire}}, nil, 1); !errors.Is(err, errWire) {
		t.Fatalf("optStr error = %v; want errWire", err)
	}
}

func TestSpecValidate_NewRegistryArms(t *testing.T) {
	valid := Spec{
		InTopic:   "chora.identity.user_mana.credited.v1",
		OutTopic:  TopicManaBalanceChanged,
		Recipient: EnvelopeRecipient{},
		Projector: ManaProjector{Reason: "credit"},
	}
	tests := []struct {
		name   string
		mutate func(s Spec) Spec
		want   string
	}{
		{"empty in-topic", func(s Spec) Spec { s.InTopic = "  "; return s }, "in-topic is required"},
		{"empty out-topic", func(s Spec) Spec { s.OutTopic = ""; return s }, "empty out-topic"},
		{"nil recipient", func(s Spec) Spec { s.Recipient = nil; return s }, "nil recipient resolver"},
		{"nil projector", func(s Spec) Spec { s.Projector = nil; return s }, "nil projector"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRegistry(tt.mutate(valid))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("NewRegistry err = %v; want contains %q", err, tt.want)
			}
		})
	}

	t.Run("duplicate in-topic", func(t *testing.T) {
		if _, err := NewRegistry(valid, valid); err == nil || !strings.Contains(err.Error(), "duplicate inbound topic") {
			t.Fatalf("NewRegistry err = %v; want duplicate inbound topic", err)
		}
	})

	t.Run("valid", func(t *testing.T) {
		r, err := NewRegistry(valid)
		if err != nil {
			t.Fatalf("NewRegistry: %v", err)
		}
		if _, ok := r.Lookup(valid.InTopic); !ok {
			t.Fatalf("Lookup(%q) = false", valid.InTopic)
		}
		if _, ok := r.Lookup("chora.unknown.x.y.v1"); ok {
			t.Fatalf("Lookup(unknown) = true")
		}
	})
}

func TestMap_ProjectorError_NacksNotDrops(t *testing.T) {
	// A registered topic whose PROJECTION fails must return ok=true + err
	// (caller nacks → DLQ), never ok=false (silent ack-drop).
	r := mustRegistry(t, &errExtractor{strs: map[int]string{3: "rcpt-1"}, strErrs: map[int]error{2: errWire}})
	_, _, ok, err := r.Map(InboundEvent{
		Topic:   "chora.notifications.in_app.created.v1",
		GCID:    "actor",
		Payload: []byte("x"),
	})
	if !ok || !errors.Is(err, errWire) {
		t.Fatalf("Map ok=%v err=%v; want true, errWire", ok, err)
	}
}

func TestMap_ZeroOccurredAt_DefaultsNow(t *testing.T) {
	before := time.Now().UTC().Add(-time.Second)
	r := mustRegistry(t, &errExtractor{})
	out, _, ok, err := r.Map(InboundEvent{Topic: "chora.identity.user_mana.credited.v1", GCID: "g-1"})
	if !ok || err != nil {
		t.Fatalf("Map ok=%v err=%v", ok, err)
	}
	if out.OccurredAt.IsZero() || out.OccurredAt.Before(before) {
		t.Fatalf("zero OccurredAt must default to now, got %v", out.OccurredAt)
	}
}

func TestDefaultRegistry_NilExtractor_ResolveFails(t *testing.T) {
	// DefaultRegistry(nil) constructs (mana specs need no extractor), but a
	// payload-recipient topic must fail loud at Resolve, not panic.
	r, err := DefaultRegistry(nil)
	if err != nil {
		t.Fatalf("DefaultRegistry(nil): %v", err)
	}
	_, _, ok, err := r.Map(InboundEvent{Topic: "chora.notifications.in_app.read.v1", GCID: "actor", Payload: []byte("x")})
	if !ok || err == nil {
		t.Fatalf("Map ok=%v err=%v; want true + nil-extractor error", ok, err)
	}
	// Mana still works without an extractor.
	if _, _, ok, err := r.Map(InboundEvent{Topic: "chora.identity.user_mana.credited.v1", GCID: "g"}); !ok || err != nil {
		t.Fatalf("mana via nil-extractor registry: ok=%v err=%v", ok, err)
	}
}

func TestRegistry_TopicsMatchesLookup(t *testing.T) {
	r := mustRegistry(t, &fakeExtractor{})
	for _, topic := range r.Topics() {
		if _, ok := r.Lookup(topic); !ok {
			t.Fatalf("Topics() returned %q but Lookup misses it", topic)
		}
	}
}
