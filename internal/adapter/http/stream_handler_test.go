package httpadapter

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-user-event-stream/internal/adapter/ticket"
	"github.com/apollo-chora/chora-user-event-stream/internal/domain/realtime"
)

// fakeValidator implements port.TicketValidator.
type fakeValidator struct {
	gcid, tenant string
	err          error

	mu     sync.Mutex
	gotRaw string
}

func (f *fakeValidator) Validate(_ context.Context, raw string) (string, string, error) {
	f.mu.Lock()
	f.gotRaw = raw
	f.mu.Unlock()
	if f.err != nil {
		return "", "", f.err
	}
	return f.gcid, f.tenant, nil
}

// fakeBus implements port.UserBus over a test-fed channel.
type fakeBus struct {
	ch  chan []byte
	err error

	mu       sync.Mutex
	gcids    []string
	releases int
	released chan struct{} // closed on first release
}

func newFakeBus(buf int) *fakeBus {
	return &fakeBus{ch: make(chan []byte, buf), released: make(chan struct{})}
}

func (f *fakeBus) Subscribe(_ context.Context, gcid string) (<-chan []byte, func(), error) {
	if f.err != nil {
		return nil, nil, f.err
	}
	f.mu.Lock()
	f.gcids = append(f.gcids, gcid)
	f.mu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			f.mu.Lock()
			f.releases++
			f.mu.Unlock()
			close(f.released)
		})
	}
	return f.ch, release, nil
}

func (f *fakeBus) releaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.releases
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var m struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	if m.Error.Message == "" {
		t.Fatalf("error body missing message: %s", rec.Body.String())
	}
	return m.Error.Code
}

func TestStream_ErrorResponses(t *testing.T) {
	okValidator := func() *fakeValidator { return &fakeValidator{gcid: "g1", tenant: "t1"} }
	tests := []struct {
		name     string
		handler  *StreamHandler
		method   string
		target   string
		wantCode int
		wantErr  string
	}{
		{
			name:     "non-GET method",
			handler:  NewStreamHandler(okValidator(), newFakeBus(0), 0),
			method:   http.MethodPost,
			target:   "/api/v1/realtime/stream?ticket=tok",
			wantCode: http.StatusMethodNotAllowed,
			wantErr:  "REALTIME_TICKET_INVALID",
		},
		{
			name:     "nil validator",
			handler:  NewStreamHandler(nil, newFakeBus(0), 0),
			method:   http.MethodGet,
			target:   "/api/v1/realtime/stream?ticket=tok",
			wantCode: http.StatusServiceUnavailable,
			wantErr:  "REALTIME_UNAVAILABLE",
		},
		{
			name:     "nil bus",
			handler:  NewStreamHandler(okValidator(), nil, 0),
			method:   http.MethodGet,
			target:   "/api/v1/realtime/stream?ticket=tok",
			wantCode: http.StatusServiceUnavailable,
			wantErr:  "REALTIME_UNAVAILABLE",
		},
		{
			name:     "missing ticket",
			handler:  NewStreamHandler(okValidator(), newFakeBus(0), 0),
			method:   http.MethodGet,
			target:   "/api/v1/realtime/stream",
			wantCode: http.StatusUnauthorized,
			wantErr:  "REALTIME_TICKET_MISSING",
		},
		{
			name:     "whitespace ticket",
			handler:  NewStreamHandler(okValidator(), newFakeBus(0), 0),
			method:   http.MethodGet,
			target:   "/api/v1/realtime/stream?ticket=%20%20",
			wantCode: http.StatusUnauthorized,
			wantErr:  "REALTIME_TICKET_MISSING",
		},
		{
			name:     "invalid ticket",
			handler:  NewStreamHandler(&fakeValidator{err: ticket.ErrBadSignature}, newFakeBus(0), 0),
			method:   http.MethodGet,
			target:   "/api/v1/realtime/stream?ticket=bad",
			wantCode: http.StatusUnauthorized,
			wantErr:  "REALTIME_TICKET_INVALID",
		},
		{
			name:     "expired ticket",
			handler:  NewStreamHandler(&fakeValidator{err: ticket.ErrExpired}, newFakeBus(0), 0),
			method:   http.MethodGet,
			target:   "/api/v1/realtime/stream?ticket=old",
			wantCode: http.StatusUnauthorized,
			wantErr:  "REALTIME_TICKET_EXPIRED",
		},
		{
			name:     "replayed ticket",
			handler:  NewStreamHandler(&fakeValidator{err: ticket.ErrReplayed}, newFakeBus(0), 0),
			method:   http.MethodGet,
			target:   "/api/v1/realtime/stream?ticket=used",
			wantCode: http.StatusUnauthorized,
			wantErr:  "REALTIME_TICKET_REPLAYED",
		},
		{
			name:     "subscribe failure",
			handler:  NewStreamHandler(okValidator(), &fakeBus{err: errors.New("backplane down")}, 0),
			method:   http.MethodGet,
			target:   "/api/v1/realtime/stream?ticket=tok",
			wantCode: http.StatusInternalServerError,
			wantErr:  "REALTIME_SUBSCRIBE_FAILED",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tt.handler.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.target, nil))
			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantCode, rec.Body.String())
			}
			if got := decodeError(t, rec); got != tt.wantErr {
				t.Fatalf("error code = %q, want %q", got, tt.wantErr)
			}
		})
	}
}

func TestMapTicketError_WrappedSentinels(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{ticket.ErrExpired, "REALTIME_TICKET_EXPIRED"},
		{ticket.ErrReplayed, "REALTIME_TICKET_REPLAYED"},
		{ticket.ErrMalformed, "REALTIME_TICKET_INVALID"},
		{errors.New("anything else"), "REALTIME_TICKET_INVALID"},
	}
	for _, tt := range tests {
		if got := mapTicketError(tt.err); got != tt.want {
			t.Fatalf("mapTicketError(%v) = %q, want %q", tt.err, got, tt.want)
		}
	}
}

func TestNewStreamHandler_KeepaliveDefaulting(t *testing.T) {
	if h := NewStreamHandler(nil, nil, 0); h.keepalive != defaultKeepalive {
		t.Fatalf("keepalive(0) = %v, want %v", h.keepalive, defaultKeepalive)
	}
	if h := NewStreamHandler(nil, nil, -time.Second); h.keepalive != defaultKeepalive {
		t.Fatalf("keepalive(-1s) = %v, want %v", h.keepalive, defaultKeepalive)
	}
	if h := NewStreamHandler(nil, nil, 42*time.Second); h.keepalive != 42*time.Second {
		t.Fatalf("keepalive(42s) = %v", h.keepalive)
	}
}

// --- streaming tests (real server so Flush + client-disconnect are real) ----

func backplaneFrame(t *testing.T, env realtime.Envelope) []byte {
	t.Helper()
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal backplane frame: %v", err)
	}
	return b
}

// openStream GETs the SSE endpoint and returns the live response + a cancel.
func openStream(t *testing.T, srv *httptest.Server) (*http.Response, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/v1/realtime/stream?ticket=tok-1", nil)
	if err != nil {
		cancel()
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("GET stream: %v", err)
	}
	return resp, cancel
}

// readSSEBlock reads one event block (lines up to a blank separator).
func readSSEBlock(t *testing.T, br *bufio.Reader) []string {
	t.Helper()
	var lines []string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE stream: %v (lines so far: %q)", err, lines)
		}
		line = strings.TrimRight(line, "\n")
		if line == "" {
			if len(lines) == 0 {
				continue // tolerate a leading separator
			}
			return lines
		}
		lines = append(lines, line)
	}
}

func waitReleased(t *testing.T, bus *fakeBus) {
	t.Helper()
	select {
	case <-bus.released:
	case <-time.After(5 * time.Second):
		t.Fatalf("subscription was not released after disconnect")
	}
}

func TestStream_HappyPath_FramingFilteringSequencing(t *testing.T) {
	validator := &fakeValidator{gcid: "g1", tenant: "tenant-A"}
	bus := newFakeBus(8)
	h := NewStreamHandler(validator, bus, time.Hour) // keepalive silenced
	srv := httptest.NewServer(h)
	defer srv.Close()

	occ := time.Unix(1700000000, 0).UTC()
	// 1: relayed. 2: malformed (skipped). 3: cross-tenant (dropped). 4: relayed.
	bus.ch <- backplaneFrame(t, realtime.Envelope{
		Topic: realtime.TopicManaBalanceChanged, OccurredAt: occ,
		Payload:  json.RawMessage(`{"reason":"credit","ledger_seq":1}`),
		TenantID: "tenant-A", GCID: "g1",
	})
	bus.ch <- []byte("{not-json")
	bus.ch <- backplaneFrame(t, realtime.Envelope{
		Topic: "leak.other.tenant", OccurredAt: occ,
		Payload: json.RawMessage(`{}`), TenantID: "tenant-B", GCID: "g1",
	})
	bus.ch <- backplaneFrame(t, realtime.Envelope{
		Topic: realtime.TopicNotificationCreated, OccurredAt: occ,
		Payload:  json.RawMessage(`{"notification_id":"n-1"}`),
		TenantID: "tenant-A", GCID: "g1",
	})

	resp, cancel := openStream(t, srv)
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream; charset=utf-8" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("Cache-Control = %q", cc)
	}
	if xb := resp.Header.Get("X-Accel-Buffering"); xb != "no" {
		t.Fatalf("X-Accel-Buffering = %q", xb)
	}

	br := bufio.NewReader(resp.Body)

	block1 := readSSEBlock(t, br)
	if len(block1) != 3 || block1[0] != "id: 1" || block1[1] != "event: "+realtime.TopicManaBalanceChanged {
		t.Fatalf("block1 = %q", block1)
	}
	if !strings.HasPrefix(block1[2], "data: ") || !strings.Contains(block1[2], `"reason":"credit"`) {
		t.Fatalf("block1 data = %q", block1[2])
	}
	// The CLIENT frame must not leak backplane-only fields.
	if strings.Contains(block1[2], "tenant_id") || strings.Contains(block1[2], "gcid") {
		t.Fatalf("client frame leaked identity fields: %q", block1[2])
	}

	// id: 2 proves the malformed + cross-tenant frames neither rendered nor
	// consumed a sequence number.
	block2 := readSSEBlock(t, br)
	if len(block2) != 3 || block2[0] != "id: 2" || block2[1] != "event: "+realtime.TopicNotificationCreated {
		t.Fatalf("block2 = %q", block2)
	}
	if !strings.Contains(block2[2], `"notification_id":"n-1"`) {
		t.Fatalf("block2 data = %q", block2[2])
	}
	if strings.Contains(block2[2], "leak.other.tenant") {
		t.Fatalf("cross-tenant frame leaked: %q", block2[2])
	}

	// Client disconnect → handler returns → release fires (refcount cleanup).
	cancel()
	waitReleased(t, bus)
	if n := bus.releaseCount(); n != 1 {
		t.Fatalf("releases = %d, want 1", n)
	}

	bus.mu.Lock()
	gcids := append([]string(nil), bus.gcids...)
	bus.mu.Unlock()
	if len(gcids) != 1 || gcids[0] != "g1" {
		t.Fatalf("Subscribe gcids = %q, want [g1]", gcids)
	}
	validator.mu.Lock()
	raw := validator.gotRaw
	validator.mu.Unlock()
	if raw != "tok-1" {
		t.Fatalf("validator received %q, want tok-1", raw)
	}
}

func TestStream_NoTenantOnTicket_RelaysFrames(t *testing.T) {
	// A ticket without a tenant claim cannot enforce the drop check — frames
	// still relay (the guard requires BOTH sides non-empty).
	bus := newFakeBus(2)
	h := NewStreamHandler(&fakeValidator{gcid: "g2", tenant: ""}, bus, time.Hour)
	srv := httptest.NewServer(h)
	defer srv.Close()

	bus.ch <- backplaneFrame(t, realtime.Envelope{
		Topic: realtime.TopicCompanionBonded, OccurredAt: time.Now().UTC(),
		Payload: json.RawMessage(`{"familiar_id":"f-1"}`), TenantID: "tenant-Z", GCID: "g2",
	})

	resp, cancel := openStream(t, srv)
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	block := readSSEBlock(t, bufio.NewReader(resp.Body))
	if len(block) != 3 || block[1] != "event: "+realtime.TopicCompanionBonded {
		t.Fatalf("block = %q", block)
	}
	cancel()
	waitReleased(t, bus)
}

func TestStream_KeepaliveEmission(t *testing.T) {
	bus := newFakeBus(0)
	h := NewStreamHandler(&fakeValidator{gcid: "g3", tenant: "t"}, bus, 25*time.Millisecond)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, cancel := openStream(t, srv)
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	br := bufio.NewReader(resp.Body)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("no keepalive observed")
		}
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if strings.TrimRight(line, "\n") == ":keepalive" {
			break
		}
	}
	cancel()
	waitReleased(t, bus)
}

// errWriter is a ResponseWriter whose body writes always fail (a client that
// vanished mid-stream without its context cancelling yet). NOT an
// http.Flusher — also exercises the flusher-nil paths.
type errWriter struct{ hdr http.Header }

func (w *errWriter) Header() http.Header {
	if w.hdr == nil {
		w.hdr = http.Header{}
	}
	return w.hdr
}
func (w *errWriter) WriteHeader(int)           {}
func (w *errWriter) Write([]byte) (int, error) { return 0, errors.New("client gone") }

func serveUntilReturn(t *testing.T, h *StreamHandler, w http.ResponseWriter) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/realtime/stream?ticket=tok", nil))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("handler did not return on write error")
	}
}

func TestStream_KeepaliveWriteError_EndsAndReleases(t *testing.T) {
	bus := newFakeBus(0)
	h := NewStreamHandler(&fakeValidator{gcid: "g5", tenant: "t"}, bus, time.Millisecond)
	serveUntilReturn(t, h, &errWriter{})
	waitReleased(t, bus)
}

func TestStream_FrameWriteError_EndsAndReleases(t *testing.T) {
	bus := newFakeBus(1)
	bus.ch <- backplaneFrame(t, realtime.Envelope{
		Topic: realtime.TopicNotificationRead, OccurredAt: time.Now().UTC(),
		Payload: json.RawMessage(`{"notification_id":"n-2"}`), TenantID: "t", GCID: "g6",
	})
	h := NewStreamHandler(&fakeValidator{gcid: "g6", tenant: "t"}, bus, time.Hour)
	serveUntilReturn(t, h, &errWriter{})
	waitReleased(t, bus)
}

func TestStream_SinkClosed_EndsStream(t *testing.T) {
	bus := newFakeBus(0)
	h := NewStreamHandler(&fakeValidator{gcid: "g4", tenant: "t"}, bus, time.Hour)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, cancel := openStream(t, srv)
	defer cancel()
	defer func() { _ = resp.Body.Close() }()

	close(bus.ch) // backplane bus shut down → handler must end the stream
	waitReleased(t, bus)

	// The body terminates (EOF) rather than hanging.
	buf := make([]byte, 64)
	for {
		if _, err := resp.Body.Read(buf); err != nil {
			break
		}
	}
}
