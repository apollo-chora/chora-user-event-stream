// userbus_test.go covers the per-gcid subscription multiplexer's happy path:
// dynamic SUBSCRIBE/UNSUBSCRIBE refcounting, dispatch fan-out to subscriber
// sinks, slow-consumer frame drop, and Close ending the dispatch goroutine.
//
// The existing redis_test.go deliberately avoids miniredis (it would churn the
// workspace go.work.sum), so these tests drive a dependency-free RESP3
// stand-in (fakeRedis below). It speaks just enough wire protocol for the
// go-redis boot handshake (HELLO 3) + the pubsub lifecycle the adapter
// exercises (PING / SUBSCRIBE / UNSUBSCRIBE push confirmations + `message`
// pushes routed to the connection(s) subscribed to a channel).
package redis

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-user-event-stream/internal/domain/realtime"
)

// --- dependency-free RESP3 stand-in ----------------------------------------

type fakeRedis struct {
	ln   net.Listener
	addr string

	mu      sync.Mutex
	conns   map[*fakeRedisConn]struct{}
	pending map[string][]string // channel → pushes queued while nobody subscribed
	unsubs  []string            // channels UNSUBSCRIBEd (assertion seam)
}

type fakeRedisConn struct {
	c    net.Conn
	mu   sync.Mutex // serializes writes so pushes never interleave with replies
	subs map[string]struct{}
}

func newFakeRedis(t *testing.T) *fakeRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake redis listen: %v", err)
	}
	f := &fakeRedis{
		ln:      ln,
		addr:    ln.Addr().String(),
		conns:   map[*fakeRedisConn]struct{}{},
		pending: map[string][]string{},
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return // listener closed by test cleanup
			}
			go f.serveConn(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

// Push enqueues one pubsub `message` for channel, delivering it immediately to
// every connection currently subscribed (or queueing it for the first
// subscriber if none is yet — the SUBSCRIBE write is async from the test's
// point of view, so ordering must not depend on the fake having processed it).
func (f *fakeRedis) Push(channel, payload string) {
	f.mu.Lock()
	var targeted []*fakeRedisConn
	for cc := range f.conns {
		if _, ok := cc.subs[channel]; ok {
			targeted = append(targeted, cc)
		}
	}
	if len(targeted) == 0 {
		f.pending[channel] = append(f.pending[channel], payload)
	}
	f.mu.Unlock()

	for _, cc := range targeted {
		cc.reply(messageFrame(channel, payload))
	}
}

// unsubscribedChannels returns the channels UNSUBSCRIBEd so far.
func (f *fakeRedis) unsubscribedChannels() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.unsubs)
}

func (f *fakeRedis) serveConn(c net.Conn) {
	defer c.Close()
	cc := &fakeRedisConn{c: c, subs: map[string]struct{}{}}
	f.mu.Lock()
	f.conns[cc] = struct{}{}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.conns, cc)
		f.mu.Unlock()
	}()

	rd := bufio.NewReader(c)
	for {
		cmd, err := readRESPCommand(rd)
		if err != nil {
			return // EOF / malformed — the client closed or is gone
		}
		switch strings.ToUpper(cmd[0]) {
		case "HELLO":
			cc.reply(helloReply())
		case "PING":
			cc.reply([]byte("+PONG\r\n"))
		case "PUBLISH":
			cc.reply([]byte(":1\r\n"))
		case "SUBSCRIBE":
			f.subscribe(cc, cmd[1:])
		case "UNSUBSCRIBE":
			f.unsubscribe(cc, cmd[1:])
		default:
			cc.reply([]byte("-ERR fake redis: unsupported command " + strings.ToUpper(cmd[0]) + "\r\n"))
		}
	}
}

func (f *fakeRedis) subscribe(cc *fakeRedisConn, channels []string) {
	type queued struct{ ch, payload string }
	var flushed []queued // pushes queued while this channel had no subscriber
	f.mu.Lock()
	for _, ch := range channels {
		cc.subs[ch] = struct{}{}
		if q := f.pending[ch]; len(q) > 0 {
			for _, payload := range q {
				flushed = append(flushed, queued{ch: ch, payload: payload})
			}
			delete(f.pending, ch)
		}
	}
	f.mu.Unlock()

	for _, ch := range channels {
		cc.reply(subscribeConfirmation(ch))
	}
	// Deliver queued pushes after the confirmations so the client sees a tidy
	// confirm-then-message order on the wire.
	for _, p := range flushed {
		cc.reply(messageFrame(p.ch, p.payload))
	}
}

func (f *fakeRedis) unsubscribe(cc *fakeRedisConn, channels []string) {
	f.mu.Lock()
	for _, ch := range channels {
		delete(cc.subs, ch)
		f.unsubs = append(f.unsubs, ch)
	}
	f.mu.Unlock()

	for _, ch := range channels {
		cc.reply(unsubscribeConfirmation(ch))
	}
}

func (cc *fakeRedisConn) reply(p []byte) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	_ = cc.c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, _ = cc.c.Write(p)
}

// readRESPCommand parses one RESP array-of-bulk-strings request.
func readRESPCommand(rd *bufio.Reader) ([]string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" || line[0] != '*' {
		return nil, fmt.Errorf("fake redis: expected request array, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		l, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		l = strings.TrimRight(l, "\r\n")
		if l == "" || l[0] != '$' {
			return nil, fmt.Errorf("fake redis: expected bulk arg, got %q", l)
		}
		size, err := strconv.Atoi(l[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		if _, err := rd.Discard(2); err != nil { // trailing \r\n
			return nil, err
		}
		args = append(args, string(buf))
	}
	return args, nil
}

func helloReply() []byte {
	return []byte("%2\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.2.0\r\n")
}

func subscribeConfirmation(channel string) []byte {
	return []byte(fmt.Sprintf(">3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:1\r\n", len(channel), channel))
}

func unsubscribeConfirmation(channel string) []byte {
	return []byte(fmt.Sprintf(">3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:0\r\n", len(channel), channel))
}

func messageFrame(channel, payload string) []byte {
	return []byte(fmt.Sprintf(">3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(channel), channel, len(payload), payload))
}

// --- tests -----------------------------------------------------------------

func newTestBus(t *testing.T, f *fakeRedis) *UserBus {
	t.Helper()
	s, err := NewStore(Config{Addr: f.addr})
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s.NewUserBus()
}

// waitSink reads one frame from sink, failing if none arrives in time.
func waitSink(t *testing.T, sink <-chan []byte, want string) {
	t.Helper()
	select {
	case got, ok := <-sink:
		if !ok {
			t.Fatalf("sink closed before delivering %q", want)
		}
		if string(got) != want {
			t.Fatalf("sink frame = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for frame %q", want)
	}
}

// assertSinkReleased fails if sink is not closed with no further frames.
func assertSinkReleased(t *testing.T, sink <-chan []byte) {
	t.Helper()
	select {
	case _, ok := <-sink:
		if ok {
			t.Fatalf("sink delivered a frame after release")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("sink not closed after release")
	}
}

// waitForUnsubscribe polls until the fake records an UNSUBSCRIBE for channel:
// UserBus's release writes the command asynchronously, so the assertion must
// not race the TCP delivery.
func waitForUnsubscribe(t *testing.T, f *fakeRedis, channel string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Contains(f.unsubscribedChannels(), channel) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for UNSUBSCRIBE of %q (recorded: %v)", channel, f.unsubscribedChannels())
}

func TestUserBus_DispatchFansOutToSubscriberSinks(t *testing.T) {
	f := newFakeRedis(t)
	bus := newTestBus(t, f)
	defer func() { _ = bus.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const gcid = "gcid-1"
	channel := realtime.UserChannel(gcid)
	frame := `{"topic":"mana.balance.changed","occurred_at":"2026-08-08T00:00:00Z","payload":{"mana":42}}`

	// Two per-connection sinks on ONE pod-shared pubsub conn: the first
	// Subscribe SUBSCRIBEs on Redis, the second only registers a sink.
	sinkA, releaseA, err := bus.Subscribe(ctx, gcid)
	if err != nil {
		t.Fatalf("Subscribe #1: %v", err)
	}
	defer releaseA()
	sinkB, releaseB, err := bus.Subscribe(ctx, gcid)
	if err != nil {
		t.Fatalf("Subscribe #2: %v", err)
	}
	defer releaseB()

	// One backplane frame must reach BOTH local sinks.
	f.Push(channel, frame)
	waitSink(t, sinkA, frame)
	waitSink(t, sinkB, frame)

	// A different learner's channel routes only to its own sink.
	const gcid2 = "gcid-2"
	sinkC, releaseC, err := bus.Subscribe(ctx, gcid2)
	if err != nil {
		t.Fatalf("Subscribe gcid-2: %v", err)
	}
	defer releaseC()
	frame2 := `{"topic":"familiar.leveled_up","occurred_at":"2026-08-08T00:00:00Z","payload":{}}`
	f.Push(realtime.UserChannel(gcid2), frame2)
	waitSink(t, sinkC, frame2)
}

func TestUserBus_ReleaseRefcountUnsubscribesAtZero(t *testing.T) {
	f := newFakeRedis(t)
	bus := newTestBus(t, f)
	defer func() { _ = bus.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const gcid = "gcid-ref"
	channel := realtime.UserChannel(gcid)
	frame := `{"topic":"payment.state.changed","occurred_at":"2026-08-08T00:00:00Z","payload":{}}`

	sinkA, releaseA, err := bus.Subscribe(ctx, gcid)
	if err != nil {
		t.Fatalf("Subscribe #1: %v", err)
	}
	sinkB, releaseB, err := bus.Subscribe(ctx, gcid)
	if err != nil {
		t.Fatalf("Subscribe #2: %v", err)
	}

	// One subscriber leaves: the refcount stays > 0, so the channel stays
	// SUBSCRIBEd and the remaining sink keeps flowing.
	releaseA()
	assertSinkReleased(t, sinkA)
	f.Push(channel, frame)
	waitSink(t, sinkB, frame)

	// Last subscriber leaves: refcount hits zero → UNSUBSCRIBE + sink closed.
	releaseB()
	assertSinkReleased(t, sinkB)
	waitForUnsubscribe(t, f, channel)
}

func TestUserBus_SlowConsumerDropsFrames(t *testing.T) {
	f := newFakeRedis(t)
	bus := newTestBus(t, f)
	defer func() { _ = bus.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sink, release, err := bus.Subscribe(ctx, "gcid-slow")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer release()

	// Phase 1 — fill the sink buffer to capacity WITHOUT draining. The
	// non-blocking dispatch send must accept exactly userBufferSize frames;
	// draining concurrently would free slots and mask the drop behaviour.
	channel := realtime.UserChannel("gcid-slow")
	for i := 0; i < userBufferSize; i++ {
		f.Push(channel, fmt.Sprintf(`{"seq":%d}`, i))
	}
	waitForSinkFull(t, sink)

	// Phase 2 — overflowing frames while the sink stays full are DROPPED by
	// dispatch's default arm (at-most-once per ADR-183: one slow tab never
	// blocks the pod-shared dispatch loop; the FE reconciles on reconnect).
	for i := userBufferSize; i < userBufferSize+6; i++ {
		f.Push(channel, fmt.Sprintf(`{"seq":%d}`, i))
	}
	// Let the overflow frames drain through the pump → dispatch pipeline so
	// every one of them hits the full sink (and drops) BEFORE we open room by
	// draining: otherwise the drain would free slots mid-flight and a straggler
	// could legitimately be delivered.
	time.Sleep(400 * time.Millisecond)

	// Drain: exactly the buffer capacity arrives, nothing beyond it.
	for i := 0; i < userBufferSize; i++ {
		select {
		case <-sink:
		case <-time.After(2 * time.Second):
			t.Fatalf("frame %d/64 not delivered", i)
		}
	}
	select {
	case got := <-sink:
		t.Fatalf("received a frame beyond buffer capacity: %q", got)
	case <-time.After(300 * time.Millisecond):
	}
}

// waitForSinkFull polls until the sink buffer holds userBufferSize frames.
func waitForSinkFull(t *testing.T, sink <-chan []byte) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(sink) == userBufferSize {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("sink buffer only reached len=%d, want %d", len(sink), userBufferSize)
}

func TestUserBus_CloseEndsDispatchLoop(t *testing.T) {
	f := newFakeRedis(t)
	bus := newTestBus(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sink, release, err := bus.Subscribe(ctx, "gcid-close")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Close tears down the shared PubSub conn; the channel pump sees the close
	// and exits, so dispatch's range over b.ps.Channel() terminates. A frame
	// pushed afterwards must never reach the sink (nothing is subscribed).
	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	f.Push(realtime.UserChannel("gcid-close"), `{"topic":"x","payload":{}}`)
	select {
	case got := <-sink:
		t.Fatalf("sink received a frame after Close: %q", got)
	case <-time.After(400 * time.Millisecond):
	}

	release() // must stay safe (no panic) after Close
}
