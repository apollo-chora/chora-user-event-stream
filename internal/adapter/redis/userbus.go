// userbus.go — the per-gcid subscription multiplexer (ADR-183 connection tier).
//
// ONE pod-shared Redis PubSub connection backs every locally-connected
// learner. A single dispatch goroutine reads the merged PubSub stream and fans
// each message out to the per-channel subscriber sinks; Subscribe/release
// refcount sinks so the conn dynamically SUBSCRIBEs `rt:user:{gcid}` on the
// first local subscriber and UNSUBSCRIBEs at zero. This keeps per-pod load
// O(local-connections), never O(firehose).
package redis

import (
	"context"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/apollo-chora/chora-user-event-stream/internal/domain/realtime"
	"github.com/apollo-chora/chora-user-event-stream/internal/port"
)

// userBufferSize bounds per-connection buffering of backplane frames. A slow
// SSE client that fills its buffer DROPS frames (at-most-once per ADR-183 — the
// FE reconciles via its authoritative GET on reconnect), so one slow tab never
// blocks the pod-shared dispatch loop or another learner.
const userBufferSize = 64

// UserBus multiplexes per-gcid backplane subscriptions over one PubSub conn.
type UserBus struct {
	ps   *goredis.PubSub
	mu   sync.Mutex
	subs map[string]map[chan []byte]struct{} // channel → set of per-connection sinks
}

func newUserBus(c *goredis.Client) *UserBus {
	// Subscribe with zero channels: the conn is established lazily and channels
	// are added dynamically on the first Subscribe(gcid).
	b := &UserBus{
		ps:   c.Subscribe(context.Background()),
		subs: map[string]map[chan []byte]struct{}{},
	}
	go b.dispatch()
	return b
}

// dispatch routes every backplane message to its channel's subscriber sinks.
// Non-blocking sends — a full sink drops the frame.
func (b *UserBus) dispatch() {
	for msg := range b.ps.Channel() {
		b.mu.Lock()
		for sink := range b.subs[msg.Channel] {
			select {
			case sink <- []byte(msg.Payload):
			default: // slow consumer — drop; FE reconciles on reconnect
			}
		}
		b.mu.Unlock()
	}
}

// Subscribe registers a per-connection sink for gcid's channel, SUBSCRIBEing on
// Redis for the first subscriber. The returned release is idempotent and
// decrements the refcount (UNSUBSCRIBE at zero).
func (b *UserBus) Subscribe(ctx context.Context, gcid string) (<-chan []byte, func(), error) {
	channel := realtime.UserChannel(gcid)
	sink := make(chan []byte, userBufferSize)

	b.mu.Lock()
	set, ok := b.subs[channel]
	if !ok {
		if err := b.ps.Subscribe(ctx, channel); err != nil {
			b.mu.Unlock()
			return nil, nil, err
		}
		set = map[chan []byte]struct{}{}
		b.subs[channel] = set
	}
	set[sink] = struct{}{}
	b.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if cur, ok := b.subs[channel]; ok {
				delete(cur, sink)
				if len(cur) == 0 {
					delete(b.subs, channel)
					// Best-effort UNSUBSCRIBE on a background ctx so release stays
					// usable after the request ctx is cancelled.
					_ = b.ps.Unsubscribe(context.Background(), channel)
				}
			}
			// Safe: the dispatch loop only sends under b.mu and we removed sink
			// from the set above, so no send can race this close.
			close(sink)
		})
	}
	return sink, release, nil
}

// Close tears down the pod-shared PubSub conn (ends the dispatch goroutine).
func (b *UserBus) Close() error { return b.ps.Close() }

var _ port.UserBus = (*UserBus)(nil)
