// Package port declares the driven-port interfaces chora-realtime depends on,
// kept free of both the domain and any infrastructure type so adapters
// (redis, ticket, http, pubsub) and the domain can be wired without a cycle.
package port

import (
	"context"
	"time"
)

// Backplane publishes a per-learner frame onto the Redis backplane channel.
// The fan-in tier is the only writer (PUBLISH rt:user:{gcid}).
type Backplane interface {
	Publish(ctx context.Context, channel string, payload []byte) error
}

// UserBus is the per-gcid subscription multiplexer over ONE pod-shared Redis
// PubSub connection (dynamic SUBSCRIBE / UNSUBSCRIBE, refcounted per gcid).
// The connection tier subscribes once per locally-connected learner.
//
// Subscribe returns a receive-only channel of raw backplane frame bytes for
// that gcid, a release func to drop the subscription (idempotent;
// decrements the refcount and UNSUBSCRIBEs at zero), and an error.
type UserBus interface {
	Subscribe(ctx context.Context, gcid string) (<-chan []byte, func(), error)
}

// TicketValidator validates a raw stream ticket and returns the bound
// gcid + tenant. It enforces single-use internally (see JTIChecker). ctx is
// threaded for the single-use backplane round-trip + cancellation.
type TicketValidator interface {
	Validate(ctx context.Context, raw string) (gcid, tenant string, err error)
}

// JTIChecker enforces one-time ticket use. Claim attempts to atomically claim
// jti for ttl; it returns true on the FIRST claim (fresh) and false when the
// jti was already claimed (replay). Backed by Redis SET NX in production.
type JTIChecker interface {
	Claim(ctx context.Context, jti string, ttl time.Duration) (claimed bool, err error)
}
