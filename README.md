# chora-user-event-stream

Real-time learner event stream for Chora (ADR-183). The service terminates a
learner-scoped SSE channel and relays domain events (mana wallet movements,
Companion growth, in-app notifications, payment state changes) as per-learner
frames.

The service is designed to run locally with Docker Compose and uses environment
variables for configuration. No cloud account or managed services are required:
the message transport is NATS JetStream, the backplane is Redis, secrets are
environment-backed, and traces are exported over OTLP.

## Two tiers, one image

The service ships two binaries that share the same domain package:

- **`cmd/server`** — the *connection tier*. Terminates `GET /api/v1/realtime/stream`,
  validates the one-time HMAC stream ticket, SUBSCRIBEs only the locally-connected
  learners' Redis backplane channels, and relays frames with a keepalive. It does
  no event-bus work.
- **`cmd/fanin`** — the *bridge tier*. Consumes the domain-event firehose on shared
  durable JetStream consumers, maps each event to a recipient GCID via the domain
  Registry, and PUBLISHes a per-learner frame to `rt:user:{gcid}` on the Redis
  backplane. It holds no client connections and serves only health probes.

Concentrating ingest in the fan-in tier keeps the connection pods at
O(local-connections) while ingest stays O(firehose / fan-in-replicas).

## Local stack

The default Compose stack contains:

- **chora-user-event-stream** — HTTP service (SSE + health probes)
- **Redis** — per-GCID backplane and single-use ticket nonce store
- **NATS JetStream** — local event transport (streams provisioned by `nats-init`)

## Configuration

Create the local environment file:

```sh
cp .env.example .env
```

The checked-in `.env.example` contains the complete local defaults. The actual
`.env` file is ignored by Git.

| Variable | Purpose | Local default |
| --- | --- | --- |
| `PORT` | HTTP port (SSE + health) | `8080` |
| `CHORA_SOURCE_PROJECT` | Source-project label for local envelopes | `chora-local` |
| `CHORA_REALTIME_KEEPALIVE_SECONDS` | SSE heartbeat interval | `15` |
| `CHORA_REDIS_ADDR` | Redis backplane (`host:port`); unset disables streaming | `redis:6379` |
| `CHORA_REDIS_PASSWORD` | Redis AUTH string | empty |
| `CHORA_REDIS_CA_CERT` | Redis server-CA PEM (enables TLS) | empty |
| `NATS_URL` | NATS JetStream event bus; unset disables fan-in | `nats://nats:4222` |
| `CHORA_REALTIME_TICKET_SIGNER_SECRET` | HS256 stream-ticket signer (≥32 bytes), shared with chora-gateway | dev value in `.env.example` |
| `CHORA_CORS_ALLOWED_ORIGINS` | Comma-separated CORS allowlist for the stream | built-in chora-web set |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/gRPC trace endpoint | `http://otel-collector:4317` |

The service exposes HTTP only (SSE + `/healthz` + `/readyz`); it has no gRPC
listener.

### Secrets

Secret values resolve from a `*_SECRET_ID` environment variable through the
shared environment-backed resolver (`chora-common/secrets`): the name maps to
`SECRET_<NORMALISED_ID>`, then `<NORMALISED_ID>`, then the raw name. A literal
env fallback (`CHORA_REDIS_PASSWORD`, …) is used only when no `*_SECRET_ID` is
set. A configured `*_SECRET_ID` that cannot be resolved is a fail-loud boot
error — the service never boots with a silently-empty secret.

## Run locally

From the repository root:

```sh
docker compose up --build
```

The SSE stream is exposed on `http://localhost:8080/api/v1/realtime/stream`
(requires a `?ticket=` minted by chora-gateway). Health probes are
`/healthz` (liveness) and `/readyz` (readiness, flips to 503 on SIGTERM so the
load balancer stops routing new streams before the pod drains).

## Event bus

Local messaging uses NATS JetStream. The event taxonomy
(`chora.{domain}.{aggregate}.{event_type}.v{N}`) is unchanged, and the transport
is brokered by `github.com/apollo-chora/chora-common/eventbus`.

`nats-init` provisions two streams: `CHORA_EVENTS` (subjects `chora.>`) and
`CHORA_DLQ` (subjects `_dlq.>`, the dead-letter convention). The fan-in tier
creates one durable consumer per registered inbound subject; a handler error
Nacks and redelivers with backoff, and after `MaxDeliver` the message is routed
to `_dlq.<subject>`. Consumer names keep the historical `chora-realtime.`
prefix; `eventbus` sanitises the dotted name into a NATS-legal durable.

## Authentication

`EventSource` cannot send an `Authorization` header, so the SSE stream
authenticates with a short-lived (~60s) one-time HS256 JWT passed as the
`?ticket=` query parameter. The ticket is minted by chora-gateway with
`aud="chora-realtime"` and validated here; the `jti` is claimed once via Redis
`SET NX` so a replayed ticket is rejected.

## Event payloads

Inbound domain events are decoded with a minimal read-only protobuf wire-format
reader (`internal/adapter/protofield`) rather than generated bindings, so the
service depends only on `chora-common`. The Registry
(`internal/domain/realtime`) maps each inbound subject to a per-learner outbound
frame; see `DefaultRegistry` for the registered subjects.
