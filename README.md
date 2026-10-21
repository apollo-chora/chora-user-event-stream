# chora-user-event-stream

The realtime push tier for Chora: a Server-Sent Events connection service plus a fan-in bridge
that turns domain events into per-learner frames. Internal name `chora-realtime` (ADR-183).

One Go module and one container image carrying **two binaries** that do different jobs. Getting
that split right is most of operating this service.

Every command, port and variable below was checked against the code in this repository and
against a running `chora-stack`.

## What it is

The service is deliberately split in two so that ingest and connections scale independently:

| Tier | Binary | Port in the stack | Holds client connections | Job |
| --- | --- | --- | --- | --- |
| Connection | `/service` (`cmd/server`) | `8080` | **yes** | serves `/api/v1/realtime/stream`, validates tickets, SUBSCRIBEs Redis |
| Bridge | `/fanin` (`cmd/fanin`) | `8081` | no | consumes 33 NATS subjects, maps each event to a recipient `gcid`, publishes frames |

The two never talk to each other directly. They meet on the Redis backplane:

```text
NATS JetStream ──► fanin ──PUBLISH rt:user:{gcid}──► Valkey ──► /service ──SSE──► browser
```

Concentrating ingest in the bridge keeps the connection pods at `O(local connections)` while
ingest stays `O(firehose / fanin replicas)`. The bridge serves **health probes only** — it has no
stream endpoint and no client surface.

### What the stack runs degraded

This is the section to read first, because the failure mode is silent from the outside: the
container looks healthy while streaming is off.

| Feature | State in the stack | Consequence |
| --- | --- | --- |
| Both tiers | **running** | `user-event-stream` on `8080`, `user-event-stream-fanin` on `8081` |
| Fan-in ingest | **wired** | `fan-in consumers started for 33 subjects` |
| SSE streaming | **can be off** | depends on a boot-time Redis ping; see below |
| Ticket validation | **can be off** | needs Redis **and** the ticket signer |

The connection tier pings Redis **once** at boot. If that ping fails, it logs
`WARNING redis ping failed (…) — streaming DISABLED`, closes the client, and continues with
health probes only. It never retries:

```text
2026/10/21 02:02:15 redis: … failed to dial after 2 attempts: dial tcp: lookup valkey: i/o timeout
2026/10/21 02:02:15 chora-user-event-stream-server: WARNING redis ping failed (…) — streaming DISABLED
2026/10/21 02:02:15 chora-user-event-stream-server: WARNING ticket validation DISABLED (need Redis + …)
```

In that state `/healthz` and `/readyz` both answer `{"status":"ok"}` and
`/api/v1/realtime/stream` returns **`503`**. A restart is the only recovery. Observed in the
running stack: the first boot at `01:31:53` wired Redis fine
(`backplane wired to Redis valkey:6379`), two later restarts failed the same lookup and left the
tier degraded.

## Prerequisites

| Route | What you need |
| --- | --- |
| In the stack | A working `chora-stack` deployment. NATS and Valkey arrive from it. |
| Standalone | Go **1.26.1** (`go.mod` declares it), a reachable NATS JetStream broker, and a reachable Redis/Valkey for anything beyond health. |

There is **no database and no migrations** — the service is stateless. `migrations/` does not
exist in this repository, and nothing here writes to PostgreSQL.

To build both binaries by hand:

```sh
go build -o /out/service ./cmd/server
go build -o /out/fanin   ./cmd/fanin
```

## Credentials

| Credential | Needed for | Where it comes from |
| --- | --- | --- |
| `CHORA_REALTIME_TICKET_SIGNER_SECRET` | validating stream tickets | A shared HS256 secret. **Must match the gateway**, which mints the tickets. Generate with `openssl rand -hex 32`. The stack falls back to `chora-local-realtime-ticket-signer-key`. |
| `CHORA_REDIS_PASSWORD` | authenticating to the backplane | Your Redis/Valkey password. **Empty in the stack** — the local Valkey has no auth. |
| `CHORA_REDIS_CA_CERT` | TLS to a managed Redis | Your provider's CA bundle, PEM. Only needed when the backplane is TLS. |

Each of the three accepts a `_SECRET_ID` variant (`…_SECRET_ID`). The resolution rule is **not**
a fallback chain: if the `_SECRET_ID` is unset, the plain variable is used; if it is set, the value
is fetched from the secret store and **a fetch failure is fatal** — the process exits rather than
falling back to the literal. So do not set both expecting the literal to rescue you when the
secret store is unavailable. Set one, or set the ID and accept the hard dependency.

The ticket signer is the only credential you must keep in sync across services: `chora-gateway`
mints with it and this service verifies with it. A mismatch produces `401` on every stream
connection while both services look healthy.

## Run it in the stack

Two Compose services, both built from this one context:

```sh
docker compose up -d user-event-stream user-event-stream-fanin
```

**Connection tier** (`user-event-stream`):

```text
PORT                                    8080
NATS_URL                                nats://nats:4222
CHORA_REDIS_ADDR                        valkey:6379
CHORA_REALTIME_TICKET_SIGNER_SECRET     from the same var in chora-stack/.env, else chora-local-realtime-ticket-signer-key
CHORA_SOURCE_PROJECT                    chora-local
OTEL_EXPORTER_OTLP_ENDPOINT             http://otel-collector:4317
```

Depends on `nats` (healthy), `nats-init` (completed), `valkey` (healthy). Exposes `8080` only —
no host port. This is the tier that fails closed on a bad Redis ping.

**Bridge tier** (`user-event-stream-fanin`):

```text
PORT                                    8081
NATS_URL                                nats://nats:4222
CHORA_REDIS_ADDR                        valkey:6379
CHORA_SOURCE_PROJECT                    chora-local
OTEL_EXPORTER_OTLP_ENDPOINT             http://otel-collector:4317
```

Same dependencies, `entrypoint: ["/fanin"]`, exposes `8081`. Its comment in `compose.yml` states
the contract plainly: without Redis "the container still starts its health server but logs
`fan-in DISABLED`".

Note the port carefully: **both binaries default `PORT` to `8080`**, because they share one
`config.Load()`. The bridge only listens on `8081` because `compose.yml` sets `PORT: "8081"` on
that service. Run `/fanin` yourself without setting `PORT` and it will collide with the connection
tier.

Because no host port is published, `localhost:8080` is not this service. In production the stream
is served on `api.chora.site` through a **dedicated gateway path rule that bypasses the
`chora-gateway` BFF pod** (ADR-183) — the BFF's own CORS middleware never touches it, which is why
this service ships its own CORS handling and why `CHORA_CORS_ALLOWED_ORIGINS` mirrors the gateway's
allowlist exactly. The gateway is still what mints the ticket.

## Run it standalone

```sh
export PORT=8080
export NATS_URL='nats://127.0.0.1:4222'
export CHORA_REDIS_ADDR='127.0.0.1:6379'
export CHORA_REALTIME_TICKET_SIGNER_SECRET="$(openssl rand -hex 32)"
go run ./cmd/server
```

Bridge tier, in a second shell:

```sh
export PORT=8081
export NATS_URL='nats://127.0.0.1:4222'
export CHORA_REDIS_ADDR='127.0.0.1:6379'
go run ./cmd/fanin
```

Run the bridge only if you want frames to appear; the connection tier alone will accept a stream
and then sit silent, because nothing is publishing to `rt:user:{gcid}`.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8080` for **both** binaries; the stack overrides the bridge to `8081` | HTTP listen port |
| `NATS_URL` | unset → bridge disabled | NATS JetStream; required by the bridge |
| `CHORA_REDIS_ADDR` | unset → **streaming disabled** | backplane `host:port`; empty means health-only |
| `CHORA_REDIS_PASSWORD` / `CHORA_REDIS_PASSWORD_SECRET_ID` | unset | backplane auth |
| `CHORA_REDIS_CA_CERT` / `CHORA_REDIS_CA_CERT_SECRET_ID` | unset → plaintext | backplane TLS CA |
| `CHORA_REALTIME_TICKET_SIGNER_SECRET` / `…_SECRET_ID` | unset → **ticket validation disabled** | HS256 key shared with the gateway |
| `CHORA_REALTIME_KEEPALIVE_SECONDS` | `15` | SSE heartbeat interval |
| `CHORA_CORS_ALLOWED_ORIGINS` | chora.site + four subdomains + localhost:4200/4201/6006 | comma-separated, scheme-qualified, no trailing slash |
| `CHORA_SOURCE_PROJECT` | `chora-local` | event envelope source project |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset → stdout | OTLP/gRPC trace endpoint |

`CHORA_CORS_ALLOWED_ORIGINS` matters for a local deploy: the default set already includes
`http://localhost:4200`, so a browser at the stack's web origin is allowed without changing
anything. Add your own origin if you serve the SPA elsewhere.

## Verify it works

### Both tiers are up

```sh
docker compose exec -T nats wget -qO- http://user-event-stream:8080/healthz
docker compose exec -T nats wget -qO- http://user-event-stream-fanin:8081/healthz
```

Both answer `{"status":"ok"}`. Note `/readyz` on the connection tier reports `{"status":"draining"}`
with `503` during shutdown, not "unhealthy" — a `readyz` blip during a rollout is expected.

### Health is not enough — check the backplane wiring

This is the check that actually matters, because a degraded tier is still healthy:

```sh
docker compose logs user-event-stream | grep -E 'backplane wired|streaming DISABLED'
```

- `backplane wired to Redis valkey:6379 (tls=false)` → streaming is live.
- `WARNING redis ping failed … streaming DISABLED` → it is not, and only a restart fixes it.

The stack observed both across restarts, so check the **latest** boot, not just any line.

### The bridge bound its consumers

```sh
docker compose logs user-event-stream-fanin | grep 'fan-in consumers started'
```

`fan-in consumers started for 33 subjects` is the pass. Fewer means the registry shrank; zero
with `fan-in DISABLED` means Redis was unreachable at boot.

### The stream refuses politely when it cannot serve

```sh
docker compose exec -T nats wget -qO- http://user-event-stream:8080/api/v1/realtime/stream
```

- **`503 REALTIME_UNAVAILABLE`** when either the ticket validator or the Redis bus is nil — what
  a degraded tier returns, and what the running stack returned while writing this.
- **`401`** when streaming is live but the request carries no valid ticket.

The handler's check is `validator == nil || bus == nil`, so a `503` means one of *four* things:
`CHORA_REDIS_ADDR` unset, the Redis client failed to build, the boot ping failed, or the ticket
signer is unset (which leaves the validator nil **even when Redis is perfectly healthy**). The
response body is always the generic `realtime backplane not configured`, so it identifies the
symptom but not the cause — the distinguishing line is in the logs.

### The broker knows the consumers

The `nats` container ships no `nats` CLI; read the monitoring endpoint from the host:

```sh
curl -s 'http://localhost:8222/jsz?consumers=1' | head -c 400
```

## Troubleshooting

- **The browser gets no events but every container is `Up`** — check
  `docker compose logs user-event-stream | grep 'streaming DISABLED'`. The health probes stay
  green in degraded mode by design; `docker compose restart user-event-stream` is the fix, and it
  usually sticks on the second try because the cause is a DNS race at boot, not a config error.
- **`dial tcp: lookup valkey: i/o timeout` at boot** — the container could not resolve the
  backplane name. `depends_on: valkey: condition: service_healthy` guarantees Valkey is up, not
  that name resolution has settled in this container. The ping has no retry, so the tier stays
  disabled.
- **`503` on `/api/v1/realtime/stream`** — the handler saw a nil validator or a nil bus. Not an
  auth failure, and not proof of a Redis problem: an unset ticket signer produces the same `503`
  with a healthy backplane. Check both `CHORA_REDIS_ADDR` and
  `CHORA_REALTIME_TICKET_SIGNER_SECRET` before blaming Redis.
- **`401` on `/api/v1/realtime/stream` with streaming wired** — the ticket failed validation.
  Either it expired (tickets are single-use, guarded by a `jti` in Redis), or
  `CHORA_REALTIME_TICKET_SIGNER_SECRET` does not match the gateway's minting key.
- **Streams connect then close immediately with streaming wired** — the ticket was already
  consumed. A single-use `jti` means replaying a captured ticket in a second tab fails.
- **Frames never arrive although the fan-in bound all 33 subjects** — the bridge is publishing to
  `rt:user:{gcid}` for a recipient your subscriber is not. Confirm the event carries a `gcid` (or
  a `learner_gcid` at payload field 3 for payments and notifications) — an event with no
  resolvable recipient is dropped rather than broadcast.
- **A payment event never reaches the learner** — `tenant_mana_topup` and `dispute` are excluded
  from the learner channel on purpose (tenant-scoped and admin-scoped respectively). `streaming`'s
  registry is not the full payments firehose.
- **`fan-in DISABLED`** — the bridge had no Redis at boot. It keeps serving `/healthz` on `8081`,
  which is why the container looks fine.
- **CORS blocks the stream from a custom origin** — add the scheme-qualified origin to
  `CHORA_CORS_ALLOWED_ORIGINS`. Trailing slashes are not tolerated, and the compare is exact.

## API surface

Three routes exist, and only one is a feature:

| Method | Path | Tier | Notes |
| --- | --- | --- | --- |
| `GET` | `/healthz` | both | `{"status":"ok"}` |
| `GET` | `/readyz` | both | `{"status":"ok"}`, or `{"status":"draining"}` with `503` |
| `GET` | `/api/v1/realtime/stream` | connection only | SSE; `503` without a backplane, `401` without a valid ticket |

The stream route is always mounted — the handler checks for a nil backplane and returns `503`
rather than the mux hiding the route. That is deliberate: a missing route and a degraded tier
should not look the same.

SSE frames arrive as named events. The eight frame topics the fan-in can emit:

```text
mana.balance.changed
familiar.leveled_up          (inbound: companion.stage_up.v1)
familiar.bonded
familiar.retired
familiar.skin_equipped
notification.created
notification.read
payment.state.changed
```

The `familiar.*` names are the shipped frontend contract and are kept on purpose even though the
consumption service renamed its events to `companion.*` (ADR-254 D9: topics renamed, frames
unchanged). Do not "fix" them.

## Architecture

**Ingest (bridge).** `cmd/fanin` registers shared durable JetStream consumers across the domain
event firehose, then for each message consults the registry in `internal/domain/realtime/registry.go`
to find the recipient `gcid` and a projector that builds the frame. It PUBLISHes to
`rt:user:{recipientGCID}`. It has no client state and holds no connections.

**Recipient resolution** is three different mechanisms depending on the event, which is worth
knowing before debugging a missing frame:

| Mechanism | Used by |
| --- | --- |
| Envelope `gcid` | mana changes, companion growth — the event is already personal |
| Payload field 3 (`learner_gcid`) | payments and in-app notifications — verified against the proto |
| none | anything else is dropped |

**Connection (server).** `cmd/server` validates the ticket, then SUBSCRIBEs to
`rt:user:{gcid}` for that learner with a refcount, so N tabs on one learner cost one Redis
subscription. Frames are fanned out to each open response, with a keepalive comment every
`CHORA_REALTIME_KEEPALIVE_SECONDS` (15).

**Ticket flow.** The browser never holds Redis credentials or the signer. It asks
`chora-gateway` for a short-lived ticket; the gateway mints an HS256 ticket with
`aud=chora-realtime` and a unique `jti`; this service verifies the signature and burns the `jti`
in Redis so a replayed ticket fails. That single-use check is why Redis is required for
validation and not just for frames — the `WARNING ticket validation DISABLED (need Redis + …)`
line names both dependencies for that reason.

**No persistence.** Nothing here survives a restart except the JetStream consumer offsets. There
is no outbox, no PostgreSQL, no migration.

## Events

| Direction | Mechanism |
| --- | --- |
| Inbound | 33 NATS JetStream subjects, shared durable consumers (`chora-realtime.*`) |
| Intermediate | Redis PUBLISH/SUBSCRIBE on `rt:user:{gcid}` |
| Outbound to the browser | SSE on `/api/v1/realtime/stream` |

The 33 inbound subjects, counted from the registry:

```text
3  chora.identity.user_mana.{credited,debited,refunded}.v1
4  chora.consumption.companion.{stage_up,bonded,retired,skin_equipped}.v1
2  chora.notifications.in_app.{created,read}.v1
24 chora.payments.<aggregate>.<suffix>.v1
```

The 24 payments subjects are 6 aggregates × 4 suffixes:

| Aggregates | Suffixes |
| --- | --- |
| `course_purchase`, `companion_egg_purchase`, `user_mana_topup`, `user_subscription`, `application_payment`, `identity_kyc_fee` | `payment_captured`, `payment_failed`, `refunded`, `expired` |

That is `33 = 3 + 4 + 2 + 24`, which is exactly what the bridge logs at boot — a quick way to
tell a stale image from a wire-up failure.

## Migrations

**None.** This repository has no `migrations/` directory and no database. The service is
stateless by design, and there is nothing to run before starting it.

Schema work that the realtime tier depends on lives in the services that own the data —
`chora-identity` for mana, `chora-consumption` for companion state, `chora-payments` for
payment state. This service only reads their events.
