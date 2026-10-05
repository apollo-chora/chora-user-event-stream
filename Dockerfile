# syntax=docker/dockerfile:1.6
#
# chora-user-event-stream Dockerfile — standalone Go service (ADR-183), TWO
# binaries in one image:
#   /service — connection tier (cmd/server): terminates learner SSE
#   /fanin   — bridge tier (cmd/fanin): JetStream firehose → Redis rt:user:{gcid}
#
# The fan-in Deployment overrides `command: ["/fanin"]`; the connection tier
# uses the default ENTRYPOINT. One image keeps the two tiers version-locked
# (they share the envelope/registry domain package).
#
# Build context = this repository. Shared Chora modules (chora-common) are
# resolved through Go modules, not a workspace. No DB; events are read via the
# protofield wire reader, so chora-contracts is not required.
#
# Standard invocation:
#   docker buildx build --platform=linux/amd64 \
#     -f Dockerfile \
#     --build-arg GIT_SHA=$(git rev-parse --short HEAD) \
#     --build-arg BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
#     -t walfa/chora-user-event-stream:latest \
#     .

ARG GO_VERSION=1.26.6
ARG ALPINE_VERSION=3.23
ARG SERVICE_NAME=chora-user-event-stream
ARG GIT_SHA=unknown
ARG BUILD_TIME=unknown

############################
# Stage 1 — build
############################
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder
ARG TARGETARCH

ARG SERVICE_NAME
ARG GIT_SHA
ARG BUILD_TIME

WORKDIR /src

RUN apk add --no-cache ca-certificates git

COPY . .

RUN go mod download

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=${TARGETARCH}
RUN go build -trimpath -ldflags "-s -w" -o /out/service ./cmd/server \
 && go build -trimpath -ldflags "-s -w" -o /out/fanin ./cmd/fanin

############################
# Stage 2 — runtime
############################
FROM alpine:${ALPINE_VERSION}

ARG SERVICE_NAME
ARG GIT_SHA
ARG BUILD_TIME

RUN apk add --no-cache ca-certificates \
    && addgroup -S app \
    && adduser -S -G app app

LABEL org.opencontainers.image.title="${SERVICE_NAME}" \
      org.opencontainers.image.source="https://github.com/apollo-chora/chora-user-event-stream" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_TIME}" \
      org.opencontainers.image.vendor="Chora Platform" \
      org.opencontainers.image.licenses="UNLICENSED" \
      io.chora.service="${SERVICE_NAME}" \
      io.chora.git-sha="${GIT_SHA}" \
      io.chora.build-time="${BUILD_TIME}"

WORKDIR /

COPY --from=builder /out/service /service
COPY --from=builder /out/fanin /fanin

USER app:app
ENTRYPOINT ["/service"]
