# Stage 1: Build
FROM golang:1.27-alpine AS builder
ARG GOPROXY=https://proxy.golang.org|https://goproxy.io|direct
ENV GOPROXY=${GOPROXY}
RUN apk add --no-cache git build-base

WORKDIR /build/dexTakerController

COPY commons/ ../commons/
COPY dexTakerController/go.mod dexTakerController/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    sh -ec 'for attempt in 1 2 3 4 5; do go mod download && exit 0; echo "go mod download attempt $attempt failed, retrying..."; sleep $((attempt * 10)); done; exit 1'

COPY dexTakerController/main.go dexTakerController/main_lnd_backup.go dexTakerController/main_lnd_provision.go dexTakerController/boltz_runtime.go ./
COPY dexTakerController/internal/ ./internal/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
	-ldflags="-linkmode external -extldflags -static -w -s" \
    -o controller .

# Stage 2: Runtime
FROM alpine:3.24

RUN apk add --no-cache ca-certificates wget

COPY --from=builder /build/dexTakerController/controller /controller

ENTRYPOINT ["/controller"]
