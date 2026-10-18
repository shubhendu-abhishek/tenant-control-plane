FROM golang:1.23-alpine AS builder
WORKDIR /src
RUN apk add --no-cache ca-certificates make git build-base
COPY go.mod ./
RUN go mod download
COPY . .
# Generate and verify module checksums after source files are available.
# Commit go.sum for reproducible local builds; this also keeps fresh CI builds bootable.
RUN go mod tidy
RUN mkdir -p /out && go build -trimpath -ldflags="-s -w" -o /out/control-plane ./cmd/control-plane \
 && go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM alpine:3.21 AS runtime
RUN apk add --no-cache ca-certificates tzdata && addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=builder /out/control-plane /app/control-plane
COPY --from=builder /out/worker /app/worker
USER app:app
EXPOSE 8080
ENTRYPOINT ["/app/control-plane"]
