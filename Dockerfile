FROM golang:1.26.2-alpine3.22 AS builder
WORKDIR /app
ARG VERSION=dev
ARG REVISION=unknown
ARG BUILD_TIME=unknown
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION}" -o /lakehouse ./cmd/lakehouse && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /healthcheck ./cmd/healthcheck && \
    mkdir -p /data/lakehouse/cache

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /lakehouse /usr/local/bin/lakehouse
COPY --from=builder /healthcheck /usr/local/bin/healthcheck
COPY --from=builder --chown=65532:65532 /data/lakehouse /data/lakehouse
USER nonroot
EXPOSE 9428 10428
ENTRYPOINT ["/usr/local/bin/lakehouse"]
