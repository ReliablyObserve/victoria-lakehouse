FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=${VERSION}" -o /lakehouse ./cmd/lakehouse

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /lakehouse /usr/local/bin/lakehouse

RUN mkdir -p /data/lakehouse/cache

EXPOSE 9428 10428

ENTRYPOINT ["/usr/local/bin/lakehouse"]
