VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)
BINARY := bin/lakehouse

.PHONY: build test lint vet clean e2e

build:
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/lakehouse
	go build -ldflags "-s -w" -o bin/healthcheck ./cmd/healthcheck

test:
	go test ./... -race -count=1 -timeout=5m

test-integration:
	go test -tags=integration ./... -race -count=1 -timeout=15m

vet:
	go vet ./...

lint: vet
	@which golangci-lint > /dev/null 2>&1 || echo "golangci-lint not installed"
	golangci-lint run ./...

clean:
	rm -rf bin/ coverage.out

coverage:
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -html=coverage.out -o coverage.html

docker:
	docker build -t ghcr.io/reliablyobserve/victoria-lakehouse:$(VERSION) .

e2e:
	docker compose -f deployment/docker/docker-compose-e2e.yml up --build --abort-on-container-exit --exit-code-from e2e-test
	docker compose -f deployment/docker/docker-compose-e2e.yml down -v
