VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo.Version=$(VERSION)

# VictoriaLogs — Go module proxy has stale cache with wrong module path.
# We clone the correct version locally and use a replace directive in go.mod.
VL_VERSION_LOGS := v1.50.0
VL_REPO := https://github.com/VictoriaMetrics/VictoriaLogs.git
VL_DIR_LOGS := deps/VictoriaLogs
VL_DIR_TRACES := lakehouse-traces/deps/VictoriaLogs

.PHONY: build build-logs build-traces test test-logs test-traces lint vet clean e2e deps-logs deps-traces

deps-logs: $(VL_DIR_LOGS)/go.mod

$(VL_DIR_LOGS)/go.mod:
	@mkdir -p deps
	git clone --depth 1 --branch $(VL_VERSION_LOGS) $(VL_REPO) $(VL_DIR_LOGS)
	cp patches/vl-logs/external.go $(VL_DIR_LOGS)/app/vlstorage/external.go
	cp patches/vl-logs/external_query.go $(VL_DIR_LOGS)/lib/logstorage/external_query.go
	cd $(VL_DIR_LOGS) && git apply ../../patches/vl-logs/vlstorage-dispatch.patch

deps-traces: $(VL_DIR_TRACES)/go.mod

$(VL_DIR_TRACES)/go.mod:
	@echo "VictoriaLogs traces dep must be prepared manually (specific commit for VT v0.8.2 compat)"
	@test -f $(VL_DIR_TRACES)/go.mod || (echo "Missing $(VL_DIR_TRACES)/go.mod — see README" && exit 1)

build: build-logs build-traces
	go build -ldflags "-s -w" -o bin/healthcheck ./cmd/healthcheck

build-logs: deps-logs
	go build -ldflags "$(LDFLAGS)" -o bin/lakehouse-logs ./cmd/lakehouse-logs

build-traces: deps-traces
	go build -ldflags "$(LDFLAGS)" -o bin/lakehouse-traces ./lakehouse-traces

test: test-logs test-traces

test-logs: deps-logs
	go test ./internal/... -race -count=1 -timeout=5m

test-traces: deps-traces
	cd lakehouse-traces && go test ./internal/... -race -count=1 -timeout=5m

test-integration-logs: deps-logs
	go test -tags=integration ./internal/... -race -count=1 -timeout=15m

test-integration-traces: deps-traces
	cd lakehouse-traces && go test -tags=integration ./internal/... -race -count=1 -timeout=15m

vet: deps-logs deps-traces
	go vet ./...
	cd lakehouse-traces && go vet ./...

lint: vet
	@which golangci-lint > /dev/null 2>&1 || echo "golangci-lint not installed"
	golangci-lint run ./...
	cd lakehouse-traces && golangci-lint run ./...

clean:
	rm -rf bin/ coverage.out deps/

coverage-logs: deps-logs
	go test ./internal/... -coverprofile=coverage-logs.out -covermode=atomic
	go tool cover -html=coverage-logs.out -o coverage-logs.html

coverage-traces: deps-traces
	cd lakehouse-traces && go test ./internal/... -coverprofile=coverage-traces.out -covermode=atomic
	cd lakehouse-traces && go tool cover -html=coverage-traces.out -o coverage-traces.html

docker-logs:
	docker build -f Dockerfile.logs -t ghcr.io/reliablyobserve/lakehouse-logs:$(VERSION) .

docker-traces:
	docker build -f Dockerfile.traces -t ghcr.io/reliablyobserve/lakehouse-traces:$(VERSION) .

docker: docker-logs docker-traces

e2e:
	docker compose -f deployment/docker/docker-compose-e2e.yml up --build --abort-on-container-exit --exit-code-from e2e-test
	docker compose -f deployment/docker/docker-compose-e2e.yml down -v

e2e-test: deps-logs
	go test -tags=e2e -v -count=1 -timeout=10m ./tests/e2e/
