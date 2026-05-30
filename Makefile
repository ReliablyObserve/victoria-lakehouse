VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo.Version=$(VERSION)

# -trimpath strips local file system paths from the recorded executable, making
# builds reproducible and slightly smaller (~150 KB per binary).
GOBUILDFLAGS := -trimpath

# Build tags that gate optional, dependency-heavy subsystems out of production
# binaries. Default is empty (slim build). Override with
# `make build BUILD_TAGS=k8s_election` to include the in-cluster K8s leader
# elector and its ~8 MB of k8s.io/client-go transitive dependencies.
BUILD_TAGS ?=

# Both Go modules use different VL commits with incompatible interfaces.
# go.work exists for IDE support only; CLI builds must disable it.
export GOWORK=off

# VictoriaLogs — Go module proxy has stale cache with wrong module path.
# We clone the correct version locally and use a replace directive in go.mod.
VL_VERSION_LOGS := v1.50.0
VL_COMMIT_TRACES := a408207c2242
VL_REPO := https://github.com/VictoriaMetrics/VictoriaLogs.git
VL_DIR_LOGS := deps/VictoriaLogs
VL_DIR_TRACES := lakehouse-traces/deps/VictoriaLogs

VT_VERSION := v0.9.0
VT_REPO := https://github.com/VictoriaMetrics/VictoriaTraces.git
VT_DIR := lakehouse-traces/deps/VictoriaTraces

.PHONY: build build-logs build-traces bench test test-logs test-traces test-full test-full-logs test-full-traces lint vet clean e2e deps-logs deps-traces deps-vt

deps-logs: $(VL_DIR_LOGS)/go.mod

$(VL_DIR_LOGS)/go.mod:
	@mkdir -p deps
	git clone --depth 1 --branch $(VL_VERSION_LOGS) $(VL_REPO) $(VL_DIR_LOGS)
	cp patches/vl-logs/external.go.src $(VL_DIR_LOGS)/app/vlstorage/external.go
	cp patches/vl-logs/external_query.go.src $(VL_DIR_LOGS)/lib/logstorage/external_query.go
	cd $(VL_DIR_LOGS) && git apply ../../patches/vl-logs/vlstorage-dispatch.patch

deps-traces: $(VL_DIR_TRACES)/go.mod

$(VL_DIR_TRACES)/go.mod:
	@mkdir -p lakehouse-traces/deps
	git clone $(VL_REPO) $(VL_DIR_TRACES)
	cd $(VL_DIR_TRACES) && git checkout $(VL_COMMIT_TRACES)
	cp patches/vl-traces/external.go.src $(VL_DIR_TRACES)/app/vlstorage/external.go
	cp patches/vl-traces/external_query.go.src $(VL_DIR_TRACES)/lib/logstorage/external_query.go
	cd $(VL_DIR_TRACES) && git apply ../../../patches/vl-traces/vlstorage-dispatch.patch

deps-vt: $(VT_DIR)/go.mod

$(VT_DIR)/go.mod:
	@mkdir -p lakehouse-traces/deps
	git clone --depth 1 --branch $(VT_VERSION) $(VT_REPO) $(VT_DIR)
	cp patches/vt-traces/external.go.src $(VT_DIR)/app/vtstorage/external.go
	cp patches/vt-traces/flag_dedup.go.src $(VT_DIR)/app/vtstorage/flag_dedup.go
	cd $(VT_DIR) && git apply ../../../patches/vt-traces/vtstorage-dispatch.patch
	cd $(VT_DIR) && git apply ../../../patches/vt-traces/vtstorage-flag-dedup.patch
	cd $(VT_DIR) && git apply ../../../patches/vt-traces/go-mod-replace.patch

build: build-logs build-traces
	go build $(GOBUILDFLAGS) -ldflags "-s -w" -o bin/healthcheck ./cmd/healthcheck

bench:
	go build -o bin/lakehouse-bench ./cmd/bench/

build-logs: deps-logs
	go build $(GOBUILDFLAGS) -tags "$(BUILD_TAGS)" -ldflags "$(LDFLAGS)" -o bin/lakehouse-logs ./cmd/lakehouse-logs

build-traces: deps-traces deps-vt
	cd lakehouse-traces && go build $(GOBUILDFLAGS) -tags "$(BUILD_TAGS)" -ldflags "$(LDFLAGS)" -o ../bin/lakehouse-traces .

test: test-logs test-traces

test-logs: deps-logs
	go test ./internal/... -short -race -count=1 -timeout=5m

test-traces: deps-traces deps-vt
	cd lakehouse-traces && go test ./internal/... -short -race -count=1 -timeout=5m

test-full-logs: deps-logs
	go test ./internal/... -race -count=1 -timeout=10m

test-full-traces: deps-traces deps-vt
	cd lakehouse-traces && go test ./internal/... -race -count=1 -timeout=10m

test-full: test-full-logs test-full-traces

test-integration-logs: deps-logs
	go test -tags=integration ./internal/... -race -count=1 -timeout=15m

test-integration-traces: deps-traces deps-vt
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
	docker compose -f deployment/docker/docker-compose-e2e.yml up -d --build
	@echo "Waiting for services..."
	@for i in $$(seq 1 90); do curl -sf http://localhost:29428/health > /dev/null 2>&1 && break; sleep 2; done
	@for i in $$(seq 1 90); do curl -sf http://localhost:20428/health > /dev/null 2>&1 && break; sleep 2; done
	LOGS_BASE_URL=http://localhost:29428 \
	TRACES_BASE_URL=http://localhost:20428 \
	LOKI_PROXY_URL=http://localhost:23100 \
	VLSELECT_URL=http://localhost:29471 \
	MINIO_URL=http://localhost:29000 \
	go test -tags=e2e -v -count=1 -timeout=10m ./tests/e2e/; \
	rc=$$?; docker compose -f deployment/docker/docker-compose-e2e.yml down -v; exit $$rc

e2e-test: deps-logs
	LOGS_BASE_URL=http://localhost:29428 \
	TRACES_BASE_URL=http://localhost:20428 \
	LOKI_PROXY_URL=http://localhost:23100 \
	VLSELECT_URL=http://localhost:29471 \
	MINIO_URL=http://localhost:29000 \
	go test -tags=e2e -v -count=1 -timeout=10m ./tests/e2e/
