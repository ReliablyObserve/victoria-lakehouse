VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo.Version=$(VERSION)

# -trimpath strips local file system paths from the recorded executable, making
# builds reproducible and slightly smaller (~150 KB per binary).
GOBUILDFLAGS := -trimpath

# FIPS=1 enables Go 1.26+ native FIPS 140-3 mode (GOFIPS140=v1.0.0 selects the
# certified crypto module). Default is FIPS off; tag images explicitly with
# `-fips` when FIPS=1.
FIPS ?= 0
ifeq ($(FIPS),1)
export GOFIPS140 := v1.0.0
endif

# Both Go modules use different VL commits with incompatible interfaces.
# go.work exists for IDE support only; CLI builds must disable it.
export GOWORK=off

# VictoriaLogs — Go module proxy has stale cache with wrong module path.
# We clone the correct version locally and use a replace directive in go.mod.
#
# Two VictoriaLogs pins, on purpose — do not collapse them:
#
#   VL_VERSION_LOGS  — the VictoriaLogs release the logs binary embeds. Free to
#                      track the newest VL release.
#   VL_COMMIT_TRACES — DERIVED, never chosen: it is always the VictoriaLogs
#                      commit that VictoriaTraces' own go.mod requires at
#                      VT_VERSION. Read it with
#                      `git show $(VT_VERSION):go.mod | grep VictoriaLogs`
#                      (VT v0.11.0 → v1.121.1-0.20260617051904-6ae2da3c11f3,
#                      i.e. VL v1.51.0) and copy the commit part here.
#                      It legitimately lags VL_VERSION_LOGS: the traces binary
#                      links VT against the exact VL VT was built and tested
#                      with. Lifting it to VL_VERSION_LOGS "because it
#                      compiles" is not allowed.
VL_VERSION_LOGS := v1.52.0
VL_COMMIT_TRACES := 6ae2da3c11f3
VL_REPO := https://github.com/VictoriaMetrics/VictoriaLogs.git
VL_DIR_LOGS := deps/VictoriaLogs
VL_DIR_TRACES := lakehouse-traces/deps/VictoriaLogs

VT_VERSION := v0.11.0
VT_REPO := https://github.com/VictoriaMetrics/VictoriaTraces.git
VT_DIR := lakehouse-traces/deps/VictoriaTraces

.PHONY: build build-logs build-traces bench test test-logs test-traces test-full test-full-logs test-full-traces lint vet clean e2e deps-logs deps-traces deps-vt sync-vmui sync-vmui-traces conformance-gen conformance-check config-surface config-docs config-drift

deps-logs: $(VL_DIR_LOGS)/go.mod

$(VL_DIR_LOGS)/go.mod:
	@mkdir -p deps
	git clone --depth 1 --branch $(VL_VERSION_LOGS) $(VL_REPO) $(VL_DIR_LOGS)
	cp patches/vl-logs/external.go.src $(VL_DIR_LOGS)/app/vlstorage/external.go
	cp patches/vl-logs/external_query.go.src $(VL_DIR_LOGS)/lib/logstorage/external_query.go
	cd $(VL_DIR_LOGS) && git apply ../../patches/vl-logs/vlstorage-dispatch.patch
	cd $(VL_DIR_LOGS) && git apply ../../patches/vl-logs/vl-export-severity.patch
	cd $(VL_DIR_LOGS) && git apply ../../patches/vl-logs/vl-export-streamtags-get.patch
	cd $(VL_DIR_LOGS) && git apply ../../patches/vl-logs/vl-const-timestamps-parse.patch

deps-traces: $(VL_DIR_TRACES)/go.mod

$(VL_DIR_TRACES)/go.mod:
	@mkdir -p lakehouse-traces/deps
	git clone $(VL_REPO) $(VL_DIR_TRACES)
	cd $(VL_DIR_TRACES) && git checkout $(VL_COMMIT_TRACES)
	cp patches/vl-traces/external.go.src $(VL_DIR_TRACES)/app/vlstorage/external.go
	cp patches/vl-traces/external_query.go.src $(VL_DIR_TRACES)/lib/logstorage/external_query.go
	cd $(VL_DIR_TRACES) && git apply ../../../patches/vl-traces/vlstorage-dispatch.patch
	cd $(VL_DIR_TRACES) && git apply ../../../patches/vl-traces/vl-export-severity.patch
	cd $(VL_DIR_TRACES) && git apply ../../../patches/vl-traces/vl-export-streamtags-get.patch
	cd $(VL_DIR_TRACES) && git apply ../../../patches/vl-traces/vl-const-timestamps-parse.patch

deps-vt: $(VT_DIR)/go.mod

$(VT_DIR)/go.mod:
	@mkdir -p lakehouse-traces/deps
	git clone --depth 1 --branch $(VT_VERSION) $(VT_REPO) $(VT_DIR)
	cp patches/vt-traces/external.go.src $(VT_DIR)/app/vtstorage/external.go
	cp patches/vt-traces/flag_dedup.go.src $(VT_DIR)/app/vtstorage/flag_dedup.go
	cd $(VT_DIR) && git apply ../../../patches/vt-traces/vtstorage-dispatch.patch
	cd $(VT_DIR) && git apply ../../../patches/vt-traces/vtstorage-flag-dedup.patch
	cd $(VT_DIR) && git apply ../../../patches/vt-traces/vtinsert-flag-dedup.patch
	# Point VT's own VictoriaLogs dependency at the sibling checkout the
	# deps-traces target prepares, so VT's vlstorage path sees the same
	# external.go replacement we apply on the logs side. `go mod edit` instead
	# of a patch: a one-line go.mod diff carries three lines of context that
	# change on every upstream dependency bump, and a context conflict here is
	# indistinguishable from a real breakage.
	cd $(VT_DIR) && go mod edit -replace github.com/VictoriaMetrics/VictoriaLogs=../VictoriaLogs

# vmui is VictoriaLogs' own web UI. Lakehouse serves it at /select/vmui/ from
# internal/ui/vmui/ via `go:embed` (internal/ui/vmui.go) and injects the
# Lakehouse tab into its index.html on the way out (internal/ui/vmui_inject.go)
# — the assets themselves are never modified, they are VL's build output.
#
# Only index.html is tracked in git; the rest of the bundle (assets/,
# favicon.svg, manifest.json, config.json, preview.jpg, robots.txt) is
# .gitignore'd and copied from the vendored VL tree at build time, so the repo
# never carries a second copy of VL's minified bundle. index.html IS tracked
# because it names the content-hashed asset filenames, which makes it the
# drift marker TestVMUIIndexMatchesVendoredVL compares against the vendored
# tree: a VL bump that rebuilds vmui changes those hashes and fails the test
# until `make sync-vmui` is re-run and index.html re-committed.
#
# The Docker builds do the same copy inline (Dockerfile.logs:30,
# Dockerfile.traces:50). These targets make a local build reproduce it.
# The vmui directory is wiped first so assets from a previous VL version
# cannot survive a downgrade or a partial copy.
sync-vmui: deps-logs
	@rm -rf internal/ui/vmui
	@mkdir -p internal/ui/vmui
	cp -R $(VL_DIR_LOGS)/app/vlselect/vmui/. internal/ui/vmui/
	@echo "vmui: internal/ui/vmui <- $(VL_DIR_LOGS)/app/vlselect/vmui (VictoriaLogs $(VL_VERSION_LOGS))"

# sync-vmui-traces is the traces-binary counterpart: lakehouse-traces embeds the
# same internal/ui package, but Dockerfile.traces copies vmui from the traces
# module's own VL checkout (VL_COMMIT_TRACES), not the logs one. Run this before
# a local `make build-traces` if the two pins have diverged and you care which
# vmui build the traces binary serves.
sync-vmui-traces: deps-traces
	@rm -rf internal/ui/vmui
	@mkdir -p internal/ui/vmui
	cp -R $(VL_DIR_TRACES)/app/vlselect/vmui/. internal/ui/vmui/
	@echo "vmui: internal/ui/vmui <- $(VL_DIR_TRACES)/app/vlselect/vmui (VictoriaLogs $(VL_COMMIT_TRACES))"

build: build-logs build-traces

bench:
	go build -o bin/lakehouse-bench ./cmd/bench/

build-logs: deps-logs sync-vmui
	go build $(GOBUILDFLAGS) -ldflags "$(LDFLAGS)" -o bin/lakehouse-logs ./cmd/lakehouse-logs

build-traces: deps-traces deps-vt sync-vmui-traces
	cd lakehouse-traces && go build $(GOBUILDFLAGS) -ldflags "$(LDFLAGS)" -o ../bin/lakehouse-traces .

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

conformance-gen: deps-logs deps-traces deps-vt
	go run ./tests/conformance/cmd/confgen -write

conformance-check: deps-logs deps-traces deps-vt
	go run ./tests/conformance/cmd/confgen -check
	CONFORMANCE_REQUIRE_DEPS=1 go test ./tests/conformance/... -count=1 -timeout=5m

# Configuration surface. The code defaults are the source of truth: both
# binaries' `print-default-config` output and the config field comments are
# pinned in golden files, and docs/configuration.md, docs/getting-started.md,
# README.md and the Helm chart are generated from or checked against them
# (docs/configuration.md#configuration-drift-gate).
CONFIG_SURFACE_TESTS := TestConfigSurface|TestPrintDefaultConfig

config-surface: deps-logs deps-traces deps-vt
	CONFIG_SURFACE_UPDATE=1 go test ./cmd/lakehouse-logs -run 'TestConfigSurfaceGolden' -count=1
	cd lakehouse-traces && CONFIG_SURFACE_UPDATE=1 go test . -run 'TestConfigSurfaceGolden' -count=1
	CONFIG_SURFACE_UPDATE=1 go test ./internal/config -run 'TestFieldDocsGolden' -count=1

config-docs: config-surface
	python3 scripts/ci/config_drift_report.py --write-docs

config-drift: deps-logs deps-traces deps-vt
	go test ./cmd/lakehouse-logs -run '$(CONFIG_SURFACE_TESTS)' -count=1
	cd lakehouse-traces && go test . -run '$(CONFIG_SURFACE_TESTS)' -count=1
	go test ./internal/config -run 'TestFieldDocsGolden|TestDocumentedConfigExamplesLoad' -count=1
	go run ./scripts/ci/helmdrift
	python3 scripts/ci/config_drift_report.py --check

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

# The Dockerfiles carry the same pins as ARG defaults (kept equal to these by
# TestDockerfilePinsMatchMakefile), but pass them explicitly so a local build is
# never one forgotten default away from cloning the wrong upstream tree and
# failing with a misleading "patch failed" hunk error.
docker-logs:
	docker build -f Dockerfile.logs 		--build-arg VL_VERSION=$(VL_VERSION_LOGS) 		-t ghcr.io/reliablyobserve/lakehouse-logs:$(VERSION) .

docker-traces:
	docker build -f Dockerfile.traces 		--build-arg VL_VERSION=$(VL_VERSION_LOGS) 		--build-arg VL_COMMIT=$(VL_COMMIT_TRACES) 		--build-arg VT_VERSION=$(VT_VERSION) 		-t ghcr.io/reliablyobserve/lakehouse-traces:$(VERSION) .

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
