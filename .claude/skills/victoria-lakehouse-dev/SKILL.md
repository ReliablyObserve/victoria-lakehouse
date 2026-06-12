---
name: victoria-lakehouse-dev
description: Architecture, conventions, and workflow for developing victoria-lakehouse (a logs+traces lakehouse over Parquet/S3). Use when working anywhere in this repo — stats/explorer, pmeta, manifest, the Lakehouse UI, compaction, or building/running the e2e stack.
---

# Victoria Lakehouse — developer skill

Project memory distilled into a skill. Read before non-trivial work in this repo, then verify specifics against current code.

## Repo shape
- **Two Go modules.** The **logs** module is at repo root; **`lakehouse-traces/`** is its own module (own `go.mod`) that SHARES `internal/` via `replace github.com/ReliablyObserve/victoria-lakehouse => ../`. So `internal/schema`, `internal/pmeta`, `internal/stats`, `internal/manifest` are edited once and used by both.
- **`internal/storage/parquets3` is MIRRORED** — near-identical copies in the logs module and `lakehouse-traces/internal/storage/parquets3/`. When you change one (e.g. `pmeta_wire.go`, `writer.go`), change BOTH.
- **Build/test:** `GOWORK=off GOFLAGS=-mod=readonly go build|test ./...`. Deps: `make deps-logs` (root) and `make deps-traces deps-vt` (run inside / for the traces module). Replace dirs: `./deps/VictoriaLogs`, `lakehouse-traces/deps/{VictoriaLogs,VictoriaTraces}`.

## Stats architecture — three truth sources, each with different guarantees
- **Manifest** (`internal/manifest`) — cluster-wide file metadata (`FileInfo`: Size, RowCount, RawBytes, **ColumnBytes** per column, `LabelAggregates`, `ColumnStats`). Snapshot-persisted to S3 + periodic refresh; **COMPACTED** (the compactor re-derives `LabelAggregates`/sizes from the merged rows — heals old files instead of propagating empties). Maintains incremental in-mem caches (`tenantAggregates`, plus a generic `SetChangeObserver` add/remove hook) so the API never rescans. **This is the post-compaction truth — use `LiveAggregate()` for headline totals.**
- **pmeta** (`internal/pmeta`) — per-partition catalog/bloom/file-meta facet bundles in S3. Persisted on flush (`persistDirty`), warm-loaded on startup (`WarmCatalogFromS3` → `WarmPartitions`), MERGED cross-instance (`PutWarm`/`absorbFacet`: HLL register-max, value set-union — commutative/idempotent), compaction-fed (`PmetaOnCompacted`). Cardinality = `Store.FieldCardinality` (HLL union of persisted + live). `EstimateBytes()` (per facet) / `ResidentBytes()` (store) = metadata RAM footprint. **CAVEAT:** a facet-encoding change makes old bundles undecodable → `SkippedFacets` self-heal rebuilds from the manifest (which drops high-card HLLs) → a one-time cardinality reset on the first restart after an encoding change (and when swapping the running stack to a different branch).
- **TenantRegistry** (`internal/stats/registry.go`) — per-tenant CRDT stats, gossiped via `SyncPusher`/`SyncHandler` + S3 snapshot at `SnapshotInterval` (`MarshalSnapshot`/`LoadSnapshot`, generic + reusable). **CUMULATIVE — NOT decremented on compaction**, so it over-counts compacted-away files (the "Storage Classes 1,815 vs live 1,425" drift). Prefer the manifest for anything that must be exact.

### Cardinality is only tracked for sketched fields
Fed in `pmeta_wire.go` `tapLogRows`/`tapTraceRows`: only `schema.LogLabelColumns`/`TraceLabelColumns` (dimensional) + `schema.DefaultSketchIDColumns` (`trace_id, span_id, container.id, service.instance.id`, unioned via `effectiveSketchFields` so an operator's `always_sketch_fields` YAML can't drop them). Everything else is structurally 0 → the `/cardinality/fields` API exposes an `indexed` flag so the UI renders "—" (not counted) vs a real 0.

## UI architecture — ONE source of truth
- **`internal/ui/static/lakehouse-ui.js`** is the render core (tabs: Storage Overview, Storage Details, Tenants + drill-down, Cardinality Explorer), exposed as `window.LakehouseUI.mount(container)`. **EDIT THE UI HERE ONLY.**
- `index.html` is a thin shell (defines the VMUI CSS theme vars, mounts the module). `vmui-tab.js` is the VMUI integration only (injects the Lakehouse tab, holds the content area across React re-renders, `ensureUI` → `mount`). Both load the same module so the standalone page and the VMUI tab can never drift. A guard test (`vmui_inject_regression_test.go`) enforces the split.
- Served at `/lakehouse/ui/` (standalone) and injected into VMUI at `/select/vmui/`; all stats responses + the UI are `no-store`.

## Conventions (hard rules)
- **NO Claude/AI attribution anywhere** — commits, PR bodies, code, docs. Author is `szibis` only; no co-author trailers, no AI mentions. Commits are SSH-signed automatically.
- **Docs per phase, internal + public** — at each phase boundary update `docs/architecture/*`, godoc/struct comments, the CHANGELOG `[Unreleased]` section, AND the public docs (`website/`) for anything user-facing. Undocumented = incomplete.
- **CI gates** — `changelog-check` (`scripts/ci/check_changelog_pr.py`) needs a NEW `[Unreleased]` bullet for any feat/fix/perf/release-impacting PR. `fuzz-matrix-drift` needs every `func Fuzz*` listed in `.github/workflows/fuzz-stress-memleak.yaml` (dual-module `internal/storage/parquets3` targets go in BOTH `fuzz-logs` and `fuzz-traces` matrices).
- TDD: code improves first, tests follow.

## gh / git gotcha
The active `GITHUB_TOKEN` env var is a long-lived classic PAT the `ReliablyObserve` org rejects. Run gh as `env -u GITHUB_TOKEN gh ...` so it falls back to the keyring OAuth token (has `read:org`). `git push` uses SSH (`git@github.com`) and is unaffected.

## Running / verifying the e2e stack live
- Compose project **`victoria-lakehouse`** (`deployment/docker/docker-compose-e2e.yml`): logs UI at `http://localhost:29428/lakehouse/ui/` (+ VMUI `:29428/select/vmui/`), traces UI at `:20428`; with minio, `datagen-continuous`, and a toxiproxy `s3-latency` (100ms).
- The UI is `go:embed`'d, so **rebuild + recreate** after any change: `docker compose -p victoria-lakehouse -f deployment/docker/docker-compose-e2e.yml build lakehouse-logs lakehouse-traces` then `... up -d --no-deps --force-recreate lakehouse-logs lakehouse-traces` (keeps minio/datagen + the data).
- **The running stack may be built from a different checkout/branch than yours** — verify (`docker compose -p victoria-lakehouse ps`, check the compose config path + branch) before assuming your code is live.
