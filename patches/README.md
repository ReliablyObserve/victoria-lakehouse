# patches/

Canonical extension points for VictoriaLogs (VL) and VictoriaTraces (VT)
upstream. These patches and the symbols they touch are **not movable**:
they are the documented surface through which Lakehouse extends VL/VT
without forking. If you find yourself rewriting a function locally
that already exists upstream, stop — add (or extend) a patch here
instead.

## Policy — "extend, don't duplicate"

The Lakehouse cold tier reuses VL/VT code as a library wherever VL/VT
already expose the behavior we need. Where they don't expose it but
already implement it (private functions, internal tables), we add a
**minimal exported wrapper** via a patch in this directory. Three
rules:

1. **Never re-implement a VL/VT behavior locally** if a one-line
   `patches/vl-*/EXPORT-*.patch` would expose the existing
   implementation. Even if the upstream function is two lines —
   the value isn't lines saved, it's having a single source of
   truth that tracks future upstream changes for free.

2. **Never delete a patch in this directory** without also removing
   every Lakehouse import that depends on it. A grep over
   `internal/`, `cmd/`, and `lakehouse-traces/` for the patched
   symbol's name is the safety check before deletion.

3. **Never edit upstream `deps/` files directly** outside a patch.
   The `deps/` trees are clean-cloned by `make deps-logs` /
   `make deps-traces` / `make deps-vt`; any uncommitted edits
   there get blown away on the next clone. Edits land via a new
   patch file or by extending an existing one.

## Active patches

### `vl-logs/` and `vl-traces/`

Applied to `deps/VictoriaLogs/...` and
`lakehouse-traces/deps/VictoriaLogs/...` respectively. Both trees
get the same patch set; if you add a patch to one, mirror it to the
other.

The two trees are **two different VictoriaLogs checkouts**: the logs
binary embeds `VL_VERSION_LOGS` (v1.53.0), the traces binary embeds
`VL_COMMIT_TRACES` (c945d2949e98 = v1.52.0 — the commit VictoriaTraces
v0.12.0 pins in its own `go.mod`). The pins may legitimately differ, so the
two patch files for the same upstream file may carry different *context*
even though the lines they add are identical.

`scripts/ci/check_patches_equal.sh` (CI job `lint-logs`) enforces that:
every file must exist in both directories and be byte-equal, unless it is
listed in **`patches/vl-traces/DIVERGENCE.md`** with the upstream change
that moved the context. The check also fails on a *stale* row — a listed
file that is in fact byte-equal — so the list empties itself once both
pins reach the same VictoriaLogs release. A patch that exists on only one
side is an error, with one exception: a traces-only patch whose row in `DIVERGENCE.md`
carries the marker `upstream-fixed-in-logs-pin` (the logs pin already includes the
upstream fix, the traces pin does not yet). A logs-only patch is never excused.

| Patch | Upstream file | Symbol exported / behavior |
| --- | --- | --- |
| `external.go.src` | `app/vlstorage/external.go` | Added file (upstream has none) that wires VL's vlstorage to LH's storage backend: the `ExternalStorage` interface the dispatch patch routes to. Copied, not diffed — so it keeps compiling when upstream changes the storage functions it mirrors; see `docs/upstream-sync.md`. |
| `external_query.go.src` | `lib/logstorage/external_query.go` | Added file (upstream has none) exposing `ExternalQuery` hooks LH calls from its own query path, mirroring the native path in `lib/logstorage/storage_search.go`, plus `GetQueryTimeBucketing` / `TruncateTimestampToBucket`, which classify whether a query can be answered without reading per-row values and with which `_time` bucketing. Copied, not diffed. |
| `vlstorage-dispatch.patch` | `app/vlstorage/main.go` | Routes VL's `RunQuery` / `GetFieldNames` / `GetFieldValues` / `GetStreamFieldNames` / `GetStreamFieldValues` / `GetStreams` / `GetStreamIDs` / `DeleteRunTask` / `DeleteStopTask` / `DeleteActiveTasks` / `GetTenantIDs` to `externalStorage` when LH has registered itself. |
| `vl-export-severity.patch` | `app/vlinsert/opentelemetry/pb.go` | Adds `FormatSeverity(int32) string` as the public wrapper around the package-local `formatSeverity`. Consumed by `internal/schema/severity.go::DeriveSeverityText` so cold rows derive `level` from `severity_number` the same way VL hot does. |
| `vl-const-timestamps-parse.patch` | `lib/logstorage/block_result.go` | Makes `tryParseTimestamps` parse a CONSTANT `_time` column once instead of once per row, using VL's own `areConstValues`. Pure optimization, no semantic change. The cold tier answers count-class queries from manifest metadata with a constant `_time` column (see `internal/storage/parquets3/manifest_fastpath.go`); without this, a metadata-only answer still cost O(rows) RFC3339 parsing. |
| `vl-partition-close-order.patch` | `lib/logstorage/partition.go` | Closes a partition's datadb before its indexdb in `mustClosePartition`. Upstream closed the indexdb first and set `pt.idb = nil` while datadb's in-memory parts mergers, which read `ddb.pt.idb`, could still be running; `-race` reported it whenever a storage was closed during a merge (the Lakehouse insert buffer closes upstream storages at seal, reap and shutdown, #351). Order only, no other change; to be dropped once upstream closes in this order. **Traces tree only since VictoriaLogs v1.53.0, which closes in this order itself (the logs tree no longer carries it).** |
| `vl-export-streamtags-get.patch` | `lib/logstorage/stream_tags.go` | Adds `(*StreamTags).Get(name)` and `(*StreamTags).UnmarshalString(s)`. The cold insert path uses `Get` to lift the stream-label `level` onto `row.SeverityText` without re-parsing the canonical string; the compactor uses `UnmarshalString` to re-parse the human-readable Stream column when backfilling SeverityText on historical files. |

### `vt-traces/`

Applied to `lakehouse-traces/deps/VictoriaTraces/...`.

| Patch | Upstream file | Symbol exported / behavior |
| --- | --- | --- |
| `external.go.src` | `app/vtstorage/external.go` | Added file (upstream has none) that wires VT's vtstorage to LH's trace storage backend. Copied, not diffed. |
| `flag_dedup.go.src` | `app/vtstorage/flag_dedup.go` | The dedup-flag guard so VT's flags don't collide with the identical flags VL's vlstorage registers in the same binary. Each `safe*` helper returns VL's registered value (the `flagutil` types directly; the standard library's scalar flags through their underlying pointer), so a command-line setting, or a password read from a file, is the one both packages see; a detached default is only the fallback for an unexpected value type. It imports `app/vlstorage`, so VL initializes first by dependency order (not by import-path sort order, which Go 1.21+ only uses among packages that are otherwise ready). |
| `vtstorage-dispatch.patch` | `app/vtstorage/main.go` | Routes VT's query handlers to `externalStorage` when LH has registered itself. |
| `vtstorage-flag-dedup.patch` | `app/vtstorage/main.go` | Wires `flag_dedup.go.src` into VT's flag parsing path so duplicate `flag.Lookup` calls don't panic. (15 flag sites in one file — helper file is cheaper than inline closures.) |
| `vtinsert-flag-dedup.patch` | `app/vtinsert/insertutil/flags.go` | `-insert.maxFieldsPerLine` and `-defaultMsgValue` are VL's flags in the lakehouse-traces binary: VT's `MaxFieldsPerLine` and `DefaultMsgValue` alias VL's pointers (`vlinsert/insertutil`), which also makes VL initialize first. An earlier form copied VL's value when the package initialised, before `flag.Parse`, so an operator's value never reached span ingest (#259). At v0.12.0 upstream moved `-defaultMsgValue` out of `common_params.go` into `flags.go`. |

#### Fragile by design: `sharedScalar` in `flag_dedup.go.src`

The `safeInt`, `safeBool`, `safeString` and `safeDuration` helpers share VL's
value through `sharedScalar`, which reads the pointer that the Go standard
library's `flag` package keeps for a flag VL registered. The standard library
does not export those types (`flag.intValue`, `flag.boolValue`,
`flag.stringValue`, `flag.durationValue`), so the helper depends on their
names and on their layout (`type intValue int`: the named type's underlying
type is the scalar itself) through `reflect` and `unsafe`. It is the only place
in the tree that does this, and it is an assumption about the Go release, not
about VL or VT.

What guards it:

- a type-name check (`reflect.TypeOf(f.Value).String()` must equal the expected
  `*flag.xxxValue`) and a size check (the pointee must be exactly the scalar's
  size); when either fails `sharedScalar` returns nil and the helper falls back
  to a detached default, so a Go upgrade that renames or reshapes a type never
  corrupts memory, it only stops sharing;
- `TestSharedScalarTypeNamesMatchStdlib` (`tests/conformance/sharedscalar_test.go`)
  reads the four type names written in `flag_dedup.go.src` and compares them and
  the sizes with what the toolchain really registers, so a Go release that
  renames or reshapes a type fails the build instead of turning sharing off
  silently;
- `TestRealVTStorage_AuthKeyIsTheSharedFlag` (`lakehouse-traces/discovery_vtstorage_test.go`)
  sets VL's flag and reads it back through VT's real `vtstorage`. That flag is a
  `flagutil.Password`, which `safePassword` shares directly, so this test covers
  the shared-value mechanism as a whole, not `sharedScalar` itself.

If a Go upgrade breaks it, the first test above fails. Fix it by updating the type
names passed to `sharedScalar` in `flag_dedup.go.src` to the new ones (print
`reflect.TypeOf(flag.Lookup("<name>").Value)` for each kind), keep the size
check, and only if the standard library stops exposing a plain pointer to the
scalar replace the helper with a `flag.Value` wrapper (`flag.Var`) registered by
one package and looked up by the other. Do not delete the check to make the
test pass.

Not every overlay is a patch file. VT's own `go.mod` needs a
`replace github.com/VictoriaMetrics/VictoriaLogs => ../VictoriaLogs`
so VT's vlstorage path sees the same `external.go` replacement we apply
on the logs side. That used to be `go-mod-replace.patch`; it is now a
`go mod edit -replace` line in the Makefile's `deps-vt` target. A
one-line `go.mod` diff carries three lines of context that change on
every upstream dependency bump, and a context conflict there is
indistinguishable from a real breakage — `go mod edit` states the intent
and cannot rot. The effect is identical (verified by `git diff` on the
cloned tree).

## Imported VL/VT symbols — natural reuse

These symbols are **imported, not re-implemented**. Future contributors
should NOT replace them with local copies; the imports document our
single-source-of-truth dependency on VL/VT.

| Lakehouse caller | Upstream symbol | Rationale |
| --- | --- | --- |
| `internal/vlstorage/insert.go::severityTextFromNumber` | `github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/opentelemetry.FormatSeverity` | Exported via `vl-export-severity.patch`. Mirrors VL hot's `level` derivation from `severity_number` so cold rows query identically. |
| `internal/vlstorage/insert.go` (`logstorage.GetLogRows`, `MustAdd`, `ForEachRow`, `StreamTags`, `UnmarshalCanonicalInplace`) | `github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage` | The canonical VL row representation; LH writes the same `LogRow` shape so VL hot tooling reads cold parquets unchanged. |
| `internal/vlstorage/insert.go::insertutil` | `github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil` | VL's shared insert helpers (timestamp normalization, stream-tag canonicalization). |
| `internal/selectapi/handler.go` (`logsql.ProcessQueryRequest` and siblings) | `github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/logsql` | VL's own HTTP handlers wired to LH's externalStorage dispatch (see `vlstorage-dispatch.patch`). Same query semantics across tiers. |
| `lakehouse-traces/internal/selectapi/handler.go` (`logsql.Process*Request`) | `github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/logsql` | VT's own LogsQL handlers, dispatched through `vtstorage-dispatch.patch` to the Lakehouse adapter. Since VT v0.12.0 this is where the LogsQL latency offset comes from; nothing in Lakehouse builds that filter.
| `lakehouse-traces/internal/selectapi/handler.go` (`tempo.RequestHandler`, `jaeger.RequestHandler`) | `github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/{tempo,jaeger}` | VT's own Tempo + Jaeger HTTP handlers, dispatched via `vtstorage-dispatch.patch`. |

If you add another import from `deps/` to LH code, append a row here.

## Patch-style choice — inline closure vs helper file

When a patch needs to dedupe a flag (or wrap any symbol) at multiple
sites, two shapes are available:

1. **Alias or inline closure**, one per site. When the upstream flag is exported
   by the other package, alias its pointer (the `vtinsert-flag-dedup.patch`
   shape: `var MaxFieldsPerLine = vlinsertutil.MaxFieldsPerLine`). Do not copy
   the value at init: package initialization runs before `flag.Parse`, so a copy
   keeps the default forever (issue #259).

2. **Helper file** (e.g. `flag_dedup.go.src`) plus a small patch that
   swaps `flag.Int(...)` → `safeInt(...)`.

Use **an alias** when there are 1–3 sites (the
`vtinsert-flag-dedup.patch` shape — two flags, no extra file).

Use the **helper-file shape** when there are 5+ sites in one file
(the `vtstorage-flag-dedup.patch` shape — 15 flags in
`vtstorage/main.go`). The 80-line helper file is recouped many times
over by the much-smaller call-site diffs.

Neither shape is "wrong" — pick by call-site count. Future patches
that add a single dedup site should always use inline.

## Patch hardening guarantees

The `internal/upstreamreuse` package backs three guarantees against
this directory:

1. **`TestRequiredPatchesExist`** fails when any expected patch file
   is missing or empty. Deleting a patch forces an update to the
   policy doc + test in the same PR.
2. **`TestVLLogsPatchesMirrorTraces`** fails when `vl-logs/` and
   `vl-traces/` drift in file membership. Both VL clones in the
   repo share the same patch set; an asymmetric edit gets caught
   before the build breaks. `scripts/ci/check_patches_equal.sh`
   goes one step further and compares the files byte for byte,
   with declared exceptions in `patches/vl-traces/DIVERGENCE.md`
   (see above); it is self-tested by
   `scripts/ci/tests/check_patches_equal_test.sh`.
3. **`TestForbiddenLocalCopiesOfUpstreamSymbols`** fails when grep
   matches a known re-implementation pattern (e.g. a local
   `logSeverities` table or a hand-rolled `extractStreamTagLevel`
   function). Reverts that drop an export-patch dependency in
   favor of a local copy fail the test loudly.

These tests are pure repo-tree checks (no upstream build needed),
so they run on every commit in CI without needing the
`make deps-*` step.

## Consolidation analysis (2026-06)

Audited the patch set for redundancy and missing upstream reuse.
Findings recorded here for future maintainers:

- **vl-logs ↔ vl-traces mirror** — near-identical files in two
  directories. Could be a single source + Makefile copy, but that
  complicates Docker COPY semantics, and the two pins can drift apart
  again (they were both v1.52.0 from VictoriaTraces v0.12.0 until the logs pin moved
  to v1.53.0; the differences are declared in `patches/vl-traces/DIVERGENCE.md`). Current
  shape preferred, with the equality guard enforcing that every *other*
  file stays identical.
- **`vlstorage-dispatch` and `external.go.src`** — split because
  `external.go.src` is a *replacement* (`cp`) and the dispatch is
  a *diff* (`git apply`). Cannot be combined cleanly.
- **`vtstorage-flag-dedup` vs `vtinsert-flag-dedup`** — different
  patch shapes (helper file vs alias) by design. See the
  "Patch-style choice" section above.
- **`computeStreamID` in `internal/vlstorage/stream_id.go`** —
  reproduces VL's private `hash128 → streamID.marshalString`
  pipeline. Could be exported via patch, but VL's internal
  representation is volatile; the current isolated mirror has
  lower upstream-coupling risk.
- **`writeJSON` duplicated in `internal/delete/handler.go` and
  `internal/stats/api.go`** — both LH-local, slightly different
  signatures. Local consolidation opportunity; orthogonal to
  upstream reuse.

If a future audit changes a row here, update the date stamp above.

## Workflow

```
# Add or modify a patch
$ vim patches/vl-logs/your-patch.patch
$ vim patches/vl-traces/your-patch.patch       # mirror to traces module
$ vim Makefile                                  # add `git apply` rules
$ rm -rf deps/VictoriaLogs lakehouse-traces/deps/VictoriaLogs
$ make deps-logs deps-traces                    # re-clones + reapplies
$ go build ./...                                # smoke test
$ go build ./lakehouse-traces/...
```

If `git apply` fails after an upstream bump (`VL_VERSION_LOGS`,
`VL_COMMIT_TRACES`, `VT_VERSION` in Makefile), regenerate the patch
against the new upstream rather than locally reimplementing the
behavior. The whole bump, of which this is one step, is described in
`docs/upstream-sync.md`; the daily upstream sync probe
(`scripts/ci/upstream_sync_probe.sh`) reports which patches no longer
apply to the newest releases, with the context each one searched for,
before anyone starts.

```
$ rm -rf deps/VictoriaLogs
$ make deps-logs                                # stops at the failing git apply
$ cd deps/VictoriaLogs
$ patch -p1 -F3 < ../../patches/vl-logs/your-patch.patch   # hand-apply with fuzz
$ git diff path/to/file > ../../patches/vl-logs/your-patch.patch
$ cd ../.. && rm -rf deps/VictoriaLogs && make deps-logs   # must apply cleanly now
```

Then mirror to `patches/vl-traces/` against
`lakehouse-traces/deps/VictoriaLogs` the same way, and run

```
$ scripts/ci/check_patches_equal.sh
```

If the two files legitimately differ (the pins straddle an upstream
change to a context line), add the row to
`patches/vl-traces/DIVERGENCE.md` naming that upstream change. If they
do not differ, the guard fails on a stale row — remove it.
