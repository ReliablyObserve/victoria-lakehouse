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
binary embeds `VL_VERSION_LOGS` (v1.52.0), the traces binary embeds
`VL_COMMIT_TRACES` (6ae2da3c11f3 = v1.51.0 — the commit VictoriaTraces
v0.11.0 pins in its own `go.mod`). The pins legitimately differ, so the
two patch files for the same upstream file may carry different *context*
even though the lines they add are identical.

`scripts/ci/check_patches_equal.sh` (CI job `lint-logs`) enforces that:
every file must exist in both directories and be byte-equal, unless it is
listed in **`patches/vl-traces/DIVERGENCE.md`** with the upstream change
that moved the context. The check also fails on a *stale* row — a listed
file that is in fact byte-equal — so the list empties itself once both
pins reach the same VictoriaLogs release. A patch that exists on only one
side is always an error; `DIVERGENCE.md` never excuses that.

| Patch | Upstream file | Symbol exported / behavior |
| --- | --- | --- |
| `external.go.src` | `app/vlstorage/external.go` | Added file (upstream has none) that wires VL's vlstorage to LH's storage backend: the `ExternalStorage` interface the dispatch patch routes to. Copied, not diffed — so it keeps compiling when upstream changes the storage functions it mirrors; see `docs/upstream-sync.md`. |
| `external_query.go.src` | `lib/logstorage/external_query.go` | Added file (upstream has none) exposing `ExternalQuery` hooks LH calls from its own query path, mirroring the native path in `lib/logstorage/storage_search.go`. Copied, not diffed. |
| `vlstorage-dispatch.patch` | `app/vlstorage/main.go` | Routes VL's `RunQuery` / `GetFieldNames` / `GetFieldValues` / `GetStreamFieldNames` / `GetStreamFieldValues` / `GetStreams` / `GetStreamIDs` / `DeleteRunTask` / `DeleteStopTask` / `DeleteActiveTasks` / `GetTenantIDs` to `externalStorage` when LH has registered itself. |
| `vl-export-severity.patch` | `app/vlinsert/opentelemetry/pb.go` | Adds `FormatSeverity(int32) string` as the public wrapper around the package-local `formatSeverity`. Consumed by `internal/schema/severity.go::DeriveSeverityText` so cold rows derive `level` from `severity_number` the same way VL hot does. |
| `vl-export-streamtags-get.patch` | `lib/logstorage/stream_tags.go` | Adds `(*StreamTags).Get(name)` and `(*StreamTags).UnmarshalString(s)`. The cold insert path uses `Get` to lift the stream-label `level` onto `row.SeverityText` without re-parsing the canonical string; the compactor uses `UnmarshalString` to re-parse the human-readable Stream column when backfilling SeverityText on historical files. |

### `vt-traces/`

Applied to `lakehouse-traces/deps/VictoriaTraces/...`.

| Patch | Upstream file | Symbol exported / behavior |
| --- | --- | --- |
| `external.go.src` | `app/vtstorage/external.go` | Added file (upstream has none) that wires VT's vtstorage to LH's trace storage backend. Copied, not diffed. |
| `flag_dedup.go.src` | `app/vtstorage/flag_dedup.go` | Adds the dedup-flag guard so VT's flags don't collide with LH's identical flags in the same binary. |
| `vtstorage-dispatch.patch` | `app/vtstorage/main.go` | Routes VT's query handlers to `externalStorage` when LH has registered itself. |
| `vtstorage-flag-dedup.patch` | `app/vtstorage/main.go` | Wires `flag_dedup.go.src` into VT's flag parsing path so duplicate `flag.Lookup` calls don't panic. (15 flag sites in one file — helper file is cheaper than inline closures.) |
| `vtinsert-flag-dedup.patch` | `app/vtinsert/insertutil/{common_params,flags}.go` | Dedupes the VT vtinsert flags (`-defaultMsgValue`, `-insert.maxFieldsPerLine`) that collide with VL's same-named flags when both packages link into the lakehouse-traces binary. Uses inline `flag.Lookup` closures (2 sites, no helper file). |

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
| `lakehouse-traces/internal/selectapi/handler.go` (`tempo.RequestHandler`, `jaeger.RequestHandler`) | `github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/{tempo,jaeger}` | VT's own Tempo + Jaeger HTTP handlers, dispatched via `vtstorage-dispatch.patch`. |

If you add another import from `deps/` to LH code, append a row here.

## Patch-style choice — inline closure vs helper file

When a patch needs to dedupe a flag (or wrap any symbol) at multiple
sites, two shapes are available:

1. **Inline `flag.Lookup` closure**, one per site:

   ```go
   var MaxFieldsPerLine = func() *int {
       if existing := flag.Lookup("insert.maxFieldsPerLine"); existing != nil {
           v, _ := strconv.Atoi(existing.Value.String())
           return &v
       }
       return flag.Int("insert.maxFieldsPerLine", 1000, "...")
   }()
   ```

2. **Helper file** (e.g. `flag_dedup.go.src`) plus a small patch that
   swaps `flag.Int(...)` → `safeInt(...)`.

Use **inline closures** when there are 1–3 sites (the
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
  complicates Docker COPY semantics, and since the 2026-09 bump the
  two directories are no longer unconditionally byte-equal (the pins
  are v1.52.0 and v1.51.0, so two patches carry one differing context
  line each — see `patches/vl-traces/DIVERGENCE.md`). Current shape
  preferred, with the equality guard enforcing that every *other*
  file stays identical.
- **`vlstorage-dispatch` and `external.go.src`** — split because
  `external.go.src` is a *replacement* (`cp`) and the dispatch is
  a *diff* (`git apply`). Cannot be combined cleanly.
- **`vtstorage-flag-dedup` vs `vtinsert-flag-dedup`** — different
  patch shapes (helper-file vs inline-closure) by design. See the
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
