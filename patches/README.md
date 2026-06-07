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

| Patch | Upstream file | Symbol exported / behavior |
| --- | --- | --- |
| `external.go.src` | `app/vlstorage/external.go` | Drop-in replacement that wires VL's vlstorage to LH's storage backend. Full-file replacement (no diff). |
| `external_query.go.src` | `lib/logstorage/external_query.go` | Drop-in replacement exposing `ExternalQuery` hooks LH calls from its own query path. Full-file replacement. |
| `vlstorage-dispatch.patch` | `app/vlstorage/main.go` | Routes VL's `RunQuery` / `GetFieldNames` / `GetFieldValues` / `GetStreamFieldNames` / `GetStreamFieldValues` / `GetStreamIDs` / `GetStreams` / `GetStats` / `GetHits` to `externalStorage` when LH has registered itself. |
| `vl-export-severity.patch` | `app/vlinsert/opentelemetry/pb.go` | Adds `FormatSeverity(int32) string` as the public wrapper around the package-local `formatSeverity`. Consumed by `internal/schema/severity.go::DeriveSeverityText` so cold rows derive `level` from `severity_number` the same way VL hot does. |
| `vl-export-streamtags-get.patch` | `lib/logstorage/stream_tags.go` | Adds `(*StreamTags).Get(name)` and `(*StreamTags).UnmarshalString(s)`. The cold insert path uses `Get` to lift the stream-label `level` onto `row.SeverityText` without re-parsing the canonical string; the compactor uses `UnmarshalString` to re-parse the human-readable Stream column when backfilling SeverityText on historical files. |

### `vt-traces/`

Applied to `lakehouse-traces/deps/VictoriaTraces/...`.

| Patch | Upstream file | Symbol exported / behavior |
| --- | --- | --- |
| `external.go.src` | `app/vtstorage/external.go` | Drop-in replacement that wires VT's vtstorage to LH's trace storage backend. |
| `flag_dedup.go.src` | `app/vtstorage/flag_dedup.go` | Adds the dedup-flag guard so VT's flags don't collide with LH's identical flags in the same binary. |
| `vtstorage-dispatch.patch` | `app/vtstorage/main.go` | Routes VT's query handlers to `externalStorage` when LH has registered itself. |
| `vtstorage-flag-dedup.patch` | `app/vtstorage/main.go` | Wires `flag_dedup.go.src` into VT's flag parsing path so duplicate `flag.Lookup` calls don't panic. (15 flag sites in one file — helper file is cheaper than inline closures.) |
| `vtinsert-flag-dedup.patch` | `app/vtinsert/insertutil/{common_params,flags}.go` | Dedupes the VT vtinsert flags (`-defaultMsgValue`, `-insert.maxFieldsPerLine`) that collide with VL's same-named flags when both packages link into the lakehouse-traces binary. Uses inline `flag.Lookup` closures (2 sites, no helper file). |
| `go-mod-replace.patch` | `go.mod` | Pins VT's VL dependency to the patched local checkout so VT's vlstorage path sees the same `external.go` replacement we apply on the logs side. |

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
   before the build breaks.
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

- **vl-logs ↔ vl-traces mirror** — bytes-identical files in two
  directories. Could be a single source + Makefile copy, but that
  complicates Docker COPY semantics. Current shape preferred.
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
behavior:

```
$ cd deps/VictoriaLogs && git diff path/to/file > /tmp/p.patch
$ cp /tmp/p.patch ../../patches/vl-logs/your-patch.patch
```
