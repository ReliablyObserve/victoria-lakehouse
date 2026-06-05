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
| `vl-export-severity.patch` | `app/vlinsert/opentelemetry/pb.go` | Adds `FormatSeverity(int32) string` as the public wrapper around the package-local `formatSeverity`. Consumed by `internal/vlstorage/insert.go::severityTextFromNumber` so cold rows derive level from `severity_number` the same way VL hot does. |

### `vt-traces/`

Applied to `lakehouse-traces/deps/VictoriaTraces/...`.

| Patch | Upstream file | Symbol exported / behavior |
| --- | --- | --- |
| `external.go.src` | `app/vtstorage/external.go` | Drop-in replacement that wires VT's vtstorage to LH's trace storage backend. |
| `flag_dedup.go.src` | `app/vtstorage/flag_dedup.go` | Adds the dedup-flag guard so VT's flags don't collide with LH's identical flags in the same binary. |
| `vtstorage-dispatch.patch` | `app/vtstorage/main.go` | Routes VT's query handlers to `externalStorage` when LH has registered itself. |
| `vtstorage-flag-dedup.patch` | `app/vtstorage/main.go` | Wires `flag_dedup.go.src` into VT's flag parsing path so duplicate `flag.Lookup` calls don't panic. |
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
