# Declared divergences between `patches/vl-logs/` and `patches/vl-traces/`

Both directories patch VictoriaLogs, but two different checkouts of it:

| Directory | Applied to | Pin |
| --- | --- | --- |
| `patches/vl-logs/` | `deps/VictoriaLogs` | `VL_VERSION_LOGS` (v1.52.0) |
| `patches/vl-traces/` | `lakehouse-traces/deps/VictoriaLogs` | `VL_COMMIT_TRACES` (6ae2da3c11f3 = v1.51.0, the commit VictoriaTraces v0.11.0 pins) |

The **content** of the two patch sets is the same — same hunks, same added
lines, same exported symbols. What can differ is the upstream **context** a
diff carries when the two pins sit on either side of an upstream edit to a
line next to ours.

`scripts/ci/check_patches_equal.sh` enforces byte-equality for every file in
both directories, and accepts a difference only for a file listed below. It
also fails on a **stale** row here: if a listed file becomes byte-equal again
(the usual case once both pins reach the same VictoriaLogs release), the row
must be removed.

Each row: `` `file` `` — which upstream change moved the context.

- `vlstorage-dispatch.patch` — the `GetTenantIDs` doc comment in
  `app/vlstorage/main.go` was rewritten upstream between v1.51.0 and v1.52.0
  ("returns tenantIDs from the storage by the given start and end" →
  "returns sorted tenantIDs on the [start..end] time range"). It is the
  context line above the hunk that adds the `externalStorage` dispatch, so
  the two patches carry one different context line. The added lines are
  identical.
- `vl-export-streamtags-get.patch` — `unmarshalStringInplace` in
  `lib/logstorage/stream_tags.go` gained a `sort.Sort(st)` and now ends in
  `return nil` at v1.52.0, where v1.51.0 ends in `return err`. That closing
  line is the context above the hunk that adds the exported
  `UnmarshalString` wrapper. The added lines are identical.
