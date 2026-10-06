# Declared divergences between `patches/vl-logs/` and `patches/vl-traces/`

Both directories patch VictoriaLogs, but two different checkouts of it:

| Directory | Applied to | Pin |
| --- | --- | --- |
| `patches/vl-logs/` | `deps/VictoriaLogs` | `VL_VERSION_LOGS` (v1.53.0) |
| `patches/vl-traces/` | `lakehouse-traces/deps/VictoriaLogs` | `VL_COMMIT_TRACES` (c945d2949e98 = v1.52.0, the commit VictoriaTraces v0.12.0 pins) |

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

- `vlstorage-dispatch.patch` — v1.53.0 guarded the body of `RunQuery` with `if localStorage != nil {` (last-N optimisation moved behind it), so the logs patch inserts the `externalStorage` branch before that guard; the traces pin (c945d2949e98) still has the unguarded body. The added lines are identical.
- `vl-partition-close-order.patch` — upstream-fixed-in-logs-pin(manual): VictoriaLogs v1.53.0 closes datadb before indexdb in `mustClosePartition` itself, so `patches/vl-logs/` no longer carries the patch. The traces pin (c945d2949e98 = v1.52.0) still has the old order, so `patches/vl-traces/` keeps it until VictoriaTraces pins a VictoriaLogs that includes the fix. Manual equivalence note: upstream's own change carries different comment text than ours, so the patch does not reverse-apply to v1.53.0; the executable change (`mustClosePartition` closes `pt.ddb` before `pt.idb`) is the same, checked by reading both.

- `vl-allow-duplicate-stream-tags.patch` — upstream-fixed-in-logs-pin: VictoriaLogs v1.53.0 accepts several stream tags with one name (VictoriaLogs #1603, #1604) in `StreamTags.checkCorrectness`, so `patches/vl-logs/` no longer carries the patch. The traces pin (c945d2949e98 = v1.52.0) rejects them and panics when such a stream is registered or read, so `patches/vl-traces/` carries the same two-line change until VictoriaTraces pins a VictoriaLogs that includes it.

- `vl-math-keep-quoted-constants.patch` — upstream-fixed-in-logs-pin: VictoriaLogs v1.53.0 (upstream commit 901ca58e0) keeps the quotes of a quoted numeric constant in the `math` pipe's string form (`math _time - "2026-10-01T00:00:00Z"`). Without it, `Query.Clone` (used by `CloneWithTimeFilter` on the insert-buffer read) re-parses `2026-10-01T00:00:00Z` as `2026 - 01 - 01T…`, fails, and `logger.Panicf("BUG: cannot parse ...")` exits the traces process. The patch is upstream's exact `pipe_math.go` change (tests dropped); `patches/vl-logs/` does not carry it. Dropped when VictoriaTraces pins a VictoriaLogs that includes it.

- `vl-syslog-rfc5424-incomplete-sd.patch` — upstream-fixed-in-logs-pin: VictoriaLogs v1.53.0 (upstream commit 877a61959, #1786) returns early when an RFC5424 structured-data parameter ends right after `=`, instead of indexing past the end of the line. Without it the syslog listener panics and `unpack_syslog` over such a stored message panics (swallowed on the cold path as zero rows). Upstream's exact `syslog_parser.go` change; `patches/vl-logs/` does not carry it.

The two pins now differ (logs v1.53.0, traces c945d2949e98 = v1.52.0). Keep the two
directories separate; this file is where each difference is declared, and rows
are removed as the traces pin catches up.
