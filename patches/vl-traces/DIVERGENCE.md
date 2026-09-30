# Declared divergences between `patches/vl-logs/` and `patches/vl-traces/`

Both directories patch VictoriaLogs, but two different checkouts of it:

| Directory | Applied to | Pin |
| --- | --- | --- |
| `patches/vl-logs/` | `deps/VictoriaLogs` | `VL_VERSION_LOGS` (v1.52.0) |
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

There are currently no declared divergences: both pins are VictoriaLogs v1.52.0,
so the two directories are byte-equal. Keep the two pins and the two
directories separate anyway; a future VictoriaTraces release can pin a VL
commit that lags `VL_VERSION_LOGS` again, and this file is where such a
difference is declared.
