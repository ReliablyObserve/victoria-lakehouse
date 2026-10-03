# Startup readiness API proof

Release source `f84e440f` (the runtime code is unchanged by metadata merge `8943ce3e`) versus the startup fix (`aa47b12f` plus current-main integration in the commit containing this report). Isolated Compose project `lhstartupfinish`, Linux arm64 binaries built from both source trees, RustFS, ports 39900–39902. Before and after use separate empty buckets and freshly created Lakehouse containers. Each signal receives the same deterministic 40-row `startup-proof` corpus after boot.

| Public state after warmup | Main, logs/traces | Fixed, logs/traces |
|---|---|---|
| GET /ready | 200 READY | 200 READY |
| GET /lakehouse/info.phase | s3_refresh | ready |
| GET /lakehouse/info.ready | true | true |
| lakehouse_startup_phase | 3 | 6 |
| lakehouse_ready | 0 | 1 |
| lakehouse_serving_ready | 1 | 1 |
| lakehouse_warmup_complete | 1 | 1 |
| lakehouse_startup_total_seconds | 0 | recorded, >0 |
| Seeded run count | 40 | 40 |

`before.json` and `after.json` contain sanitized API/metric responses. `capture.py` can be rerun against ports 39901/39902 with an output variant argument; it uses the upstream logs stream selector and traces span-attribute selector. This is an API response proof for Lakehouse-specific startup state, not a Grafana screenshot or a read-throughput benchmark. The seeded query controls show no count change; broader read correctness remains held by the existing parity suites.

The change preserves the distinction between completing warmup and being safe to serve. Unit regressions separately prove manifest and WAL-replay gates still prevent readiness after warmup, including legacy phase transitions and concurrent gate updates. Independent full suites and startup race checks pass under both dependency graphs. Eight independently removed guards per graph were caught by semantic regressions (16 checks); the original timing-data race was reproduced and the metric synchronization was verified under forced scheduler yields. Startup statement coverage is 96.8%.

Readiness-reader microbenchmark: Apple M5 Pro, Darwin arm64, ten sequential 100 ms repetitions per case, all gates satisfied. Combined readiness is 1.635 ns before versus 1.658 ns after (+1.47%, benchstat p=0.005); serving readiness is statistically unchanged at about 1.64 ns (p=0.304). Both cases allocate zero bytes and objects. This is a nanosecond-scale reader check, not an end-to-end query-latency claim. Reproduce with `GOWORK=off go test ./internal/startup -run '^$' -bench BenchmarkReadyGates -benchmem -benchtime=100ms -count=10`; compare the committed `bench-before.txt` and `bench-after.txt` with benchstat. Logs were removed without dropping any of the twenty numeric samples per build.

Fresh CI on the published head is required for lint, full suites, e2e and parity. The exposed smoke assertions are preserved; the separate trace field_names product gap is not part of this fix.
