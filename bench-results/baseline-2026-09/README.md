# Baseline 2026-09 (pre-upgrade: VL v1.50.0 / VT v0.9.2 / parquet-go v0.30.1)

Files:
- `run-baseline.json` / `run-baseline.md` — `scripts/bench/run.sh --signals both --s3-latency "0 100"` (benchmark compose, parity gate first)
- `full-scope-lat0.csv|md`, `full-scope-lat100.csv|md`, `metrics-lat*/` — `scripts/bench/full-scope-s3-bench.sh` (e2e compose) with the per-scenario S3-ops table
- `env.txt` — image tags, git sha, host, docker version

Judge every later PR against these numbers (perf gate: any LH/CH >= 1.0 cell or an LH/VL regression > 10 % on any scenario blocks).
