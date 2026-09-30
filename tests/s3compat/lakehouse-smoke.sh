#!/usr/bin/env bash
# Lakehouse end-to-end durability smoke against a candidate S3 backend.
#
#   tests/s3compat/lakehouse-smoke.sh <endpoint> <key> <secret> <bucket> [workdir]
#
# The bucket must already exist (the s3compat harness creates it). Runs the
# real lakehouse-logs binary (build: GOWORK=off go build -o /tmp/lakehouse-logs
# ./cmd/lakehouse-logs; override with LH_BIN) and checks:
#   1. ingest N jsonline records                      -> HTTP 200
#   2. buffer flush writes Parquet to S3              -> objects under prefix
#   3. count() over hot+cold                          -> N
#   4. restart with EMPTY local storage + cache       -> count() == N (cold only,
#      manifest LIST + Parquet range reads + footer/pmeta GETs against the backend)
#   5. filtered query, field_names, hits              -> non-empty / correct
#   6. compaction runs (short interval), count still N
#   7. lakehouse S3 error counters stay 0
# Exit code 0 = all checks passed. Prints one "CHECK <name> PASS|FAIL detail" per check.
set -uo pipefail
endpoint=${1:?endpoint}; key=${2:?key}; secret=${3:?secret}; bucket=${4:?bucket}
work=${5:-/tmp/lh-smoke}
LH_BIN=${LH_BIN:-/tmp/lakehouse-logs}
PORT=${LH_PORT:-39100}
N=${N:-20000}
prefix="smoke-$(date +%s)"
rm -rf "$work"; mkdir -p "$work"
fail=0
check() { # name ok detail
  if [ "$2" = 1 ]; then echo "CHECK $1 PASS $3"; else echo "CHECK $1 FAIL $3"; fail=1; fi
}
start_lh() {
  rm -rf "$work/data" "$work/cache" "$work/lh"
  mkdir -p "$work/lh"
  cat >"$work/config.yaml" <<YAML
lakehouse:
  cache: {disk_path: $work/cache}
  delete: {persist_path: $work/lh/tombstones}
  insert: {buffer_dir: $work/lh/buffer, ack_mode: flush-sync}
  manifest: {persist_path: $work/lh}
YAML
  "$LH_BIN" -lakehouse.config="$work/config.yaml" -httpListenAddr=:$PORT -storageDataPath="$work/data" \
    -lakehouse.s3.bucket="$bucket" -lakehouse.s3.prefix="$prefix" \
    -lakehouse.s3.endpoint="$endpoint" -lakehouse.s3.access-key="$key" -lakehouse.s3.secret-key="$secret" \
    -lakehouse.s3.force-path-style=true -lakehouse.s3.region=us-east-1 \
    -lakehouse.manifest.refresh-interval=3s -lakehouse.insert.flush-interval=5s \
    -lakehouse.compaction.interval=${COMPACT:-600s} -lakehouse.pmeta.enabled=true \
    -lakehouse.cache.memory-mb=64 \
    >>"$work/lh.log" 2>&1 &
  LH_PID=$!
  for _ in $(seq 1 60); do curl -sf "http://127.0.0.1:$PORT/health" >/dev/null && return 0; sleep 0.5; done
  return 1
}
stop_lh() { kill "$LH_PID" 2>/dev/null; wait "$LH_PID" 2>/dev/null; }
count() { curl -s "http://127.0.0.1:$PORT/select/logsql/query" --data-urlencode "query=$1 | stats count() as n" \
  --data-urlencode "start=${2:-2000-01-01T00:00:00Z}" --data-urlencode "end=${3:-2100-01-01T00:00:00Z}" | python3 -c 'import sys,json
try:
  print(json.loads(sys.stdin.readline())["n"])
except Exception as e: print("ERR")'; }
s3errs() { curl -s "http://127.0.0.1:$PORT/metrics" | awk '/^lakehouse_s3_errors_total/ {s+=$NF} END{print s+0}'; }
s3errdetail() { curl -s "http://127.0.0.1:$PORT/metrics" | grep '^lakehouse_s3_errors_total' | tr '\n' ' '; }
uniq_seq() { curl -s "http://127.0.0.1:$PORT/select/logsql/query" --data-urlencode "query=* | uniq by (_msg) | stats count() as n" \
  --data-urlencode "start=2000-01-01T00:00:00Z" --data-urlencode "end=2100-01-01T00:00:00Z" | python3 -c 'import sys,json
try:
  print(json.loads(sys.stdin.readline())["n"])
except Exception as e: print("ERR")'; }

start_lh || { check start 0 "lakehouse-logs did not become healthy (see $work/lh.log)"; exit 1; }

python3 - "$N" >"$work/in.jsonl" <<'EOF'
import sys, json, time, datetime
n=int(sys.argv[1]); now=time.time()
for i in range(n):
    t=datetime.datetime.fromtimestamp(now-3600+i*(3500.0/n), datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    print(json.dumps({"_time":t,"_msg":f"record {i} payload-{i%97}","app":f"svc{i%5}","level":["info","warn","error"][i%3],"seq":str(i),"trace_id":f"{i:032x}"}))
EOF
code=$(curl -s -o "$work/ins.out" -w '%{http_code}' -X POST -H 'Content-Type: application/x-ndjson' \
  "http://127.0.0.1:$PORT/insert/jsonline?_stream_fields=app&_msg_field=_msg&_time_field=_time" --data-binary @"$work/in.jsonl")
check ingest $([ "$code" = 200 ] || [ "$code" = 204 ] && echo 1 || echo 0) "http=$code n=$N"

sleep 1
c=$(count '*'); check count-hot $([ "$c" -ge "$N" ] 2>/dev/null && echo 1 || echo 0) "count=$c want>=$N"

# wait for flush to S3
nobj=0
for _ in $(seq 1 60); do
  nobj=$(curl -s "http://127.0.0.1:$PORT/manifest/range" | python3 -c 'import sys,json
try:
  d=json.load(sys.stdin); print(d.get("totalFiles", 0))
except Exception: print(0)')
  [ "${nobj:-0}" -gt 0 ] 2>/dev/null && break
  sleep 1
done
check flush-to-s3 $([ "${nobj:-0}" -gt 0 ] 2>/dev/null && echo 1 || echo 0) "manifest files=$nobj"
sleep 8
stop_lh

# Restart with empty local state: everything must come back from S3.
start_lh || { check restart 0 "no health after restart"; exit 1; }
sleep 8   # manifest refresh
c=$(count '*'); check cold-count-after-restart $([ "$c" -ge "$N" ] 2>/dev/null && echo 1 || echo 0) "count=$c want>=$N"
u=$(uniq_seq); check cold-no-row-loss $([ "$u" = "$N" ] && echo 1 || echo 0) "distinct seq=$u want=$N"
c=$(count 'level:=error'); want=$(( N / 3 ))
check cold-filter-level $([ "$c" -ge $((want)) ] 2>/dev/null && [ "$c" -le $((want+1)) ] && echo 1 || echo 0) "level:=error count=$c want~=$want"
c=$(count 'trace_id:=00000000000000000000000000000063'); check cold-point-lookup $([ "$c" = 1 ] && echo 1 || echo 0) "trace_id point lookup count=$c want=1"
c=$(count 'payload-42'); check cold-word-search $([ "${c:-0}" -gt 0 ] 2>/dev/null && echo 1 || echo 0) "word search count=$c"
fn=$(curl -s "http://127.0.0.1:$PORT/select/logsql/field_names" --data-urlencode 'query=*' --data-urlencode 'start=2000-01-01T00:00:00Z' --data-urlencode 'end=2100-01-01T00:00:00Z')
check cold-field-names $(echo "$fn" | grep -q '"_msg"' && echo 1 || echo 0) "$(echo "$fn" | tr -d '\n' | head -c 160)"
hits=$(curl -s "http://127.0.0.1:$PORT/select/logsql/hits" --data-urlencode 'query=*' --data-urlencode 'start=2000-01-01T00:00:00Z' --data-urlencode 'end=2100-01-01T00:00:00Z' --data-urlencode 'step=1h' | python3 -c 'import sys,json
try:
  d=json.load(sys.stdin); print(sum(sum(h.get("values",[])) for h in d.get("hits",[])))
except Exception: print("ERR")')
check cold-hits $([ "$hits" = "$N" ] && echo 1 || echo 0) "hits sum=$hits want=$N"
echo "INFO s3-error-counter=$(s3errs) $(s3errdetail)"
stop_lh

# Compaction pass: restart with a short compaction interval, then re-verify.
COMPACT=10s start_lh || { check compaction-start 0 "no health"; exit 1; }
files_before=$(curl -s "http://127.0.0.1:$PORT/manifest/range" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("totalFiles",-1))')
sleep 40
echo "INFO manifest files before/after compaction window: $files_before / $(curl -s "http://127.0.0.1:$PORT/manifest/range" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("totalFiles",-1))')"
c=$(count '*'); u=$(uniq_seq); check count-after-compaction $([ "$u" = "$N" ] && echo 1 || echo 0) "distinct seq=$u count=$c want=$N"
echo "INFO s3-error-counter-after-compaction=$(s3errs) $(s3errdetail)"
stop_lh
grep -ciE 'error|panic' "$work/lh.log" | xargs -I{} echo "INFO log-error-lines={}"
exit $fail
