#!/usr/bin/env bash
# =============================================================================
# scripts/bench/run.sh — THE one place to benchmark the cold tier.
#
# Comparison model:
#   VL / VT (disk, optionally simulated gp3)  = REFERENCE BASELINE
#   Lakehouse (S3 Parquet)                    = system under test
#   ClickHouse (S3, the SAME Parquet LH wrote) = engine-vs-engine on identical bytes
#
# Every S3 engine (LH, CH) reads MinIO through the toxiproxy s3-latency proxy, so
# one knob injects identical object-store latency. VL/VT are disk-native; with
# --disk-profile gp3-loop their disk is throttled to AWS gp3 (125 MB/s, 3000 IOPS)
# so a fast laptop NVMe doesn't flatter the baseline.
#
# It sweeps systems x signals x query-types x time-ranges x S3-latency, runs a
# parity gate first (so we compare EQUAL answers, incl. CH), and writes one
# consolidated report (JSON + a markdown table normalized to the VL/VT baseline)
# that flags where LH is slow vs baseline and vs CH.
#
# Usage:
#   scripts/bench/run.sh [options]
#     --disk-profile local-ssd|gp3-loop   (default local-ssd)
#     --s3-latency "0 100 300"            ms levels to sweep (default "0")
#     --signals logs|traces|both          (default both)
#     --ranges  "1h 6h 24h"               (default "1h 6h 24h")
#     --iterations N                       (default 20)   --warmup N (default 3)
#     --output FILE                        (default bench-results/run-<stamp>.json)
#     --stamp YYYYmmdd-HHMMSS              timestamp for output (default: caller-supplied or 'latest')
#     --no-up      assume the stack is already running (skip compose up)
#     --no-ingest  skip the data-generation step
#     --keep       leave the stack running on exit
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1

DISK_PROFILE=local-ssd
S3_LATENCIES="0"
SIGNALS=both
RANGES="1h 6h 24h"
ITERATIONS=20
WARMUP=3
STAMP="latest"
OUTPUT=""
DO_UP=1; DO_INGEST=1; KEEP=0; COLD=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --disk-profile) DISK_PROFILE="$2"; shift 2 ;;
    --s3-latency)   S3_LATENCIES="$2"; shift 2 ;;
    --signals)      SIGNALS="$2"; shift 2 ;;
    --ranges)       RANGES="$2"; shift 2 ;;
    --iterations)   ITERATIONS="$2"; shift 2 ;;
    --warmup)       WARMUP="$2"; shift 2 ;;
    --output)       OUTPUT="$2"; shift 2 ;;
    --stamp)        STAMP="$2"; shift 2 ;;
    --no-up)        DO_UP=0; shift ;;
    --no-ingest)    DO_INGEST=0; shift ;;
    --keep)         KEEP=1; shift ;;
    --cold)         COLD=1; WARMUP=0; shift ;;   # clear LH cache before each request
    --queries)      QUERY_OVERRIDE="$2"; shift 2 ;;
    -h|--help)      sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
[[ -z "$OUTPUT" ]] && OUTPUT="bench-results/run-${STAMP}.json"
mkdir -p "$(dirname "$OUTPUT")"

# BENCH_TMP: every timed/warmup request's response body is captured here (one
# file per request) so it can be validated (HTTP-2xx AND parseable AND
# non-empty AND stable) before its latency counts for anything. Cleaned up
# per-request (measure_query removes each tmpfile right after validating it)
# and as a safety net in teardown().
BENCH_TMP="$(mktemp -d)"

COMPOSE_BASE="deployment/docker/docker-compose-benchmark.yml"
COMPOSE_GP3="deployment/docker/docker-compose-benchmark.gp3.yml"
COMPOSE_ARGS=(-f "$COMPOSE_BASE")
[[ "$DISK_PROFILE" == "gp3-loop" ]] && COMPOSE_ARGS+=(-f "$COMPOSE_GP3")

# --- endpoints (benchmark compose published ports) ----------------------------
declare -A EP=(
  [lh_logs]=http://localhost:39428
  [vl]=http://localhost:39401
  [lh_traces]=http://localhost:30428
  [vt]=http://localhost:30401
  [ch]=http://localhost:38123
)
# ClickHouse HTTP auth (CLICKHOUSE_USER/PASSWORD in the benchmark compose).
CH_USER="${CH_USER:-default}"; CH_PASS="${CH_PASS:-benchmark}"

log() { printf '\033[1;36m[bench]\033[0m %s\n' "$*" >&2; }

# --- correctness validation ---------------------------------------------------
# A benchmark cell is only meaningful when every timed request returned a
# correct, valid response — a fast wrong/empty/error answer is a BROKEN
# response and must never be counted as latency (this is exactly the
# VT-empty-cells failure mode found while recording the pre-validation baseline).
#
# MISS_QUERIES lists query kinds whose *correct* answer can legitimately be
# empty (a cross-signal lookup that doesn't correlate); every other query's
# empty result is treated as invalid, not a real answer.
MISS_QUERIES=" trace_lookup "
is_miss_query() { [[ "$MISS_QUERIES" == *" $1 "* ]]; }

# SCAN_LIMIT: the `limit` every scan query uses (see _prep_body). VictoriaLogs
# documents that `limit N` without an explicit `sort` returns rows "selected
# in arbitrary order because of performance reasons … can return different
# sets of logs every time" once more than N rows match — so per-iteration
# IDENTITY can never hold for a truncated scan, and adding a `sort` would
# change what the query measures (a sort has its own cost). Validity for a
# truncated scan is therefore MEMBERSHIP (every returned row is a real row of
# the reference window) + CARDINALITY (exactly SCAN_LIMIT rows), not
# identity — see build_scan_window()/_validate_iter() below.
SCAN_LIMIT=1000

# strip_scan_limit <method> <body> -> the scan query body with its trailing
# limit clause removed, so the SAME filter/window can be re-run unbounded to
# get the reference "how many rows actually match, and what are they" — used
# once per (system, cell) to build the membership/cardinality reference.
strip_scan_limit() {
  local method="$1" body="$2"
  if [[ "$method" == CH ]]; then
    printf '%s' "${body% LIMIT $SCAN_LIMIT}"
  else
    printf '%s' "${body% | limit $SCAN_LIMIT}"
  fi
}

result_is_empty() {
  case "$1" in
    0|rows=0|spans=0) return 0 ;;
    "rows=0;hash="*) return 0 ;;
    *) return 1 ;;
  esac
}

# extract_result <qkind> <system> <file> [keysout] -> a comparable value
# string, or "invalid:<reason>" on any parse failure. Formats:
#   - scalar (count/filter/group-by kinds): the count as a plain integer
#     string — VL/VT/LH sum the LogsQL JSON-line `n` (or fall back to row
#     count when there's no count column); ClickHouse sums the numeric last
#     TSV column (or falls back to row count) — same heuristic, applied per
#     response instead of once after the loop.
#   - scan: "rows=<N>;hash=<sha256 of the sorted set of stable row keys>" —
#     key is `_msg` for logs, `trace_id:span_id` for traces (so a degenerate
#     but same-count answer still diverges). ClickHouse's scan is a DIFFERENT
#     projection with no comparable key, so CH scan is "rows=<N>" only —
#     compared by row count alone. When `keysout` is given (non-CH only),
#     the sorted+deduped key set is also written there, one per line — used
#     both to build the per-cell reference window and, per iteration, to
#     check a truncated scan's rows are members of that window.
#   - trace_by_id / trace_lookup: "spans=<N>", the matched-span/log count.
extract_result() {
  local qkind="$1" system="$2" file="$3" keysout="${4:-}"
  if [[ "$system" == clickhouse ]]; then
    case "$qkind" in
      scan)
        awk 'END{print "rows="NR}' "$file" ;;
      trace_by_id)
        awk -F'\t' '{n++; v=$NF} END{if(n==0){print "invalid:empty-body"} else {print "spans="(v+0)}}' "$file" ;;
      *)
        awk -F'\t' '{n++; v=$NF; if(v+0==v && v!="") s+=v; else nn=1} END{if(n==0){print "invalid:empty-body"} else {print (nn?n:s+0)}}' "$file" ;;
    esac
  else
    case "$qkind" in
      scan)
        python3 - "$file" "$keysout" <<'PY'
import sys, json, hashlib
path = sys.argv[1]
keysout = sys.argv[2] if len(sys.argv) > 2 else ""
keys = []
n = parsed = 0
with open(path) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        n += 1
        try:
            d = json.loads(line)
        except Exception:
            continue
        parsed += 1
        if "trace_id" in d or "span_id" in d:
            keys.append("{}:{}".format(d.get("trace_id", ""), d.get("span_id", "")))
        else:
            keys.append(d.get("_msg", ""))
if n and not parsed:
    print("invalid:parse-error")
else:
    uniq = sorted(set(keys))
    h = hashlib.sha256("\n".join(uniq).encode()).hexdigest()
    if keysout:
        # NUL-delimited, NOT newline-delimited: a real log `_msg` (e.g. a
        # multi-line stack trace) can contain embedded newlines, which would
        # silently split one key into several bogus "lines" in a
        # newline-delimited file and corrupt the membership check that reads
        # it back (comm/set-difference assumes one key per record). NUL never
        # appears in JSON-decoded text.
        with open(keysout, "wb") as kf:
            kf.write(b"\x00".join(u.encode("utf-8", "surrogatepass") for u in uniq))
    print("rows={};hash={}".format(n, h))
PY
        ;;
      trace_by_id|trace_lookup)
        python3 - "$file" <<'PY'
import sys, json
n = parsed = tot = 0
with open(sys.argv[1]) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        n += 1
        try:
            d = json.loads(line)
        except Exception:
            continue
        parsed += 1
        v = d.get("n")
        if v is not None:
            try:
                tot += int(v)
            except Exception:
                pass
# An empty body (n==0) is LogsQL's real, legitimate answer for a zero-match
# `stats count()` — VictoriaLogs emits no output line at all when the filtered
# stream is empty, confirmed empirically (the pre-validation baseline's
# trace_lookup miss cells are `[0]`, not missing/errored, on every system).
# Only "got bytes but none of them parsed as JSON" is an actual parse failure.
if n and not parsed:
    print("invalid:parse-error")
else:
    print("spans={}".format(tot))
PY
        ;;
      *)
        python3 - "$file" <<'PY'
import sys, json
n = parsed = tot = 0
has_count = False
with open(sys.argv[1]) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        n += 1
        try:
            d = json.loads(line)
        except Exception:
            continue
        parsed += 1
        v = d.get("n", d.get("count(*)", d.get("count(")))
        if v is not None:
            has_count = True
            try:
                tot += int(v)
            except Exception:
                pass
# Same reasoning as the trace_by_id/trace_lookup branch above: an empty body
# is LogsQL's legitimate zero-match answer, not a parse failure.
if n and not parsed:
    print("invalid:parse-error")
else:
    print(tot if has_count else n)
PY
        ;;
    esac
  fi
}

# scan_key_has_foreign <iterkeys_file> <window_keys_file> -> echoes "1" if any
# key in iterkeys_file is absent from window_keys_file, else "0". Both files
# are NUL-delimited key sets (extract_result's scan `keysout`). A python
# set-difference, not `comm`: `comm` needs newline-sorted input, and a real
# log `_msg` can contain embedded newlines (e.g. a Java stack trace), which
# would silently fragment one key into several bogus lines and corrupt a
# line-based comparison.
scan_key_has_foreign() {
  local iterkeys="$1" windowkeys="$2"
  python3 -c "
import sys
def load(path):
    with open(path, 'rb') as f:
        data = f.read()
    return set(data.split(b'\x00')) if data else set()
iterset = load(sys.argv[1])
winset = load(sys.argv[2])
print('1' if (iterset - winset) else '0')
" "$iterkeys" "$windowkeys"
}

# _validate_iter: validates one iteration's HTTP response against the correctness
# rules above. Sets globals VALID (0/1), REASON, RESULT.
# $1 qkind  $2 system  $3 http_code  $4 tmpfile  $5 first_result (""=none yet)
# $6 window_rows (""=no reference / not a scan)  $7 window_keys (path, non-CH only)
_validate_iter() {
  local qkind="$1" system="$2" code="$3" tmpfile="$4" first_result="$5"
  local window_rows="${6:-}" window_keys="${7:-}"
  if [[ "$code" != 2* ]]; then
    VALID=0; REASON="http $code"; RESULT=""
    return
  fi
  if [[ "$qkind" == scan && -n "$window_rows" && "$window_rows" -gt "$SCAN_LIMIT" ]]; then
    # Truncated scan (more than SCAN_LIMIT rows match the window): identity
    # can never hold (see SCAN_LIMIT's comment above), so validity is
    # membership + cardinality against the per-cell reference window instead.
    if [[ "$system" == clickhouse ]]; then
      RESULT=$(extract_result "$qkind" "$system" "$tmpfile")
      if [[ "$RESULT" == invalid:* ]]; then
        VALID=0; REASON="${RESULT#invalid:}"; return
      fi
      local rows="${RESULT#rows=}"
      if [[ "$rows" != "$SCAN_LIMIT" ]]; then
        VALID=0; REASON="truncated scan returned $rows rows, expected $SCAN_LIMIT"; return
      fi
      VALID=1; REASON=""; return
    fi
    local iterkeys; iterkeys=$(mktemp "$BENCH_TMP/iterkeys.XXXXXX")
    RESULT=$(extract_result "$qkind" "$system" "$tmpfile" "$iterkeys")
    if [[ "$RESULT" == invalid:* ]]; then
      rm -f "$iterkeys"; VALID=0; REASON="${RESULT#invalid:}"; return
    fi
    local rows="${RESULT%%;*}"; rows="${rows#rows=}"
    if [[ "$rows" != "$SCAN_LIMIT" ]]; then
      rm -f "$iterkeys"; VALID=0; REASON="truncated scan returned $rows rows, expected $SCAN_LIMIT"; return
    fi
    local has_foreign=""
    has_foreign=$(scan_key_has_foreign "$iterkeys" "$window_keys")
    rm -f "$iterkeys"
    if [[ "$has_foreign" == 1 ]]; then
      VALID=0; REASON="row not in reference window (membership check failed)"; return
    fi
    VALID=1; REASON=""; return
  fi
  # Untruncated scan (window_rows <= SCAN_LIMIT, i.e. the query returned every
  # matching row — nothing to truncate, so identity IS meaningful), or any
  # non-scan qkind: identity-based validation as before.
  RESULT=$(extract_result "$qkind" "$system" "$tmpfile")
  if [[ "$RESULT" == invalid:* ]]; then
    VALID=0; REASON="${RESULT#invalid:}"
    return
  fi
  if ! is_miss_query "$qkind" && result_is_empty "$RESULT"; then
    VALID=0; REASON="empty result"
    return
  fi
  if [[ -n "$first_result" && "$RESULT" != "$first_result" ]]; then
    # Generic (not "$RESULT vs $first_result") on purpose: a scan's result
    # embeds a full sha256 row-set hash, which would make every flapping
    # iteration's reason a distinct string and defeat de-duplication —
    # reasons_seen is keyed on this exact text in measure_query.
    VALID=0; REASON="flapping (differs from cell's first valid result)"
    return
  fi
  VALID=1; REASON=""
}

# --- measure_query: time a prepared request N times, emit p50/p95/p99 JSON -----
# Uses curl's own %{time_total} (no shell-timing overhead, so fast queries aren't
# inflated by subprocess startup); percentiles computed once at the end, over
# VALID iterations only. Every iteration's body is captured and validated
# (HTTP 2xx, parseable, non-empty for non-miss scenarios, identical to the
# cell's first valid result) BEFORE its latency counts for anything — an
# invalid iteration is excluded from p50/p95/p99 and recorded in
# iters_invalid/invalid_reasons instead.
# build_scan_window <system> <method> <url> <body> -> sets globals WINDOW_ROWS
# (row count of the FULL, unlimited match — "" on failure), WINDOW_HASH (sha256
# of its sorted key set; "" for ClickHouse or on failure), WINDOW_KEYS_FILE
# (path to that key set, one per line; "" for ClickHouse). ONE untimed request
# per (system, cell), issued before the warmup loop, with the same filter/
# window as the timed query but its `limit` clause stripped — this is the
# reference a truncated scan's per-iteration rows are checked against.
build_scan_window() {
  local system="$1" method="$2" url="$3" body="$4" wbody wcode wtt wsz wtmp wres
  WINDOW_ROWS=""; WINDOW_HASH=""; WINDOW_KEYS_FILE=""
  wbody=$(strip_scan_limit "$method" "$body")
  local out; out=$(_do_req "$method" "$url" "$wbody"); read -r wcode wtt wsz wtmp <<<"$out"
  if [[ "$wcode" != 2* ]]; then
    log "WARN: scan window reference request failed (system=$system http=$wcode) — falling back to identity validation for this cell"
    rm -f "$wtmp"; return
  fi
  if [[ "$system" == clickhouse ]]; then
    wres=$(extract_result scan clickhouse "$wtmp")
    rm -f "$wtmp"
    [[ "$wres" == invalid:* ]] && { log "WARN: scan window reference unparseable (system=$system: ${wres#invalid:})"; return; }
    WINDOW_ROWS="${wres#rows=}"
  else
    WINDOW_KEYS_FILE=$(mktemp "$BENCH_TMP/window.XXXXXX")
    wres=$(extract_result scan "$system" "$wtmp" "$WINDOW_KEYS_FILE")
    rm -f "$wtmp"
    if [[ "$wres" == invalid:* ]]; then
      log "WARN: scan window reference unparseable (system=$system: ${wres#invalid:})"
      rm -f "$WINDOW_KEYS_FILE"; WINDOW_KEYS_FILE=""; return
    fi
    WINDOW_ROWS="${wres%%;*}"; WINDOW_ROWS="${WINDOW_ROWS#rows=}"
    WINDOW_HASH="${wres#*hash=}"
  fi
}

# --- measure_query: time a prepared request N times, emit p50/p95/p99 JSON -----
# Uses curl's own %{time_total} (no shell-timing overhead, so fast queries aren't
# inflated by subprocess startup); percentiles computed once at the end, over
# VALID iterations only. Every iteration's body is captured and validated
# (HTTP 2xx, parseable, non-empty for non-miss scenarios, identical to the
# cell's first valid result — or, for a truncated scan, a member of the
# reference window in exactly SCAN_LIMIT rows) BEFORE its latency counts for
# anything — an invalid iteration is excluded from p50/p95/p99 and recorded in
# iters_invalid/invalid_reasons instead.
# $1 label  $2 system  $3 method(GET|POST|CH)  $4 url  $5 body  $6 qkind (query name, e.g. "scan")
measure_query() {
  local label="$1" system="$2" method="$3" url="$4" body="${5:-}" qkind="${6:-}"
  local secs=() bytes=() errors=0 i out code tt sz tmpfile
  local first_result="" iters_valid=0 iters_invalid=0
  local -A reasons_seen=()
  local reasons_list=()
  local window_rows="" window_hash="" window_keys=""

  if [[ "$qkind" == scan ]]; then
    build_scan_window "$system" "$method" "$url" "$body"
    window_rows="$WINDOW_ROWS"; window_hash="$WINDOW_HASH"; window_keys="$WINDOW_KEYS_FILE"
  fi

  for ((i=0; i<WARMUP; i++)); do
    out=$(_do_req "$method" "$url" "$body"); read -r code tt sz tmpfile <<<"$out"
    _validate_iter "$qkind" "$system" "$code" "$tmpfile" "" "$window_rows" "$window_keys"   # warms caches; never counted
    rm -f "$tmpfile"
  done

  for ((i=0; i<ITERATIONS; i++)); do
    # Cold mode: evict LH's mem/disk/footer caches before each request so every
    # sample is a true cold scan (re-fetch from S3 + re-decode Parquet). Only the
    # system under test is cleared; VL/VT are hot-tier by design and CH re-scans
    # S3 each query anyway.
    [[ "$COLD" == 1 && "$system" == lakehouse ]] && clear_lh_cache "$url"
    out=$(_do_req "$method" "$url" "$body"); read -r code tt sz tmpfile <<<"$out"
    _validate_iter "$qkind" "$system" "$code" "$tmpfile" "$first_result" "$window_rows" "$window_keys"
    [[ "$code" != 2* ]] && errors=$((errors+1))
    if [[ "$VALID" == 1 ]]; then
      [[ -z "$first_result" ]] && first_result="$RESULT"
      secs+=("$tt"); bytes+=("$sz")
      iters_valid=$((iters_valid+1))
    else
      iters_invalid=$((iters_invalid+1))
      if [[ -z "${reasons_seen[$REASON]:-}" ]]; then reasons_seen[$REASON]=1; reasons_list+=("$REASON"); fi
    fi
    rm -f "$tmpfile"
  done
  rm -f "$window_keys"

  local invalid_reasons="" r
  for r in "${reasons_list[@]:-}"; do
    [[ -z "$r" ]] && continue
    if [[ -z "$invalid_reasons" ]]; then invalid_reasons="$r"; else invalid_reasons="${invalid_reasons}; ${r}"; fi
  done
  local content_hash=""
  [[ "$first_result" == *";hash="* ]] && content_hash="${first_result#*;hash=}"

  python3 - "$label" "$system" "$errors" "$first_result" "$iters_valid" "$iters_invalid" "$invalid_reasons" "$content_hash" "$window_rows" "$window_hash" "${#bytes[@]}" "${bytes[@]}" -- "${secs[@]}" <<'PY'
import sys, json
a = sys.argv
label, system, errors, result, iters_valid, iters_invalid, invalid_reasons, content_hash, window_rows, window_hash, nb = (
    a[1], a[2], int(a[3]), a[4], int(a[5]), int(a[6]), a[7], a[8], a[9], a[10], int(a[11]))
b = list(map(float, a[12:12+nb]))
secs = list(map(float, a[a.index('--')+1:]))
vals = sorted(s * 1000 for s in secs)
n = len(vals)
pct = lambda p: round(vals[min(int(n * p / 100), n - 1)], 1) if n else None
avg_bytes = round(sum(b)/len(b)) if b else 0
out = {"label": label, "system": system, "p50_ms": pct(50), "p95_ms": pct(95),
       "p99_ms": pct(99), "iters": n, "errors": errors,
       "avg_bytes": avg_bytes, "result": result if result else None,
       "iters_valid": iters_valid, "iters_invalid": iters_invalid,
       "invalid_reasons": invalid_reasons if invalid_reasons else None,
       "content_hash": content_hash if content_hash else None,
       "window_rows": int(window_rows) if window_rows else None,
       "window_hash": window_hash if window_hash else None}
print(json.dumps(out))
PY
}
clear_lh_cache() { # $1 = any LH url; POSTs /internal/cache/clear to its host
  local base="${1%%/select/*}"; base="${base%%/api/*}"
  curl -sf -o /dev/null -X POST --max-time 10 "${base}/internal/cache/clear" 2>/dev/null || true
}
_do_req() { # echoes "<http_code> <time_total_s> <size_download_bytes> <tmpfile>"
  # No `-f`: with it, curl treats a 4xx/5xx as a curl-level failure and skips
  # the `-w` output entirely, so a real server error came back indistinguishable
  # from "000" (a genuine connection failure) — the validity gate then saw
  # "http 000" for both a dead server AND a real 500, hiding which one
  # happened. Without `-f`, curl exits 0 on any HTTP response (only a true
  # transport failure — refused connection, DNS, timeout — hits the `||`
  # fallback), so `%{http_code}` always records the real status.
  local method="$1" url="$2" body="$3" tmp result
  tmp=$(mktemp "$BENCH_TMP/req.XXXXXX")
  case "$method" in
    GET)  result=$(curl -s -o "$tmp" -w '%{http_code} %{time_total} %{size_download}' --max-time 60 "$url" 2>/dev/null) || result="000 0 0" ;;
    POST) result=$(curl -s -o "$tmp" -w '%{http_code} %{time_total} %{size_download}' --max-time 60 --data-urlencode "query=$body" "$url" 2>/dev/null) || result="000 0 0" ;;
    CH)   result=$(curl -s -o "$tmp" -w '%{http_code} %{time_total} %{size_download}' --max-time 60 --user "$CH_USER:$CH_PASS" --data-binary "$body" "$url/" 2>/dev/null) || result="000 0 0" ;;
  esac
  printf '%s %s\n' "$result" "$tmp"
}
# fetch_scalar: one un-timed request via _do_req + extract_result — used by
# parity_gate for a quick baseline-agreement count. (The per-iteration result
# used by the report's validity/equality checks comes from extract_result
# directly, inside measure_query.) Echoes the value, or "ERR".
fetch_scalar() {
  local method="$1" url="$2" body="$3" qkind="$4" system="$5" out code tt sz tmpfile result
  out=$(_do_req "$method" "$url" "$body"); read -r code tt sz tmpfile <<<"$out"
  if [[ "$code" != 2* ]]; then rm -f "$tmpfile"; echo ERR; return; fi
  result=$(extract_result "$qkind" "$system" "$tmpfile")
  rm -f "$tmpfile"
  [[ "$result" == invalid:* ]] && { echo ERR; return; }
  echo "$result"
}

# --- time helpers (ns epoch for LogsQL, unix seconds for CH) -------------------
range_to_secs() { case "$1" in 15m) echo 900;; 1h) echo 3600;; 6h) echo 21600;; 24h) echo 86400;; 7d) echo 604800;; *) echo 3600;; esac; }
start_ns() { python3 -c "import time;print(int((time.time()-$1)*1e9))"; }
end_ns()   { python3 -c "import time;print(int(time.time()*1e9))"; }
start_s()  { python3 -c "import time;print(int(time.time()-$1))"; }
end_s()    { python3 -c "import time;print(int(time.time()))"; }

# --- the matrix: (signal, query) -> per-system prepared request ---------------
# Each query function echoes "<method>\t<url>\t<body>" for the given system+range.
# LogsQL systems (lh/vl/vt) hit /select/logsql/query; ClickHouse hits its HTTP
# SQL endpoint over the otel_logs/otel_traces views (same Parquet on S3).
prep() { # $1 signal  $2 query  $3 system  $4 range_secs
  # Logs the exact prepared request (method/url/body) via the shared log() helper
  # (stderr, so it never pollutes the "<method>\t<url>\t<body>" stdout contract
  # that callers capture with $(...)), then re-emits _prep_body's stdout verbatim.
  local signal="$1" query="$2" sys="$3" secs="$4" row
  row="$(_prep_body "$signal" "$query" "$sys" "$secs")"
  if [[ -n "$row" ]]; then
    local m u b
    IFS=$'\t' read -r m u b <<<"$row"
    log "req ${signal}/${query} ${sys} ${m} ${u} body=${b}"
  fi
  printf '%s' "$row"
}
_prep_body() { # $1 signal  $2 query  $3 system  $4 range_secs
  local signal="$1" query="$2" sys="$3" secs="$4"
  local sns ens ss es
  sns=$(start_ns "$secs"); ens=$(end_ns "$secs"); ss=$(start_s "$secs"); es=$(end_s "$secs")
  local logs_url traces_url
  case "$sys" in
    lakehouse) logs_url="${EP[lh_logs]}/select/logsql/query"; traces_url="${EP[lh_traces]}/select/logsql/query" ;;
    victorialogs) logs_url="${EP[vl]}/select/logsql/query" ;;
    victoriatraces) traces_url="${EP[vt]}/select/logsql/query" ;;
  esac
  if [[ "$signal" == logs ]]; then
    case "$query" in
      count_total)      [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_logs WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s)' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\t* | stats count() n' "$logs_url" "$sns" "$ens" ;;
      count_by_service) [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT ServiceName,count() FROM lakehouse.otel_logs WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) GROUP BY ServiceName' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\t* | stats by (service.name) count()' "$logs_url" "$sns" "$ens" ;;
      fulltext)         [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_logs WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) AND position(Body,'"'"'error'"'"')>0' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\terror | stats count() n' "$logs_url" "$sns" "$ens" ;;
      level_filter)     [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_logs WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) AND SeverityText='"'"'ERROR'"'"'' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\tlevel:ERROR | stats count() n' "$logs_url" "$sns" "$ens" ;;
      multi_filter)     [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_logs WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) AND SeverityText='"'"'ERROR'"'"' AND ServiceName='"'"'api-gateway'"'"'' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\tlevel:ERROR service.name:="api-gateway" | stats count() n' "$logs_url" "$sns" "$ens" ;;
      negation)         [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_logs WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) AND SeverityText!='"'"'INFO'"'"'' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\t-level:INFO | stats count() n' "$logs_url" "$sns" "$ens" ;;
      trace_lookup)     [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_logs WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) AND TraceId='"'"'%s'"'"'' "${EP[ch]}" "$ss" "$es" "$SAMPLE_TID" || printf 'POST\t%s?start=%s&end=%s\ttrace_id:=%s | stats count() n' "$logs_url" "$sns" "$ens" "$SAMPLE_TID" ;;
      high_card)        [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT TraceId,count() FROM lakehouse.otel_logs WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) GROUP BY TraceId' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\t* | stats by (trace_id) count()' "$logs_url" "$sns" "$ens" ;;
      scan)             [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT Body,ServiceName FROM lakehouse.otel_logs WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) LIMIT 1000' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\t* | fields _msg, service.name | limit 1000' "$logs_url" "$sns" "$ens" ;;
    esac
  else # traces. trace_id:* counts only REAL spans — VictoriaTraces also
       # returns internal `trace_id_idx_stream` index rows (fields
       # trace_id_idx, start_time, end_time, duration) that carry a
       # trace-level duration and no trace_id; LH/CH (reading the same
       # Parquet) correctly drop those, so without this filter the VT
       # baseline over-counts.
    case "$query" in
      count_total)      [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_traces WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s)' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\ttrace_id:* | stats count() n' "$traces_url" "$sns" "$ens" ;;
      count_by_service) [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT ServiceName,count() FROM lakehouse.otel_traces WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) GROUP BY ServiceName' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\ttrace_id:* | stats by (`resource_attr:service.name`) count() as n' "$traces_url" "$sns" "$ens" ;;
      service_filter)   [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_traces WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) AND ServiceName='"'"'api-gateway'"'"'' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\t`resource_attr:service.name`:="api-gateway" | stats count() as n' "$traces_url" "$sns" "$ens" ;;
      trace_by_id)      [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_traces WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) AND TraceId='"'"'%s'"'"'' "${EP[ch]}" "$ss" "$es" "$SAMPLE_TID" || printf 'POST\t%s?start=%s&end=%s\ttrace_id:=%s | stats count() n' "$traces_url" "$sns" "$ens" "$SAMPLE_TID" ;;
      span_name)        [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_traces WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) AND SpanName='"'"'HTTP GET /api/v1/users'"'"'' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\tname:="HTTP GET /api/v1/users" | stats count() as n' "$traces_url" "$sns" "$ens" ;;
      # 100ms was the original threshold; the seed's spans are 5-54ms
      # (cmd/datagen), so >50ms selects the slowest ~8% and is the largest
      # round number giving a non-zero, exactly-equal count on VT/LH/CH
      # (verified at 10/20/30/40/45/50/53ms — all matched exactly).
      # trace_id:* is REQUIRED here: VictoriaTraces' internal
      # `trace_id_idx_stream` index rows (fields trace_id_idx, start_time,
      # end_time, duration) carry a trace-level duration and no trace_id;
      # some of those exceed 100ms, so a bare `duration:>N` filter
      # over-counts on VT only (1075 vs LH's real 0) — confirmed empirically.
      slow_spans)       [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT count() FROM lakehouse.otel_traces WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) AND Duration>50000000' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\ttrace_id:* duration:>50000000 | stats count() as n' "$traces_url" "$sns" "$ens" ;;
      # trace_id, span_id are included in the projection (in addition to
      # name/service.name/duration) so extract_result's scan-key hash
      # (trace_id:span_id for traces) is actually meaningful —
      # without them every row's key degenerates to the same empty string,
      # so the hash is constant regardless of content (a false-negative that
      # can never catch a real divergence). Same filter/limit, so the count
      # and row selection are unaffected; only two extra small fields ride
      # along in the response.
      scan)             [[ "$sys" == clickhouse ]] && printf 'CH\t%s\tSELECT SpanName,ServiceName,Duration FROM lakehouse.otel_traces WHERE Timestamp>=fromUnixTimestamp(%s) AND Timestamp<fromUnixTimestamp(%s) LIMIT 1000' "${EP[ch]}" "$ss" "$es" || printf 'POST\t%s?start=%s&end=%s\ttrace_id:* | fields trace_id, span_id, name, `resource_attr:service.name`, duration | limit 1000' "$traces_url" "$sns" "$ens" ;;
    esac
  fi
}

LOG_QUERIES="count_total count_by_service fulltext level_filter multi_filter negation trace_lookup high_card scan"
TRACE_QUERIES="count_total count_by_service service_filter trace_by_id span_name slow_spans scan"
# --queries "a b c" overrides the per-signal list (intersected with what's valid
# for each signal), so a focused cold run can target just scan/count.
[[ -n "${QUERY_OVERRIDE:-}" ]] && { LOG_QUERIES="$QUERY_OVERRIDE"; TRACE_QUERIES="$QUERY_OVERRIDE"; }

# SAMPLE_TID: a real trace_id pulled from the data at runtime, used by the
# point-lookup edge cases (trace-by-id is the key trace-UX query — bloom/smartCache
# path). Filled by fetch_sample_tid before the matrix.
SAMPLE_TID="ffffffffffffffffffffffffffffffff"
fetch_sample_tid() {
  local tid
  tid=$(curl -sf --max-time 20 --data-urlencode 'query=trace_id:* | sort by (_time) desc | fields trace_id | limit 1' \
    --data-urlencode "start=$(start_ns 3600)" --data-urlencode "end=$(end_ns 3600)" \
    "${EP[lh_traces]}/select/logsql/query" 2>/dev/null | python3 -c "
import sys,json
for l in sys.stdin:
  l=l.strip()
  if l:
    try:
      t=json.loads(l).get('trace_id','')
      if t: print(t); break
    except: pass")
  if [[ -z "$tid" ]]; then
    log "FATAL: fetch_sample_tid found no trace_id in the last hour window (${EP[lh_traces]})"
    exit 1
  fi
  SAMPLE_TID="$tid"
  log "sample trace_id for point lookups: $SAMPLE_TID"
}
LOG_SYSTEMS="victorialogs lakehouse clickhouse"      # VL = baseline
TRACE_SYSTEMS="victoriatraces lakehouse clickhouse"  # VT = baseline

# --- orchestration ------------------------------------------------------------
up_stack() {
  log "bringing up benchmark stack (disk-profile=$DISK_PROFILE)…"
  docker compose "${COMPOSE_ARGS[@]}" up -d --build
  log "waiting for systems to report healthy…"
  local tries=0
  until curl -sf "${EP[lh_logs]}/health" >/dev/null 2>&1 && curl -sf "${EP[vl]}/health" >/dev/null 2>&1; do
    sleep 3; tries=$((tries+1)); (( tries > 60 )) && { log "stack did not become healthy"; return 1; }
  done
}
teardown() { rm -rf "$BENCH_TMP" 2>/dev/null || true; (( KEEP )) && { log "leaving stack up (--keep)"; return; }; log "tearing down stack…"; docker compose "${COMPOSE_ARGS[@]}" down -v >/dev/null 2>&1 || true; }
# The datagen-seed service (in the compose) backfills ~7d of logs+traces into
# VL/VT/LH at `up`. Wait for it to finish, then let LH flush to S3 so ClickHouse's
# s3() views see the Parquet; finally run the preflight data/parity check.
ingest() {
  (( DO_INGEST )) || return 0
  log "waiting for datagen-seed to backfill (~7d of data)…"
  local tries=0 state
  while :; do
    state=$(docker compose "${COMPOSE_ARGS[@]}" ps -a --format '{{.Service}} {{.State}}' 2>/dev/null | awk '$1=="datagen-seed"{print $2}')
    [[ "$state" == exited* || "$state" == "exited" ]] && break
    sleep 5; tries=$((tries+1)); (( tries > 180 )) && { log "datagen-seed not finished after 15m; continuing"; break; }
  done
  log "seed done; waiting for LH flush to CONVERGE to the baseline (so we measure equivalent data)…"
  local maxsecs; maxsecs=$(for r in $RANGES; do range_to_secs "$r"; done | sort -n | tail -1)
  for signal in $([[ "$SIGNALS" == both ]] && echo "logs traces" || echo "$SIGNALS"); do
    wait_for_flush "$signal" "$maxsecs"
  done
}

# logsql_count: total row count over [now-secs, now] from a LogsQL endpoint.
logsql_count() { # $1 base_url  $2 secs
  curl -sf --max-time 30 --data-urlencode "query=* | stats count() n" \
    --data-urlencode "start=$(start_ns "$2")" --data-urlencode "end=$(end_ns "$2")" \
    "$1/select/logsql/query" 2>/dev/null | python3 -c "
import sys,json
for l in sys.stdin:
  l=l.strip()
  if l:
    try: print(int(json.loads(l).get('n',0))); break
    except: pass
else: print(0)"
}

# wait_for_flush: wait until the LH cold tier's count over the widest measured
# window has SETTLED — either it converges to the VL/VT baseline (≥98%, real flush
# lag that resolves), or it stabilizes (3 unchanged polls) at a lower value, which
# means flush is COMPLETE and the shortfall is a genuine gap (e.g. ingest drop
# under a bursty seed, or the cardinality gate) — reported, and flagged per-cell by
# the validity check. Either way we stop measuring over still-moving data. CH reads
# LH's Parquet so it tracks LH.
wait_for_flush() { # $1 signal  $2 secs
  local signal="$1" secs="$2" base_url cold_url
  if [[ "$signal" == logs ]]; then base_url="${EP[vl]}"; cold_url="${EP[lh_logs]}"
  else base_url="${EP[vt]}"; cold_url="${EP[lh_traces]}"; fi
  local tries=0 base cold prev=-1 stable=0 ratio
  while (( tries < 90 )); do   # up to ~15min
    base=$(logsql_count "$base_url" "$secs"); cold=$(logsql_count "$cold_url" "$secs")
    if [[ "$base" =~ ^[0-9]+$ && "$cold" =~ ^[0-9]+$ && "$base" -gt 0 ]]; then
      ratio=$(awk "BEGIN{printf \"%.3f\",$cold/$base}")
      printf '    %-7s baseline=%-8s LH=%-8s ratio=%s\n' "$signal" "$base" "$cold" "$ratio" >&2
      awk "BEGIN{exit !($cold/$base >= 0.98)}" && { log "flush converged ($signal): LH=$cold ≈ baseline=$base"; return 0; }
      if [[ "$cold" == "$prev" ]]; then
        stable=$((stable+1))
        (( stable >= 3 )) && { log "flush SETTLED ($signal) but LH=$cold is only ratio=$ratio of baseline=$base — REAL gap, not lag (cells will be flagged ✗ where it matters)"; return 0; }
      else stable=0; prev="$cold"; fi
    fi
    sleep 10; tries=$((tries+1))
  done
  log "WARN: $signal did not settle in 15m (LH=$cold vs baseline=$base); proceeding"
}
set_latency() { local ms="$1"; if [[ "$ms" == 0 ]]; then log "S3 latency: passthrough (0ms)"; scripts/inject-s3-latency.sh 0 0 >/dev/null 2>&1 || true; else log "S3 latency: ${ms}ms"; scripts/inject-s3-latency.sh "$ms" "$((ms/3))" >/dev/null 2>&1 || true; fi; }

# Parity gate: count_total over 24h must agree across systems (within tolerance)
# so the benchmark compares EQUAL answers (incl. ClickHouse). ENFORCING: returns 1
# (and the caller aborts the run) on any mismatch beyond ±5% of the baseline.
parity_gate() {
  local signal="$1" secs; secs=$(range_to_secs 24h)
  log "parity gate ($signal, 24h): count_total across systems…"
  local sysset; [[ "$signal" == logs ]] && sysset="$LOG_SYSTEMS" || sysset="$TRACE_SYSTEMS"
  local base="" s n mismatch=0
  for s in $sysset; do
    local m u b
    read -r m u b <<<"$(prep "$signal" count_total "$s" "$secs")"
    n=$(fetch_scalar "$m" "$u" "$b" count_total "$s")
    printf '    %-16s count=%s\n' "$s" "${n:-ERR}" >&2
    if [[ -z "$base" ]]; then
      base="$n"
      log "parity gate ($signal): baseline ($s) = $base"
      if [[ ! "$base" =~ ^[0-9]+$ ]] || [[ "$base" == 0 ]]; then
        log "parity gate ($signal) MISMATCH: baseline ($s) is empty/non-numeric ('$base') — refusing to sweep on empty data"
        mismatch=1
      fi
      continue
    fi
    if [[ ! "$n" =~ ^[0-9]+$ ]]; then
      log "parity gate ($signal) MISMATCH: $s returned non-numeric/ERR ('${n:-}') vs baseline=$base"
      mismatch=1
    elif [[ ! "$base" =~ ^[0-9]+$ ]] || [[ "$base" == 0 ]]; then
      : # baseline already flagged empty/non-numeric above; skip the
        # percentage comparison (would divide by zero for base=0)
    else
      local diff_pct
      diff_pct=$(awk -v n="$n" -v b="$base" 'BEGIN{d=n-b; if(d<0) d=-d; printf "%.4f", d/b}')
      if awk -v d="$diff_pct" 'BEGIN{exit !(d > 0.05)}'; then
        log "parity gate ($signal) MISMATCH: $s=$n vs baseline=$base (Δ=${diff_pct}, > 5%)"
        mismatch=1
      fi
    fi
  done
  return "$mismatch"
}

# --- run --------------------------------------------------------------------
trap teardown EXIT
(( DO_UP )) && { up_stack || exit 1; }
ingest
fetch_sample_tid

RESULTS="["; first=1
for lat in $S3_LATENCIES; do
  set_latency "$lat"
  for signal in $([[ "$SIGNALS" == both ]] && echo "logs traces" || echo "$SIGNALS"); do
    if ! parity_gate "$signal"; then
      log "parity gate FAILED for $signal — aborting (no sweep on unequal or empty data)"
      exit 1
    fi
    queries=$([[ "$signal" == logs ]] && echo "$LOG_QUERIES" || echo "$TRACE_QUERIES")
    systems=$([[ "$signal" == logs ]] && echo "$LOG_SYSTEMS" || echo "$TRACE_SYSTEMS")
    for range in $RANGES; do
      secs=$(range_to_secs "$range")
      for q in $queries; do
        for sys in $systems; do
          IFS=$'\t' read -r method url body <<<"$(prep "$signal" "$q" "$sys" "$secs")"; unset IFS
          [[ -z "${method:-}" ]] && continue
          row=$(measure_query "${signal}/${q}/${range}/lat${lat}" "$sys" "$method" "$url" "$body" "$q")
          row=$(python3 -c "import sys,json;d=json.loads(sys.argv[1]);d.update(signal='$signal',query='$q',range='$range',latency_ms=$lat,disk_profile='$DISK_PROFILE');print(json.dumps(d))" "$row")
          (( first )) && first=0 || RESULTS+=","
          RESULTS+="$row"
          # valid=<k>/<N> always follows p95 so a 1/20 cell can never read as
          # a clean latency; invalid_reasons is only appended when non-empty.
          IFS=$'\x1f' read -r p95 ivalid itotal ireasons <<<"$(python3 -c "
import sys, json
d = json.loads(sys.argv[1])
v, iv = d.get('iters_valid'), d.get('iters_invalid')
tot = (v or 0) + (iv or 0)
print('\x1f'.join([str(d.get('p95_ms')), str(v if v is not None else ''), str(tot), d.get('invalid_reasons') or '']))
" "$row")"
          unset IFS
          if [[ -n "$ireasons" ]]; then
            printf '    %-34s %-16s p95=%s ms valid=%s/%s invalid_reasons=%s\n' "${signal}/${q}/${range}/lat${lat}" "$sys" "$p95" "$ivalid" "$itotal" "$ireasons" >&2
          else
            printf '    %-34s %-16s p95=%s ms valid=%s/%s\n' "${signal}/${q}/${range}/lat${lat}" "$sys" "$p95" "$ivalid" "$itotal" >&2
          fi
        done
      done
    done
  done
done
RESULTS+="]"
echo "$RESULTS" | python3 -m json.tool > "$OUTPUT"
log "raw results -> $OUTPUT"

# --- report: markdown table normalized to the VL/VT baseline ------------------
python3 scripts/bench/report.py "$OUTPUT" "${OUTPUT%.json}.md" && log "report -> ${OUTPUT%.json}.md"
