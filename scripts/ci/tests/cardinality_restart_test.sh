#!/usr/bin/env bash
# Real-binary check of #389: rows acknowledged under a tenant's stream cardinality
# limit are not dropped at the buffer drain after a restart. It starts a
# lakehouse-logs or lakehouse-traces binary with cardinality.max_streams: 1 for
# tenant 1:2, acknowledges rows of stream "a", stops the process (SIGTERM, the
# buffer is closed), starts it again with an empty limiter, sends rows of stream
# "b" first (it takes the tenant's only slot) and then new rows of "a", and reads
# everything back after the buffer has drained and the segment retired.
#
# Expected (the registry cells lh.durability.restart_cardinality_gate_*):
#   the rows of "a" acknowledged before the restart: stored (3)
#   the rows of "b": stored (3)
#   the new rows of "a": not stored, counted in
#     lakehouse_insert_rows_dropped_total{reason="cardinality_limit"} (2)   [the known gap of #485]
# A binary that judges the rows again at the drain (the code before the fix)
# loses the three rows of "a"; the counter does not exist in it.
#
# Usage: BIN=<binary> KIND=logs|traces S3_ENDPOINT=http://127.0.0.1:9000 \
#   S3_BUCKET=<existing bucket> S3_ACCESS_KEY=... S3_SECRET_KEY=... PORT=<free port> \
#   scripts/ci/tests/cardinality_restart_test.sh
# Optional: WORKDIR (default: a temp dir), LH_WAIT_SECS (default 75: the flush
# interval of 5 s plus the grace of the committed segment).
set -u
: "${BIN:?}" "${KIND:?}" "${S3_ENDPOINT:?}" "${S3_BUCKET:?}" "${S3_ACCESS_KEY:?}" "${S3_SECRET_KEY:?}" "${PORT:?}"
LH_WAIT_SECS="${LH_WAIT_SECS:-75}"
work="${WORKDIR:-$(mktemp -d)}"
mkdir -p "$work"
url="http://127.0.0.1:$PORT"
pid=""
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null; [ -z "${WORKDIR:-}" ] && rm -rf "$work"; }
trap cleanup EXIT

cat >"$work/lh.yml" <<YAML
lakehouse:
  insert:
    buffer_dir: $work/buffer
    buffer_flush_interval: 5s
  tenant:
    overrides:
      "1:2":
        cardinality:
          max_streams: 1
YAML

start() {
	"$BIN" -httpListenAddr=":$PORT" -storageDataPath="$work/data" -lakehouse.config="$work/lh.yml" \
		-lakehouse.s3.bucket="$S3_BUCKET" -lakehouse.s3.endpoint="$S3_ENDPOINT" \
		-lakehouse.s3.access-key="$S3_ACCESS_KEY" -lakehouse.s3.secret-key="$S3_SECRET_KEY" -lakehouse.s3.force-path-style=true \
		-lakehouse.manifest.refresh-interval=5s -lakehouse.cache.disk-path="$work/cache" -loggerLevel=INFO >>"$work/run.log" 2>&1 &
	pid=$!
	for _ in $(seq 1 600); do
		curl -sf -o /dev/null "$url/health" && return 0
		sleep 0.05
	done
	echo "the binary did not become ready"; exit 1
}
stop() { kill -TERM "$pid"; wait "$pid" 2>/dev/null; pid=""; }

now_ns() { python3 -c 'import time; print(int(time.time()*1e9))'; }
# send <stream> <prefix> <count>
send() {
	local stream="$1" prefix="$2" count="$3" i body=""
	if [ "$KIND" = logs ]; then
		for i in $(seq 1 "$count"); do body+="{\"app\":\"$stream\",\"_msg\":\"$prefix-$i\"}"$'\n'; done
		printf '%s' "$body" | curl -s -o /dev/null -w '%{http_code}' -H 'AccountID: 1' -H 'ProjectID: 2' -H 'Content-Type: application/stream+json' \
			-X POST --data-binary @- "$url/insert/jsonline?_stream_fields=app&_msg_field=_msg"
	else
		# A span's stream is its service name and its span name, so the stream is
		# named by the span name and each span carries its own marker as an attribute.
		local spans="" n
		n="$(now_ns)"
		for i in $(seq 1 "$count"); do
			spans+="{\"traceId\":\"$(printf '%032x' $((RANDOM * 65536 + RANDOM + i)))\",\"spanId\":\"$(printf '%016x' $((RANDOM + i)))\",\"name\":\"$stream\",\"kind\":1,\"startTimeUnixNano\":\"$n\",\"endTimeUnixNano\":\"$((n + 1000000))\",\"attributes\":[{\"key\":\"tag\",\"value\":{\"stringValue\":\"$prefix-$i\"}}]},"
		done
		curl -s -o /dev/null -w '%{http_code}' -H 'AccountID: 1' -H 'ProjectID: 2' -H 'Content-Type: application/json' -X POST \
			-d "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"svc\"}}]},\"scopeSpans\":[{\"spans\":[${spans%,}]}]}]}" \
			"$url/insert/opentelemetry/v1/traces"
	fi
}
# stored <prefix> -> number of rows whose message/name starts with prefix
stored() {
	local q
	if [ "$KIND" = logs ]; then q="_msg:~\"^$1-\""; else q="\"span_attr:tag\":~\"^$1-\""; fi
	curl -s "$url/select/logsql/query" -H 'AccountID: 1' -H 'ProjectID: 2' --data-urlencode "query=$q" \
		--data-urlencode 'start=2000-01-01T00:00:00Z' --data-urlencode 'end=2100-01-01T00:00:00Z' --data-urlencode 'disable_latency_offset=1' | grep -c .
}
metric() { curl -s "$url/metrics" | awk -v n="$1" 'index($0, n) == 1 { v = $NF } END { print (v == "" ? 0 : v) }'; }

fails=0
check() { # name expected got
	if [ "$2" = "$3" ]; then printf '%-8s %-52s ok (%s)\n' "$KIND" "$1" "$3"; else printf '%-8s %-52s FAIL: expected %s, got %s\n' "$KIND" "$1" "$2" "$3"; fails=$((fails + 1)); fi
}

start
c1="$(send a before 3)"
sleep 1
stop
start # a new process: its limiter is empty
c2="$(send b other 3)"
c3="$(send a after 2)"
check "all requests answered 2xx" "200,200,200" "$c1,$c2,$c3"
sleep "$LH_WAIT_SECS"
check "rows of a acknowledged before the restart are stored" 3 "$(stored before)"
check "rows of b are stored" 3 "$(stored other)"
check "new rows of a are not stored (known gap #485)" 0 "$(stored after)"
n="$(metric 'lakehouse_insert_rows_dropped_total{reason="cardinality_limit"}')"
check "new rows of a counted as refused for the limit" 2 "${n%.*}"

if [ "$fails" -gt 0 ]; then echo "$fails check(s) failed"; exit 1; fi
echo "all checks passed"
