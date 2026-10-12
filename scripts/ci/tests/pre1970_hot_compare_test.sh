#!/usr/bin/env bash
# Compares what hot VictoriaLogs / VictoriaTraces and the Lakehouse do with rows
# whose _time is before 1970 (issue #484), by sending the same requests to each
# and reading back the answers. Every answer is validated: a "not stored" result
# only counts when the same instance stored the control row sent with it.
#
# Targets are given as comma-separated kind=url lists:
#   LOGS_TARGETS   hot-default=http://h1:9428,hot-100y=http://h2:9428,lh=http://l:9428
#   TRACES_TARGETS hot-default=http://h3:10428,hot-100y=http://h4:10428,lh=http://l2:10428
# kinds: hot-default (upstream with its default -retentionPeriod), hot-100y
# (-retentionPeriod=100y), lh (the Lakehouse binary under test; set LH_WAIT_SECS
# to the time its buffer takes to flush and retire the segment, default 75, so
# the row is read after the drain and not from the buffer).
#
# Expected (the registry cells lh.durability.pre1970_stored_*):
#   logs   jsonline row of 1965   hot-default: dropped   hot-100y: stored   lh: refused, counted
#   traces /insert/native span with a 1965 _time   hot-default: dropped   hot-100y: stored   lh: refused, counted
#   traces OTLP span with an end time above 2^63-1 ns   every system: not stored (VictoriaTraces itself drops it)
# Prints one line per check and exits 1 if any check fails (nothing is skipped).
#
# To bring the hot instances up (a throwaway compose project, tear down with
# `docker compose -p <project> down -v`):
#   see pre1970_hot_compare.compose.yml in this directory
set -u

LH_WAIT_SECS="${LH_WAIT_SECS:-75}"
HOT_WAIT_SECS="${HOT_WAIT_SECS:-25}"
# Every target must answer its readiness endpoint before anything is sent to it
# (/health for upstream, /ready for the Lakehouse): a request sent to a process
# that is still starting is lost, and the control row then fails the check for a
# reason that has nothing to do with the system under test.
READY_WAIT_SECS="${READY_WAIT_SECS:-90}"
fails=0
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

say() { printf '%-9s %-12s %-46s %s\n' "$1" "$2" "$3" "$4"; }
check() { # signal kind name expected(bool) got(bool)
	if [ "$4" = "$5" ]; then say "$1" "$2" "$3" "ok ($5)"; else say "$1" "$2" "$3" "FAIL: expected $4, got $5"; fails=$((fails + 1)); fi
}
wait_ready() { # kind url: waits for readiness, counts a failure when it never comes
	local kind="$1" url="$2" path=/health i
	[ "$kind" = lh ] && path=/ready
	for ((i = 0; i < READY_WAIT_SECS; i++)); do
		if [ "$(curl -s -o /dev/null -w '%{http_code}' "$url$path")" = 200 ]; then return 0; fi
		sleep 1
	done
	say "$kind" "$kind" "ready within ${READY_WAIT_SECS}s ($url$path)" "FAIL: never answered 200"
	fails=$((fails + 1))
	return 1
}
metric() { # url name -> value or 0
	curl -s "$1/metrics" | awk -v n="$2" 'index($0, n) == 1 { v = $NF } END { print (v == "" ? 0 : v) }'
}

# --- logs -----------------------------------------------------------------
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
IFS=',' read -r -a logs_targets <<<"${LOGS_TARGETS:-}"
for t in "${logs_targets[@]:-}"; do
	[ -n "$t" ] || continue
	kind="${t%%=*}"; url="${t#*=}"
	tag="$kind-$$"
	wait_ready "$kind" "$url" || continue
	printf '{"_time":"1965-03-01T12:00:00Z","app":"%s","_msg":"old-1965"}\n{"_time":"%s","app":"%s","_msg":"control-now"}\n' "$tag" "$now" "$tag" |
		curl -s -o /dev/null -H 'Content-Type: application/stream+json' -X POST --data-binary @- "$url/insert/jsonline?_stream_fields=app&_time_field=_time&_msg_field=_msg"
done
if [ "${#logs_targets[@]}" -gt 0 ]; then sleep "$HOT_WAIT_SECS"; fi
for t in "${logs_targets[@]:-}"; do
	[ -n "$t" ] || continue
	kind="${t%%=*}"; url="${t#*=}"
	tag="$kind-$$"
	[ "$kind" = lh ] && sleep "$LH_WAIT_SECS"
	out="$(curl -s "$url/select/logsql/query" --data-urlencode "query=app:=$tag" --data-urlencode 'start=1900-01-01T00:00:00Z' --data-urlencode 'end=2100-01-01T00:00:00Z')"
	has_ctl=false; has_old=false
	grep -q 'control-now' <<<"$out" && has_ctl=true
	grep -q 'old-1965' <<<"$out" && has_old=true
	check logs "$kind" "control row stored (proof the answer is valid)" true "$has_ctl"
	case "$kind" in
	hot-default) check logs "$kind" "row of 1965 stored" false "$has_old" ;;
	hot-100y) check logs "$kind" "row of 1965 stored" true "$has_old" ;;
	lh)
		check logs "$kind" "row of 1965 stored" false "$has_old"
		n="$(metric "$url" 'lakehouse_insert_rows_dropped_total{reason="too_small_timestamp"}')"
		got=false; [ "${n%.*}" -ge 1 ] && got=true
		check logs "$kind" "refusal counted (lakehouse_insert_rows_dropped_total)" true "$got"
		;;
	esac
done

# --- traces ---------------------------------------------------------------
IFS=',' read -r -a traces_targets <<<"${TRACES_TARGETS:-}"
if [ "${#traces_targets[@]}" -gt 0 ] && [ -n "${traces_targets[0]}" ]; then
	repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
	cat >"$tmp/nativerow.go" <<'GO'
package main

import (
	"os"
	"strconv"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// nativerow <unix-ns> <trace id>: the body of one /insert/native row.
func main() {
	ts, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		panic(err)
	}
	ir := logstorage.GetInsertRow()
	ir.Timestamp = ts
	st := logstorage.GetStreamTags()
	st.Add("resource_attr:service.name", "pre1970")
	st.Add("name", "op")
	ir.StreamTagsCanonical = string(st.MarshalCanonical(nil))
	ir.Fields = []logstorage.Field{
		{Name: "resource_attr:service.name", Value: "pre1970"}, {Name: "name", Value: "op"},
		{Name: "span_id", Value: "00f067aa0ba902b7"}, {Name: "trace_id", Value: os.Args[2]},
	}
	os.Stdout.Write(ir.Marshal(nil))
}
GO
	(cd "$repo/lakehouse-traces" && GOWORK=off go build -o "$tmp/nativerow" "$tmp/nativerow.go") || { echo "cannot build the native-row helper"; exit 1; }
	nowns="$(python3 -c 'import time; print(int(time.time()*1e9))')"
	old_ns=-157766400000000000                  # 1965-01-01T00:00:00Z
	wrapped=$(python3 -c 'print(2**64 - 5_000_000_000)') # an OTLP time above 2^63-1
	for t in "${traces_targets[@]}"; do
		kind="${t%%=*}"; url="${t#*=}"
		h="$(python3 -c 'import hashlib, sys; print(hashlib.md5(sys.argv[1].encode()).hexdigest()[:26])' "$kind$$")"
		wait_ready "$kind" "$url" || continue
		for spec in "old:$old_ns:aa$h" "ctl:$nowns:bb$h"; do
			IFS=: read -r _ ts id <<<"$spec"
			"$tmp/nativerow" "$ts" "${id}0000" | curl -s -o /dev/null -H 'Content-Type: application/octet-stream' -X POST --data-binary @- "$url/insert/native?version=v1"
		done
		for spec in "otlpw:$wrapped:cc$h" "otlpc:$nowns:dd$h"; do
			IFS=: read -r _ end id <<<"$spec"
			python3 - "${id}0000" "$nowns" "$end" >"$tmp/otlp.json" <<'PY'
import json, sys
tid, start, end = sys.argv[1:4]
print(json.dumps({"resourceSpans": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "pre1970"}}]},
  "scopeSpans": [{"scope": {"name": "t"}, "spans": [{"traceId": tid, "spanId": "00f067aa0ba902b7", "name": "op", "kind": 1,
  "startTimeUnixNano": start, "endTimeUnixNano": end}]}]}]}))
PY
			curl -s -o /dev/null -H 'Content-Type: application/json' -X POST --data-binary @"$tmp/otlp.json" "$url/insert/opentelemetry/v1/traces"
		done
	done
	sleep "$HOT_WAIT_SECS"
	for t in "${traces_targets[@]}"; do
		kind="${t%%=*}"; url="${t#*=}"
		h="$(python3 -c 'import hashlib, sys; print(hashlib.md5(sys.argv[1].encode()).hexdigest()[:26])' "$kind$$")"
		[ "$kind" = lh ] && sleep "$LH_WAIT_SECS"
		out="$(curl -s "$url/select/logsql/query" --data-urlencode "query=trace_id:in(\"aa${h}0000\",\"bb${h}0000\",\"cc${h}0000\",\"dd${h}0000\") | fields trace_id" --data-urlencode 'start=1900-01-01T00:00:00Z' --data-urlencode 'end=2100-01-01T00:00:00Z' --data-urlencode 'disable_latency_offset=1')"
		has() { if grep -q "$1${h}0000" <<<"$out"; then echo true; else echo false; fi; }
		check traces "$kind" "native control span stored (proof the answer is valid)" true "$(has bb)"
		check traces "$kind" "OTLP control span stored (proof the answer is valid)" true "$(has dd)"
		case "$kind" in
		hot-default) check traces "$kind" "native span of 1965 stored" false "$(has aa)" ;;
		hot-100y) check traces "$kind" "native span of 1965 stored" true "$(has aa)" ;;
		lh)
			check traces "$kind" "native span of 1965 stored" false "$(has aa)"
			n="$(metric "$url" 'lakehouse_insert_rows_dropped_total{reason="too_small_timestamp"}')"
			got=false; [ "${n%.*}" -ge 1 ] && got=true
			check traces "$kind" "refusal counted (lakehouse_insert_rows_dropped_total)" true "$got"
			;;
		esac
		check traces "$kind" "OTLP span with an end time above 2^63-1 stored" false "$(has cc)"
	done
fi

if [ "$fails" -gt 0 ]; then echo "$fails check(s) failed"; exit 1; fi
echo "all checks passed"
