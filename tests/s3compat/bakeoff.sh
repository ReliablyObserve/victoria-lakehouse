#!/usr/bin/env bash
# Run the s3compat harness against one docker-compose candidate and record
# cold-start, idle RSS and image size.
#
#   tests/s3compat/bakeoff.sh <minio|rustfs|seaweedfs|versitygw|garage> [out-dir]
#
# Uses compose project "s3bake-<candidate>" and host ports 39000-39199 only.
# Tears the project down at the end unless KEEP=1.
set -uo pipefail

cand=${1:?candidate}
out=${2:-/tmp/s3bake-out}
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
mkdir -p "$out"
proj="s3bake-$cand"
cf="$here/compose/$cand.yml"

key=s3bakekey; secret=s3bakesecret
case $cand in
  minio)     port=39000 ;;
  rustfs)    port=39010 ;;
  seaweedfs) port=39020 ;;
  versitygw) port=39030 ;;
  garage)    port=39040; key=GK0123456789abcdef01234567
             secret=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef ;;
  *) echo "unknown candidate $cand" >&2; exit 2 ;;
esac
endpoint="http://127.0.0.1:$port"
bin="$out/s3compat.test"
(cd "$root" && GOWORK=off go test -c -o "$bin" ./tests/s3compat) || exit 1

now_ms() { python3 -c 'import time;print(int(time.time()*1000))'; }

docker compose -p "$proj" -f "$cf" down -v >/dev/null 2>&1
t0=$(now_ms)
docker compose -p "$proj" -f "$cf" up -d >/dev/null 2>&1 || { echo "compose up failed"; exit 1; }
if [ "$cand" = garage ]; then "$here/compose/garage-init.sh" "$proj" s3compat lhsmoke >"$out/$cand.init.log" 2>&1; fi
ready=""
for _ in $(seq 1 240); do
  if S3COMPAT_ENDPOINT=$endpoint S3COMPAT_KEY=$key S3COMPAT_SECRET=$secret \
     "$bin" -test.run '^TestProd_PutGetRoundtrip$' -test.count=1 -test.timeout 20s >/dev/null 2>&1; then
    ready=$(( $(now_ms) - t0 )); break
  fi
  sleep 0.25
done
echo "cold_start_ms=$ready" | tee "$out/$cand.coldstart"
[ -z "$ready" ] && { echo "never became ready"; docker compose -p "$proj" -f "$cf" logs --tail 40 >"$out/$cand.logs" 2>&1; exit 1; }

S3COMPAT_ENDPOINT=$endpoint S3COMPAT_KEY=$key S3COMPAT_SECRET=$secret \
  "$bin" -test.v -test.count=1 -test.timeout 30m >"$out/$cand.test.txt" 2>&1
echo "exit=$?" >>"$out/$cand.test.txt"
grep -E '^\s*--- (PASS|FAIL|SKIP)' "$out/$cand.test.txt" >"$out/$cand.results.txt"

# Lakehouse end-to-end durability smoke (own bucket: Lakehouse writes tenant data
# at the bucket root regardless of -lakehouse.s3.prefix, so runs must not share a bucket).
if [ -x "${LH_BIN:-/tmp/lakehouse-logs}" ]; then
  S3COMPAT_ENDPOINT=$endpoint S3COMPAT_KEY=$key S3COMPAT_SECRET=$secret S3COMPAT_BUCKET=lhsmoke \
    "$bin" -test.run '^TestProd_PutGetRoundtrip$' -test.count=1 >/dev/null 2>&1
  "$here/lakehouse-smoke.sh" "$endpoint" "$key" "$secret" lhsmoke "$out/$cand.lh" >"$out/$cand.smoke.txt" 2>&1
  echo "smoke: $(grep -c 'CHECK .* PASS' "$out/$cand.smoke.txt") pass, $(grep -c 'CHECK .* FAIL' "$out/$cand.smoke.txt") fail"
fi

sleep 5
docker stats --no-stream --format '{{.Name}} mem={{.MemUsage}} cpu={{.CPUPerc}}' $(docker compose -p "$proj" -f "$cf" ps -q) >"$out/$cand.stats" 2>&1
cat "$out/$cand.stats"
# Optional ceph/s3-tests subset (S3TESTS=1); runs after the RSS sample so it does not inflate it.
if [ "${S3TESTS:-0}" = 1 ]; then
  "$here/s3tests.sh" "127.0.0.1:$port" "$key" "$secret" "$out/$cand.s3tests" >"$out/$cand.s3tests.log" 2>&1
  cat "$out/$cand.s3tests.log"
fi
[ "${KEEP:-0}" = 1 ] || docker compose -p "$proj" -f "$cf" down -v >/dev/null 2>&1
echo "pass=$(grep -c -- '--- PASS' "$out/$cand.results.txt") fail=$(grep -c -- '--- FAIL' "$out/$cand.results.txt") skip=$(grep -c -- '--- SKIP' "$out/$cand.results.txt")"
