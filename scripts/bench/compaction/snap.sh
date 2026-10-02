#!/usr/bin/env bash
# snap.sh: object layout of both buckets. Prints one JSON line per
# (build, signal, tenant, partition): objects, bytes, top compaction level.
# The level is read from the key (compacted-L<N>-...); a flushed file is L0.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
dc=(docker compose -p lhcmp343 -f "$here/compose.yml")
for pair in main:lhm343 pr:lhp343; do
  build=${pair%%:*}; bucket=${pair##*:}
  "${dc[@]}" run --rm --no-deps -e "MC_HOST_l=http://minioadmin:minioadmin@s3:9000" --entrypoint mc s3-init \
    --json ls -r "l/$bucket" | BUILD=$build python3 -c '
import json, os, re, sys, collections
build = os.environ["BUILD"]
rx = re.compile(r"^(\d+)/(\d+)/(logs|traces)/dt=([^/]+)/hour=(\d+)/(.*\.parquet)$")
agg = collections.OrderedDict()
for line in sys.stdin:
    try:
        o = json.loads(line)
    except ValueError:
        continue
    m = rx.match(o.get("key", ""))
    if not m:
        continue
    acct, proj, sig, dt, hour, name = m.groups()
    lv = re.search(r"compacted-L(\d+)-", name)
    level = int(lv.group(1)) if lv else 0
    k = (sig, f"{acct}:{proj}", f"dt={dt}/hour={hour}")
    a = agg.setdefault(k, {"objects": 0, "bytes": 0, "top_level": 0})
    a["objects"] += 1; a["bytes"] += o.get("size", 0); a["top_level"] = max(a["top_level"], level)
for (sig, tenant, part), a in sorted(agg.items()):
    print(json.dumps({"build": build, "signal": sig, "tenant": tenant, "partition": part, **a}))
'
done
