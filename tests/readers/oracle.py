#!/usr/bin/env python3
"""Record Lakehouse's own answers for the fixed query set: the oracle every external
engine is compared against.

  oracle.py record <layer> <out.json> [--params params.json]

The query parameters (time window, trace id, day) come from the writer-side truth
(truth.py build), so Lakehouse is asked exactly what the engines are asked; the answers
are then compared with that truth (truth.py compare-oracle).
"""
import json
import sys
from datetime import datetime, timedelta, timezone

import lib


def answers(signal, tenant, p):
    q = lambda s: lib.lh_scalar(signal, tenant, s)
    t0 = lib.rfc3339_ns(p["from_ns"])
    t1 = lib.rfc3339_ns(p["to_ns"])
    d0 = datetime.strptime(p["dt"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    d1 = d0 + timedelta(days=1)
    svc = "service.name" if signal == "logs" else '"resource_attr:service.name"'
    if signal == "logs":
        ff = 'level:=ERROR service.name:=api-gateway | stats count() c'
        mf = 'format:=nginx | stats count() c'
    else:
        ff = '"resource_attr:service.name":=api-gateway status_code:=2 | stats count() c'
        mf = '"span_attr:rpc.system":=grpc | stats count() c'
    mm = lib.lh_query(signal, tenant, "* | stats min(_time) mn, max(_time) mx")[0]
    a = {
        "count": q("* | stats count() c"),
        "by_service": {r["service.name" if signal == "logs" else "resource_attr:service.name"]: int(r["c"])
                       for r in lib.lh_query(signal, tenant, "* | stats by (%s) count() c" % svc)},
        "field_filter": q(ff),
        "time_range": q("_time:[%s,%s) | stats count() c" % (t0, t1)),
        "map_filter": q(mf),
        "trace_by_id": q("trace_id:=%s | stats count() c" % p["trace_id"]),
        "dt_filter": q("_time:[%sT00:00:00Z,%sT00:00:00Z) | stats count() c" % (d0.strftime("%Y-%m-%d"), d1.strftime("%Y-%m-%d"))),
        "ts_bounds": [lib.parse_rfc3339_ns(mm["mn"]), lib.parse_rfc3339_ns(mm["mx"])],
        "utc_check": 0,
        "tenant": [[tenant["account"], tenant["project"]]],
    }
    if signal == "traces":
        j = lib.jaeger_span_count(tenant, p["trace_id"])
        assert j == a["trace_by_id"], "Jaeger and LogsQL disagree on the span count of %s: %d vs %d" % (p["trace_id"], j, a["trace_by_id"])
    return a


def compare(a_path, b_path):
    """Compaction must not change any answer Lakehouse gives."""
    a, b = lib.read_json(a_path), lib.read_json(b_path)
    diffs = [(k, q, a["answers"][k][q], b["answers"][k][q]) for k in a["answers"] for q in a["answers"][k]
             if a["answers"][k][q] != b["answers"][k][q]]
    if diffs:
        sys.exit("Lakehouse answers changed across compaction: %s" % diffs)
    print("oracle: Lakehouse answers are identical before and after compaction (%d cells)" % len(a["answers"]))


def main():
    if sys.argv[1] == "compare":
        return compare(sys.argv[2], sys.argv[3])
    layer, out = sys.argv[2], sys.argv[3]
    assert len(sys.argv) > 5 and sys.argv[4] == "--params", "oracle.py record <layer> <out.json> --params <truth.json>"
    params = lib.read_json(sys.argv[5])
    res = {"layer": layer, "params": {}, "answers": {}}
    for tn, tenant in lib.TENANTS.items():
        for sig in lib.SIGNALS:
            key = "%s/%s" % (tn, sig)
            p = params["params"][key]
            res["params"][key] = p
            res["answers"][key] = answers(sig, tenant, p)
    lib.write_json(out, res, indent=1, sort_keys=True)
    print("oracle recorded:", out)
    for k, a in sorted(res["answers"].items()):
        print("  %-16s count=%d trace_by_id=%d time_range=%d" % (k, a["count"], a["trace_by_id"], a["time_range"]))


if __name__ == "__main__":
    main()
