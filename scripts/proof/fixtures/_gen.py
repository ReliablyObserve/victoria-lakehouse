"""Generate the offline fixture cases under scripts/proof/fixtures/cases/.

The bodies are synthesised in the wire formats of VictoriaLogs LogsQL
(query, stats_query, stats_query_range, hits, field_values, field_names,
streams), VictoriaTraces rows, Jaeger (trace by id, search, services) and
Tempo (OTLP trace by id, search, metrics) and Loki (query_range matrix).
Run: python3 scripts/proof/fixtures/_gen.py   (deterministic output)

Each case directory holds meta.json (surface, kind, options and the
expectation), ref.json, base.json and pr.json answers.
"""
from __future__ import annotations

import copy
import json
import os
import random
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "cases")
T0 = 1_790_000_000  # 2026-09-21T12:53:20Z, hour-aligned windows are not needed for fixtures


def iso(sec: int, ns: int = 0) -> str:
    import datetime
    d = datetime.datetime.fromtimestamp(sec, datetime.timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S") + f".{ns:09d}Z"


def ans(body, status=200, ms=10.0, **extra):
    return {"status": status, "latency_ms": ms, "body": body, **extra}


def ndjson(rows):
    return "\n".join(json.dumps(r, sort_keys=True) for r in rows) + ("\n" if rows else "")


def write(surface, name, meta, ref=None, base=None, pr=None, extra=None, resamples=None):
    d = os.path.join(OUT, surface, name)
    os.makedirs(d, exist_ok=True)
    meta = {"id": f"{surface}/{name}", "surface": surface, **meta}
    with open(os.path.join(d, "meta.json"), "w") as f:
        json.dump(meta, f, indent=1, sort_keys=True)
        f.write("\n")
    files = {"ref": ref, "base": base, "pr": pr, **(extra or {})}
    for k, v in files.items():
        if v is not None:
            with open(os.path.join(d, k + ".json"), "w") as f:
                json.dump(v, f, indent=1, sort_keys=True)
                f.write("\n")
    for i, rs in enumerate(resamples or [], 1):
        sd = os.path.join(d, f"resample-{i}")
        os.makedirs(sd, exist_ok=True)
        for k, v in rs.items():
            with open(os.path.join(sd, k + ".json"), "w") as f:
                json.dump(v, f, indent=1, sort_keys=True)
                f.write("\n")


def log_rows(n=40, seed=1):
    rnd = random.Random(seed)
    rows = []
    for i in range(n):
        rows.append({
            "_time": iso(T0 + i * 7, rnd.randrange(0, 999) * 1000),
            "_stream_id": f"0000000000000000{(i % 4):016x}",
            "_stream": '{app="api",pod="p%d"}' % (i % 4),
            "_msg": f"request {i} handled in {rnd.randrange(1, 90)}ms",
            "level": rnd.choice(["info", "warn", "error"]),
            "service": rnd.choice(["api", "web", "db"]),
        })
    rows.sort(key=lambda r: r["_time"], reverse=True)
    return rows


def span_rows(n=24, seed=2, with_extra=True):
    rnd = random.Random(seed)
    rows = []
    for i in range(n):
        r = {
            "_time": iso(T0 + i, 0),
            "_stream_id": "00000000000000000000000000000001",
            "trace_id": f"{(i // 6):032x}",
            "span_id": f"{i + 1:016x}",
            "parent_span_id": "" if i % 6 == 0 else f"{i:016x}",
            "name": f"op{i % 5}",
            "resource_attr:service.name": rnd.choice(["frontend", "cart", "pay"]),
            "duration": str(rnd.randrange(1000, 90000)),
            "kind": "2",
            "status_code": "0",
        }
        if with_extra:
            r["events"] = json.dumps([{"name": "exception", "time_unix_nano": 1}]) if i % 3 == 0 else "[]"
            r["links"] = json.dumps([{"trace_id": f"{99:032x}", "span_id": f"{7:016x}"}]) if i % 4 == 0 else "[]"
            r["scope_name"] = "io.opentelemetry.instrumentation"
        rows.append(r)
    return rows


def prom_matrix(series, ts_values):
    return {"status": "success", "data": {"resultType": "matrix", "result": [
        {"metric": m, "values": [[t, str(v)] for t, v in zip(ts_values, vals)]} for m, vals in series]}}


def prom_vector(n, name="count(*)"):
    return {"status": "success", "data": {"resultType": "vector", "result": [{"metric": {"__name__": name}, "value": [T0, str(n)]}]}}


def values_body(pairs):
    return {"values": [{"value": v, "hits": h} for v, h in pairs]}


# ---------------------------------------------------------------- vl-native
def vl_native():
    rows = log_rows()
    S = "vl-native"
    # B1: columnar reader adds <null>, account_id, ded_s0x fields
    junk = []
    for r in rows:
        r2 = dict(r)
        r2.update({"<null>": "", "account_id": "0", "ded_s0x": ""})
        junk.append(r2)
    write(S, "b1_junk_fields", {
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.cold", "claimed_gap": "B1",
        "order": ["_time"], "query": "* | limit 100",
        "expect": {"verdict": "fixed", "base": {"row_set": 0.0, "field_coverage": 66.6, "count": 100.0},
                   "pr": {"row_set": 100.0, "field_coverage": 100.0},
                   "details": {"base.field_coverage.extra": ["<null>", "account_id", "ded_s0x"]}},
    }, ans(ndjson(rows), ms=20), ans(ndjson(junk), ms=24), ans(ndjson(rows), ms=21))

    # B5: stats_query_range, right totals, wrong distribution
    ts = [T0 + 60 * i for i in range(24)]
    good = [10 + (i * 7) % 11 for i in range(24)]
    shifted = list(good)
    for i in (1, 5, 9, 13, 17, 21):  # neighbouring buckets swapped: same total, different distribution
        shifted[i], shifted[i + 1] = shifted[i + 1], shifted[i]
    m = {"level": "error"}
    write(S, "b5_stats_range_distribution", {
        "kind": "series_prom", "endpoint": "/select/logsql/stats_query_range", "row": "vl.select.stats_query_range.cold",
        "claimed_gap": "B5",
        "expect": {"verdict": "fixed", "base": {"totals": 100.0, "points_within_tol": 50.0, "series_set": 100.0},
                   "pr": {"points_within_tol": 100.0},
                   "details": {"base.totals.delta": 0.0}},
    }, ans(prom_matrix([(m, good)], ts)), ans(prom_matrix([(m, shifted)], ts)), ans(prom_matrix([(m, good)], ts)))

    # B8 / #429: sort by (_time) ties; base orders a block wrongly, PR only permutes inside ties
    tie_rows = log_rows(30, seed=5)
    for i in (4, 5, 6):  # a 3-row tie group on _time
        tie_rows[i]["_time"] = tie_rows[3]["_time"]
    tie_rows.sort(key=lambda r: r["_time"], reverse=True)
    wrong = copy.deepcopy(tie_rows)
    wrong[10:16] = list(reversed(wrong[10:16]))
    permuted = copy.deepcopy(tie_rows)
    grp = [i for i, r in enumerate(permuted) if r["_time"] == tie_rows[3]["_time"]]
    permuted[grp[0]], permuted[grp[-1]] = permuted[grp[-1]], permuted[grp[0]]
    write(S, "b8_429_sort_order", {
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.pipes.sort_time", "claimed_gap": "B8",
        "order": ["_time"], "query": "* | sort by (_time) desc",
        "expect": {"verdict": "fixed", "base": {"row_set": 100.0, "order": 96.5, "value_equality": 100.0},
                   "pr": {"order": 100.0}, "details": {"pr.order.tie_groups": 1}},
    }, ans(ndjson(tie_rows)), ans(ndjson(wrong)), ans(ndjson(permuted)))

    # B8: limit cuts a tie group; each side keeps a different member: accepted
    g = [{"_time": iso(T0 + 50, 5), "_stream_id": "s1", "_msg": f"tie {c}", "level": "info"} for c in "ABC"]
    head = [{"_time": iso(T0 + 100 - i, 0), "_stream_id": "s1", "_msg": f"top {i}", "level": "info"} for i in range(3)]
    ref_rows, sut_rows = head + [g[0], g[1]], head + [g[0], g[2]]
    tie = {"ref": ndjson(g), "sut": ndjson(list(reversed(g)))}
    write(S, "b8_tie_cut_accepted", {
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.pipes.sort_time_limit",
        "order": ["_time"], "query": "* | sort by (_time) desc | limit 5", "tie_group": tie,
        "expect": {"verdict": "exact", "base": {"row_set": 100.0}, "pr": {"row_set": 100.0, "order": 100.0}},
    }, ans(ndjson(ref_rows)), ans(ndjson(sut_rows)), ans(ndjson(sut_rows)))
    # same shape but the group differs between tiers: not accepted, PR regresses vs the exact base
    bad_tie = {"ref": ndjson(g), "sut": ndjson(g[:2])}
    write(S, "b8_tie_cut_rejected", {
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.pipes.sort_time_limit_b",
        "order": ["_time"], "query": "* | sort by (_time) desc | limit 5", "tie_group": bad_tie,
        "expect": {"verdict": "regressed", "base": {"row_set": 100.0}, "pr": {"row_set": 66.6}},
    }, ans(ndjson(ref_rows)), ans(ndjson(ref_rows)), ans(ndjson(sut_rows)))

    # map field_values: a map field's sub-field values, base and PR both partial
    ref_vals = [(f"v{i}", 100 - i * 3) for i in range(10)]
    write(S, "map_field_values", {
        "kind": "values", "endpoint": "/select/logsql/field_values", "row": "vl.select.field_values.map",
        "claimed_gap": "B-map",
        "expect": {"verdict": "improved", "base": {"value_set": 50.0, "hits_equality": 100.0},
                   "pr": {"value_set": 80.0, "hits_equality": 100.0},
                   "details": {"base.value_set.recall": 50.0, "pr.value_set.recall": 80.0}},
    }, ans(values_body(ref_vals)), ans(values_body(ref_vals[:5])), ans(values_body(ref_vals[:8])))
    # same values, hits drift on the PR
    drift = [(v, h + (1 if i == 2 else 0)) for i, (v, h) in enumerate(ref_vals)]
    write(S, "field_values_hits_drift", {
        "kind": "values", "endpoint": "/select/logsql/field_values", "row": "vl.select.field_values.hits",
        "expect": {"verdict": "regressed", "base": {"value_set": 100.0, "hits_equality": 100.0},
                   "pr": {"value_set": 100.0, "hits_equality": 90.0}},
    }, ans(values_body(ref_vals)), ans(values_body(ref_vals)), ans(values_body(drift)))

    write(S, "field_names_exact", {
        "kind": "values", "endpoint": "/select/logsql/field_names", "row": "vl.select.field_names",
        "expect": {"verdict": "exact", "base": {"value_set": 100.0}, "pr": {"value_set": 100.0}},
    }, *[ans(values_body([("_msg", 40), ("_time", 40), ("level", 40), ("service", 40)]))] * 3)
    write(S, "streams_exact", {
        "kind": "values", "endpoint": "/select/logsql/streams", "row": "vl.select.streams",
        "expect": {"verdict": "exact", "base": {"value_set": 100.0}, "pr": {"value_set": 100.0}},
    }, *[ans(values_body([('{app="api",pod="p0"}', 10), ('{app="api",pod="p1"}', 10)]))] * 3)

    # hits: exact on both
    hts = [iso(T0 + 3600 * i) for i in range(6)]
    hits = {"hits": [{"fields": {}, "timestamps": hts, "values": [5, 6, 7, 8, 9, 10], "total": 45}]}
    write(S, "hits_exact", {
        "kind": "series_hits", "endpoint": "/select/logsql/hits", "row": "vl.select.hits",
        "expect": {"verdict": "exact", "base": {"points_within_tol": 100.0}, "pr": {"points_within_tol": 100.0}},
    }, ans(hits), ans(hits), ans(hits))

    # count: cold count short by 3 percent, fixed on PR
    write(S, "stats_count_fixed", {
        "kind": "count", "endpoint": "/select/logsql/stats_query", "row": "vl.select.stats_query.count", "claimed_gap": "cold-count",
        "expect": {"verdict": "fixed", "base": {"count": 97.0}, "pr": {"count": 100.0}},
    }, ans(prom_vector(10000)), ans(prom_vector(9700)), ans(prom_vector(10000)))

    # regression: PR drops a field and one row
    dropped = [{k: v for k, v in r.items() if k != "service"} for r in rows][:-1]
    write(S, "rows_regressed", {
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.regress",
        "expect": {"verdict": "regressed", "base": {"row_set": 100.0}, "pr": {"field_coverage": 83.3}},
    }, ans(ndjson(rows)), ans(ndjson(rows)), ans(ndjson(dropped)))

    # claimed fix that never reproduced on base
    write(S, "claim_not_reproduced", {
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.claimed", "claimed_gap": "B9",
        "expect": {"verdict": "not-reproduced-on-base", "base": {"row_set": 100.0}, "pr": {"row_set": 100.0}},
    }, *[ans(ndjson(rows))] * 3)

    # vacuous: three empty 200 answers prove nothing
    write(S, "vacuous_empty", {
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.empty",
        "expect": {"verdict": "vacuous"},
    }, *[ans("")] * 3)

    # blocked: the reference answered 503
    write(S, "blocked_ref_503", {
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.blocked",
        "expect": {"verdict": "blocked"},
    }, ans("service unavailable", status=503), ans(ndjson(rows)), ans(ndjson(rows)))

    # errors: the same error in different quoting is the same error
    err = lambda t: ans(t, status=400)  # noqa: E731
    write(S, "error_same_text", {
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.badsyntax",
        "expect": {"verdict": "exact", "base": {"status": 100.0, "error": 100.0}, "pr": {"error": 100.0}},
    }, err("cannot parse query: unexpected token 'foo'"), err('cannot parse query:  unexpected token "foo"'),
        err("cannot  parse query: unexpected token `foo`"))
    write(S, "error_text_changed", {
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.badsyntax2",
        "expect": {"verdict": "regressed", "base": {"error": 100.0}, "pr": {"error": 0.0}},
    }, err("cannot parse query: unexpected token 'foo'"), err("cannot parse query: unexpected token 'foo'"),
        err("bad request"))

    # nondeterministic: the PR answer flips between samples
    flip = ans(ndjson(rows[:-1]))
    write(S, "flaky_answer", {
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.flaky",
        "expect": {"verdict": "nondeterministic", "base": {"row_set": 100.0}},
    }, ans(ndjson(rows)), ans(ndjson(rows)), ans(ndjson(rows)),
        resamples=[{"ref": ans(ndjson(rows)), "base": ans(ndjson(rows)), "pr": flip}])

    # Lakehouse-only: stats overview schema vs base and truth vs hot count()
    ov_base = {"rows": 10000, "bytes": 123456, "tenants": [{"account_id": 0, "project_id": 0}], "oldest": "2026-09-01T00:00:00Z"}
    ov_pr = dict(ov_base, extra_counter=3)
    truth = prom_vector(10000)
    write(S, "lh_overview_schema_change", {
        "kind": "schema", "lh_only": True, "endpoint": "/lakehouse/api/v1/stats/overview", "row": "lh.stats.overview",
        "contract": {"rows": "number", "bytes": "number", "oldest": "string"},
        "truth": {"shape": "scalar", "lh_path": "rows"},
        "expect": {"verdict": "regressed", "base": {"schema_keys": 100.0, "truth": 100.0}, "pr": {"schema_keys": 83.3, "truth": 100.0}},
    }, None, ans(ov_base), ans(ov_pr), extra={"truth_base": ans(truth), "truth_pr": ans(truth)})
    write(S, "lh_overview_truth_fixed", {
        "kind": "schema", "lh_only": True, "endpoint": "/lakehouse/api/v1/stats/overview", "row": "lh.stats.overview.truth",
        "truth": {"shape": "scalar", "lh_path": "rows", "unflushed_rows": 120},
        "expect": {"verdict": "fixed", "base": {"truth": 98.8}, "pr": {"truth": 100.0}, "details": {"base.truth.unflushed_rows": 120}},
    }, None, ans(dict(ov_base, rows=10000)), ans(dict(ov_base, rows=10120)),
        extra={"truth_base": ans(prom_vector(10120)), "truth_pr": ans(prom_vector(10120))})


# ---------------------------------------------------------------- vt-native
def vt_native():
    S = "vt-native"
    spans = span_rows()
    base = [{k: v for k, v in r.items() if k not in ("events", "links", "scope_name")} for r in spans]
    write(S, "430_events_links_scope", {
        "kind": "rows", "signal": "traces", "endpoint": "/select/logsql/query", "row": "vt.select.query.span_fields",
        "claimed_gap": "430",
        "expect": {"verdict": "fixed", "base": {"row_set": 0.0, "field_coverage": 76.9},
                   "pr": {"row_set": 100.0, "value_equality": 100.0}},
    }, ans(ndjson(spans)), ans(ndjson(base)), ans(ndjson(spans)))

    # B2: field_names built from Parquet columns only: 22 of 63 fields
    names = [f"field_{i:02d}" for i in range(63)]
    pairs = lambda ns: values_body([(n, 100) for n in ns])  # noqa: E731
    write(S, "b2_field_names_recall", {
        "kind": "values", "endpoint": "/select/logsql/field_names", "row": "vt.select.field_names.cold", "claimed_gap": "B2",
        "expect": {"verdict": "same", "base": {"value_set": 34.9}, "pr": {"value_set": 34.9},
                   "details": {"base.value_set.recall": 34.9, "base.value_set.precision": 100.0}},
    }, ans(pairs(names)), ans(pairs(names[:22])), ans(pairs(names[:22])))
    write(S, "b2_field_names_partial_fix", {
        "kind": "values", "endpoint": "/select/logsql/field_names", "row": "vt.select.field_names.cold2", "claimed_gap": "B2",
        "expect": {"verdict": "improved", "base": {"value_set": 34.9}, "pr": {"value_set": 63.5}},
    }, ans(pairs(names)), ans(pairs(names[:22])), ans(pairs(names[:40])))
    write(S, "stats_count_exact", {
        "kind": "count", "signal": "traces", "endpoint": "/select/logsql/stats_query", "row": "vt.select.stats_query.count",
        "expect": {"verdict": "exact", "base": {"count": 100.0}, "pr": {"count": 100.0}},
    }, *[ans(prom_vector(24))] * 3)


# ---------------------------------------------------------------- jaeger
def jaeger_trace(tid, spans_n=6, full=True, wrong_parent=()):
    procs = {"p1": {"serviceName": "frontend", "tags": []}, "p2": {"serviceName": "cart", "tags": []}}
    spans = []
    for i in range(spans_n):
        sid = f"{(i + 1) + int(tid, 16) * 16:016x}"
        parent = None if i == 0 else f"{(i) + int(tid, 16) * 16:016x}"
        if i in wrong_parent:
            parent = f"{1 + int(tid, 16) * 16:016x}"
        tags = [{"key": "span.kind", "type": "string", "value": "server"}, {"key": "http.status_code", "type": "int64", "value": 200}]
        sp = {"traceID": tid, "spanID": sid, "operationName": f"op{i % 3}", "startTime": 1_790_000_000_000_000 + i * 1000,
              "duration": 500 + i * 10, "processID": "p1" if i % 2 == 0 else "p2",
              "references": [{"refType": "CHILD_OF", "traceID": tid, "spanID": parent}] if parent else [],
              "tags": tags, "logs": []}
        if full:
            sp["tags"].append({"key": "otel.scope.name", "type": "string", "value": "cart-instrumentation"})
            if i % 3 == 0:
                sp["logs"] = [{"timestamp": 1_790_000_000_000_100 + i, "fields": [{"key": "event", "type": "string", "value": "exception"}]}]
            if i % 4 == 0:
                sp["references"].append({"refType": "FOLLOWS_FROM", "traceID": f"{255:032x}", "spanID": f"{9:016x}"})
        spans.append(sp)
    return {"traceID": tid, "spans": spans, "processes": procs}


def jaeger_resp(traces):
    return {"data": traces, "total": 0, "limit": 0, "offset": 0, "errors": None}


def jaeger():
    S = "jaeger"
    tid = f"{5:032x}"
    write(S, "430_trace_events_links_scope", {
        "kind": "trace_jaeger", "signal": "traces", "endpoint": "/select/jaeger/api/traces/{id}", "row": "jaeger.trace.by_id",
        "claimed_gap": "430",
        "expect": {"verdict": "fixed", "base": {"span_set": 100.0, "parent_links": 100.0, "span_fields": 0.0},
                   "pr": {"span_fields": 100.0, "span_count": 100.0},
                   "details": {"base.span_fields.min_field": "scope"}},
    }, ans(jaeger_resp([jaeger_trace(tid)])), ans(jaeger_resp([jaeger_trace(tid, full=False)])), ans(jaeger_resp([jaeger_trace(tid)])))
    write(S, "trace_parent_links", {
        "kind": "trace_jaeger", "signal": "traces", "endpoint": "/select/jaeger/api/traces/{id}", "row": "jaeger.trace.parents",
        "expect": {"verdict": "fixed", "base": {"parent_links": 66.6, "span_fields": 100.0}, "pr": {"parent_links": 100.0}},
    }, ans(jaeger_resp([jaeger_trace(tid)])), ans(jaeger_resp([jaeger_trace(tid, wrong_parent=(3, 4))])), ans(jaeger_resp([jaeger_trace(tid)])))
    write(S, "trace_span_count_regressed", {
        "kind": "trace_jaeger", "signal": "traces", "endpoint": "/select/jaeger/api/traces/{id}", "row": "jaeger.trace.spans",
        "expect": {"verdict": "regressed", "base": {"span_set": 100.0}, "pr": {"span_set": 83.3, "span_count": 83.3}},
    }, ans(jaeger_resp([jaeger_trace(tid)])), ans(jaeger_resp([jaeger_trace(tid)])), ans(jaeger_resp([jaeger_trace(tid, spans_n=5)])))
    full = [jaeger_trace(f"{i:032x}", 3) for i in range(1, 11)]
    write(S, "search_missing_traces", {
        "kind": "trace_jaeger", "signal": "traces", "endpoint": "/select/jaeger/api/traces", "row": "jaeger.search",
        "expect": {"verdict": "fixed", "base": {"trace_set": 80.0}, "pr": {"trace_set": 100.0},
                   "details": {"base.trace_set.recall": 80.0}},
    }, ans(jaeger_resp(full)), ans(jaeger_resp(full[:8])), ans(jaeger_resp(full)))
    svc = {"data": ["cart", "frontend", "pay"], "total": 3, "limit": 0, "offset": 0, "errors": None}
    write(S, "services_exact", {
        "kind": "values", "signal": "traces", "endpoint": "/select/jaeger/api/services", "row": "jaeger.services",
        "expect": {"verdict": "exact", "base": {"value_set": 100.0}, "pr": {"value_set": 100.0}},
    }, ans(svc), ans(svc), ans(svc))
    ops = {"data": [{"name": "op0", "spanKind": "server"}, {"name": "op1", "spanKind": "server"}], "total": 2}
    write(S, "operations_exact", {
        "kind": "values", "signal": "traces", "endpoint": "/select/jaeger/api/services/{svc}/operations", "row": "jaeger.operations",
        "expect": {"verdict": "exact", "base": {"value_set": 100.0}, "pr": {"value_set": 100.0}},
    }, ans(ops), ans(ops), ans(ops))


# ---------------------------------------------------------------- tempo
def otlp_trace(tid, n=4, full=True):
    spans = []
    for i in range(n):
        sp = {"traceId": tid, "spanId": f"{i + 1:016x}", "parentSpanId": "" if i == 0 else f"{i:016x}", "name": f"op{i}",
              "kind": "SPAN_KIND_SERVER", "startTimeUnixNano": str(1_790_000_000_000_000_000 + i * 1000),
              "endTimeUnixNano": str(1_790_000_000_000_000_000 + i * 1000 + 500),
              "attributes": [{"key": "http.method", "value": {"stringValue": "GET"}}],
              "status": {"code": "STATUS_CODE_OK"}}
        if full:
            if i % 2 == 0:
                sp["events"] = [{"timeUnixNano": str(1_790_000_000_000_000_100 + i), "name": "exception", "attributes": []}]
            if i % 3 == 0:
                sp["links"] = [{"traceId": f"{9:032x}", "spanId": f"{3:016x}"}]
        spans.append(sp)
    scope = {"name": "cart-instrumentation"} if full else {}
    return {"trace": {"resourceSpans": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "cart"}}]},
                                         "scopeSpans": [{"scope": scope, "spans": spans}]}]}}


def tempo():
    S = "tempo"
    tid = f"{7:032x}"
    write(S, "430_trace_events_links_scope", {
        "kind": "trace_otlp", "signal": "traces", "endpoint": "/api/v2/traces/{id}", "row": "tempo.trace.by_id", "claimed_gap": "430",
        "expect": {"verdict": "fixed", "base": {"span_fields": 0.0, "span_set": 100.0}, "pr": {"span_fields": 100.0}},
    }, ans(otlp_trace(tid)), ans(otlp_trace(tid, full=False)), ans(otlp_trace(tid)))
    s = {"traces": [{"traceID": f"{i:032x}", "rootServiceName": "cart", "rootTraceName": "op0", "durationMs": 5} for i in range(5)]}
    write(S, "search_exact", {
        "kind": "tempo_search", "signal": "traces", "endpoint": "/api/search", "row": "tempo.search",
        "expect": {"verdict": "exact", "base": {"trace_set": 100.0}, "pr": {"trace_set": 100.0}},
    }, ans(s), ans(s), ans(s))
    lbl = lambda v: [{"key": "service", "value": {"stringValue": v}}]  # noqa: E731
    smp = lambda vals: [{"timestampMs": str(T0 * 1000 + 15000 * i), "value": v} for i, v in enumerate(vals)]  # noqa: E731
    ref = {"series": [{"labels": lbl("cart"), "samples": smp([1, 2, 3, 4])}, {"labels": lbl("pay"), "samples": smp([0, 1, 0, 1])}]}
    base = {"series": [{"labels": lbl("cart"), "samples": smp([1, 2, 3, 4])}]}
    pr = {"series": [{"labels": lbl("cart"), "samples": smp([1, 2, 3, 4])}, {"labels": lbl("pay"), "samples": smp([0, 1, 0, 2])}]}
    write(S, "metrics_missing_series", {
        "kind": "series_tempo", "signal": "traces", "endpoint": "/api/metrics/query_range", "row": "tempo.metrics.rate",
        "expect": {"verdict": "improved", "base": {"series_set": 50.0}, "pr": {"series_set": 100.0, "points_within_tol": 87.5}},
    }, ans(ref), ans(base), ans(pr))


# ---------------------------------------------------------------- loki
def loki():
    S = "loki"
    ts = [T0 + 60 * i for i in range(6)]
    ref = prom_matrix([({"level": "error"}, [3, 4, 5, 6, 7, 8]), ({"level": "info"}, [30, 40, 50, 60, 70, 80])], ts)
    base = prom_matrix([({"level": "error"}, [3, 4, 5, 6, 7, 8])], ts)
    write(S, "query_range_matrix", {
        "kind": "series_prom", "endpoint": "/loki/api/v1/query_range", "row": "loki.query_range.rate",
        "expect": {"verdict": "fixed", "base": {"series_set": 50.0, "ts_alignment": 100.0}, "pr": {"series_set": 100.0}},
    }, ans(ref), ans(base), ans(ref))


def main():
    if os.path.isdir(OUT):
        shutil.rmtree(OUT)
    vl_native(); vt_native(); jaeger(); tempo(); loki()
    n = sum(1 for _, _, f in os.walk(OUT) if "meta.json" in f)
    print(f"wrote {n} cases under {OUT}")


if __name__ == "__main__":
    main()
