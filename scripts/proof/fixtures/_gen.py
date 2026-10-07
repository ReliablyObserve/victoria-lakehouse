"""Generate the offline fixture cases under scripts/proof/fixtures/cases/.

Three kinds of case, labelled in every meta.json as `provenance`:

  recorded               ref, base and PR are the same recorded answer (recorded/*.json[.gz], taken
                         from hot VictoriaLogs v1.53.0 and VictoriaTraces v0.12.0 by record.py)
  derived-from-recorded  the reference is a recorded answer; the base and/or PR answer is that
                         recorded answer after the transformation named in `derivation`
  synthetic              hand-written body (Lakehouse-only APIs, Loki and the Tempo metrics API,
                         which have no recording yet); the shapes follow the documented formats

No answer here is a recording of Lakehouse itself: recording real Lakehouse base and PR answers
belongs to the runner that calls these functions. Latencies of derived and synthetic cases are
those of the recorded call they derive from, or a placeholder.

Run: python3 scripts/proof/fixtures/_gen.py   (deterministic output)
"""
from __future__ import annotations

import copy
import gzip
import json
import os
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "cases")
REC = os.path.join(HERE, "recorded")


# ----------------------------------------------------------------------- helpers
def rec(name):
    f = os.path.join(REC, name + ".json")
    if os.path.exists(f):
        return json.load(open(f))
    return json.load(gzip.open(f + ".gz"))


def ans(body, status=200, ms=10.0, ctype="application/json", **extra):
    if not isinstance(body, str):
        body = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return {"status": status, "latency_ms": ms, "content_type": ctype, "body": body, **extra}


def from_rec(name, body=None):
    r = rec(name)
    return {"status": r["status"], "latency_ms": r.get("latency_ms", 10.0), "content_type": r["content_type"],
            "body": r["body"] if body is None else (body if isinstance(body, str) else json.dumps(body, sort_keys=True, separators=(",", ":")))}


def nd(rows):
    return "".join(json.dumps(r, sort_keys=True, separators=(",", ":")) + "\n" for r in rows)


def rows_of(name):
    return [json.loads(l) for l in rec(name)["body"].splitlines() if l.strip()]


def jbody(name):
    return json.loads(rec(name)["body"])


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


REC_META = {"provenance": "recorded"}


def derived(desc, *sources, **kw):
    return {"provenance": "derived-from-recorded", "derivation": desc, "recorded_from": list(sources), **kw}


def synthetic(why, **kw):
    return {"provenance": "synthetic", "derivation": why, **kw}


# ------------------------------------------------------------------- vl-native
def vl_native():
    S = "vl-native"
    rows = rows_of("vl_query")
    q = from_rec("vl_query")
    nfields = len({k for r in rows for k in r})

    write(S, "query_exact", {**REC_META, "recorded_from": ["vl_query"], "kind": "rows", "endpoint": "/select/logsql/query",
                             "row": "vl.select.query.recorded",
                             "expect": {"verdict": "exact", "base": {"row_set": 100.0}, "pr": {"row_set": 100.0}}}, q, q, q)

    junk = [dict(r, **{"<null>": "", "account_id": "0", "ded_s0x": ""}) for r in rows]
    write(S, "b1_junk_fields", {
        **derived("base adds the fields <null>, account_id and ded_s0x to every recorded row", "vl_query"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.cold", "claimed_gap": "B1",
        "expect": {"verdict": "fixed",
                   "base": {"row_set": 0.0, "field_coverage": round(100.0 * nfields / (nfields + 3), 2), "count": 100.0},
                   "pr": {"row_set": 100.0, "field_coverage": 100.0},
                   "details": {"base.field_coverage.extra": ["<null>", "account_id", "ded_s0x"]}},
    }, q, ans(nd(junk), ms=q["latency_ms"]), q)

    rng = jbody("vl_stats_query_range")
    swapped = copy.deepcopy(rng)
    same_pos = tot = 0
    for s in swapped["data"]["result"]:
        v = s["values"]
        a, b = v[0][1], v[1][1]
        v[0][1], v[1][1] = b, a  # swap the first two buckets: the total stays, the distribution does not
        tot += len(v)
        same_pos += (len(v) - 2) + (2 if a == b else 0)
    write(S, "b5_stats_range_distribution", {
        **derived("base swaps the values of the first two buckets of every series (same total, different distribution)", "vl_stats_query_range"),
        "kind": "series_prom", "endpoint": "/select/logsql/stats_query_range", "row": "vl.select.stats_query_range.cold", "claimed_gap": "B5",
        "expect": {"verdict": "fixed", "base": {"totals": 100.0, "points_within_tol": round(100.0 * same_pos / tot, 2), "series_set": 100.0},
                   "pr": {"points_within_tol": 100.0}, "details": {"base.totals.delta": 0.0}},
    }, from_rec("vl_stats_query_range"), ans(swapped), from_rec("vl_stats_query_range"))

    srows = rows_of("vl_query_sorted")
    n = len(srows)
    times = [r["_time"] for r in srows]
    tie_idx = [i for i in range(n) if times.count(times[i]) > 1]
    assert len(tie_idx) == 2, tie_idx
    i0, i1 = tie_idx
    blk = 20
    wrong = srows[:40] + list(reversed(srows[40:40 + blk])) + srows[40 + blk:]
    assert not (40 <= i0 < 40 + blk)
    permuted = list(srows)
    permuted[i0], permuted[i1] = permuted[i1], permuted[i0]
    comparable = n * (n - 1) // 2 - 1
    sq = from_rec("vl_query_sorted")
    write(S, "b8_429_sort_order", {
        **derived(f"base reverses rows 40-{40 + blk - 1}; PR swaps the two rows that tie on _time", "vl_query_sorted"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.pipes.sort_time", "claimed_gap": "B8",
        "order": ["_time"], "query": "* | sort by (_time) desc | limit 120",
        "expect": {"verdict": "fixed",
                   "base": {"row_set": 100.0, "order": round(100.0 * (1 - blk * (blk - 1) / 2 / comparable), 2), "value_equality": 100.0},
                   "pr": {"order": 100.0}, "details": {"pr.order.tie_groups": 1, "pr.order.comparable_pairs": comparable}},
    }, sq, ans(nd(wrong), ms=sq["latency_ms"]), ans(nd(permuted), ms=sq["latency_ms"]))

    a, b = srows[i0], srows[i1]
    group = [a, b]
    cut_ref = srows[: i0 + 1]
    cut_sut = srows[:i0] + [b]
    qtxt = f"* | sort by (_time) desc | limit {i0 + 1}"
    write(S, "b8_tie_cut_accepted", {
        **derived("a limit cuts the recorded 2-row tie group: the reference kept one member, base and PR the other; the group is re-read identically", "vl_query_sorted"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.pipes.sort_time_limit", "order": ["_time"], "query": qtxt,
        "tie_group": {"ref": nd(group), "sut": nd(list(reversed(group)))},
        "expect": {"verdict": "exact", "base": {"row_set": 100.0}, "pr": {"row_set": 100.0, "order": 100.0}},
    }, ans(nd(cut_ref)), ans(nd(cut_sut)), ans(nd(cut_sut)))
    write(S, "b8_tie_cut_rejected", {
        **derived("as b8_tie_cut_accepted, but the PR side re-reads a group that lacks a member", "vl_query_sorted"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.pipes.sort_time_limit_b", "order": ["_time"], "query": qtxt,
        "tie_group": {"ref": nd(group), "sut": nd(group[:1])},
        "expect": {"verdict": "regressed", "base": {"row_set": 100.0},
                   "pr": {"row_set": round(100.0 * i0 / (i0 + 2), 2)}},
    }, ans(nd(cut_ref)), ans(nd(cut_ref)), ans(nd(cut_sut)))

    fv = jbody("vl_field_values")
    vals = fv["values"]
    k = len(vals)
    part = lambda m: {"values": vals[:m]}  # noqa: E731
    write(S, "field_values_partial_fix", {
        **derived("base keeps the first half of the recorded values, PR keeps all but the last", "vl_field_values"),
        "kind": "values", "endpoint": "/select/logsql/field_values", "row": "vl.select.field_values.partial", "claimed_gap": "field-values",
        "expect": {"verdict": "improved", "base": {"value_set": 100.0 * (k // 2) / k, "hits_equality": 100.0},
                   "pr": {"value_set": 100.0 * (k - 1) / k, "hits_equality": 100.0},
                   "details": {"base.value_set.recall": 100.0 * (k // 2) / k}},
    }, from_rec("vl_field_values"), ans(part(k // 2)), ans(part(k - 1)))
    drift = {"values": [dict(v, hits=v["hits"] + 1) if i == 2 else v for i, v in enumerate(vals)]}
    write(S, "field_values_hits_drift", {
        **derived("PR adds 1 to the hits of the third value", "vl_field_values"),
        "kind": "values", "endpoint": "/select/logsql/field_values", "row": "vl.select.field_values.hits",
        "expect": {"verdict": "regressed", "base": {"hits_equality": 100.0}, "pr": {"value_set": 100.0, "hits_equality": 100.0 * (k - 1) / k}},
    }, from_rec("vl_field_values"), from_rec("vl_field_values"), ans(drift))
    lost = {"values": [{"value": v["value"]} for v in vals]}
    write(S, "field_values_lost_hits", {
        **derived("PR answers the same values without their hits", "vl_field_values"),
        "kind": "values", "endpoint": "/select/logsql/field_values", "row": "vl.select.field_values.lost_hits",
        "expect": {"verdict": "regressed", "base": {"hits_equality": 100.0}, "pr": {"value_set": 100.0, "hits_equality": 0.0}},
    }, from_rec("vl_field_values"), from_rec("vl_field_values"), ans(lost))

    for nm, ep, kind, row in (("field_names", "/select/logsql/field_names", "values", "vl.select.field_names"),
                              ("streams", "/select/logsql/streams", "values", "vl.select.streams"),
                              ("facets", "/select/logsql/facets", "values", "vl.select.facets"),
                              ("hits", "/select/logsql/hits", "series_hits", "vl.select.hits"),
                              ("hits_by_level", "/select/logsql/hits", "series_hits", "vl.select.hits.by_level"),
                              ("stats_query_by", "/select/logsql/stats_query", "count", "vl.select.stats_query.by"),
                              ("stats_query_range", "/select/logsql/stats_query_range", "series_prom", "vl.select.stats_query_range")):
        src = {"hits_by_level": "vl_hits_by_level", "stats_query_by": "vl_stats_query_by", "stats_query_range": "vl_stats_query_range"}.get(nm, "vl_" + nm)
        r = from_rec(src)
        write(S, nm + "_exact", {**REC_META, "recorded_from": [src], "kind": kind, "endpoint": ep, "row": row,
                                 "expect": {"verdict": "exact"}}, r, r, r)

    fac = jbody("vl_facets")
    fac2 = copy.deepcopy(fac)
    fac2["facets"][0]["values"][0]["hits"] += 1
    write(S, "facets_hits_changed", {
        **derived("PR adds 1 to the hits of the first value of the first facet", "vl_facets"),
        "kind": "values", "endpoint": "/select/logsql/facets", "row": "vl.select.facets.hits",
        "expect": {"verdict": "regressed", "pr": {"value_set": 100.0}},
    }, from_rec("vl_facets"), from_rec("vl_facets"), ans(fac2))
    fac3 = copy.deepcopy(fac)
    fac3["facets"][0]["values"][0]["field_value"] = "other"
    write(S, "facets_value_changed", {
        **derived("PR answers a different value for the first facet value", "vl_facets"),
        "kind": "values", "endpoint": "/select/logsql/facets", "row": "vl.select.facets.value",
        "expect": {"verdict": "regressed"},
    }, from_rec("vl_facets"), from_rec("vl_facets"), ans(fac3))

    cnt = jbody("vl_stats_query")
    total = float(cnt["data"]["result"][0]["value"][1])
    low = copy.deepcopy(cnt)
    low["data"]["result"][0]["value"][1] = str(int(total * 0.97))
    delta = total - int(total * 0.97)
    write(S, "stats_count_fixed", {
        **derived("base reports 97% of the recorded count", "vl_stats_query"),
        "kind": "count", "endpoint": "/select/logsql/stats_query", "row": "vl.select.stats_query.count", "claimed_gap": "cold-count",
        "expect": {"verdict": "fixed", "base": {"count": 100.0 * (1 - delta / total)}, "pr": {"count": 100.0}},
    }, from_rec("vl_stats_query"), ans(low), from_rec("vl_stats_query"))

    by = jbody("vl_stats_query_by")
    lost_group = copy.deepcopy(by)
    lost_group["data"]["result"] = [r for r in lost_group["data"]["result"] if r["metric"]["level"] != "WARN"]
    write(S, "stats_by_group_missing", {
        **derived("base lacks the WARN group of the recorded `stats by (level)` vector", "vl_stats_query_by"),
        "kind": "count", "endpoint": "/select/logsql/stats_query", "row": "vl.select.stats_query.by_missing", "claimed_gap": "cold-count",
        "expect": {"verdict": "fixed", "base": {"count": 0.0}, "pr": {"count": 100.0}},
    }, from_rec("vl_stats_query_by"), ans(lost_group), from_rec("vl_stats_query_by"))

    fields = sorted({k for r in rows for k in r})
    drop = "http.target"
    dropped = [{k: v for k, v in r.items() if k != drop} for r in rows]
    untouched = sum(1 for r in rows[:-1] if drop not in r)
    jac = 100.0 * untouched / (len(rows) + (len(rows) - 1) - untouched)
    write(S, "rows_regressed", {
        **derived(f"PR drops the field {drop} from every row and the last row", "vl_query"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.regress",
        "expect": {"verdict": "regressed", "base": {"row_set": 100.0}, "pr": {"row_set": jac, "field_coverage": 100.0 * (len(fields) - 1) / len(fields), "value_equality": 0.0}},
    }, q, q, ans(nd(dropped[:-1])))

    write(S, "claim_not_reproduced", {**REC_META, "recorded_from": ["vl_query"], "kind": "rows", "endpoint": "/select/logsql/query",
                                      "row": "vl.select.query.claimed", "claimed_gap": "B9",
                                      "expect": {"verdict": "not-reproduced-on-base"}}, q, q, q)
    write(S, "blocked_ref_503", {
        **derived("the reference answered 503 (a recorded answer with its status replaced)", "vl_query"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.blocked", "expect": {"verdict": "blocked"},
    }, ans("service unavailable", status=503, ctype="text/plain"), q, q)

    bad = rec("vl_bad_query")
    txt = bad["body"]
    same = txt.replace('"', "'").replace(": ", ":  ")
    write(S, "error_same_text", {
        **derived("base and PR answer the recorded error with quotes changed and whitespace doubled", "vl_bad_query"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.badsyntax",
        "expect": {"verdict": "exact", "base": {"status": 100.0, "error": 100.0}, "pr": {"error": 100.0}},
    }, from_rec("vl_bad_query"), ans(same, status=400, ctype="text/plain"), ans(same, status=400, ctype="text/plain"))
    write(S, "error_text_changed", {
        **derived("PR answers a different error text for the recorded bad query", "vl_bad_query"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.badsyntax2",
        "expect": {"verdict": "regressed", "base": {"error": 100.0}, "pr": {"error": 0.0}},
    }, from_rec("vl_bad_query"), from_rec("vl_bad_query"), ans("bad request", status=400, ctype="text/plain"))
    write(S, "error_case_changed", {
        **derived("PR answers the recorded error text in upper case: case is part of the message", "vl_bad_query"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.badsyntax3",
        "expect": {"verdict": "regressed", "pr": {"error": 0.0}},
    }, from_rec("vl_bad_query"), from_rec("vl_bad_query"), ans(txt.upper(), status=400, ctype="text/plain"))
    write(S, "base_error_pr_partial", {
        **derived("base answers 500; PR answers all but the last recorded row", "vl_query"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.one_side_error",
        "expect": {"verdict": "improved", "base": {"status": 0.0, "row_set": 0.0, "count": 0.0, "value_equality": 0.0},
                   "pr": {"status": 100.0, "field_coverage": 100.0, "value_equality": 100.0}},
    }, q, ans("internal error", status=500, ctype="text/plain"), ans(nd(rows[:-1])))
    flip = ans(nd(rows[:-1]))
    write(S, "flaky_answer", {
        **derived("PR answers all rows in the first sample and all but the last in the second", "vl_query"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.flaky",
        "expect": {"verdict": "nondeterministic", "base": {"row_set": 100.0}},
    }, q, q, q, resamples=[{"ref": q, "base": q, "pr": flip}])
    write(S, "empty_core_row_harness_error", {
        **derived("all three answers are empty on a core row", "vl_query"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.empty_core", "expect": {"verdict": "harness-error"},
    }, ans(""), ans(""), ans(""))
    html = ans("<html><body>502 Bad Gateway</body></html>", ctype="text/html")
    write(S, "unknown_shape_html_200", {
        **synthetic("an HTML page served with status 200 where rows were expected"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vl.select.query.html", "expect": {"verdict": "harness-error"},
    }, html, html, html)


# ------------------------------------------------------------------- vt-native
def vt_native():
    S = "vt-native"
    trows = rows_of("vt_query_trace_rows")
    tq = from_rec("vt_query_trace_rows")
    lost = [{k: v for k, v in r.items() if not (k.startswith("event:") or k.startswith("link:") or k.startswith("scope_attr:"))} for r in trows]
    allf = {k for r in trows for k in r}
    keptf = {k for r in lost for k in r}
    write(S, "430_events_links_scope_attrs", {
        **derived("base lacks every event:*, link:* and scope_attr:* field (scope_name and scope_version are kept)", "vt_query_trace_rows"),
        "kind": "rows", "endpoint": "/select/logsql/query", "row": "vt.select.query.span_fields", "claimed_gap": "430",
        "expect": {"verdict": "fixed",
                   "base": {"row_set": 0.0, "field_coverage": 100.0 * len(keptf) / len(allf), "value_equality": 0.0},
                   "pr": {"row_set": 100.0, "value_equality": 100.0}},
    }, tq, ans(nd(lost), ms=tq["latency_ms"]), tq)
    names = jbody("vt_field_names")["values"]
    n = len(names)
    k = round(0.349 * n)
    keep = lambda m: {"values": names[:m]}  # noqa: E731
    write(S, "b2_field_names_recall", {
        **derived(f"base and PR keep the first {k} of the {n} recorded field names (35%)", "vt_field_names"),
        "kind": "values", "endpoint": "/select/logsql/field_names", "row": "vt.select.field_names.cold", "claimed_gap": "B2",
        "expect": {"verdict": "same", "base": {"value_set": 100.0 * k / n}, "pr": {"value_set": 100.0 * k / n},
                   "details": {"base.value_set.recall": 100.0 * k / n, "base.value_set.precision": 100.0}},
    }, from_rec("vt_field_names"), ans(keep(k)), ans(keep(k)))
    m = round(0.667 * n)
    write(S, "b2_field_names_partial_fix", {
        **derived(f"base keeps {k} of {n} recorded field names, PR keeps {m}", "vt_field_names"),
        "kind": "values", "endpoint": "/select/logsql/field_names", "row": "vt.select.field_names.cold2", "claimed_gap": "B2",
        "expect": {"verdict": "improved", "base": {"value_set": 100.0 * k / n}, "pr": {"value_set": 100.0 * m / n}},
    }, from_rec("vt_field_names"), ans(keep(k)), ans(keep(m)))
    for nm, src, ep, kind in (("stats_count_exact", "vt_stats_query", "/select/logsql/stats_query", "count"),
                              ("field_values_service_exact", "vt_field_values_service", "/select/logsql/field_values", "values")):
        r = from_rec(src)
        write(S, nm, {**REC_META, "recorded_from": [src], "kind": kind, "endpoint": ep, "row": "vt.select." + nm,
                      "expect": {"verdict": "exact"}}, r, r, r)


# ---------------------------------------------------------------------- jaeger
def jaeger():
    S = "jaeger"
    full = jbody("jaeger_trace")
    spans = full["data"][0]["spans"]
    jt = from_rec("jaeger_trace")
    nsp = len(spans)

    def strip430(sp):
        sp = copy.deepcopy(sp)
        sp["logs"] = []
        sp["references"] = [r for r in sp["references"] if r["refType"] == "CHILD_OF"]
        sp["tags"] = [t for t in sp["tags"] if not t["key"].startswith("scope_attr:")]
        return sp

    base = copy.deepcopy(full)
    base["data"][0]["spans"] = [strip430(s) for s in spans]
    with_logs = sum(1 for s in spans if s["logs"])
    with_links = sum(1 for s in spans if any(r["refType"] != "CHILD_OF" for r in s["references"]))
    with_sa = sum(1 for s in spans if any(t["key"].startswith("scope_attr:") for t in s["tags"]))
    write(S, "430_trace_events_links_scope_attrs", {
        **derived("base lacks span logs (events), FOLLOWS_FROM references (links) and scope_attr:* tags; the scope name tag is kept", "jaeger_trace"),
        "kind": "trace_jaeger", "endpoint": "/select/jaeger/api/traces/{id}", "row": "jaeger.trace.by_id", "claimed_gap": "430",
        "expect": {"verdict": "fixed", "base": {"span_set": 100.0, "parent_links": 100.0, "span_fields": 100.0 * (nsp - with_sa) / nsp},
                   "pr": {"span_fields": 100.0, "span_count": 100.0},
                   "details": {"base.span_fields.min_field": "scope_attributes",
                               "base.span_fields.per_field.events": 100.0 * (nsp - with_logs) / nsp,
                               "base.span_fields.per_field.links": 100.0 * (nsp - with_links) / nsp,
                               "base.span_fields.per_field.scope": 100.0}},
    }, jt, ans(base, ms=jt["latency_ms"]), jt)

    ids = [s["spanID"] for s in spans]
    rep = copy.deepcopy(full)
    moved = 0
    for s in rep["data"][0]["spans"]:
        if s["spanID"] in ids[2:]:
            for r in s["references"]:
                if r["refType"] == "CHILD_OF":
                    r["spanID"] = ids[0]
                    moved += 1
    write(S, "trace_parent_links", {
        **derived("base re-parents the spans after the second onto the root span", "jaeger_trace"),
        "kind": "trace_jaeger", "endpoint": "/select/jaeger/api/traces/{id}", "row": "jaeger.trace.parents",
        "expect": {"verdict": "fixed", "base": {"parent_links": 100.0 * (nsp - moved) / nsp}, "pr": {"parent_links": 100.0}},
    }, jt, ans(rep), jt)
    fewer = copy.deepcopy(full)
    fewer["data"][0]["spans"] = spans[:-1]
    write(S, "trace_span_count_regressed", {
        **derived("PR drops the last span of the recorded trace", "jaeger_trace"),
        "kind": "trace_jaeger", "endpoint": "/select/jaeger/api/traces/{id}", "row": "jaeger.trace.spans",
        "expect": {"verdict": "regressed", "base": {"span_set": 100.0}, "pr": {"span_set": 100.0 * (nsp - 1) / nsp, "span_count": 100.0 * (nsp - 1) / nsp}},
    }, jt, jt, ans(fewer))
    srch = jbody("jaeger_search_order")
    nt = len(srch["data"])
    trimmed = copy.deepcopy(srch)
    trimmed["data"] = trimmed["data"][: nt - 2]
    sr = from_rec("jaeger_search_order")
    write(S, "search_missing_traces", {
        **derived("base lacks the last two traces of the recorded search", "jaeger_search_order"),
        "kind": "trace_jaeger", "endpoint": "/select/jaeger/api/traces", "row": "jaeger.search", "claimed_gap": "search",
        "expect": {"verdict": "fixed", "base": {"trace_set": 100.0 * (nt - 2) / nt}, "pr": {"trace_set": 100.0},
                   "details": {"base.trace_set.recall": 100.0 * (nt - 2) / nt}},
    }, sr, ans(trimmed), sr)
    for nm, src, ep, kind, ex in (("services_exact", "jaeger_services", "/select/jaeger/api/services", "values", "exact"),
                                  ("operations_exact", "jaeger_operations", "/select/jaeger/api/services/{svc}/operations", "values", "exact"),
                                  ("search_exact", "jaeger_search", "/select/jaeger/api/traces", "trace_jaeger", "exact"),
                                  ("trace_not_found_404_same", "jaeger_trace_missing", "/select/jaeger/api/traces/{id}", "trace_jaeger", "exact")):
        r = from_rec(src)
        write(S, nm, {**REC_META, "recorded_from": [src], "kind": kind, "endpoint": ep, "row": "jaeger." + nm,
                      "expect": {"verdict": ex}}, r, r, r)
    r = from_rec("jaeger_dependencies")
    write(S, "dependencies_empty", {**REC_META, "recorded_from": ["jaeger_dependencies"], "kind": "values",
                                    "endpoint": "/select/jaeger/api/dependencies", "row": "jaeger.dependencies", "may_be_empty": True,
                                    "expect": {"verdict": "vacuous"}}, r, r, r)
    ops = jbody("jaeger_services")
    ops2 = copy.deepcopy(ops)
    ops2["data"] = ops2["data"][:-1]
    write(S, "services_missing_one", {
        **derived("PR lacks the last recorded service", "jaeger_services"),
        "kind": "values", "endpoint": "/select/jaeger/api/services", "row": "jaeger.services.missing",
        "expect": {"verdict": "regressed", "pr": {"value_set": 100.0 * (len(ops["data"]) - 1) / len(ops["data"])}},
    }, from_rec("jaeger_services"), from_rec("jaeger_services"), ans(ops2))


# ----------------------------------------------------------------------- tempo
def tempo():
    S = "tempo"
    full = jbody("tempo_trace_v2")
    base = copy.deepcopy(full)
    for rs in base["trace"]["resourceSpans"]:
        for ss in rs["scopeSpans"]:
            ss["scope"].pop("attributes", None)
            for sp in ss["spans"]:
                sp.pop("events", None)
                sp.pop("links", None)
    tt = from_rec("tempo_trace_v2")
    write(S, "430_trace_events_links_scope_attrs", {
        **derived("base lacks scope attributes, span events and span links", "tempo_trace_v2"),
        "kind": "trace_otlp", "endpoint": "/api/v2/traces/{id}", "row": "tempo.trace.by_id", "claimed_gap": "430",
        "expect": {"verdict": "fixed", "base": {"span_set": 100.0, "span_fields": 0.0}, "pr": {"span_fields": 100.0},
                   "details": {"base.span_fields.min_field": "scope_attributes", "base.span_fields.per_field.events": 25.0}},
    }, tt, ans(base, ms=tt["latency_ms"]), tt)
    for nm, src, kind, ep in (("search_exact", "tempo_search", "tempo_search", "/api/search"),
                              ("tag_values_exact", "tempo_tag_values", "values", "/api/v2/search/tag/{tag}/values"),
                              ("tags_exact", "tempo_tags", "values", "/api/v2/search/tags")):
        r = from_rec(src)
        write(S, nm, {**REC_META, "recorded_from": [src], "kind": kind, "endpoint": ep, "row": "tempo." + nm,
                      "expect": {"verdict": "exact"}}, r, r, r)
    tv = jbody("tempo_tag_values")
    tv2 = copy.deepcopy(tv)
    tv2["tagValues"] = tv2["tagValues"][:-2]
    nv = len(tv["tagValues"])
    write(S, "tag_values_missing", {
        **derived("base lacks the last two recorded tag values", "tempo_tag_values"),
        "kind": "values", "endpoint": "/api/v2/search/tag/{tag}/values", "row": "tempo.tag_values.missing", "claimed_gap": "tags",
        "expect": {"verdict": "fixed", "base": {"value_set": 100.0 * (nv - 2) / nv}},
    }, from_rec("tempo_tag_values"), ans(tv2), from_rec("tempo_tag_values"))
    lbl = lambda v: [{"key": "service", "value": {"stringValue": v}}]  # noqa: E731
    smp = lambda vals: [{"timestampMs": str(1791357600000 + 15000 * i), "value": v} for i, v in enumerate(vals)]  # noqa: E731
    ref = {"series": [{"labels": lbl("cart"), "samples": smp([1, 2, 3, 4])}, {"labels": lbl("pay"), "samples": smp([0, 1, 0, 1])}]}
    b = {"series": [{"labels": lbl("cart"), "samples": smp([1, 2, 3, 4])}]}
    p = {"series": [{"labels": lbl("cart"), "samples": smp([1, 2, 3, 4])}, {"labels": lbl("pay"), "samples": smp([0, 1, 0, 2])}]}
    write(S, "metrics_missing_series", {
        **synthetic("TraceQL metrics query_range body in the documented format; not recorded"),
        "kind": "series_tempo", "endpoint": "/api/metrics/query_range", "row": "tempo.metrics.rate",
        "expect": {"verdict": "improved", "base": {"series_set": 50.0}, "pr": {"series_set": 100.0, "points_within_tol": 87.5}},
    }, ans(ref), ans(b), ans(p))


# ------------------------------------------------------------------------ loki
def loki():
    S = "loki"
    T0 = 1791357600
    mat = lambda series: {"status": "success", "data": {"resultType": "matrix", "result": [  # noqa: E731
        {"metric": m, "values": [[T0 + 60 * i, str(v)] for i, v in enumerate(vs)]} for m, vs in series]}}
    ref = mat([({"level": "error"}, [3, 4, 5, 6, 7, 8]), ({"level": "info"}, [30, 40, 50, 60, 70, 80])])
    base = mat([({"level": "error"}, [3, 4, 5, 6, 7, 8])])
    write(S, "query_range_matrix", {
        **synthetic("Loki query_range matrix body in the documented format; not recorded"),
        "kind": "series_prom", "endpoint": "/loki/api/v1/query_range", "row": "loki.query_range.rate",
        "expect": {"verdict": "fixed", "base": {"series_set": 50.0}, "pr": {"series_set": 100.0}},
    }, ans(ref), ans(base), ans(ref))
    st = lambda app, line: {"status": "success", "data": {"resultType": "streams", "result": [  # noqa: E731
        {"stream": {"app": app}, "values": [[str(T0 * 10**9), line]]}]}}
    write(S, "streams_lines_differ", {
        **synthetic("Loki streams bodies in the documented format; not recorded"),
        "kind": "series_prom", "endpoint": "/loki/api/v1/query_range", "row": "loki.query_range.streams",
        "expect": {"verdict": "regressed", "base": {"row_set": 100.0}, "pr": {"row_set": 0.0}},
    }, ans(st("a", "line one")), ans(st("a", "line one")), ans(st("b", "totally different")))
    write(S, "empty_vacuous", {
        **synthetic("three empty matrices"), "kind": "series_prom", "endpoint": "/loki/api/v1/query_range",
        "row": "loki.query_range.empty", "may_be_empty": True, "expect": {"verdict": "vacuous"},
    }, *[ans(mat([]))] * 3)


# ------------------------------------------------------------------- lh-logs/traces
def lh():
    cnt = jbody("vl_stats_query_by")
    total = int(sum(float(r["value"][1]) for r in cnt["data"]["result"]))
    truth = from_rec("vl_stats_query_by")
    ov = lambda rows: {"total_rows": rows, "total_files": 12, "total_bytes": 123456, "total_raw_bytes": 654321,  # noqa: E731
                       "oldest_data": "2026-10-07T06:50:01Z", "newest_data": "2026-10-07T07:49:58Z", "tenant_count": 1,
                       "partition_count": 3, "storage_by_class": [{"class": "STANDARD", "bytes": 123456}]}
    write("lh-logs", "stats_overview_truth", {
        **synthetic("Lakehouse /stats/overview body with its documented keys (not recorded); the truth answer is the recorded stats by (level) vector, summed",
                    recorded_from=["vl_stats_query_by"]),
        "kind": "schema", "endpoint": "/lakehouse/api/v1/stats/overview", "row": "lh.stats.overview.truth",
        "truth": {"shape": "scalar_sum", "lh_path": "total_rows", "unflushed_rows": 10},
        "expect": {"verdict": "fixed", "base": {"truth": 100.0 * (1 - 10 / total)}, "pr": {"truth": 100.0},
                   "details": {"base.truth.unflushed_rows": 10, "base.truth.delta": -10}},
    }, None, ans(ov(total - 10)), ans(ov(total)), extra={"truth_base": truth, "truth_pr": truth})
    changed = dict(ov(total), extra_counter=3, total_files="12")
    write("lh-logs", "stats_overview_schema_change", {
        **synthetic("Lakehouse /stats/overview body (not recorded): the PR adds a key and turns total_files into a string"),
        "kind": "schema", "endpoint": "/lakehouse/api/v1/stats/overview", "row": "lh.stats.overview",
        "contract": {"total_rows": "number", "total_bytes": "number", "oldest_data": "string"},
        "truth": {"shape": "scalar_sum", "lh_path": "total_rows"},
        "expect": {"verdict": "regressed", "base": {"schema_keys": 100.0, "schema_types": 100.0, "truth": 100.0},
                   "pr": {"schema_keys": 100.0 * 10 / 11, "schema_types": 90.0, "truth": 100.0}},
    }, None, ans(ov(total)), ans(changed), extra={"truth_base": truth, "truth_pr": truth})
    uniq = from_rec("vl_stats_count_uniq")
    card = lambda lv, hm: {"fields": [  # noqa: E731
        {"name": "level", "cardinality": lv, "type": "string", "has_bloom": True, "indexed": True, "storage_bytes": 100},
        {"name": "http.method", "cardinality": hm, "type": "string", "has_bloom": False, "indexed": True, "storage_bytes": 50},
        {"name": "not_indexed", "cardinality": 99999, "type": "string", "has_bloom": False, "indexed": False, "storage_bytes": 0}]}
    write("lh-logs", "cardinality_fields_hll", {
        **synthetic("Lakehouse /cardinality/fields body with its documented keys (not recorded); truth is the recorded count_uniq vector",
                    recorded_from=["vl_stats_count_uniq"]),
        "kind": "schema", "endpoint": "/lakehouse/api/v1/cardinality/fields", "row": "lh.cardinality.fields",
        "truth": {"shape": "per_key", "lh_path": "fields", "key_field": "name", "value_field": "cardinality", "key_label": "__name__",
                  "where": {"indexed": True}, "rel_tol": 0.05},
        "expect": {"verdict": "fixed", "base": {"truth": 40.0}, "pr": {"truth": 100.0}},
    }, None, ans(card(4, 8)), ans(card(4, 5)), extra={"truth_base": uniq, "truth_pr": uniq})
    tids = from_rec("vt_tenant_ids")
    tenants = lambda ids: {"tenants": [{"account_id": a, "project_id": p} for a, p in ids], "total_tenants": len(ids)}  # noqa: E731
    write("lh-traces", "tenants_truth", {
        **synthetic("Lakehouse /tenants body with its documented keys (not recorded); truth is the recorded /select/tenant_ids answer",
                    recorded_from=["vt_tenant_ids"]),
        "kind": "schema", "endpoint": "/lakehouse/api/v1/tenants", "row": "lh.tenants",
        "truth": {"shape": "tenant_set", "lh_path": "tenants"},
        "expect": {"verdict": "fixed", "base": {"truth": 0.0}, "pr": {"truth": 100.0}},
    }, None, ans(tenants([("1", "0")])), ans(tenants([("0", "0")])), extra={"truth_base": tids, "truth_pr": tids})


def main():
    if os.path.isdir(OUT):
        shutil.rmtree(OUT)
    vl_native(); vt_native(); jaeger(); lh(); loki(); tempo()
    n = sum(1 for _, _, f in os.walk(OUT) if "meta.json" in f)
    print(f"wrote {n} cases under {OUT}")


if __name__ == "__main__":
    main()
