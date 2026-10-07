"""M9 trace match: Jaeger and Tempo/OTLP trace answers."""
from __future__ import annotations

from typing import Any

from .common import FacetResult, cap_inexact, delta_score, dumps, set_scores, to_number

SPAN_FIELDS = ("name", "service", "start", "duration", "status", "kind", "attributes", "scope", "events", "links")
Traces = dict[str, dict[str, dict]]  # trace_id -> span_id -> span


def _int(v: Any) -> int | None:
    n = to_number(v)
    return None if n is None else int(n)


def _kv_map(items) -> dict:
    out = {}
    for kv in items or []:
        if isinstance(kv, dict) and "key" in kv:
            v = kv.get("value")
            if isinstance(v, dict):
                for k in ("stringValue", "intValue", "doubleValue", "boolValue"):
                    if k in v:
                        n = v[k]
                        v = int(n) if k == "intValue" and str(n).lstrip("-").isdigit() else n
                        break
            out[kv["key"]] = v
    return out


def decode_jaeger(body: Any) -> Traces:
    out: Traces = {}
    data = body.get("data") if isinstance(body, dict) else None
    for tr in data or []:
        tid = tr.get("traceID")
        procs = tr.get("processes", {}) or {}
        spans = out.setdefault(tid, {})
        for sp in tr.get("spans", []) or []:
            tags = {t["key"]: t.get("value") for t in sp.get("tags", []) or [] if "key" in t}
            refs = sp.get("references", []) or []
            parent = next((r.get("spanID") for r in refs if r.get("refType") == "CHILD_OF"), None)
            links = sorted(
                [{"trace_id": r.get("traceID"), "span_id": r.get("spanID")} for r in refs if r.get("refType") != "CHILD_OF"],
                key=dumps,
            )
            events = []
            for lg in sp.get("logs", []) or []:
                f = _kv_map(lg.get("fields"))
                events.append({"time": (_int(lg.get("timestamp")) or 0) * 1000, "name": f.pop("event", None), "attributes": f})
            scope = tags.pop("otel.scope.name", None) or tags.pop("otel.library.name", None)
            tags.pop("otel.scope.version", None)
            tags.pop("otel.library.version", None)
            kind = tags.pop("span.kind", None)
            status = tags.pop("otel.status_code", None)
            tags.pop("otel.status_description", None)
            spans[sp.get("spanID")] = {
                "name": sp.get("operationName"),
                "service": (procs.get(sp.get("processID"), {}) or {}).get("serviceName"),
                "start": (_int(sp.get("startTime")) or 0) * 1000,
                "duration": (_int(sp.get("duration")) or 0) * 1000,
                "status": status,
                "kind": kind,
                "attributes": tags,
                "scope": scope,
                "events": sorted(events, key=dumps),
                "links": links,
                "parent": parent,
            }
    return out


def decode_otlp(body: Any) -> Traces:
    """Tempo trace by id (OTLP JSON), {"trace": {...}} or {"batches": [...]} or bare."""
    out: Traces = {}
    root = body
    if isinstance(root, dict):
        root = root.get("trace", root)
    rs_list = []
    if isinstance(root, dict):
        rs_list = root.get("resourceSpans") or root.get("batches") or []
    for rs in rs_list:
        res_attrs = _kv_map((rs.get("resource") or {}).get("attributes"))
        service = res_attrs.get("service.name")
        for ss in rs.get("scopeSpans") or rs.get("instrumentationLibrarySpans") or []:
            sc = ss.get("scope") or ss.get("instrumentationLibrary") or {}
            scope = sc.get("name")
            for sp in ss.get("spans", []) or []:
                tid, sid = sp.get("traceId"), sp.get("spanId")
                st = sp.get("status") or {}
                start, end = _int(sp.get("startTimeUnixNano")) or 0, _int(sp.get("endTimeUnixNano")) or 0
                out.setdefault(tid, {})[sid] = {
                    "name": sp.get("name"),
                    "service": service,
                    "start": start,
                    "duration": max(0, end - start),
                    "status": st.get("code"),
                    "kind": sp.get("kind"),
                    "attributes": _kv_map(sp.get("attributes")),
                    "scope": scope,
                    "events": sorted(
                        [
                            {"time": _int(e.get("timeUnixNano")) or 0, "name": e.get("name"), "attributes": _kv_map(e.get("attributes"))}
                            for e in sp.get("events", []) or []
                        ],
                        key=dumps,
                    ),
                    "links": sorted(
                        [{"trace_id": l.get("traceId"), "span_id": l.get("spanId")} for l in sp.get("links", []) or []],
                        key=dumps,
                    ),
                    "parent": sp.get("parentSpanId") or None,
                }
    return out


def decode_tempo_search(body: Any) -> Traces:
    """Tempo search: trace ids only (no spans)."""
    out: Traces = {}
    for t in (body or {}).get("traces", []) if isinstance(body, dict) else []:
        out.setdefault(t.get("traceID"), {})
    return out


def evaluate_traces(ref: Traces, ans: Traces) -> FacetResult:
    res = FacetResult()
    ts = set_scores(ref.keys(), ans.keys())
    res.facets["trace_set"] = cap_inexact(ts["jaccard"], set(ref) == set(ans))
    res.details["trace_set"] = {k: ts[k] for k in ("jaccard", "recall", "precision", "missing", "extra")}
    ref_pairs = {(t, s) for t, sp in ref.items() for s in sp}
    ans_pairs = {(t, s) for t, sp in ans.items() for s in sp}
    if not ref_pairs and not ans_pairs:
        return res  # search answers carry trace ids only
    ss = set_scores(ref_pairs, ans_pairs)
    res.facets["span_set"] = cap_inexact(ss["jaccard"], ref_pairs == ans_pairs)
    res.details["span_set"] = {k: ss[k] for k in ("jaccard", "recall", "precision", "missing", "extra")}

    common = sorted(ref_pairs & ans_pairs)
    eq = {f: 0 for f in SPAN_FIELDS}
    parent_eq = 0
    for t, s in common:
        r, a = ref[t][s], ans[t][s]
        for f in SPAN_FIELDS:
            if dumps(r.get(f)) == dumps(a.get(f)):
                eq[f] += 1
        if r.get("parent") == a.get("parent"):
            parent_eq += 1
    n = len(common)
    if n:
        per = {f: 100.0 * eq[f] / n for f in SPAN_FIELDS}
        worst = min(per, key=lambda f: (per[f], f))
        res.facets["span_fields"] = cap_inexact(per[worst], all(eq[f] == n for f in SPAN_FIELDS))
        res.details["span_fields"] = {
            "common_spans": n,
            "min_field": worst,
            "per_field": {f: round(v, 3) for f, v in per.items()},
        }
        res.facets["parent_links"] = cap_inexact(100.0 * parent_eq / n, parent_eq == n)
    d, frac, score = delta_score(float(len(ans_pairs)), float(len(ref_pairs)))
    res.facets["span_count"] = cap_inexact(score, d == 0)
    res.details["span_count"] = {"delta": d, "delta_pct": 100.0 * frac, "ref": len(ref_pairs), "ans": len(ans_pairs)}
    return res
