"""M9 trace match: Jaeger and Tempo/OTLP trace answers.

VictoriaTraces' Jaeger API carries span data as tags: span.kind, error (false/true),
otel.status_description, otel.scope.name, otel.scope.version, scope_attr:<k>, and the
span attributes; resource attributes are the process tags; events are logs; links are
non-CHILD_OF references. The Tempo API answers OTLP/JSON with base64 ids.
"""
from __future__ import annotations

from typing import Any

from .common import FacetResult, UnknownShape, cap_inexact, delta_score, dumps, set_scores, to_number

SPAN_FIELDS = (
    "name", "service", "start", "duration", "status", "status_message", "kind", "attributes",
    "scope", "scope_version", "scope_attributes", "resource", "events", "links",
)
Traces = dict[str, dict[str, dict]]  # trace_id -> span_id -> span


def _int(v: Any) -> int | None:
    """Exact integer: nanosecond timestamps (~1.8e18) lose their last digits as floats."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().lstrip("-").isdigit():
        return int(v.strip())
    n = to_number(v)
    return None if n is None or n != n or n in (float("inf"), float("-inf")) else int(n)


def _plain(v: Any) -> Any:
    if isinstance(v, dict):
        for k in ("stringValue", "intValue", "doubleValue", "boolValue"):
            if k in v:
                n = v[k]
                return int(n) if k == "intValue" and str(n).lstrip("-").isdigit() else n
        return dumps(v)
    return v


def _kv_map(items) -> dict:
    return {kv["key"]: _plain(kv.get("value")) for kv in items or [] if isinstance(kv, dict) and "key" in kv}


def _tag_map(items) -> dict:
    return {t["key"]: t.get("value") for t in items or [] if isinstance(t, dict) and "key" in t}


def decode_jaeger(body: Any) -> Traces:
    if body is None:
        return {}
    if not isinstance(body, dict) or not isinstance(body.get("data"), (list, type(None))):
        raise UnknownShape("jaeger answer: no data list")
    out: Traces = {}
    for tr in body.get("data") or []:
        tid = tr.get("traceID")
        procs = tr.get("processes", {}) or {}
        spans = out.setdefault(tid, {})
        for sp in tr.get("spans", []) or []:
            tags = _tag_map(sp.get("tags"))
            proc = procs.get(sp.get("processID"), {}) or {}
            refs = sp.get("references", []) or []
            parent = next((r.get("spanID") for r in refs if r.get("refType") == "CHILD_OF"), None)
            links = sorted(
                [{"trace_id": r.get("traceID"), "span_id": r.get("spanID"), "type": r.get("refType")}
                 for r in refs if r.get("refType") != "CHILD_OF"],
                key=dumps,
            )
            events = []
            for lg in sp.get("logs", []) or []:
                f = _tag_map(lg.get("fields"))
                events.append({"time": (_int(lg.get("timestamp")) or 0) * 1000, "name": f.pop("event", None), "attributes": f})
            scope_attrs = {k[len("scope_attr:"):]: tags.pop(k) for k in [k for k in tags if k.startswith("scope_attr:")]}
            status = tags.pop("error", None)
            if status is None:
                status = tags.pop("otel.status_code", None)
            else:
                tags.pop("otel.status_code", None)
            spans[sp.get("spanID")] = {
                "name": sp.get("operationName"),
                "service": proc.get("serviceName"),
                "start": (_int(sp.get("startTime")) or 0) * 1000,
                "duration": (_int(sp.get("duration")) or 0) * 1000,
                "status": status,
                "status_message": tags.pop("otel.status_description", None),
                "kind": tags.pop("span.kind", None),
                "scope": tags.pop("otel.scope.name", None) or tags.pop("otel.library.name", None),
                "scope_version": tags.pop("otel.scope.version", None) or tags.pop("otel.library.version", None),
                "scope_attributes": scope_attrs,
                "resource": _tag_map(proc.get("tags")),
                "attributes": tags,
                "events": sorted(events, key=dumps),
                "links": links,
                "parent": parent,
            }
    return out


def decode_otlp(body: Any) -> Traces:
    """Tempo trace by id (OTLP JSON): {"trace": {"resourceSpans": [...]}}, {"batches": [...]} or bare."""
    if body is None:
        return {}
    if not isinstance(body, dict):
        raise UnknownShape("otlp answer: not an object")
    root = body.get("trace", body)
    if not isinstance(root, dict) or not any(k in root for k in ("resourceSpans", "batches")) and "trace" not in body:
        raise UnknownShape("otlp answer: no resourceSpans")
    out: Traces = {}
    for rs in root.get("resourceSpans") or root.get("batches") or []:
        res_attrs = _kv_map((rs.get("resource") or {}).get("attributes"))
        for ss in rs.get("scopeSpans") or rs.get("instrumentationLibrarySpans") or []:
            sc = ss.get("scope") or ss.get("instrumentationLibrary") or {}
            for sp in ss.get("spans", []) or []:
                tid, sid = sp.get("traceId"), sp.get("spanId")
                st = sp.get("status") or {}
                start, end = _int(sp.get("startTimeUnixNano")) or 0, _int(sp.get("endTimeUnixNano")) or 0
                out.setdefault(tid, {})[sid] = {
                    "name": sp.get("name"),
                    "service": res_attrs.get("service.name"),
                    "start": start,
                    "duration": max(0, end - start),
                    "status": st.get("code"),
                    "status_message": st.get("message") or None,
                    "kind": sp.get("kind"),
                    "attributes": _kv_map(sp.get("attributes")),
                    "scope": sc.get("name"),
                    "scope_version": sc.get("version") or None,
                    "scope_attributes": _kv_map(sc.get("attributes")),
                    "resource": res_attrs,
                    "events": sorted(
                        [{"time": _int(e.get("timeUnixNano")) or 0, "name": e.get("name"),
                          "attributes": _kv_map(e.get("attributes")),
                          "dropped": _int(e.get("droppedAttributesCount")) or 0}
                         for e in sp.get("events", []) or []],
                        key=dumps,
                    ),
                    "links": sorted(
                        [{"trace_id": l.get("traceId"), "span_id": l.get("spanId"), "trace_state": l.get("traceState") or None,
                          "flags": _int(l.get("flags")) or 0, "attributes": _kv_map(l.get("attributes"))}
                         for l in sp.get("links", []) or []],
                        key=dumps,
                    ),
                    "parent": sp.get("parentSpanId") or None,
                }
    return out


def decode_tempo_search(body: Any) -> Traces:
    """Tempo search: trace ids only (no spans)."""
    if body is None:
        return {}
    if not isinstance(body, dict) or not isinstance(body.get("traces"), list):
        raise UnknownShape("tempo search answer: no traces list")
    out: Traces = {}
    for t in body["traces"]:
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
        res.details["span_fields"] = {"common_spans": n, "min_field": worst,
                                      "per_field": {f: round(v, 3) for f, v in per.items()}}
        res.facets["parent_links"] = cap_inexact(100.0 * parent_eq / n, parent_eq == n)
    elif ref_pairs:
        # No span in common: nothing matches, which is not the same as nothing to compare.
        res.facets["span_fields"] = 0.0
        res.facets["parent_links"] = 0.0
        res.details["span_fields"] = {"common_spans": 0, "min_field": "", "per_field": {}}
    d, frac, score = delta_score(float(len(ans_pairs)), float(len(ref_pairs)))
    res.facets["span_count"] = cap_inexact(score, d == 0)
    res.details["span_count"] = {"delta": d, "delta_pct": 100.0 * frac, "ref": len(ref_pairs), "ans": len(ans_pairs)}
    return res
