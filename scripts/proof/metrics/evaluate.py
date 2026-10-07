"""Evaluate one request (reference, base, PR answers) into facet vectors."""
from __future__ import annotations

from typing import Any

from .common import SURFACE_SIGNAL, SURFACES, FacetResult
from .decode import (
    decode_hits,
    decode_prom,
    decode_tempo_metrics,
    decode_values,
    load_body,
    stats_scalar,
)
from .generic import (
    evaluate_json_leaves,
    evaluate_schema,
    evaluate_status,
    evaluate_truth,
    is_error,
)
from .rows import LOG_IDENTITY, SPAN_IDENTITY, evaluate_count, evaluate_rows, parse_ndjson
from .series import evaluate_series
from .trace import decode_jaeger, decode_otlp, decode_tempo_search, evaluate_traces
from .values import evaluate_values
from .ties import tie_group_query

KINDS = (
    "rows", "count", "series_prom", "series_hits", "series_tempo", "values",
    "trace_jaeger", "trace_otlp", "tempo_search", "json", "schema", "truth",
)


def merge(*parts: FacetResult) -> FacetResult:
    out = FacetResult()
    for p in parts:
        out.facets.update(p.facets)
        out.details.update(p.details)
        out.notes.extend(p.notes)
    return out


def _body(ans: dict) -> Any:
    return load_body(ans.get("body"))


def answer_empty(kind: str, ans: dict) -> bool:
    """True for a 2xx answer that carries no data (an empty answer proves nothing)."""
    if is_error(ans):
        return False
    b = _body(ans)
    if kind == "rows":
        return not parse_ndjson(b)
    if kind in ("series_prom",):
        return not any(decode_prom(b).values())
    if kind == "series_hits":
        return not any(decode_hits(b).values())
    if kind == "series_tempo":
        return not any(decode_tempo_metrics(b).values())
    if kind == "values":
        return not decode_values(b)
    if kind == "count":
        return stats_scalar(b) is None
    if kind == "trace_jaeger":
        return not decode_jaeger(b)
    if kind == "trace_otlp":
        return not decode_otlp(b)
    if kind == "tempo_search":
        return not decode_tempo_search(b)
    return b in (None, "", {}, [])


def _tie_fetch(meta: dict):
    """Offline tie-group re-read: meta['tie_group'] holds the re-read bodies."""
    tg = meta.get("tie_group")
    if not tg:
        return None
    ref_rows, sut_rows = parse_ndjson(tg["ref"]), parse_ndjson(tg["sut"])
    rs, ss = tg.get("ref_status", 200), tg.get("sut_status", 200)
    return lambda _at: (rs, ref_rows, ss, sut_rows)


def evaluate_body(meta: dict, ref: dict, ans: dict) -> FacetResult:
    kind = meta["kind"]
    if kind not in KINDS:
        raise ValueError(f"unknown kind {kind!r}")
    res = evaluate_status(ref, ans)
    if is_error(ref) or is_error(ans):
        return res  # an error answer has no body to score
    rb, ab = _body(ref), _body(ans)
    skip = frozenset(meta.get("skip_fields", []))
    rel, ab_tol = meta.get("rel_tol", 1e-9), meta.get("abs_tol", 0.0)
    if kind == "rows":
        ident = tuple(meta.get("identity") or (SPAN_IDENTITY if meta.get("signal") == "traces" else LOG_IDENTITY))
        order = meta.get("order")
        tie_fields = None
        tq = None
        if meta.get("query"):
            tq = tie_group_query(meta["query"])
            tie_fields = tq[1] if tq else None
        fetch = _tie_fetch(meta) if (meta.get("query") is None or tq) else None
        body = evaluate_rows(parse_ndjson(rb), parse_ndjson(ab), identity=ident, skip=skip, order=order,
                             tie_fetch=fetch, tie_sort_fields=tie_fields)
    elif kind == "count":
        body = evaluate_count(stats_scalar(rb), stats_scalar(ab))
    elif kind == "series_prom":
        body = evaluate_series(decode_prom(rb), decode_prom(ab), rel_tol=rel, abs_tol=ab_tol)
    elif kind == "series_hits":
        body = evaluate_series(decode_hits(rb), decode_hits(ab), rel_tol=rel, abs_tol=ab_tol)
    elif kind == "series_tempo":
        body = evaluate_series(decode_tempo_metrics(rb), decode_tempo_metrics(ab), rel_tol=rel, abs_tol=ab_tol)
    elif kind == "values":
        body = evaluate_values(decode_values(rb), decode_values(ab), rel_tol=rel if rel > 1e-9 else 0.0)
    elif kind == "trace_jaeger":
        body = evaluate_traces(decode_jaeger(rb), decode_jaeger(ab))
    elif kind == "trace_otlp":
        body = evaluate_traces(decode_otlp(rb), decode_otlp(ab))
    elif kind == "tempo_search":
        body = evaluate_traces(decode_tempo_search(rb), decode_tempo_search(ab))
    elif kind == "json":
        body = evaluate_json_leaves(rb, ab)
    else:  # schema, truth: LH-only answers are compared as schemas against the reference
        body = evaluate_schema(rb, ab, meta.get("contract"))
    return merge(res, body)


def evaluate_request(meta: dict, answers: dict[str, dict]) -> tuple[FacetResult, FacetResult]:
    """(base result, PR result). answers has base, pr and, for upstream surfaces, ref.

    Upstream surfaces score base and PR against ref. Lakehouse-only rows
    (meta lh_only) score the PR against base (schema, M12) and both against the
    truth answers (M13) when meta has a truth block and answers has truth_base / truth_pr.
    """
    base, pr = answers["base"], answers["pr"]
    if not meta.get("lh_only"):
        ref = answers["ref"]
        return evaluate_body(meta, ref, base), evaluate_body(meta, ref, pr)
    b = evaluate_body(meta, base, base)
    p = evaluate_body(meta, base, pr)
    t = meta.get("truth")
    if t:
        for res, key, ans in ((b, "truth_base", base), (p, "truth_pr", pr)):
            tr = answers[key]
            res.facets.update(
                evaluate_truth(t["shape"], _body(ans), _body(tr), lh_path=t.get("lh_path", ""),
                               key_label=t.get("key_label", ""), unflushed_rows=t.get("unflushed_rows", 0)).facets
            )
            res.details["truth"] = evaluate_truth(
                t["shape"], _body(ans), _body(tr), lh_path=t.get("lh_path", ""),
                key_label=t.get("key_label", ""), unflushed_rows=t.get("unflushed_rows", 0)).details
    return b, p


def check_meta(meta: dict) -> None:
    s = meta.get("surface")
    if s not in SURFACES:
        raise ValueError(f"surface {s!r} not one of {SURFACES}")
    if meta.get("signal", SURFACE_SIGNAL[s]) != SURFACE_SIGNAL[s]:
        raise ValueError(f"surface {s} belongs to signal {SURFACE_SIGNAL[s]}")
