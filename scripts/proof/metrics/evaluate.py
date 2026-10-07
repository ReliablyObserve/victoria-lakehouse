"""Evaluate one request (reference, base, PR answers) into facet vectors."""
from __future__ import annotations

from typing import Any

from .common import LH_SURFACES, SURFACE_SIGNAL, SURFACES, FacetResult
from .decode import (
    decode_hits,
    decode_prom,
    decode_tempo_metrics,
    decode_values,
    load_body,
    loki_streams_rows,
    prom_result_type,
    stats_vector,
)
from .generic import (
    evaluate_json_leaves,
    evaluate_schema,
    evaluate_status,
    evaluate_truth,
    is_error,
)
from .rows import LOG_IDENTITY, SPAN_IDENTITY, evaluate_count_vector, evaluate_rows, key_order_agreement, parse_ndjson
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


def is_lh_only(meta: dict) -> bool:
    """Lakehouse-only APIs have no upstream reference."""
    return bool(meta.get("lh_only", meta.get("surface") in LH_SURFACES))


def signal_of(meta: dict) -> str:
    """The signal follows the surface, never a free-form meta field."""
    return SURFACE_SIGNAL[meta["surface"]]


def _body(ans: dict) -> Any:
    return load_body(ans.get("body"))


def _is_streams(body: Any) -> bool:
    return prom_result_type(body) == "streams"


def answer_empty(kind: str, ans: dict) -> bool:
    """True for a 2xx answer that carries no data (an empty answer proves nothing)."""
    if is_error(ans):
        return False
    b = _body(ans)
    if kind == "rows":
        return not parse_ndjson(b)
    if kind in ("series_prom", "count"):
        if _is_streams(b):
            return not loki_streams_rows(b)
        return not any(decode_prom(b).values())
    if kind == "series_hits":
        return not any(decode_hits(b).values())
    if kind == "series_tempo":
        return not any(decode_tempo_metrics(b).values())
    if kind == "values":
        return not decode_values(b)
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
    r_err, a_err = is_error(ref), is_error(ans)
    if r_err and a_err:
        return res  # both answered with an error: status and error text are all there is to score
    # One side is an error: its body facets are scored against an empty body, never skipped
    # (a missing answer must not look like a perfect one).
    rb = None if r_err else _body(ref)
    ab = None if a_err else _body(ans)
    skip = frozenset(meta.get("skip_fields", []))
    rel, ab_tol = meta.get("rel_tol", 1e-9), meta.get("abs_tol", 0.0)
    if kind == "rows":
        ident = tuple(meta.get("identity") or (SPAN_IDENTITY if signal_of(meta) == "traces" else LOG_IDENTITY))
        tie_fields = None
        tq = None
        if meta.get("query"):
            tq = tie_group_query(meta["query"])
            tie_fields = tq[1] if tq else None
        fetch = _tie_fetch(meta) if (meta.get("query") is None or tq) else None
        body = evaluate_rows(parse_ndjson(rb), parse_ndjson(ab), identity=ident, skip=skip, order=meta.get("order"),
                             tie_fetch=fetch, tie_sort_fields=tie_fields, tie_decision=ans.get("tie_cut"))
        if meta.get("key_order"):
            body = merge(body, key_order_agreement(parse_ndjson(rb), parse_ndjson(ab), ident))
    elif kind in ("series_prom", "count") and (_is_streams(rb) or _is_streams(ab)):
        # Loki log queries: per-stream labels and lines, scored as rows
        body = evaluate_rows(loki_streams_rows(rb) if rb else [], loki_streams_rows(ab) if ab else [],
                             identity=tuple(meta.get("identity") or ("_time", "_msg")), skip=skip)
    elif kind == "count":
        body = evaluate_count_vector(stats_vector(rb), stats_vector(ab))
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
    else:  # schema, truth: Lakehouse-only answers are compared as schemas against the reference
        body = evaluate_schema(rb, ab, meta.get("contract"))
    return merge(res, body)


def evaluate_request(meta: dict, answers: dict[str, dict]) -> tuple[FacetResult, FacetResult]:
    """(base result, PR result). answers has base, pr and, for upstream surfaces, ref.

    Upstream surfaces score base and PR against ref. Lakehouse-only rows
    (meta lh_only, surfaces lh-logs and lh-traces) score the PR against base (schema, M12)
    and both against the truth answers (M13) when meta has a truth block and answers has
    truth_base / truth_pr.
    """
    base, pr = answers["base"], answers["pr"]
    if not is_lh_only(meta):
        ref = answers["ref"]
        return evaluate_body(meta, ref, base), evaluate_body(meta, ref, pr)
    b = evaluate_body(meta, base, base)
    p = evaluate_body(meta, base, pr)
    t = meta.get("truth")
    if t:
        opts = {k: t[k] for k in ("lh_path", "key_label", "key_field", "value_field", "where", "rel_tol") if k in t}
        for res, key, ans in ((b, "truth_base", base), (p, "truth_pr", pr)):
            tr = evaluate_truth(t["shape"], _body(ans), _body(answers[key]), unflushed_rows=t.get("unflushed_rows", 0), **opts)
            res.facets.update(tr.facets)
            res.details["truth"] = {**tr.details["truth"], "unflushed_rows": tr.details["unflushed_rows"]}
    return b, p


def check_meta(meta: dict) -> None:
    s = meta.get("surface")
    if s not in SURFACES:
        raise ValueError(f"surface {s!r} not one of {SURFACES}")
    if "signal" in meta and meta["signal"] != SURFACE_SIGNAL[s]:
        raise ValueError(f"surface {s} belongs to signal {SURFACE_SIGNAL[s]}")
    if meta.get("lh_only") is False and s in LH_SURFACES:
        raise ValueError(f"surface {s} is Lakehouse-only")
