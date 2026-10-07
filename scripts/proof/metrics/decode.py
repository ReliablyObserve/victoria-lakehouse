"""Decoders: wire bodies of the proof's surfaces into comparable structures."""
from __future__ import annotations

import json
from typing import Any

from .common import to_number
from .ties import parse_time_ns

Series = dict[str, dict[int, float]]


def load_body(body: Any) -> Any:
    """A body stored as text is parsed as JSON when it is; otherwise returned as is."""
    if isinstance(body, str):
        s = body.strip()
        if s[:1] in ("{", "[") and "\n" not in s:
            try:
                return json.loads(s)
            except ValueError:
                return body
    return body


def _label_key(labels: dict) -> str:
    return json.dumps({str(k): str(v) for k, v in sorted(labels.items())}, separators=(",", ":"))


def _ts_ns(ts: Any) -> int | None:
    if isinstance(ts, str):
        n = parse_time_ns(ts)
        if n is not None:
            return n
    f = to_number(ts)
    if f is None:
        return None
    return int(round(f * 1_000_000_000))  # seconds, as Prometheus-style answers carry


def decode_prom(body: Any) -> Series:
    """Prometheus-style vector or matrix (stats_query, stats_query_range, Loki metric queries)."""
    out: Series = {}
    data = (body or {}).get("data", {}) if isinstance(body, dict) else {}
    for item in data.get("result", []) or []:
        pts = out.setdefault(_label_key(item.get("metric", {}) or {}), {})
        values = item.get("values")
        if values is None and item.get("value") is not None:
            values = [item["value"]]
        for pair in values or []:
            ts = _ts_ns(pair[0])
            v = to_number(pair[1])
            if ts is not None:
                pts[ts] = v if v is not None else float("nan")
    return out


def decode_hits(body: Any) -> Series:
    """VictoriaLogs /select/logsql/hits."""
    out: Series = {}
    for h in (body or {}).get("hits", []) if isinstance(body, dict) else []:
        pts = out.setdefault(_label_key(h.get("fields", {}) or {}), {})
        for ts, v in zip(h.get("timestamps", []) or [], h.get("values", []) or []):
            t = _ts_ns(ts)
            n = to_number(v)
            if t is not None:
                pts[t] = n if n is not None else float("nan")
    return out


def _otlp_value(v: Any) -> Any:
    if isinstance(v, dict):
        for k in ("stringValue", "intValue", "doubleValue", "boolValue"):
            if k in v:
                x = v[k]
                return int(x) if k == "intValue" and str(x).lstrip("-").isdigit() else x
        return json.dumps(v, sort_keys=True)
    return v


def decode_tempo_metrics(body: Any) -> Series:
    """Tempo TraceQL metrics query_range."""
    out: Series = {}
    for s in (body or {}).get("series", []) if isinstance(body, dict) else []:
        labels = {l.get("key"): _otlp_value(l.get("value")) for l in s.get("labels", []) or []}
        pts = out.setdefault(_label_key(labels), {})
        for smp in s.get("samples", []) or []:
            ms = to_number(smp.get("timestampMs"))
            n = to_number(smp.get("value"))
            if ms is not None:
                pts[int(ms) * 1_000_000] = n if n is not None else float("nan")
    return out


def decode_values(body: Any) -> dict[str, float | None]:
    """field_values, field_names, streams, facets-style value lists, Jaeger services/operations,
    Tempo tags. Returns value -> hits (None when the answer carries no hits)."""
    out: dict[str, float | None] = {}
    if isinstance(body, dict):
        items = body.get("values")
        if items is None:
            items = body.get("data")
        if items is None:
            items = body.get("tagNames") or body.get("scopes") or []
    else:
        items = body or []
    if isinstance(items, dict):
        # Tempo v2 tags: {"scopes":[{"name":..,"tags":[..]}]}; flatten
        items = list(items.values())
    for it in items or []:
        if isinstance(it, dict) and "value" in it:
            out[str(it["value"])] = to_number(it.get("hits"))
        elif isinstance(it, dict) and "name" in it and "tags" in it:
            for t in it["tags"]:
                out[f'{it["name"]}.{t}'] = None
        elif isinstance(it, dict) and "operationName" in it:
            out[str(it["operationName"])] = None
        elif isinstance(it, dict) and "tagValues" in it:
            for t in it["tagValues"]:
                out[str(t)] = None
        elif isinstance(it, (str, int, float)):
            out[str(it)] = None
        elif isinstance(it, dict) and "name" in it:
            out[str(it["name"])] = None
    return out


def stats_scalar(body: Any) -> float | None:
    """The single number of a one-series, one-point stats_query answer."""
    s = decode_prom(body)
    vals = [v for pts in s.values() for v in pts.values()]
    return vals[0] if len(vals) == 1 else None
