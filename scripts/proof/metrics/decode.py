"""Decoders: wire bodies of the proof's surfaces into comparable structures.

A decoder raises UnknownShape for a body it does not recognise, so an unfamiliar
answer becomes a harness-error rather than an empty (and therefore matching) one.
None means "no body" (an error answer) and decodes to an empty structure.
"""
from __future__ import annotations

import json
from typing import Any

from .common import UnknownShape, to_number
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


def _need_dict(body: Any, what: str) -> dict:
    if not isinstance(body, dict):
        raise UnknownShape(f"{what}: expected a JSON object, got {type(body).__name__}")
    return body


def prom_result_type(body: Any) -> str | None:
    data = body.get("data") if isinstance(body, dict) else None
    return data.get("resultType") if isinstance(data, dict) else None


def decode_prom(body: Any) -> Series:
    """Prometheus-style vector or matrix (stats_query, stats_query_range, Loki metric queries)."""
    if body is None:
        return {}
    d = _need_dict(body, "prometheus answer")
    data = d.get("data")
    if not isinstance(data, dict) or data.get("resultType") not in ("vector", "matrix"):
        raise UnknownShape(f"prometheus answer: resultType {prom_result_type(d)!r} is not vector or matrix")
    out: Series = {}
    for item in data.get("result") or []:
        pts = out.setdefault(_label_key(item.get("metric", {}) or {}), {})
        values = item.get("values")
        if values is None and item.get("value") is not None:
            values = [item["value"]]
        for pair in values or []:
            ts = _ts_ns(pair[0])
            v = to_number(pair[1])
            if ts is None:
                raise UnknownShape(f"prometheus answer: unparsable timestamp {pair[0]!r}")
            pts[ts] = v if v is not None else float("nan")
    return out


def stats_vector(body: Any) -> dict[str, float]:
    """A stats_query vector keyed by its labels (without __name__): `stats count()` has one
    key ({}), `stats by (level) count()` one per group."""
    out: dict[str, float] = {}
    for key, pts in decode_prom(body).items():
        labels = {k: v for k, v in json.loads(key).items() if k != "__name__"}
        for v in pts.values():
            out[_label_key(labels)] = v
    return out


def loki_streams_rows(body: Any) -> list[dict]:
    """Loki `resultType: streams` as rows: one per log line with its stream labels."""
    d = _need_dict(body, "loki streams answer")
    data = d.get("data")
    if not isinstance(data, dict) or data.get("resultType") != "streams":
        raise UnknownShape("loki streams answer: resultType is not streams")
    rows = []
    for st in data.get("result") or []:
        labels = st.get("stream", {}) or {}
        for ts, line in st.get("values") or []:
            row = {"_time": str(ts), "_msg": line}
            row.update({f"stream.{k}": v for k, v in labels.items()})
            rows.append(row)
    return rows


def decode_hits(body: Any) -> Series:
    """VictoriaLogs /select/logsql/hits."""
    if body is None:
        return {}
    d = _need_dict(body, "hits answer")
    if not isinstance(d.get("hits"), list):
        raise UnknownShape("hits answer: no hits list")
    out: Series = {}
    for h in d["hits"]:
        pts = out.setdefault(_label_key(h.get("fields", {}) or {}), {})
        for ts, v in zip(h.get("timestamps", []) or [], h.get("values", []) or []):
            t = _ts_ns(ts)
            n = to_number(v)
            if t is None:
                raise UnknownShape(f"hits answer: unparsable timestamp {ts!r}")
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
    if body is None:
        return {}
    d = _need_dict(body, "tempo metrics answer")
    if not isinstance(d.get("series"), list):
        raise UnknownShape("tempo metrics answer: no series list")
    out: Series = {}
    for s in d["series"]:
        labels = {l.get("key"): _otlp_value(l.get("value")) for l in s.get("labels", []) or []}
        pts = out.setdefault(_label_key(labels), {})
        for smp in s.get("samples", []) or []:
            ms = to_number(smp.get("timestampMs"))
            n = to_number(smp.get("value"))
            if ms is None:
                raise UnknownShape("tempo metrics answer: sample without timestampMs")
            pts[int(ms) * 1_000_000] = n if n is not None else float("nan")
    return out


def decode_values(body: Any) -> dict[str, float | None]:
    """Value lists: field_values, field_names, streams, facets, Jaeger services, operations and
    dependencies, Tempo tags and tag values. Returns value -> hits (None when the answer carries none)."""
    if body is None:
        return {}
    out: dict[str, float | None] = {}
    if isinstance(body, dict):
        if "facets" in body:  # VictoriaLogs /select/logsql/facets
            for f in body["facets"] or []:
                for v in f.get("values", []) or []:
                    out[f'{f.get("field_name")}={v.get("field_value")}'] = to_number(v.get("hits"))
            return out
        for key in ("values", "data", "tagValues", "tagNames", "scopes", "tags"):
            if key in body:
                items = body[key]
                break
        else:
            raise UnknownShape(f"value list: no known key in {sorted(body)[:6]}")
    elif isinstance(body, list):
        items = body
    else:
        raise UnknownShape(f"value list: got {type(body).__name__}")
    if items is None:
        return out
    if isinstance(items, dict):
        items = list(items.values())
    for it in items:
        if isinstance(it, dict) and "value" in it:  # VL {"value","hits"}, Tempo {"type","value"}
            out[str(it["value"])] = to_number(it.get("hits"))
        elif isinstance(it, dict) and "child" in it and "parent" in it:  # Jaeger dependency edge
            out[f'{it["parent"]}->{it["child"]}'] = to_number(it.get("callCount"))
        elif isinstance(it, dict) and "name" in it and "tags" in it:  # Tempo scopes
            for t in it["tags"]:
                out[f'{it["name"]}.{t}'] = None
        elif isinstance(it, dict) and "operationName" in it:
            out[str(it["operationName"])] = None
        elif isinstance(it, dict) and "tagValues" in it:
            for t in it["tagValues"]:
                out[str(t)] = None
        elif isinstance(it, dict) and "name" in it:  # Jaeger operations with spanKind
            out[str(it["name"])] = None
        elif isinstance(it, (str, int, float)) and not isinstance(it, bool):
            out[str(it)] = None
        else:
            raise UnknownShape(f"value list: unrecognised item {str(it)[:60]!r}")
    return out


def stats_scalar(body: Any) -> float | None:
    """The single number of a one-series, one-point stats_query answer."""
    s = decode_prom(body)
    vals = [v for pts in s.values() for v in pts.values()]
    return vals[0] if len(vals) == 1 else None
