"""Turn the backend traffic of a captured page into answers the metrics library scores.

One page load sends Grafana /api/ds/query requests (one answer per refId: metric frames become a Prometheus
matrix, logs frames become rows, trace frames become rows keyed by span id), datasource resource calls
(field_names, field_values, stream_field_values, hits, Jaeger services and operations, ...) and, for VMUI,
VTUI and the Jaeger UI, their own API calls. Each question is keyed so the same question on base, PR and
reference is matched: the request with its volatile parts removed (datasource ids, request ids, intervals).
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

VOLATILE = {"datasource", "datasourceId", "requestId", "key", "uid", "queryCachingTTL", "intervalMs", "maxDataPoints",
            "timezoneOffset", "_"}
VALUE_ENDPOINTS = ("field_names", "field_values", "stream_field_names", "stream_field_values", "streams", "stream_ids",
                   "facets")


def strip(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: strip(v) for k, v in obj.items() if k not in VOLATILE}
    if isinstance(obj, list):
        return [strip(v) for v in obj]
    return obj


def digest(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:10]


def canon(v: Any) -> Any:
    """Nested lists (tags, logs, references) compared as content: sorted by their canonical JSON, so the order a
    producer happens to emit a set of tags in is not a difference of the row."""
    if isinstance(v, dict):
        return {k: canon(x) for k, x in v.items()}
    if isinstance(v, list):
        return sorted((canon(x) for x in v), key=lambda x: json.dumps(x, sort_keys=True, default=str))
    return v


def _frame_rows(fr: dict) -> list[dict]:
    fields = fr["schema"]["fields"]
    values = fr["data"]["values"]
    n = len(values[0]) if values else 0
    rows = []
    for i in range(n):
        row = {}
        for f, col in zip(fields, values):
            v = col[i] if i < len(col) else None
            row[f["name"]] = json.dumps(canon(v), sort_keys=True) if isinstance(v, (dict, list)) else v
        rows.append(row)
    return rows


def _is_metric(fr: dict) -> bool:
    f = fr["schema"]["fields"]
    return len(f) == 2 and f[0].get("type") == "time" and f[1].get("type") == "number"


def _matrix(frames: list[dict]) -> dict:
    result = []
    for fr in frames:
        f = fr["schema"]["fields"]
        t, v = fr["data"]["values"]
        result.append({"metric": f[1].get("labels") or {}, "values": [[x / 1000.0, str(y)] for x, y in zip(t, v) if y is not None]})
    return {"status": "success", "data": {"resultType": "matrix", "result": result}}


def answer_of_result(res: dict) -> tuple[str, str, dict]:
    """(kind, body text, meta options) for one refId of a /api/ds/query response."""
    frames = res.get("frames") or []
    if res.get("error"):
        return "error", json.dumps({"error": res["error"]}), {}
    if frames and all(_is_metric(fr) for fr in frames):
        return "series_prom", json.dumps(_matrix(frames)), {}
    rows = [r for fr in frames for r in _frame_rows(fr)]
    names = set(rows[0]) if rows else set()
    if "spanID" in names:
        return "rows", "\n".join(json.dumps(r, sort_keys=True) for r in rows), {"identity": ["traceID", "spanID"]}
    ident = [k for k in ("Time", "Line") if k in names] or sorted(names)[:2]
    return "rows", "\n".join(json.dumps(r, sort_keys=True) for r in rows), {"identity": ident, "order": None}


def resource_meta(url: str) -> dict:
    """Options for the answer of a call: the rows of a VMUI/VTUI query are scored with the order of their JSON members."""
    return {"key_order": True} if url.split("?")[0].endswith("/select/logsql/query") else {}


def resource_kind(url: str) -> str | None:
    path = url.split("?")[0]
    ep = path.rsplit("/", 1)[-1]
    if ep in VALUE_ENDPOINTS or re.search(r"/api/(services|operations)", path) or path.endswith("/operations"):
        return "values"
    if ep == "hits":
        return "series_hits"
    if path.endswith("/select/logsql/query"):
        return "rows"
    if re.search(r"/api/traces/[0-9a-f]+$", path):
        return "trace_jaeger"
    return None


def _norm_url(url: str) -> str:
    u = re.sub(r"/api/datasources/(proxy/)?uid/[^/]+", "/api/datasources/uid/*", url)
    u = re.sub(r"(&|\?)(_|requestId)=[^&]*", "", u)
    return u


def answers_of(records: list[dict]) -> dict[str, dict]:
    """key -> {kind, body, meta, status}. A key seen twice keeps the last answer."""
    out: dict[str, dict] = {}
    for r in records:
        url, resp, status = r["url"], r.get("response"), r["status"]
        if "/api/ds/query" in url:
            for q in (r.get("request") or {}).get("queries", []):
                key = f"q:{q.get('refId')}:{q.get('queryType')}:{digest(strip(q))}"
                label = f"{q.get('refId')} {q.get('queryType') or ''} {str(q.get('expr') or q.get('query') or '')[:50]}".strip()
                if status != 200:
                    out[key] = {"kind": "error", "body": json.dumps({"status": status}), "meta": {}, "status": status, "label": label}
                    continue
                res = ((resp or {}).get("results") or {}).get(q.get("refId")) or {}
                kind, body, meta = answer_of_result(res)
                out[key] = {"kind": kind, "body": body, "meta": meta, "status": 500 if kind == "error" else 200, "label": label}
            continue
        kind = resource_kind(url)
        if kind is None:
            continue
        req = strip(r.get("request")) if r.get("request") else None
        key = f"r:{_norm_url(url)}:{digest(req)}"
        body = resp if isinstance(resp, str) else json.dumps(resp)
        ep = url.split("?")[0].rsplit("/", 1)[-1]
        field = (r.get("request") or {}).get("field") if isinstance(r.get("request"), dict) else None
        out[key] = {"kind": kind, "body": body, "meta": resource_meta(url), "status": status, "label": f"{ep}({field})" if field else ep}
    return out
