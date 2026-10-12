"""Registry rows -> API matrix requests.

Every row of the conformance registry (tests/conformance/registry/rows) that declares a request becomes one
smoke request, with the row's method, path, params and tenant headers, or is listed as *not exercised* with the
reason. Nothing is dropped silently: `translate` returns both lists and the counts per reason.

Not exercised (and why) is part of the report: a row that writes, deletes, drains, needs a fixture this stack does
not seed (a second tenant, edge-case logs), or has no HTTP request (a flag) cannot be compared by a read-only run.

The comparison itself is scripts/proof's: each request is sent to hot VictoriaLogs/VictoriaTraces (`ref`, for the
rows that declare `hot` as a target), to Lakehouse built from the base commit and from the PR, and scored by
scripts.proof.metrics (status, error text, rows, values, series, traces, JSON schema). Rows without `hot` target are
Lakehouse-only: the PR is scored against the base.
"""
from __future__ import annotations

import re

from scripts.proof.metrics.ties import tie_group_query

PLACEHOLDER = re.compile(r"\{\{([a-z_.]+)\}\}")
# Paths a read-only matrix never calls: they change data, state or availability of the stack under test.
MUTATING_PREFIXES = ("/insert/", "/internal/insert", "/internal/delete", "/delete/", "/lakehouse/drain", "/internal/cache/clear",
                     "/lakehouse/compaction/recompact", "/internal/force", "/internal/partition", "/internal/lifecycle/drain",
                     "/lakehouse/api/v1/tenants/", "/lakehouse/api/v1/lifecycle")
SEEDED = {"logs.base", "traces.base"}  # datasets this stack seeds (tenant 0:0 of scripts/bench/ab/stack.py)
PROOF_SURFACE = {"vl": "vl-native", "vt": "vt-native"}
# registry compare type -> scripts.proof metrics kind, by what the endpoint answers
STATUS_ONLY = ("status", "error", "absent")


class Skip(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def window_values(window: dict) -> dict[str, str]:
    """Values of the {{seed.*}} placeholders for a window {"start", "end"} (RFC3339)."""
    import datetime as dt
    def ts(s):
        return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp()
    return {"seed.start": window["start"], "seed.cold_end": window["end"],
            "seed.start_us": str(int(ts(window["start"]) * 1e6)), "seed.cold_end_us": str(int(ts(window["end"]) * 1e6)),
            "seed.start_ms": str(int(ts(window["start"]) * 1e3)), "seed.cold_end_ms": str(int(ts(window["end"]) * 1e3)),
            "tenant.account": "0", "tenant.project": "0"}


def substitute(value, values: dict[str, str]):
    """Replace {{x}} placeholders; raises Skip for one this stack cannot provide."""
    if isinstance(value, str):
        def sub(m):
            if m.group(1) not in values:
                raise Skip(f"placeholder {{{{{m.group(1)}}}}} has no value on this stack")
            return values[m.group(1)]
        return PLACEHOLDER.sub(sub, value)
    if isinstance(value, dict):
        return {k: substitute(v, values) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute(v, values) for v in value]
    return value


VALUE_PATHS = ("/field_values", "/field_names", "/streams", "/stream_field_names", "/stream_field_values", "/stream_ids")


def proof_kind(row: dict, path: str) -> str | None:
    """The scripts.proof metrics kind for a request, None for a status-only comparison. The endpoint decides first (its
    answer has one shape whatever the registry's comparator says), then the comparator."""
    ctype = row["compare"]["type"]
    if ctype in STATUS_ONLY:
        return None
    if ctype in ("golden", "ui"):
        raise Skip(f"comparator {ctype!r} is not run by a read-only matrix")
    if path.endswith("/stats_query_range"):
        return "series_prom"
    if path.endswith("/stats_query"):
        return "count"
    if path.endswith("/hits"):
        return "series_hits"
    if path.endswith(VALUE_PATHS):
        return "values"
    if "/metrics/query_range" in path:
        return "series_tempo"
    if ctype == "trace":
        if "/jaeger/" in path or path.startswith("/api/"):
            return "trace_jaeger"
        if "/tempo/api/search" in path:
            return "tempo_search"
        return "trace_otlp"
    if ctype == "ndjson-multiset":
        return "rows"
    if ctype == "exact-json":
        return "json"
    if ctype == "values-with-hits":
        return "values"
    if ctype == "count":
        return "rows"
    if ctype == "series":
        return "series_prom"
    if ctype == "schema":
        return "schema"
    raise Skip(f"comparator {ctype!r} is not run by a read-only matrix")


LIMIT_RE = re.compile(r"\|\s*limit\s+(\d+)\s*$")


def limit_of(params: dict, path: str) -> tuple[int, bool] | None:
    """(N, aggregated) when a LogsQL query ends in `limit N` and has no `sort`: it keeps an arbitrary subset of the rows
    that reach it (upstream documents it), so two correct systems answer different rows. `aggregated` says a `stats`
    pipe comes first: then the limit only cuts when more than N groups exist."""
    q = (params.get("query") or "") if path.endswith("/query") else ""
    m = LIMIT_RE.search(q.strip())
    if not m:
        return None
    if "sort" in q:  # a sort keeps the first N, but rows that tie on the sort key past the limit are still an arbitrary choice
        return (int(m.group(1)), True) if tie_group_query(q) is not None else None
    return int(m.group(1)), "stats" in q


def random_pipe(params: dict) -> bool:
    """`sample N` keeps every N-th row at random: two runs of the same system answer different rows."""
    return bool(re.search(r"\|\s*sample\s+\d+", params.get("query") or ""))


# Sample- and tie-dependent by design: hot VictoriaTraces itself answers a different tag list on every call.
ARBITRARY_PATHS = ("/facets", "/search/tags", "/search/tag/")


def tempo_seconds(path: str, params: dict) -> dict:
    """Tempo's start/end are unix seconds; the registry spells them RFC3339 like the LogsQL rows."""
    if "/tempo/" not in path:
        return params
    import datetime as dt
    out = dict(params)
    for k in ("start", "end"):
        v = out.get(k, "")
        if re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", v):
            out[k] = str(int(dt.datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp()))
    return out


def signals_for(row: dict, path: str) -> list[str]:
    """The binaries a row is sent to. Rows of vl/vt follow their surface; Lakehouse-only rows follow the endpoint, and
    the binary-agnostic ones (/lakehouse/*, /internal/*) go to both."""
    if row["surface"] == "vt" or "/jaeger" in path or "/tempo" in path or "traces" in row["id"]:
        return ["traces"]
    if row["surface"] == "vl" or path.startswith("/select/logsql") or "logs" in row["id"]:
        return ["logs"]
    return ["logs", "traces"]


def proof_surface(row: dict, path: str, signal: str, lh_only: bool) -> str:
    if lh_only:
        return "lh-traces" if signal == "traces" else "lh-logs"
    if path.startswith(("/select/jaeger", "/api/")):
        return "jaeger"
    if "/tempo/" in path:
        return "tempo"
    return "vt-native" if signal == "traces" else "vl-native"


def translate_row(row: dict, values: dict[str, str]) -> list[dict]:
    """One registry row -> its requests (one per binary it applies to); raises Skip with the reason."""
    req = row.get("request")
    if not req:
        raise Skip("no HTTP request (flag or configuration row)")
    path = req["path"]
    if row["kind"] in ("insert", "flag", "internal", "admin") and not path.startswith(("/select/", "/lakehouse/api/", "/lakehouse/info", "/api/")):
        raise Skip(f"kind {row['kind']} is not read-only")
    if path.startswith(MUTATING_PREFIXES) or req["method"] not in ("GET", "POST"):
        raise Skip("mutating or state-changing endpoint")
    if path.endswith("/tail") or "/tail?" in path or "/search/tail" in path:
        raise Skip("streaming endpoint (tail): the answer never ends")
    if req["method"] == "POST" and not path.startswith(("/select/", "/lakehouse/api/")):
        raise Skip("POST outside the select and stats APIs")
    missing = [x for x in (row.get("seed") or []) if x not in SEEDED]
    if missing:
        raise Skip(f"needs seed {', '.join(missing)}, which this stack does not seed")
    if "protobuf" in str(req.get("headers", {})).lower():
        raise Skip("protobuf answer (binary): not decoded by the matrix")
    if "tenant_scope" in row["id"]:
        raise Skip("tenant isolation rows need the isolation fixture (tenants.iso)")
    resolved = substitute({"path": path, "params": req.get("params") or {}, "headers": req.get("headers") or {}}, values)
    kind = proof_kind(row, resolved["path"])
    lh_only = "hot" not in (row.get("targets") or []) or row["surface"] == "lh"
    out = []
    for signal in signals_for(row, resolved["path"]):
        ps = proof_surface(row, resolved["path"], signal, lh_only)
        k = kind
        if lh_only and k is not None and row["compare"]["type"] in ("exact-json", "schema"):
            k = "schema"  # no reference: the PR's answer is scored against the base's, as a schema
        rid = row["id"] if len(signals_for(row, resolved["path"])) == 1 else f"{row['id']}@{signal}"
        out.append({"id": rid, "row": row["id"], "registry_surface": row["surface"], "surface": ps, "signal": signal,
                    "kind": k, "method": resolved["path"] and req["method"], "path": resolved["path"],
                    "params": tempo_seconds(resolved["path"], {a: str(b) for a, b in resolved["params"].items()}), "headers": resolved["headers"],
                    "limit": limit_of(resolved["params"], resolved["path"]),
                    "arbitrary": resolved["path"].endswith(ARBITRARY_PATHS) or random_pipe(resolved["params"]),
                    "expect": row["expect"], "compare": row["compare"]["type"], "lh_only": lh_only,
                    "expect_error": row["compare"]["type"] in ("error", "status", "absent")})
    return out


def translate(rows: list[dict], values: dict[str, str], picks: dict[str, str] | None = None) -> tuple[list[dict], list[dict]]:
    """(requests, not_exercised[{id, reason}]) for all registry rows."""
    vals = dict(values)
    vals.update(picks or {})
    out, skipped = [], []
    for r in rows:
        try:
            out += translate_row(r, vals)
        except Skip as sk:
            skipped.append({"id": r["id"], "reason": sk.reason})
    return out, skipped


def skip_counts(skipped: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for s in skipped:
        key = re.sub(r"\{\{[a-z_.]+\}\}", "{{...}}", s["reason"])
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: -kv[1]))
