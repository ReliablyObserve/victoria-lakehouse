"""Request rows: what the runner sends, to which surface, in which tenant form and data layer.

A row file is JSON: {"rows": [...]}. A row holds `id`, `row` (the registry row it exercises, or a free
label), `surface`, `kind`, `path`, `params`, `method`, `window` ("layer" adds start/end for the data
layer, "jaeger" adds microsecond start/end, "none" sends no window), `layers`, `forms`, and
optional `meta` (options handed to the metrics library: skip_fields, order, rel_tol, ...), `pick`
(a value taken once from the reference and substituted as {name}), `may_be_empty`, `claimed_gap`.
"""
from __future__ import annotations

import datetime as dt
import json
import os

from ..metrics.common import SURFACE_SIGNAL, SURFACES
from ..metrics.evaluate import KINDS

HERE = os.path.dirname(os.path.abspath(__file__))
ROWS_DIR = os.path.join(HERE, "rows")
LAYERS = ("cold", "buffer", "all")
FORMS = ("numeric", "numeric1001", "alias")
WINDOWS = ("layer", "jaeger", "none")


class RowError(ValueError):
    pass


def load_rows(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    rows = doc["rows"]
    seen = set()
    for r in rows:
        validate_row(r)
        if r["id"] in seen:
            raise RowError(f"duplicate row id {r['id']!r}")
        seen.add(r["id"])
    return rows


def validate_row(r: dict) -> None:
    for k in ("id", "surface", "kind", "path"):
        if k not in r:
            raise RowError(f"row {r.get('id')!r}: missing {k}")
    if r["surface"] not in SURFACES:
        raise RowError(f"row {r['id']}: surface {r['surface']!r} not in {SURFACES}")
    if r["kind"] not in KINDS:
        raise RowError(f"row {r['id']}: kind {r['kind']!r} not in {KINDS}")
    for k, allowed in (("layers", LAYERS), ("forms", FORMS)):
        for v in r.get(k, []):
            if v not in allowed:
                raise RowError(f"row {r['id']}: {k} value {v!r} not in {allowed}")
    if r.get("window", "layer") not in WINDOWS:
        raise RowError(f"row {r['id']}: window {r.get('window')!r} not in {WINDOWS}")
    if not r["path"].startswith("/"):
        raise RowError(f"row {r['id']}: path must start with /")


def load_tier(names: list[str]) -> list[dict]:
    """Rows of one or more row sets. The same row in two sets runs once; two different rows with one id are an error."""
    rows: list[dict] = []
    seen: dict[str, dict] = {}
    for n in names:
        for r in load_rows(n if os.path.exists(n) else os.path.join(ROWS_DIR, n + ".json")):
            if r["id"] in seen:
                if seen[r["id"]] != r:
                    raise RowError(f"row id {r['id']!r} is defined twice with different content")
                continue
            seen[r["id"]] = r
            rows.append(r)
    return rows


def _micro(ts: str) -> str:
    return str(int(dt.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp() * 1_000_000))


def window_params(row: dict, layer: str, state: dict) -> dict:
    w = row.get("window", "layer")
    if w == "none":
        return {}
    win = {"start": state["cold"]["start"], "end": state["buffer"]["end"]} if layer == "all" else state[layer]
    if w == "jaeger":
        return {"start": _micro(win["start"]), "end": _micro(win["end"])}
    return {"start": win["start"], "end": win["end"]}


def expand(row: dict, state: dict, picks=None) -> list[dict]:
    """The concrete requests of a row: one per (form, layer). `picks` is a dict of values for the {name}
    placeholders of the path and params, or a function (form, layer) -> dict (a value taken from the reference
    per tenant form and layer)."""
    out = []
    signal = SURFACE_SIGNAL[row["surface"]]
    for form in row.get("forms", ["numeric", "alias"]):
        for layer in row.get("layers", ["cold", "buffer"]):
            win = {"end": state["buffer"]["end"]} if layer == "all" else state[layer]
            got = picks(form, layer) if callable(picks) else (picks or {})
            fmt = {**got, "END": win["end"]}
            params = {k: str(v).format(**fmt) if isinstance(v, str) else v for k, v in row.get("params", {}).items()}
            params.update(window_params(row, layer, state))
            path = row["path"].format(**fmt)
            out.append({"row": row, "signal": signal, "form": form, "layer": layer, "path": path, "params": params,
                        "method": row.get("method", "POST" if "/logsql/" in row["path"] else "GET")})
    return out


def tenant_headers(form: str, target: str) -> dict:
    """Numeric forms are sent as they are. The alias form goes to Lakehouse as the string OrgID and to hot
    as its numeric equivalent: upstream has no aliases."""
    if form == "numeric":
        return {"AccountID": "0", "ProjectID": "0"}
    if form == "numeric1001":
        return {"AccountID": "1001", "ProjectID": "0"}
    if form == "alias":
        if target == "ref":
            return {"AccountID": "1001", "ProjectID": "0"}
        return {"X-Scope-OrgID": "acme-corp"}
    raise RowError(f"unknown tenant form {form!r}")
