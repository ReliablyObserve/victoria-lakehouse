"""M1 status, M2 error, M11 JSON leaf, M12 schema, M13 truth, M14 latency."""
from __future__ import annotations

import re
from typing import Any

from .common import FacetResult, cap_inexact, delta_score, dumps, numbers_equal, percentile, set_scores, to_number
from .decode import decode_prom, load_body, stats_scalar

# An answer as captured by the runner.
# {"status": 200, "latency_ms": 12.5, "body": <json or text>, "warnings": [..], "timeout": false}


def is_error(ans: dict) -> bool:
    body = load_body(ans.get("body"))
    if (ans.get("status") or 0) >= 400 or ans.get("timeout"):
        return True
    return isinstance(body, dict) and (body.get("status") == "error" or "error" in body and body.get("error"))


def error_text(ans: dict) -> str:
    """Error message, normalised for whitespace and quoting only (case is part of the message)."""
    body = load_body(ans.get("body"))
    if isinstance(body, dict):
        msg = body.get("error") or body.get("message") or body.get("errors") or dumps(body)
    else:
        msg = body or ""
    s = str(msg)
    s = re.sub(r"[\"'`]", '"', s)
    return re.sub(r"\s+", " ", s).strip()


def evaluate_status(ref: dict, ans: dict) -> FacetResult:
    """M1 always; M2 when either side is an error."""
    res = FacetResult()
    rs, as_ = ref.get("status"), ans.get("status")
    res.facets["status"] = 100.0 if rs == as_ else 0.0
    res.details["status"] = {"ref": rs, "ans": as_}
    if is_error(ref) or is_error(ans):
        same = error_text(ref) == error_text(ans)
        res.facets["error"] = 100.0 if same else 0.0
        res.details["error"] = {"ref": error_text(ref)[:200], "ans": error_text(ans)[:200]}
    return res


def flatten_leaves(v: Any, path: str = "") -> dict[str, str]:
    """Leaf path -> canonical leaf text. Arrays are indexed."""
    out: dict[str, str] = {}
    if isinstance(v, dict):
        if not v and path:
            out[path] = "{}"
        for k, x in v.items():
            out.update(flatten_leaves(x, f"{path}.{k}" if path else str(k)))
    elif isinstance(v, list):
        if not v and path:
            out[path] = "[]"
        for i, x in enumerate(v):
            out.update(flatten_leaves(x, f"{path}[{i}]"))
    else:
        out[path] = dumps(v)
    return out


def evaluate_json_leaves(ref: Any, ans: Any) -> FacetResult:
    """M11: equal leaf paths / union of leaf paths."""
    rl, al = flatten_leaves(ref), flatten_leaves(ans)
    union = rl.keys() | al.keys()
    equal = sum(1 for k in union if k in rl and k in al and rl[k] == al[k])
    res = FacetResult()
    res.facets["json_leaves"] = cap_inexact(100.0 if not union else 100.0 * equal / len(union), equal == len(union))
    diff = sorted(k for k in union if rl.get(k) != al.get(k))
    res.details["json_leaves"] = {"equal": equal, "union": len(union), "differing_paths": diff[:20]}
    return res


def _type_name(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, (int, float)):
        return "number"
    if isinstance(v, str):
        return "string"
    return "array" if isinstance(v, list) else "object"


def schema_paths(v: Any, path: str = "") -> dict[str, set[str]]:
    """Leaf paths (arrays collapsed to []) -> set of types seen there."""
    out: dict[str, set[str]] = {}
    if isinstance(v, dict):
        for k, x in v.items():
            for p, t in schema_paths(x, f"{path}.{k}" if path else str(k)).items():
                out.setdefault(p, set()).update(t)
    elif isinstance(v, list):
        if not v:
            out.setdefault(path + "[]", set())
        for x in v:
            for p, t in schema_paths(x, path + "[]").items():
                out.setdefault(p, set()).update(t)
    else:
        out.setdefault(path, set()).add(_type_name(v))
    return out


def evaluate_schema(ref: Any, ans: Any, contract: dict[str, str] | None = None) -> FacetResult:
    """M12: key-set Jaccard and type agreement vs the reference, and vs a contract
    ({path: type, or "any"}) when given. Counter values moving does not matter: only
    keys and types are compared."""
    rs, as_ = schema_paths(ref), schema_paths(ans)
    res = FacetResult()
    ks = set_scores(rs.keys(), as_.keys())
    res.facets["schema_keys"] = cap_inexact(ks["jaccard"], set(rs) == set(as_))
    common = [p for p in rs if p in as_ and rs[p] and as_[p]]
    agree = sum(1 for p in common if rs[p] == as_[p])
    # Nothing in common while either side has paths means nothing agrees; two empty schemas agree.
    types_pct = (100.0 if not (rs or as_) else 0.0) if not common else 100.0 * agree / len(common)
    res.facets["schema_types"] = cap_inexact(types_pct, types_pct >= 100.0)
    res.details["schema"] = {
        "missing": sorted(set(rs) - set(as_))[:20],
        "extra": sorted(set(as_) - set(rs))[:20],
        "type_mismatch": sorted(p for p in common if rs[p] != as_[p])[:20],
    }
    if contract is not None:
        ok = 0
        bad = []
        for p, t in contract.items():
            seen = as_.get(p)
            if seen is not None and (t == "any" or seen <= {t}):
                ok += 1
            else:
                bad.append(p)
        res.facets["schema_contract"] = cap_inexact(100.0 if not contract else 100.0 * ok / len(contract), not bad)
        res.details["schema_contract"] = {"violations": sorted(bad)[:20]}
    return res


def _dig(body: Any, path: str) -> Any:
    cur = body
    for part in path.split(".") if path else []:
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
            cur = cur[int(part)]
        else:
            return None
    return cur


def _tenant_ids(v: Any) -> set[str]:
    out = set()
    for t in v or []:
        if isinstance(t, dict):
            out.add(f"{t.get('account_id', t.get('AccountID', 0))}:{t.get('project_id', t.get('ProjectID', 0))}")
        elif isinstance(t, int) and not isinstance(t, bool):
            out.add(f"{t}:0")
        else:
            out.add(str(t))
    return out


def _vector_by(body: Any, key_label: str) -> dict[str, float]:
    import json as _json
    out: dict[str, float] = {}
    for k, pts in decode_prom(body).items():
        labels = _json.loads(k)
        if key_label in labels and pts:
            out[labels[key_label]] = next(iter(pts.values()))
    return out


def _matches(item: dict, where: dict | None) -> bool:
    return all(item.get(k) == v for k, v in (where or {}).items())


def evaluate_truth(shape: str, lh_body: Any, truth_body: Any, *, lh_path: str = "", key_label: str = "",
                   key_field: str = "", value_field: str = "", where: dict | None = None,
                   rel_tol: float = 0.0, unflushed_rows: int = 0) -> FacetResult:
    """M13: a Lakehouse number against the same calculation over hot VL/VT.

    Shapes (response paths are bound per registry row via lh_path):
      scalar      lh number at lh_path vs a one-point stats_query answer (rows vs count())
      scalar_sum  lh number at lh_path vs the SUM over all groups of a stats_query vector
                  (total_rows vs count() by tenant)
      per_key     lh mapping or list at lh_path vs a vector keyed by label key_label
                  (cardinality vs count_uniq); a list of objects needs key_field and
                  value_field, `where` keeps only matching objects (indexed: true), and
                  rel_tol allows for a probabilistic count (HLL)
      tenant_set  lh tenant list vs hot's tenant list (/select/tenant_ids); ids compare as
                  "account:project" strings, bare numbers are an account with project 0
    unflushed_rows is reported next to the score and never subtracted from it.
    """
    res = FacetResult()
    if shape in ("scalar", "scalar_sum"):
        lh = to_number(_dig(lh_body, lh_path))
        if shape == "scalar":
            tr = stats_scalar(truth_body)
        else:
            vec = decode_prom(truth_body)
            vals = [v for pts in vec.values() for v in pts.values()]
            tr = sum(vals) if vals else None
        if lh is None or tr is None:
            res.facets["truth"] = 0.0
            res.details["truth"] = {"lh": lh, "truth": tr, "error": "value not found"}
        else:
            d, frac, s = delta_score(lh, tr)
            res.facets["truth"] = cap_inexact(s, d == 0)
            res.details["truth"] = {"lh": lh, "truth": tr, "delta": d, "delta_pct": 100.0 * frac}
    elif shape == "per_key":
        raw = _dig(lh_body, lh_path)
        if isinstance(raw, list):
            raw = {str(it[key_field]): it[value_field] for it in raw
                   if isinstance(it, dict) and key_field in it and _matches(it, where)}
        lh_map = {str(k): to_number(v) for k, v in raw.items()} if isinstance(raw, dict) else {}
        truth = _vector_by(truth_body, key_label)
        scores = {}
        for k in lh_map.keys() | truth.keys():
            if k in lh_map and k in truth and lh_map[k] is not None:
                if numbers_equal(lh_map[k], truth[k], rel_tol):
                    scores[k] = 100.0
                else:
                    scores[k] = min(delta_score(lh_map[k], truth[k])[2], 99.999)
            else:
                scores[k] = 0.0
        exact = all(v >= 100.0 for v in scores.values())
        res.facets["truth"] = cap_inexact(min(scores.values()) if scores else 100.0, exact)
        res.details["truth"] = {"keys": len(scores), "mean_score": sum(scores.values()) / len(scores) if scores else 100.0,
                                "differing": sorted(k for k, v in scores.items() if v < 100.0)[:20]}
    elif shape == "tenant_set":
        lh = _tenant_ids(_dig(lh_body, lh_path) if lh_path else lh_body)
        tr = _tenant_ids(truth_body.get("data") if isinstance(truth_body, dict) else truth_body)
        sc = set_scores(tr, lh)
        res.facets["truth"] = cap_inexact(sc["jaccard"], lh == tr)
        res.details["truth"] = {"jaccard": sc["jaccard"], "missing": sorted(tr - lh), "extra": sorted(lh - tr)}
    else:
        raise ValueError(f"unknown truth shape {shape!r}")
    res.details["unflushed_rows"] = unflushed_rows
    return res


def latency_ratio(pr: list[tuple[float, bool]], base: list[tuple[float, bool]],
                  ref: list[tuple[float, bool]] | None = None) -> dict[str, Any]:
    """M14: PR / base and PR / ref p50 over validated samples only.

    Each sample is (latency_ms, valid). Invalid answers never count as latency;
    their number is reported. Ratios are None when a side has no valid sample."""
    def p50(samples):
        vals = [l for l, ok in samples if ok]
        return (percentile(vals, 0.5) if vals else None), len(samples) - len(vals)

    pp, pi = p50(pr)
    bp, bi = p50(base)
    out: dict[str, Any] = {
        "pr_p50_ms": pp, "base_p50_ms": bp,
        "pr_over_base": (pp / bp) if pp is not None and bp else None,
        "invalid": {"pr": pi, "base": bi},
    }
    if ref is not None:
        rp, ri = p50(ref)
        out["ref_p50_ms"] = rp
        out["pr_over_ref"] = (pp / rp) if pp is not None and rp else None
        out["invalid"]["ref"] = ri
    return out
