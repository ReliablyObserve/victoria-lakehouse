"""Captured answer triples on disk: loading, evaluation, expectations."""
from __future__ import annotations

import json
import os
from typing import Any

from .common import SURFACE_SIGNAL, SURFACES
from .evaluate import answer_empty, check_meta, evaluate_request
from .generic import is_error, latency_ratio
from .verdict import CaseResult, classify, classify_samples

ANSWER_FILES = ("ref", "base", "pr", "truth_base", "truth_pr")


def _read_json(path: str) -> Any:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_answers(d: str) -> dict[str, dict]:
    out = {}
    for name in ANSWER_FILES:
        p = os.path.join(d, name + ".json")
        if os.path.exists(p):
            out[name] = _read_json(p)
    return out


def case_dirs(root: str) -> list[str]:
    found = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        if "meta.json" in filenames:
            found.append(dirpath)
            dirnames[:] = [x for x in dirnames if not x.startswith("resample-")]
    return found


def _blocked(meta: dict, answers: dict[str, dict]) -> bool:
    """A reference answer with 5xx, a timeout or a warning is blocked, never a difference.
    Lakehouse-only rows have no reference: an unhealthy base blocks them."""
    ref = answers.get("base") if meta.get("lh_only") else answers.get("ref")
    if ref is None:
        return False
    return (ref.get("status") or 0) >= 500 or bool(ref.get("timeout")) or bool(ref.get("warnings"))


def _vacuous(meta: dict, answers: dict[str, dict]) -> bool:
    names = ("base", "pr") if meta.get("lh_only") else ("ref", "base", "pr")
    if not all(n in answers for n in names):
        return False
    return all(answer_empty(meta["kind"], answers[n]) for n in names)


def evaluate_case(meta: dict, answers: dict[str, dict], resamples: list[dict[str, dict]] | None = None) -> CaseResult:
    check_meta(meta)
    surface = meta["surface"]
    res = CaseResult(
        id=meta["id"],
        row=meta.get("row", meta["id"]),
        surface=surface,
        signal=SURFACE_SIGNAL[surface],
        endpoint=meta.get("endpoint", meta["kind"]),
        verdict="harness-error",
        claimed=bool(meta.get("claimed_gap")),
    )
    need = ["base", "pr"] + ([] if meta.get("lh_only") else ["ref"]) + (["truth_base", "truth_pr"] if meta.get("truth") else [])

    def one(ans: dict[str, dict]) -> tuple[str, Any, Any]:
        if any(n not in ans for n in need):
            return "harness-error", None, None
        try:
            b, p = evaluate_request(meta, ans)
        except Exception as e:  # a malformed answer is the harness's problem, never a pass
            res.latency["error"] = f"{type(e).__name__}: {e}"
            return "harness-error", None, None
        v = classify(b, p, claimed=res.claimed, blocked=_blocked(meta, ans), vacuous=_vacuous(meta, ans))
        return v, b, p

    v, b, p = one(answers)
    res.base, res.pr = b, p
    verdicts = [v] + [one(r)[0] for r in (resamples or [])]
    res.samples = verdicts
    res.verdict = classify_samples(verdicts)
    if "base" in answers and "pr" in answers:
        def lat(a):
            return [(a.get("latency_ms") or 0.0, not is_error(a))]
        res.latency = {**res.latency, **latency_ratio(lat(answers["pr"]), lat(answers["base"]),
                                                    lat(answers["ref"]) if "ref" in answers else None)}
    return res


def load_case(d: str) -> tuple[dict, CaseResult]:
    meta = _read_json(os.path.join(d, "meta.json"))
    answers = load_answers(d)
    resamples = []
    for sub in sorted(x for x in os.listdir(d) if x.startswith("resample-")):
        resamples.append(load_answers(os.path.join(d, sub)))
    return meta, evaluate_case(meta, answers, resamples)


def run_dir(root: str) -> list[tuple[dict, CaseResult]]:
    out = [load_case(d) for d in case_dirs(root)]
    order = {s: i for i, s in enumerate(SURFACES)}  # native surfaces first
    out.sort(key=lambda mc: (order.get(mc[1].surface, 99), mc[1].id))
    return out


def check_expectations(meta: dict, res: CaseResult, tol: float = 0.1) -> list[str]:
    """Differences between a case's recorded expectation and what was computed."""
    exp = meta.get("expect")
    if not exp:
        return ["no expectation recorded"]
    errs = []
    if res.verdict != exp.get("verdict"):
        errs.append(f"verdict {res.verdict} != expected {exp.get('verdict')}")
    for side, fr in (("base", res.base), ("pr", res.pr)):
        for facet, want in (exp.get(side) or {}).items():
            got = fr.facets.get(facet) if fr else None
            if got is None or abs(got - want) > tol:
                errs.append(f"{side}.{facet} = {got} != expected {want}")
    for path, want in (exp.get("details") or {}).items():
        side, _, rest = path.partition(".")
        fr = res.base if side == "base" else res.pr
        cur: Any = fr.details if fr else None
        for part in rest.split("."):
            cur = cur.get(part) if isinstance(cur, dict) else None
        if isinstance(want, (int, float)) and isinstance(cur, (int, float)):
            if abs(cur - want) > tol:
                errs.append(f"{path} = {cur} != expected {want}")
        elif cur != want:
            errs.append(f"{path} = {cur!r} != expected {want!r}")
    return errs
