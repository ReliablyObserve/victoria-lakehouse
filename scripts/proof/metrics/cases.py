"""Captured answer triples on disk: loading, evaluation, expectations."""
from __future__ import annotations

import json
import os
from typing import Any

from .common import CORE_SURFACES, SURFACE_SIGNAL, SURFACES
from .evaluate import answer_empty, check_meta, evaluate_request, is_lh_only
from .generic import is_error, latency_ratio
from .verdict import DEFAULT_TOL, EXCLUDED_SAMPLES, CaseResult, classify, classify_samples

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
    ref = answers.get("base") if is_lh_only(meta) else answers.get("ref")
    if ref is None:
        return False
    return (ref.get("status") or 0) >= 500 or bool(ref.get("timeout")) or bool(ref.get("warnings"))


def _vacuous(meta: dict, answers: dict[str, dict]) -> bool:
    names = ("base", "pr") if is_lh_only(meta) else ("ref", "base", "pr")
    if not all(n in answers for n in names):
        return False
    return all(answer_empty(meta["kind"], answers[n]) for n in names)


def _seeded(meta: dict) -> bool:
    """Core rows run on seeded data: an answer with nothing in it there is a broken harness,
    not a pass. A row that legitimately answers empty says so with may_be_empty."""
    return meta["surface"] in CORE_SURFACES and not meta.get("may_be_empty")


def _valid_states(verdict: str) -> tuple[bool, bool]:
    """(base answer valid, PR answer valid) for latency: an answer counts only when it is in the
    expected state, which here means no other side beats it on the facet vector (an exact answer,
    or a pre-existing difference nobody improved on). Wrong answers are not latency."""
    if verdict in ("fixed", "improved"):
        return False, True  # base was the wrong one
    if verdict == "regressed":
        return True, False
    return True, True


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
    lh_only = is_lh_only(meta)
    need = ["base", "pr"] + ([] if lh_only else ["ref"]) + (["truth_base", "truth_pr"] if meta.get("truth") else [])
    tol = meta.get("score_tolerance", DEFAULT_TOL)

    def one(ans: dict[str, dict]) -> tuple[str, Any, Any]:
        if any(n not in ans for n in need):
            return "harness-error", None, None
        try:
            b, p = evaluate_request(meta, ans)
            vacuous = _vacuous(meta, ans)
        except Exception as e:  # a malformed or unknown answer is the harness's problem, never a pass
            res.latency["error"] = f"{type(e).__name__}: {e}"
            return "harness-error", None, None
        if vacuous and _seeded(meta):
            res.latency["error"] = "empty answers on a seeded core row"
            return "harness-error", b, p
        return classify(b, p, claimed=res.claimed, blocked=_blocked(meta, ans), vacuous=vacuous, tol=tol), b, p

    v, b, p = one(answers)
    res.base, res.pr = b, p
    verdicts = [v] + [one(r)[0] for r in (resamples or [])]
    res.samples = verdicts
    res.verdict = classify_samples(verdicts)
    res.excluded_samples = [x for x in verdicts if x in EXCLUDED_SAMPLES]
    if "base" in answers and "pr" in answers:
        b_ok, p_ok = _valid_states(res.verdict)

        def lat(a, ok=True):
            return [(a.get("latency_ms") or 0.0, ok and not is_error(a))]

        res.latency = {**res.latency, **latency_ratio(lat(answers["pr"], p_ok), lat(answers["base"], b_ok),
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


def _close(got: float, want: float, tol: float) -> bool:
    """Within tol, except that an expected 100 means exactly 100: an inexact 99.99x is not a pass."""
    if want >= 100.0:
        return got >= 100.0
    return abs(got - want) <= tol


def check_expectations(meta: dict, res: CaseResult, tol: float = 0.01) -> list[str]:
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
            if got is None or not _close(got, want, tol):
                errs.append(f"{side}.{facet} = {got} != expected {want}")
    for path, want in (exp.get("details") or {}).items():
        side, _, rest = path.partition(".")
        fr = res.base if side == "base" else res.pr
        cur: Any = fr.details if fr else None
        for part in rest.split("."):
            cur = cur.get(part) if isinstance(cur, dict) else None
        if isinstance(want, (int, float)) and isinstance(cur, (int, float)):
            if not _close(cur, want, tol):
                errs.append(f"{path} = {cur} != expected {want}")
        elif cur != want:
            errs.append(f"{path} = {cur!r} != expected {want!r}")
    return errs
