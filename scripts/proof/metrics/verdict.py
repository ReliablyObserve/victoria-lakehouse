"""Verdict classifier, worst-facet score and roll-ups."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from .common import FacetResult, display_pct

VERDICTS = (
    "exact", "same", "fixed", "improved", "regressed", "not-reproduced-on-base",
    "vacuous", "blocked", "nondeterministic", "harness-error",
)
# Verdicts a PR cannot ship with, absent an accepted registry change.
FAILING = ("regressed", "nondeterministic", "harness-error")

# Verdicts that prove nothing about quality: left out of every roll-up.
UNSCORED = ("blocked", "vacuous", "nondeterministic", "harness-error")

DEFAULT_TOL = 1e-9  # percentage points


def classify(
    base: FacetResult | None,
    pr: FacetResult | None,
    *,
    claimed: bool = False,
    blocked: bool = False,
    vacuous: bool = False,
    harness_error: bool = False,
    tol: float = DEFAULT_TOL,
) -> str:
    """One request's verdict from the whole facet vector, never from the worst-facet score.

    Precedence: harness-error, blocked, vacuous, then the facet comparison.
    """
    if harness_error or base is None or pr is None:
        return "harness-error"
    if blocked:
        return "blocked"
    if vacuous:
        return "vacuous"
    if pr.exact:
        if claimed and base.exact:
            return "not-reproduced-on-base"
        return "fixed" if not base.exact else "exact"
    keys = base.facets.keys() | pr.facets.keys()
    better = worse = False
    for k in keys:
        b, p = base.facets.get(k, 100.0), pr.facets.get(k, 100.0)
        if p < b - tol:
            worse = True
        elif p > b + tol:
            better = True
    if worse:
        return "regressed"
    if better:
        return "improved"
    return "same"


def classify_samples(verdicts: list[str]) -> str:
    """Re-sampling only classifies: a verdict that differs between samples is nondeterministic."""
    if not verdicts:
        return "harness-error"
    return verdicts[0] if len(set(verdicts)) == 1 else "nondeterministic"


@dataclass
class CaseResult:
    id: str
    row: str
    surface: str
    signal: str
    endpoint: str
    verdict: str
    base: FacetResult | None = None
    pr: FacetResult | None = None
    latency: dict[str, Any] = field(default_factory=dict)
    claimed: bool = False
    samples: list[str] = field(default_factory=list)

    @property
    def q_base(self) -> float | None:
        return None if self.base is None else self.base.score

    @property
    def q_pr(self) -> float | None:
        return None if self.pr is None else self.pr.score

    @property
    def delta(self) -> float | None:
        if self.base is None or self.pr is None:
            return None
        return self.pr.score - self.base.score


def pct_text(r: FacetResult | None) -> str:
    return "-" if r is None else display_pct(r.score)


def _stats(rows: list[tuple[float, float]], exact_b: int, exact_p: int) -> dict[str, Any]:
    n = len(rows)
    return {
        "rows": n,
        "base_score": sum(b for b, _ in rows) / n if n else None,
        "pr_score": sum(p for _, p in rows) / n if n else None,
        "base_exact_pct": 100.0 * exact_b / n if n else None,
        "pr_exact_pct": 100.0 * exact_p / n if n else None,
    }


def rollup_rows(results: list[CaseResult]) -> dict[tuple[str, str, str], dict[str, Any]]:
    """Per registry row: the minimum facet score over its requests (mean shown too).

    Keyed (signal, surface, row). Cases without both answers, and verdicts that prove nothing (UNSCORED), are left out."""
    grouped: dict[tuple[str, str, str], list[CaseResult]] = defaultdict(list)
    for r in results:
        if r.verdict in UNSCORED:
            continue
        if r.base is not None and r.pr is not None:
            grouped[(r.signal, r.surface, r.row)].append(r)
    out = {}
    for key, rs in grouped.items():
        out[key] = {
            "requests": len(rs),
            "base_min": min(r.base.score for r in rs),
            "pr_min": min(r.pr.score for r in rs),
            "base_mean": sum(r.base.score for r in rs) / len(rs),
            "pr_mean": sum(r.pr.score for r in rs) / len(rs),
            "base_exact": all(r.base.exact for r in rs),
            "pr_exact": all(r.pr.exact for r in rs),
        }
    return out


def rollup_surfaces(results: list[CaseResult]) -> dict[tuple[str, str], dict[str, Any]]:
    """Per (signal, surface): mean of row scores plus exact rows %."""
    rows = rollup_rows(results)
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for (sig, surf, _), v in rows.items():
        grouped[(sig, surf)].append(v)
    return {
        key: _stats(
            [(v["base_min"], v["pr_min"]) for v in vs],
            sum(v["base_exact"] for v in vs),
            sum(v["pr_exact"] for v in vs),
        )
        for key, vs in grouped.items()
    }


def rollup_signals(results: list[CaseResult]) -> dict[str, dict[str, Any]]:
    """Per signal. Logs and traces are reported separately and never averaged together:
    there is deliberately no all-signals entry."""
    rows = rollup_rows(results)
    grouped: dict[str, list[dict]] = defaultdict(list)
    for (sig, _, _), v in rows.items():
        grouped[sig].append(v)
    return {
        sig: _stats(
            [(v["base_min"], v["pr_min"]) for v in vs],
            sum(v["base_exact"] for v in vs),
            sum(v["pr_exact"] for v in vs),
        )
        for sig, vs in grouped.items()
    }
