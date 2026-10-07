"""Shared helpers: canonical JSON, number handling, set scores, display."""
from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable

# Surfaces a request belongs to. The core tier is native-only (vl-native,
# vt-native, jaeger); loki and tempo belong to the focused and daily tiers.
# lh-logs and lh-traces are Lakehouse-only APIs (/lakehouse/api/v1/*, /lakehouse/info):
# they have no upstream reference, so PR is scored against base and a derived truth.
SURFACES = ("vl-native", "vt-native", "jaeger", "lh-logs", "lh-traces", "loki", "tempo")
UPSTREAM_CORE_SURFACES = ("vl-native", "vt-native", "jaeger")
LH_SURFACES = ("lh-logs", "lh-traces")
CORE_SURFACES = UPSTREAM_CORE_SURFACES + LH_SURFACES
SURFACE_SIGNAL = {
    "vl-native": "logs",
    "lh-logs": "logs",
    "loki": "logs",
    "vt-native": "traces",
    "jaeger": "traces",
    "lh-traces": "traces",
    "tempo": "traces",
}


class UnknownShape(ValueError):
    """A body no decoder recognises: the harness's problem, never a pass."""

DEFAULT_REL_TOL = 1e-9


@dataclass
class FacetResult:
    """Scores for one answer against its reference.

    facets maps a facet name to a score in [0, 100]. A score is 100 only when
    the facet matches exactly; details carries the numbers behind the scores.
    """

    facets: dict[str, float] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    @property
    def exact(self) -> bool:
        return all(v >= 100.0 for v in self.facets.values())

    @property
    def worst(self) -> tuple[str, float]:
        """The minimum facet: what the compact table shows."""
        if not self.facets:
            return ("", 100.0)
        name = min(self.facets, key=lambda k: (self.facets[k], k))
        return name, self.facets[name]

    @property
    def score(self) -> float:
        return self.worst[1]


def to_number(v: Any) -> float | None:
    """Parse a JSON value as a number; None when it is not one.

    Accepts ints, floats and numeric strings ("1.5", "NaN", "+Inf").
    Booleans are not numbers.
    """
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        low = s.lower()
        if low in ("nan",):
            return math.nan
        if low in ("inf", "+inf"):
            return math.inf
        if low == "-inf":
            return -math.inf
        try:
            return float(s)
        except ValueError:
            return None
    return None


def canon(v: Any) -> Any:
    """Canonical, hashable-by-dumps form: numbers compare as numbers (1 == 1.0)."""
    if isinstance(v, bool) or v is None:
        return v
    if isinstance(v, int):
        return v  # ints stay ints: no float rounding for huge counters
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        if math.isinf(v):
            return "+Inf" if v > 0 else "-Inf"
        if v == int(v) and abs(v) < 1e15:
            return int(v)  # 1 == 1.0
        return v
    if isinstance(v, dict):
        return {str(k): canon(x) for k, x in sorted(v.items(), key=lambda kv: str(kv[0]))}
    if isinstance(v, (list, tuple)):
        return [canon(x) for x in v]
    return v


def dumps(v: Any) -> str:
    return json.dumps(canon(v), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def numbers_equal(a: float, b: float, rel: float = 0.0, abs_tol: float = 0.0) -> bool:
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    if math.isinf(a) or math.isinf(b):
        return a == b
    if a == b:
        return True
    diff = abs(a - b)
    return diff <= abs_tol or diff <= rel * max(abs(a), abs(b))


def rel_err(s: float, r: float) -> float:
    if math.isnan(s) and math.isnan(r):
        return 0.0
    if math.isnan(s) or math.isnan(r):
        return math.inf
    if math.isinf(s) or math.isinf(r):
        return 0.0 if s == r else math.inf
    return abs(s - r) / max(abs(r), 1e-12)


def percentile(values: list[float], q: float) -> float:
    """Nearest-rank percentile over a list; 0.0 for an empty list."""
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, math.ceil(q * len(s)) - 1))
    return s[k]


def multiset_scores(ref: Counter, ans: Counter) -> dict[str, float]:
    """Multiset Jaccard, recall and precision as percentages.

    Both empty is a perfect (and vacuous) match.
    """
    inter = sum((ref & ans).values())
    union = sum((ref | ans).values())
    nref, nans = sum(ref.values()), sum(ans.values())
    return {
        "jaccard": 100.0 if union == 0 else 100.0 * inter / union,
        "recall": 100.0 if nref == 0 else 100.0 * inter / nref,
        "precision": 100.0 if nans == 0 else 100.0 * inter / nans,
        "missing": nref - inter,
        "extra": nans - inter,
    }


def set_scores(ref: Iterable, ans: Iterable) -> dict[str, float]:
    return multiset_scores(Counter(set(ref)), Counter(set(ans)))


def delta_score(ans: float, ref: float) -> tuple[float, float, float]:
    """(delta, delta fraction, score): score = 100 * (1 - min(1, |delta%|))."""
    d = ans - ref
    frac = d / max(abs(ref), 1.0)
    return d, frac, 100.0 * (1.0 - min(1.0, abs(frac)))


def cap_inexact(score: float, exact: bool) -> float:
    """A score of 100 means exact: an inexact facet never reaches it."""
    if exact:
        return 100.0
    return min(score, math.nextafter(100.0, 0.0))


def display_pct(score: float) -> str:
    """Whole % except between 99 and 100 (one decimal, floored).

    100 is shown only for a score of exactly 100, so a difference never
    rounds up to it.
    """
    if math.isnan(score):
        return "NaN"
    if score >= 100.0:
        return "100"
    if score >= 99.0:
        return f"{math.floor(score * 10) / 10:.1f}"
    return str(int(round(score)))
