"""M7 series match."""
from __future__ import annotations

import math

from .common import (
    DEFAULT_REL_TOL,
    FacetResult,
    cap_inexact,
    numbers_equal,
    percentile,
    rel_err,
    set_scores,
)
from .decode import Series


def evaluate_series(
    ref: Series,
    ans: Series,
    *,
    rel_tol: float = DEFAULT_REL_TOL,
    abs_tol: float = 0.0,
) -> FacetResult:
    """Series-set Jaccard, timestamp alignment, point-wise error, totals, NaN agreement."""
    res = FacetResult()
    ss = set_scores(ref.keys(), ans.keys())
    res.facets["series_set"] = cap_inexact(ss["jaccard"], set(ref) == set(ans))
    res.details["series_set"] = {k: ss[k] for k in ("jaccard", "recall", "precision", "missing", "extra")}

    common = ref.keys() & ans.keys()
    ts_common = ts_union = 0
    within = total_points = 0
    nan_agree = nan_total = 0
    errs: list[float] = []
    # Points of series present on only one side count as out of tolerance.
    for k in ref.keys() | ans.keys():
        r, a = ref.get(k, {}), ans.get(k, {})
        both = r.keys() & a.keys()
        union = len(r) + len(a) - len(both)
        total_points += union
        if k not in common:
            continue
        ts_common += len(both)
        ts_union += union
        for t in both:
            rv, av = r[t], a[t]
            nan_total += 1
            if math.isnan(rv) == math.isnan(av):
                nan_agree += 1
            if numbers_equal(av, rv, rel_tol, abs_tol):
                within += 1
                errs.append(0.0)
            else:
                errs.append(rel_err(av, rv))
    # With points on either side but none in common, nothing is aligned.
    align = 100.0 if total_points == 0 else (100.0 * ts_common / ts_union if ts_union else 0.0)
    res.facets["ts_alignment"] = cap_inexact(align, total_points == 0 or (ts_union > 0 and ts_common == ts_union))
    within_pct = 100.0 if total_points == 0 else 100.0 * within / total_points
    res.facets["points_within_tol"] = cap_inexact(within_pct, within == total_points)

    ref_total = sum(v for p in ref.values() for v in p.values() if not math.isnan(v))
    ans_total = sum(v for p in ans.values() for v in p.values() if not math.isnan(v))
    tdelta = ans_total - ref_total
    tfrac = tdelta / max(abs(ref_total), 1.0)
    tol_ok = numbers_equal(ans_total, ref_total, rel_tol, abs_tol)
    res.facets["totals"] = cap_inexact(100.0 * (1.0 - min(1.0, abs(tfrac))), tol_ok)
    nan_pct = (100.0 if total_points == 0 else 0.0) if nan_total == 0 else 100.0 * nan_agree / nan_total
    res.facets["nan_agreement"] = cap_inexact(nan_pct, nan_pct >= 100.0)
    finite = [e for e in errs if not math.isinf(e)]
    res.details.update(
        {
            "ts_alignment": {"common": ts_common, "union": ts_union},
            "points_within_tol": {
                "within": within,
                "points": total_points,
                "rel_tol": rel_tol,
                "rel_err_p50": percentile(finite, 0.50),
                "rel_err_p95": percentile(finite, 0.95),
                "rel_err_max": max(finite) if finite else 0.0,
                "infinite_errors": len(errs) - len(finite),
            },
            "totals": {"ref": ref_total, "ans": ans_total, "delta": tdelta, "delta_pct": 100.0 * tfrac},
        }
    )
    return res
