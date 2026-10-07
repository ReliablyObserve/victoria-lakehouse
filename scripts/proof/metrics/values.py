"""M8 value-set match."""
from __future__ import annotations

from .common import FacetResult, cap_inexact, numbers_equal, percentile, rel_err, set_scores


def evaluate_values(ref: dict, ans: dict, *, rel_tol: float = 0.0) -> FacetResult:
    """Values Jaccard/recall/precision; hits equality and error over common values."""
    res = FacetResult()
    ss = set_scores(ref.keys(), ans.keys())
    exact = set(ref) == set(ans)
    res.facets["value_set"] = cap_inexact(ss["jaccard"], exact)
    res.details["value_set"] = {
        "jaccard": ss["jaccard"],
        "recall": ss["recall"],
        "precision": ss["precision"],
        "missing": sorted(set(ref) - set(ans))[:20],
        "extra": sorted(set(ans) - set(ref))[:20],
        "ref_n": len(ref),
        "ans_n": len(ans),
    }
    common = [v for v in ref if v in ans and ref[v] is not None and ans[v] is not None]
    if common:
        eq = sum(1 for v in common if numbers_equal(ans[v], ref[v], rel_tol))
        errs = [rel_err(ans[v], ref[v]) for v in common if not numbers_equal(ans[v], ref[v], rel_tol)]
        errs_all = errs + [0.0] * (len(common) - len(errs))
        res.facets["hits_equality"] = cap_inexact(100.0 * eq / len(common), eq == len(common))
        res.details["hits_equality"] = {
            "equal": eq,
            "common": len(common),
            "rel_err_p95": percentile([e for e in errs_all if e != float("inf")], 0.95),
        }
    return res
