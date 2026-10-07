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
    # Hits: a value present on both sides whose hits are missing on one side counts as unequal
    # (a lost `hits` is a lost number, never a match). Values without hits on either side
    # (field_names, services) have nothing to compare.
    common = [v for v in ref if v in ans and (ref[v] is not None or ans[v] is not None)]
    ref_has_hits = any(h is not None for h in ref.values())
    ans_has_hits = any(h is not None for h in ans.values())
    if common or ref_has_hits or ans_has_hits:
        def same(v):
            r, a = ref[v], ans[v]
            return r is not None and a is not None and numbers_equal(a, r, rel_tol)

        eq = sum(1 for v in common if same(v))
        errs = [rel_err(ans[v], ref[v]) if ref[v] is not None and ans[v] is not None else float("inf")
                for v in common if not same(v)]
        n = len(common)
        res.facets["hits_equality"] = cap_inexact(100.0 * eq / n if n else 0.0, n > 0 and eq == n)
        finite = [e for e in errs if e != float("inf")] + [0.0] * eq
        res.details["hits_equality"] = {
            "equal": eq,
            "common": n,
            "lost_hits": sum(1 for v in common if (ref[v] is None) != (ans[v] is None)),
            "rel_err_p95": percentile(finite, 0.95),
        }
    return res
