"""Row-shaped answers: M3 row-set, M4 field coverage, M5 value equality,
M6 count delta, M10 order agreement."""
from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from typing import Any

from .common import (
    FacetResult,
    UnknownShape,
    cap_inexact,
    delta_score,
    dumps,
    multiset_scores,
    set_scores,
)
from .ties import TieFetcher, explained_by_truncated_tie, row_key

LOG_IDENTITY = ("_time", "_stream_id", "_msg")
SPAN_IDENTITY = ("trace_id", "span_id")
MAX_LISTED = 20


def parse_ndjson(body: Any) -> list[dict]:
    """Rows from an NDJSON string, a list of rows, or a single row.

    A line that is not a JSON object is an unknown shape (an HTML error page served as 200,
    a truncated body): it raises instead of being skipped, so it can never look like fewer rows.
    """
    if body is None:
        return []
    if isinstance(body, list):
        if not all(isinstance(r, dict) for r in body):
            raise UnknownShape("rows: a list with a non-object element")
        return list(body)
    if isinstance(body, dict):
        return [body]
    if not isinstance(body, str):
        raise UnknownShape(f"rows: unexpected {type(body).__name__} body")
    rows = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            v = json.loads(line)
        except ValueError as e:
            raise UnknownShape(f"rows: line is not JSON: {line[:60]!r}") from e
        if not isinstance(v, dict):
            raise UnknownShape(f"rows: line is not a JSON object: {line[:60]!r}")
        rows.append(v)
    return rows


def _drop(rows: list[dict], skip) -> list[dict]:
    if not skip:
        return rows
    return [{k: v for k, v in r.items() if k not in skip} for r in rows]


def _identity(row: dict, ident: tuple[str, ...]) -> str:
    """Identity of a row for pairing. A row that carries none of the identity fields (a service-graph
    or trace-index row has no span_id) falls back to its stream id, else to its whole canonical row,
    so such rows never all collapse into one identity."""
    vals = [row.get(k) for k in ident]
    if any(v is not None for v in vals):
        return dumps(vals)
    if row.get("_stream_id") is not None:
        return dumps(["_stream_id", row["_stream_id"]])
    return "row:" + row_key(row)


def kendall_order(
    ref_rows: list[dict],
    ans_rows: list[dict],
    sort_keys: list[str],
    skip=frozenset(),
    ref_keys: list[str] | None = None,
    ans_keys: list[str] | None = None,
) -> dict[str, Any]:
    """Kendall-based order agreement over the rows both answers hold.

    Pairs inside one group that ties on the full sort key are not comparable.
    Returns score (0-100), comparable pairs, discordant pairs, tie groups.
    """
    # Position of each canonical row in each answer; duplicates are matched in order.
    if ref_keys is None:
        ref_keys = [row_key(r, skip) for r in ref_rows]
    if ans_keys is None:
        ans_keys = [row_key(r, skip) for r in ans_rows]
    ref_pos: dict[str, list[int]] = defaultdict(list)
    for i, k in enumerate(ref_keys):
        ref_pos[k].append(i)
    ans_pos: dict[str, list[int]] = defaultdict(list)
    for i, k in enumerate(ans_keys):
        ans_pos[k].append(i)
    common = []  # (ref_index, ans_index, tie_key)
    for k, rp in ref_pos.items():
        ap = ans_pos.get(k)
        if not ap:
            continue
        for a, b in zip(rp, ap):
            row = ref_rows[a]
            common.append((a, b, dumps([row.get(f) for f in sort_keys])))
    common.sort()
    n = len(common)
    seq = [c[1] for c in common]
    total = n * (n - 1) // 2
    disc = _inversions(seq)
    groups: dict[str, list[int]] = defaultdict(list)
    for _, b, tk in common:
        groups[tk].append(b)
    tie_pairs = 0
    tie_disc = 0
    tied_groups = 0
    for g in groups.values():
        m = len(g)
        if m > 1:
            tied_groups += 1
            tie_pairs += m * (m - 1) // 2
            tie_disc += _inversions(g)
    comparable = total - tie_pairs
    discordant = disc - tie_disc
    score = 100.0 if comparable <= 0 else 100.0 * (1.0 - discordant / comparable)
    return {
        "score": score,
        "comparable_pairs": comparable,
        "discordant_pairs": discordant,
        "tie_groups": tied_groups,
        "rows_compared": n,
    }


def _inversions(seq: list[int]) -> int:
    """Number of inversions via a Fenwick tree over ranks (ties are distinct indices)."""
    n = len(seq)
    if n < 2:
        return 0
    order = sorted(set(seq))
    rank = {v: i + 1 for i, v in enumerate(order)}
    size = len(order)
    tree = [0] * (size + 1)
    inv = 0
    for i, v in enumerate(seq):
        r = rank[v]
        # count of earlier elements strictly greater than v
        s = 0
        j = r
        while j > 0:
            s += tree[j]
            j -= j & -j
        inv += i - s
        j = r
        while j <= size:
            tree[j] += 1
            j += j & -j
    return inv


def evaluate_rows(
    ref_rows: list[dict],
    ans_rows: list[dict],
    *,
    identity: tuple[str, ...] = LOG_IDENTITY,
    skip=frozenset(),
    order: list[str] | None = None,
    tie_fetch: TieFetcher | None = None,
    tie_sort_fields: list[str] | None = None,
    tie_decision: dict | None = None,
) -> FacetResult:
    """Score row answers against a reference.

    order: sort keys when the query defines an order, else None (M10 is skipped).
    tie_fetch: re-reads a tie group; when the difference is fully explained by a
    limit cutting that group, the row facets count as matching (reported in notes).
    tie_decision: the same decision recorded by the runner ({"explained": bool, "why": str}),
    consumed instead of re-deriving it; tie_fetch is then not needed.
    """
    skip = set(skip)
    ref_rows, ans_rows = _drop(ref_rows, skip), _drop(ans_rows, skip)
    res = FacetResult()

    ref_keys = [row_key(r) for r in ref_rows]
    ans_keys = [row_key(r) for r in ans_rows]
    ref_c, ans_c = Counter(ref_keys), Counter(ans_keys)
    ms = multiset_scores(ref_c, ans_c)
    exact_set = ms["jaccard"] >= 100.0 and ref_c == ans_c
    tie_cut = False
    if not exact_set and (tie_decision is not None or tie_fetch is not None):
        if tie_decision is not None:
            ok, why = bool(tie_decision.get("explained")), str(tie_decision.get("why", ""))
        else:
            ok, why = explained_by_truncated_tie(tie_fetch, ref_rows, ans_rows, tie_sort_fields, skip)
        if ok:
            tie_cut = True
            res.notes.append("tie-cut accepted: a limit cut one tie group")
        else:
            res.notes.append(f"tie-cut not accepted: {why}")
    if tie_cut:
        # Drop the rows of the cut group that differ; every other facet is
        # still scored on the rest (a wrong order elsewhere stays visible).
        ref_only = ref_c - ans_c
        ans_only = ans_c - ref_c
        ref_rows = _without(ref_rows, ref_only)
        ans_rows = _without(ans_rows, ans_only)
        ref_keys = [row_key(r) for r in ref_rows]
        ans_keys = [row_key(r) for r in ans_rows]
        ref_c, ans_c = Counter(ref_keys), Counter(ans_keys)
        ms = multiset_scores(ref_c, ans_c)
        exact_set = True
        res.details["tie_cut"] = {"dropped_rows_per_side": sum(ref_only.values())}
    res.facets["row_set"] = cap_inexact(ms["jaccard"], exact_set)
    res.details["row_set"] = {**ms, "exact": exact_set}

    # M4 field coverage
    ref_fields = {k for r in ref_rows for k in r}
    ans_fields = {k for r in ans_rows for k in r}
    fs = set_scores(ref_fields, ans_fields)
    fexact = ref_fields == ans_fields
    res.facets["field_coverage"] = cap_inexact(fs["jaccard"], fexact)
    res.details["field_coverage"] = {
        "jaccard": fs["jaccard"],
        "missing": sorted(ref_fields - ans_fields)[:MAX_LISTED],
        "extra": sorted(ans_fields - ref_fields)[:MAX_LISTED],
    }

    # M5 value equality over rows paired by identity
    per_field_eq, per_field_n, paired = _pair_value_equality(ref_rows, ans_rows, identity)
    veq = {f: (100.0 * per_field_eq[f] / per_field_n[f]) for f in per_field_n}
    unpaired = max(len(ref_rows), len(ans_rows)) - paired
    min_field = min(veq, key=lambda f: (veq[f], f)) if veq else ""
    min_score = veq[min_field] if veq else 100.0
    all_eq = all(per_field_eq[f] == per_field_n[f] for f in per_field_n)
    if not veq and (ref_rows or ans_rows):
        min_score, all_eq = 0.0, False  # rows on a side but none paired: nothing matches
    res.facets["value_equality"] = cap_inexact(min_score, all_eq)
    res.details["value_equality"] = {
        "paired_rows": paired,
        "unpaired_rows": unpaired,
        "min_field": min_field,
        "below_100": dict([(f, round(v, 3)) for f, v in sorted(veq.items()) if v < 100.0][:MAX_LISTED]),
    }

    # M6 count delta
    d, frac, cscore = delta_score(float(len(ans_rows)), float(len(ref_rows)))
    res.facets["count"] = cap_inexact(cscore, d == 0)
    res.details["count"] = {"delta": d, "delta_pct": 100.0 * frac, "ref": len(ref_rows), "ans": len(ans_rows)}

    # M10 order
    if order:
        k = kendall_order(ref_rows, ans_rows, order, ref_keys=ref_keys, ans_keys=ans_keys)
        exact_order = k["discordant_pairs"] == 0
        res.facets["order"] = cap_inexact(k["score"], exact_order)
        res.details["order"] = k
    return res


def _same_value(a: Any, b: Any) -> bool:
    if type(a) is type(b) and isinstance(a, str):
        return a == b  # fast path: strings dominate log rows
    return dumps(a) == dumps(b)


def _without(rows: list[dict], drop: Counter) -> list[dict]:
    left = Counter(drop)
    out = []
    for r in rows:
        k = row_key(r)
        if left[k] > 0:
            left[k] -= 1
            continue
        out.append(r)
    return out


def _pair_value_equality(ref_rows, ans_rows, identity):
    """Pair rows by identity (duplicates in occurrence order); count equal values per field."""
    ref_by: dict[str, list[dict]] = defaultdict(list)
    for r in ref_rows:
        ref_by[_identity(r, identity)].append(r)
    ans_by: dict[str, list[dict]] = defaultdict(list)
    for r in ans_rows:
        ans_by[_identity(r, identity)].append(r)
    eq: Counter = Counter()
    n: Counter = Counter()
    paired = 0
    for ident, rrows in ref_by.items():
        arows = ans_by.get(ident, [])
        for rr, ar in zip(rrows, arows):
            paired += 1
            for f in set(rr) | set(ar):
                n[f] += 1
                if f in rr and f in ar and _same_value(rr[f], ar[f]):
                    eq[f] += 1
    return eq, n, paired


def evaluate_count_vector(ref: dict[str, float], ans: dict[str, float]) -> FacetResult:
    """M6 for a stats answer keyed by its group labels (`stats count()` has one key,
    `stats by (level) count()` one per group). The score is the worst group; a group
    missing on one side scores 0."""
    res = FacetResult()
    scores: dict[str, float] = {}
    for k in ref.keys() | ans.keys():
        if k in ref and k in ans:
            d, _, sc = delta_score(ans[k], ref[k])
            scores[k] = 100.0 if d == 0 or (math.isnan(ans[k]) and math.isnan(ref[k])) else sc
        else:
            scores[k] = 0.0
    exact = all(v >= 100.0 for v in scores.values())
    res.facets["count"] = cap_inexact(min(scores.values()) if scores else 100.0, exact)
    differing = sorted(k for k, v in scores.items() if v < 100.0)
    res.details["count"] = {"groups": len(scores), "differing": differing[:MAX_LISTED]}
    if len(scores) == 1 and next(iter(scores)) in ref and next(iter(scores)) in ans:
        k = next(iter(scores))
        d, frac, _ = delta_score(ans[k], ref[k])
        res.details["count"].update({"delta": d, "delta_pct": 100.0 * frac, "ref": ref[k], "ans": ans[k]})
    return res


def evaluate_count(ref_value: float | None, ans_value: float | None) -> FacetResult:
    """M6 for a single-number answer (count(), stats totals)."""
    res = FacetResult()
    if ref_value is None or ans_value is None:
        res.facets["count"] = 0.0 if ref_value != ans_value else 100.0
        res.details["count"] = {"ref": ref_value, "ans": ans_value}
        return res
    d, frac, s = delta_score(ans_value, ref_value)
    res.facets["count"] = cap_inexact(s, d == 0)
    res.details["count"] = {"delta": d, "delta_pct": 100.0 * frac, "ref": ref_value, "ans": ans_value}
    return res


def key_order_agreement(ref_rows: list[dict], ans_rows: list[dict], identity: tuple[str, ...]) -> FacetResult:
    """Order of the keys inside each row (the order of the JSON members as the API writes them).

    Rows are paired by identity (a second row with the same identity pairs with the second one); for each pair the
    keys both rows carry must come in the same relative order. Score: share of paired rows whose key order equals
    the reference's. Rows without a partner are the row-set facet's business, not this one's. 100 only when exact.
    """
    ref_by: dict[tuple[str, int], dict] = {}
    seen: Counter = Counter()
    for r in ref_rows:
        k = _identity(r, identity)
        ref_by[(k, seen[k])] = r
        seen[k] += 1
    seen = Counter()
    paired = equal = 0
    first = ""
    for r in ans_rows:
        k = _identity(r, identity)
        ref = ref_by.get((k, seen[k]))
        seen[k] += 1
        if ref is None:
            continue
        common = set(ref) & set(r)
        a = [x for x in ref if x in common]
        b = [x for x in r if x in common]
        paired += 1
        if a == b:
            equal += 1
        elif not first:
            first = f"key order differs for {k[:60]}: reference {a[:6]} answer {b[:6]}"
    res = FacetResult()
    res.facets["key_order"] = 100.0 if paired == equal else 100.0 * equal / paired
    res.details["key_order"] = {"paired_rows": paired, "equal_rows": equal}
    if first:
        res.notes.append(first)
    return res
