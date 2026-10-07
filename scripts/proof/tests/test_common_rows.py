import math

import pytest

from scripts.proof.metrics.common import (
    UnknownShape,
    FacetResult, cap_inexact, canon, delta_score, display_pct, dumps, multiset_scores,
    numbers_equal, percentile, rel_err, set_scores, to_number,
)
from collections import Counter
from scripts.proof.metrics.rows import (
    evaluate_count, evaluate_rows, kendall_order, parse_ndjson, _inversions,
)


def R(i, **kw):
    r = {"_time": f"2026-01-01T00:00:{i:02d}.000000000Z", "_stream_id": "s", "_msg": f"m{i}"}
    r.update(kw)
    return r


# ---- common
def test_display_never_rounds_up_to_100():
    assert display_pct(100.0) == "100"
    assert display_pct(99.96) == "99.9"
    assert display_pct(99.0) == "99.0"
    assert display_pct(98.6) == "99"
    assert display_pct(math.nextafter(100.0, 0)) == "99.9"
    assert display_pct(0.0) == "0"


def test_cap_inexact():
    assert cap_inexact(100.0, True) == 100.0
    assert cap_inexact(100.0, False) < 100.0
    assert cap_inexact(42.0, False) == 42.0


@pytest.mark.parametrize("v,want", [(1, 1.0), ("1.5", 1.5), ("+Inf", math.inf), ("-inf", -math.inf), (True, None),
                                    (None, None), ("", None), ("abc", None), ([], None)])
def test_to_number(v, want):
    assert to_number(v) == want
    assert math.isnan(to_number("NaN"))


def test_canon_numbers_compare_as_numbers():
    assert dumps({"a": 1}) == dumps({"a": 1.0})
    assert canon(float("nan")) == "NaN"
    assert dumps({"b": 1, "a": [1.0, "x"]}) == dumps({"a": [1, "x"], "b": 1})
    assert canon(1e300) == 1e300  # huge values stay floats
    assert canon(True) is True and canon(None) is None


def test_numbers_equal_and_rel_err():
    assert numbers_equal(math.nan, math.nan)
    assert not numbers_equal(math.nan, 1.0)
    assert numbers_equal(math.inf, math.inf) and not numbers_equal(math.inf, -math.inf)
    assert numbers_equal(1.0, 1.0 + 1e-12, rel=1e-9)
    assert not numbers_equal(1.0, 1.1, rel=1e-9)
    assert numbers_equal(0.0, 1e-13, abs_tol=1e-12)
    assert rel_err(1.0, 1.0) == 0 and rel_err(2.0, 1.0) == 1.0
    assert rel_err(math.nan, math.nan) == 0 and rel_err(math.nan, 1) == math.inf
    assert rel_err(math.inf, math.inf) == 0 and rel_err(math.inf, 1) == math.inf
    assert rel_err(1e300, 1e300 * 2) == pytest.approx(0.5)


def test_percentile_edges():
    assert percentile([], 0.5) == 0.0
    assert percentile([5], 0.95) == 5
    assert percentile([1, 2, 3, 4], 0.5) == 2
    assert percentile([1, 2, 3, 4], 1.0) == 4


def test_multiset_and_delta():
    s = multiset_scores(Counter({"a": 2, "b": 1}), Counter({"a": 1, "c": 1}))
    assert s["jaccard"] == pytest.approx(25.0)  # inter 1, union 4
    assert s["recall"] == pytest.approx(100 / 3) and s["precision"] == 50.0
    assert multiset_scores(Counter(), Counter())["jaccard"] == 100.0
    assert set_scores([1, 2], [2, 3])["jaccard"] == pytest.approx(100 / 3)
    d, frac, sc = delta_score(90, 100)
    assert (d, frac, sc) == (-10, -0.1, pytest.approx(90.0))
    assert delta_score(5, 0)[2] == 0.0  # ref 0: delta over max(ref,1)
    assert delta_score(1000, 1)[2] == 0.0  # clamped


def test_facet_result_worst():
    r = FacetResult({"a": 100.0, "b": 70.0, "c": 70.0})
    assert r.worst == ("b", 70.0) and r.score == 70.0 and not r.exact
    assert FacetResult().exact and FacetResult().worst == ("", 100.0)


# ---- rows: parsing
def test_parse_ndjson_is_strict_about_shapes():
    assert parse_ndjson(None) == []
    assert parse_ndjson('{"a":1}\n\n{"b":2}\n') == [{"a": 1}, {"b": 2}]
    assert parse_ndjson([{"a": 1}]) == [{"a": 1}]
    assert parse_ndjson({"a": 1}) == [{"a": 1}]
    for bad in ('{"a":1}\nnot json', '{"a":1}\n[1]', [{"a": 1}, 3], 42):
        with pytest.raises(UnknownShape):
            parse_ndjson(bad)


# ---- M3 M4 M5 M6
def test_identical_rows_exact():
    rows = [R(i, level="info") for i in range(5)]
    res = evaluate_rows(rows, list(reversed(rows)))  # unordered: multiset compare, no order asked
    assert res.exact and set(res.facets) == {"row_set", "field_coverage", "value_equality", "count"}


def test_empty_answers_are_exact_here_vacuity_is_the_verdict_layer():
    assert evaluate_rows([], []).exact


def test_missing_and_extra_rows():
    ref = [R(i) for i in range(10)]
    ans = ref[:8] + [R(50), R(51)]
    res = evaluate_rows(ref, ans)
    rs = res.details["row_set"]
    assert rs["recall"] == 80.0 and rs["precision"] == 80.0
    assert rs["jaccard"] == pytest.approx(100 * 8 / 12)
    assert rs["missing"] == 2 and rs["extra"] == 2 and not rs["exact"]
    assert res.facets["count"] == 100.0 and res.details["count"]["delta"] == 0


def test_duplicate_rows_count_as_multiset():
    ref = [R(1), R(1), R(2)]
    ans = [R(1), R(2), R(2)]
    res = evaluate_rows(ref, ans)
    assert res.details["row_set"]["jaccard"] == pytest.approx(50.0)  # inter 2 / union 4
    assert evaluate_rows(ref, [R(1), R(2)]).details["count"]["delta"] == -1


def test_field_coverage_penalises_missing_and_extra():
    ref = [R(1, level="a")]
    ans = [R(1, account_id="0", **{"<null>": ""})]
    fc = evaluate_rows(ref, ans).details["field_coverage"]
    assert fc["missing"] == ["level"] and fc["extra"] == ["<null>", "account_id"]
    assert fc["jaccard"] == pytest.approx(100 * 3 / 6)


def test_value_equality_pairs_by_identity_and_names_the_min_field():
    ref = [R(i, level="info", code=i) for i in range(4)]
    ans = [R(i, level="info" if i < 3 else "warn", code=i) for i in range(4)]
    res = evaluate_rows(ref, list(reversed(ans)))
    ve = res.details["value_equality"]
    assert ve["min_field"] == "level" and res.facets["value_equality"] == 75.0
    assert ve["below_100"] == {"level": 75.0} and ve["paired_rows"] == 4


def test_value_equality_numbers_compare_as_numbers():
    assert evaluate_rows([R(1, n=1)], [R(1, n=1.0)]).exact
    huge = 10 ** 30
    assert evaluate_rows([R(1, n=huge)], [R(1, n=huge)]).exact
    assert not evaluate_rows([R(1, n=huge)], [R(1, n=2 * huge)]).exact


def test_duplicate_identity_pairs_in_order():
    ref = [R(1, v="a"), R(1, v="b")]
    res = evaluate_rows(ref, [R(1, v="a"), R(1, v="c")])
    assert res.details["value_equality"]["paired_rows"] == 2
    assert res.facets["value_equality"] == 50.0


def test_unpaired_rows_reported():
    res = evaluate_rows([R(1), R(2)], [R(1), R(3)])
    assert res.details["value_equality"]["unpaired_rows"] == 1


def test_skip_fields_dropped_everywhere():
    ref = [R(1, took=5)]
    ans = [R(1, took=9)]
    assert not evaluate_rows(ref, ans).exact
    assert evaluate_rows(ref, ans, skip={"took"}).exact


def test_evaluate_count():
    assert evaluate_count(100, 100).exact
    r = evaluate_count(100, 97)
    assert r.facets["count"] == pytest.approx(97.0) and r.details["count"]["delta"] == -3
    assert evaluate_count(None, None).exact
    assert evaluate_count(None, 5).facets["count"] == 0.0
    assert evaluate_count(0, 0).exact
    assert evaluate_count(1e300, 1e300).exact


# ---- M10
def test_order_perfect_and_reversed():
    rows = [R(i) for i in range(6)]
    assert evaluate_rows(rows, rows, order=["_time"]).facets["order"] == 100.0
    rev = evaluate_rows(rows, list(reversed(rows)), order=["_time"])
    assert rev.facets["order"] == 0.0 and rev.details["order"]["comparable_pairs"] == 15


def test_order_not_scored_when_not_defined():
    rows = [R(i) for i in range(4)]
    assert "order" not in evaluate_rows(rows, list(reversed(rows))).facets


def test_order_single_swap_kendall():
    rows = [R(i) for i in range(5)]
    swapped = [rows[1], rows[0]] + rows[2:]
    k = kendall_order(rows, swapped, ["_time"])
    assert k["discordant_pairs"] == 1 and k["comparable_pairs"] == 10 and k["score"] == pytest.approx(90.0)


def test_order_ignores_pairs_inside_a_full_key_tie_group():
    t = "2026-01-01T00:00:00.000000000Z"
    a = [{"_time": t, "_msg": "x"}, {"_time": t, "_msg": "y"}, {"_time": t, "_msg": "z"},
         {"_time": "2026-01-01T00:00:09.000000000Z", "_msg": "w"}]
    permuted = [a[2], a[0], a[1], a[3]]
    k = kendall_order(a, permuted, ["_time"])
    assert k["discordant_pairs"] == 0 and k["tie_groups"] == 1 and k["comparable_pairs"] == 3
    # a tied group moving relative to a different key is still a real disagreement
    moved = [a[3], a[0], a[1], a[2]]
    assert kendall_order(a, moved, ["_time"])["discordant_pairs"] == 3


def test_order_multi_key_tie_group_needs_every_key_equal():
    rows = [{"_time": "t", "level": "a", "_msg": "1"}, {"_time": "t", "level": "b", "_msg": "2"}]
    k = kendall_order(rows, list(reversed(rows)), ["level", "_time"])
    assert k["discordant_pairs"] == 1  # keys differ on level: not a tie


def test_order_all_tied_is_vacuous_100():
    rows = [{"_time": "t", "_msg": str(i)} for i in range(4)]
    k = kendall_order(rows, list(reversed(rows)), ["_time"])
    assert k["comparable_pairs"] == 0 and k["score"] == 100.0


def test_order_only_over_common_rows_and_duplicates():
    ref = [R(1), R(2), R(2), R(3)]
    ans = [R(3), R(2), R(9), R(2), R(1)]
    k = kendall_order(ref, ans, ["_time"])
    assert k["rows_compared"] == 4
    assert kendall_order([], [], ["_time"])["score"] == 100.0


def test_inversions_matches_bruteforce():
    import random
    rnd = random.Random(3)
    for n in (0, 1, 2, 17, 60):
        seq = rnd.sample(range(200), n)
        brute = sum(1 for i in range(n) for j in range(i + 1, n) if seq[i] > seq[j])
        assert _inversions(seq) == brute


# ---- tie-cut integration
def test_tie_cut_accepted_via_fetch_and_wrong_order_elsewhere_still_seen():
    t = "2026-01-01T00:00:50.000000000Z"
    g = [{"_time": t, "_stream_id": "s", "_msg": c} for c in "ABC"]
    head = [R(i) for i in (53, 52, 51)]
    ref, sut = head + [g[0], g[1]], head + [g[0], g[2]]
    fetch = lambda at: (200, g, 200, list(reversed(g)))  # noqa: E731
    ok = evaluate_rows(ref, sut, order=["_time"], tie_fetch=fetch, tie_sort_fields=["_time"])
    assert ok.exact and any("tie-cut accepted" in n for n in ok.notes)
    # swap two head rows too: the tie is explained but order elsewhere is not
    sut2 = [head[1], head[0], head[2], g[0], g[2]]
    bad = evaluate_rows(ref, sut2, order=["_time"], tie_fetch=fetch, tie_sort_fields=["_time"])
    assert bad.facets["order"] < 100.0
    assert not evaluate_rows(ref, sut, order=["_time"]).exact


def test_tie_cut_rejected_when_group_not_larger_or_fetch_missing():
    t = "2026-01-01T00:00:50.000000000Z"
    g = [{"_time": t, "_stream_id": "s", "_msg": c} for c in "AB"]
    ref, sut = [R(51), g[0]], [R(51), g[1]]
    res = evaluate_rows(ref, sut, tie_fetch=lambda at: (200, g[:1], 200, g[:1]))
    assert not res.exact and any("no limit cut it" in n for n in res.notes)
    res = evaluate_rows(ref, sut, tie_fetch=lambda at: (500, [], 200, g))
    assert any("status" in n for n in res.notes)
    res = evaluate_rows(ref, sut, tie_fetch=lambda at: (200, g, 200, g[:1]))
    assert any("differs between sides" in n for n in res.notes)
    res = evaluate_rows(ref, sut)  # no fetcher: strict
    assert not res.exact
