"""Key order inside rows (#429 / #432: the JSON members of a cold row come in upstream's order)."""
import json

from scripts.proof.metrics.evaluate import evaluate_body
from scripts.proof.metrics.rows import key_order_agreement

IDENT = ("_time", "_stream_id", "_msg")


def row(*keys):
    return {k: "v" if k not in ("_time", "_stream_id", "_msg") else k + "1" for k in keys}


def test_same_order_is_exact_and_different_order_is_scored():
    ref = [row("_time", "_stream_id", "_stream", "_msg", "a", "b")]
    same = key_order_agreement(ref, [dict(ref[0])], IDENT)
    assert same.facets["key_order"] == 100.0 and same.exact
    swapped = [row("_time", "_stream_id", "_msg", "_stream", "b", "a")]
    r = key_order_agreement(ref, swapped, IDENT)
    assert r.facets["key_order"] == 0.0 and "key order differs" in r.notes[0]
    assert r.details["key_order"] == {"paired_rows": 1, "equal_rows": 0}


def test_only_common_keys_count_and_unpaired_rows_are_ignored():
    ref = [row("_time", "_stream_id", "_msg", "a")]
    extra = [row("_time", "x", "_stream_id", "_msg", "a")]  # an extra key is the field-coverage facet's business
    assert key_order_agreement(ref, extra, IDENT).facets["key_order"] == 100.0
    other = [{"_time": "other", "_stream_id": "s", "_msg": "m"}]
    r = key_order_agreement(ref, other, IDENT)
    assert r.details["key_order"]["paired_rows"] == 0 and r.facets["key_order"] == 100.0


def test_duplicate_identities_pair_by_occurrence_and_share_is_a_percentage():
    a, b = row("_time", "_stream_id", "_msg", "x", "y"), row("_time", "_stream_id", "_msg", "x", "y")
    swapped = row("_time", "_stream_id", "_msg", "y", "x")
    r = key_order_agreement([a, b], [dict(a), swapped], IDENT)
    assert r.facets["key_order"] == 50.0


def test_evaluate_body_adds_the_facet_only_when_asked():
    ref = "\n".join(json.dumps(x) for x in [row("_time", "_stream_id", "_msg", "a", "b")])
    ans = "\n".join(json.dumps(x) for x in [row("_time", "_stream_id", "_msg", "b", "a")])
    meta = {"kind": "rows", "surface": "vl-native"}
    off = evaluate_body(meta, {"status": 200, "body": ref}, {"status": 200, "body": ans})
    on = evaluate_body({**meta, "key_order": True}, {"status": 200, "body": ref}, {"status": 200, "body": ans})
    assert "key_order" not in off.facets and off.exact
    assert on.facets["key_order"] == 0.0 and not on.exact


# ---- a limit that cuts the list: upstream keeps arbitrary entries past it ----

def test_limited_lists_compare_size_and_membership_not_which_values():
    from scripts.proof.metrics.values import evaluate_limited
    universe = ["a", "b", "c", "d", "e"]
    ok = evaluate_limited({"a": 0, "b": 0}, {"d": 0, "e": 0}, universe)
    assert ok.exact and ok.details["limited"]["universe_n"] == 5
    short = evaluate_limited({"a": 0, "b": 0}, {"d": 0}, universe)
    assert short.facets["cardinality"] == 50.0 and short.facets["membership"] == 100.0
    foreign = evaluate_limited({"a": 0, "b": 0}, {"d": 0, "zzz": 0}, universe)
    assert foreign.facets["membership"] == 50.0 and foreign.details["limited"]["outside"] == ["zzz"]
    assert evaluate_limited({}, {}, []).exact


def test_evaluate_body_scores_limit_arbitrary_rows_with_the_universe():
    meta = {"kind": "values", "surface": "vl-native", "limit_arbitrary": True, "universe": ["a", "b", "c", "d"]}
    ref = json.dumps({"values": [{"value": "a", "hits": 0}, {"value": "b", "hits": 0}]})
    ans = json.dumps({"values": [{"value": "c", "hits": 0}, {"value": "d", "hits": 0}]})
    r = evaluate_body(meta, {"status": 200, "body": ref}, {"status": 200, "body": ans})
    assert r.exact and "value_set" not in r.facets
    wrong = json.dumps({"values": [{"value": "c", "hits": 0}, {"value": "x", "hits": 0}]})
    assert evaluate_body(meta, {"status": 200, "body": ref}, {"status": 200, "body": wrong}).facets["membership"] == 50.0


def test_rows_with_one_identity_pair_by_occurrence_with_their_own_key_order():
    ident = ("_time", "_stream_id", "_msg")
    r1 = {"_time": "t", "_stream_id": "s", "_msg": "m", "x": 1, "y": 2}
    r2 = {"_time": "t", "_stream_id": "s", "_msg": "m", "y": 2, "x": 1}
    both = key_order_agreement([r1, r2], [dict(r1), dict(r2)], ident)
    assert both.facets["key_order"] == 100.0 and both.details["key_order"]["paired_rows"] == 2
    crossed = key_order_agreement([r1, r2], [dict(r2), dict(r1)], ident)
    assert crossed.facets["key_order"] == 0.0


# ---- rows that share an identity are paired by content first ----

def test_rows_sharing_an_identity_pair_equal_rows_first_so_a_reordering_is_not_a_value_difference():
    from scripts.proof.metrics.rows import _pair_group, _pair_value_equality, evaluate_rows
    ident = ("_time",)
    a = {"_time": "t", "hits": "1"}
    b = {"_time": "t", "hits": "9"}
    ref, ans = [a, b], [dict(b), dict(a)]  # the same two rows, in the other order
    pairs = _pair_group(ref, ans)
    assert len(pairs) == 2 and all(x == y for x, y in pairs)
    eq, n, paired = _pair_value_equality(ref, ans, ident)
    assert paired == 2 and eq["hits"] == n["hits"] == 2
    res = evaluate_rows(ref, ans, identity=ident)
    assert res.facets["value_equality"] == 100.0
    # a row that really differs still pairs with its counterpart and is counted
    ref2, ans2 = [a, b], [dict(b), {"_time": "t", "hits": "5"}]
    eq2, n2, _ = _pair_value_equality(ref2, ans2, ident)
    assert n2["hits"] == 2 and eq2["hits"] == 1
    # extra rows on one side stay unpaired
    assert len(_pair_group([a], [dict(a), dict(b)])) == 1


def test_limited_lists_past_the_limit_must_carry_zeroed_hits_like_the_reference():
    from scripts.proof.metrics.values import evaluate_limited
    universe = ["a", "b", "c", "d"]
    zeroed = evaluate_limited({"a": 0, "b": 0}, {"c": 0, "d": 0}, universe)
    assert zeroed.exact and zeroed.facets["hits_zeroed"] == 100.0
    counted = evaluate_limited({"a": 0, "b": 0}, {"c": 7, "d": 0}, universe)
    assert counted.facets["hits_zeroed"] == 50.0 and counted.details["hits_zeroed"]["not_zero"] == ["c"]
    # not cut, or a reference that carries counts: no such contract
    assert "hits_zeroed" not in evaluate_limited({"a": 3, "b": 4}, {"c": 1, "d": 2}, universe).facets
    assert "hits_zeroed" not in evaluate_limited({"a": 0, "b": 0}, {"a": 5, "b": 6}, ["a", "b"]).facets
