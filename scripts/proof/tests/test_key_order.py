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
