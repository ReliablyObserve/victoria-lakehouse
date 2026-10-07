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
