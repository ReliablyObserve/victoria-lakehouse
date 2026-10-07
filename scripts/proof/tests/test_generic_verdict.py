import pytest

from scripts.proof.metrics.common import FacetResult
from scripts.proof.metrics.generic import (
    error_text, evaluate_json_leaves, evaluate_schema, evaluate_status, evaluate_truth,
    flatten_leaves, is_error, latency_ratio, schema_paths,
)
from scripts.proof.metrics.verdict import (
    CaseResult, classify, classify_samples, pct_text, rollup_rows, rollup_signals, rollup_surfaces,
)


def A(status=200, body=None, **kw):
    return {"status": status, "body": body, **kw}


# ---- M1 M2
def test_status_and_error_agreement():
    assert evaluate_status(A(200), A(200)).exact
    r = evaluate_status(A(200), A(500))
    assert r.facets["status"] == 0.0 and "error" in r.facets
    r = evaluate_status(A(400, "bad  'x'"), A(400, 'bad "x"'))
    assert r.exact and r.facets["error"] == 100.0
    assert evaluate_status(A(400, {"status": "error", "error": "Boom"}), A(400, {"error": "boom"})).exact
    assert evaluate_status(A(400, "a"), A(400, "b")).facets["error"] == 0.0
    assert "error" not in evaluate_status(A(200), A(200)).facets


def test_is_error_variants():
    assert is_error(A(503)) and is_error(A(200, timeout=True)) and is_error(A(200, {"status": "error"}))
    assert not is_error(A(200, {"status": "success"}))
    assert error_text(A(400, {"errors": ["x"]})) == "['x']".lower().replace("'", '"')


# ---- M11
def test_json_leaf_agreement():
    assert evaluate_json_leaves({"a": 1, "b": [1, 2]}, {"b": [1, 2], "a": 1.0}).exact
    r = evaluate_json_leaves({"a": 1, "b": [1, 2]}, {"a": 1, "b": [1, 3], "c": 1})
    assert r.facets["json_leaves"] == pytest.approx(100 * 2 / 4)
    assert r.details["json_leaves"]["differing_paths"] == ["b[1]", "c"]
    assert evaluate_json_leaves({}, {}).exact
    assert flatten_leaves({"a": {}, "b": []}) == {"a": "{}", "b": "[]"}
    assert flatten_leaves(None) == {"": "null"}


# ---- M12
def test_schema_agreement_and_contract():
    base = {"rows": 10, "name": "x", "list": [{"a": 1}, {"a": 2}]}
    assert evaluate_schema(base, {"rows": 99, "name": "y", "list": [{"a": 7}]}).exact  # moving values are fine
    r = evaluate_schema(base, {"rows": "10", "name": "x", "extra": 1, "list": [{"a": 1}]})
    assert r.facets["schema_keys"] == pytest.approx(100 * 3 / 4)
    assert r.details["schema"]["type_mismatch"] == ["rows"] and r.details["schema"]["extra"] == ["extra"]
    r = evaluate_schema(base, base, {"rows": "number", "name": "string", "missing": "any"})
    assert r.facets["schema_contract"] == pytest.approx(100 * 2 / 3)
    assert r.details["schema_contract"]["violations"] == ["missing"]
    assert evaluate_schema(base, base, {"rows": "string"}).facets["schema_contract"] == 0.0
    assert schema_paths([]) == {"[]": set()}


# ---- M13
def test_truth_scalar_reports_unflushed_without_absorbing():
    t = {"data": {"result": [{"metric": {}, "value": [1, "10120"]}]}}
    r = evaluate_truth("scalar", {"rows": 10000}, t, lh_path="rows", unflushed_rows=120)
    assert r.facets["truth"] == pytest.approx(100 * (1 - 120 / 10120)) and r.facets["truth"] < 100
    assert r.details["unflushed_rows"] == 120 and r.details["truth"]["delta"] == -120
    assert evaluate_truth("scalar", {"rows": 10120}, t, lh_path="rows").exact
    r = evaluate_truth("scalar", {"x": 1}, t, lh_path="rows")
    assert r.facets["truth"] == 0.0 and "error" in r.details["truth"]
    assert evaluate_truth("scalar", {"a": {"b": [5]}}, {"data": {"result": [{"metric": {}, "value": [1, "5"]}]}},
                          lh_path="a.b.0").exact


def test_truth_per_key_uniq():
    t = {"data": {"result": [{"metric": {"f": "a"}, "value": [1, "5"]}, {"metric": {"f": "b"}, "value": [1, "7"]}]}}
    ok = evaluate_truth("per_key", {"c": {"a": 5, "b": 7}}, t, lh_path="c", key_label="f")
    assert ok.exact
    r = evaluate_truth("per_key", {"c": {"a": 5, "b": 6, "z": 1}}, t, lh_path="c", key_label="f")
    assert r.facets["truth"] == 0.0 and r.details["truth"]["differing"] == ["b", "z"]
    assert evaluate_truth("per_key", {"c": 3}, t, lh_path="c", key_label="f").facets["truth"] == 0.0


def test_truth_tenant_set():
    hot = {"data": [{"account_id": 0, "project_id": 0}, {"account_id": 1, "project_id": 2}]}
    assert evaluate_truth("tenant_set", ["0:0", "1:2"], hot).exact
    r = evaluate_truth("tenant_set", {"tenants": [{"account_id": 0, "project_id": 0}]}, hot, lh_path="tenants")
    assert r.facets["truth"] == 50.0 and r.details["truth"]["missing"] == ["1:2"]
    with pytest.raises(ValueError):
        evaluate_truth("nope", {}, {})


# ---- M14
def test_latency_validated_only():
    r = latency_ratio([(20, True), (30, True), (1, False)], [(10, True), (10, True)], [(5, True)])
    assert r["pr_p50_ms"] == 20 and r["pr_over_base"] == 2.0 and r["pr_over_ref"] == 4.0
    assert r["invalid"] == {"pr": 1, "base": 0, "ref": 0}
    r = latency_ratio([(5, False)], [(10, True)])
    assert r["pr_over_base"] is None and "pr_over_ref" not in r


# ---- classifier truth table
def FR(**f):
    return FacetResult(dict(f))


EX = lambda: FR(a=100.0, b=100.0)  # noqa: E731


@pytest.mark.parametrize("base,pr,kw,want", [
    (EX(), EX(), {}, "exact"),
    (FR(a=80.0, b=100.0), EX(), {}, "fixed"),
    (FR(a=80.0, b=100.0), FR(a=80.0, b=100.0), {}, "same"),
    (FR(a=80.0, b=100.0), FR(a=90.0, b=100.0), {}, "improved"),
    (FR(a=80.0, b=100.0), FR(a=90.0, b=99.0), {}, "regressed"),  # better on one, worse on another
    (EX(), FR(a=99.0, b=100.0), {}, "regressed"),  # a new difference
    (FR(a=50.0, b=100.0), FR(a=40.0, b=100.0), {}, "regressed"),
    (EX(), EX(), {"claimed": True}, "not-reproduced-on-base"),
    (FR(a=80.0, b=100.0), EX(), {"claimed": True}, "fixed"),
    (EX(), EX(), {"vacuous": True}, "vacuous"),
    (EX(), EX(), {"blocked": True}, "blocked"),
    (EX(), EX(), {"harness_error": True}, "harness-error"),
    (None, EX(), {}, "harness-error"),
    (EX(), None, {}, "harness-error"),
    (EX(), EX(), {"blocked": True, "vacuous": True}, "blocked"),
    (EX(), FR(a=0.0), {"blocked": True}, "blocked"),
])
def test_classifier_truth_table(base, pr, kw, want):
    assert classify(base, pr, **kw) == want


def test_a_gain_on_a_non_minimum_facet_is_improved_not_same():
    # the worst facet (a=50) is unchanged; b improved: verdicts use the whole vector
    base, pr = FR(a=50.0, b=60.0), FR(a=50.0, b=90.0)
    assert base.score == pr.score == 50.0
    assert classify(base, pr) == "improved"


def test_tolerance_on_facet_compare():
    assert classify(FR(a=80.0), FR(a=79.99999999999), tol=1e-3) == "same"
    assert classify(FR(a=80.0), FR(a=79.0), tol=1e-3) == "regressed"


def test_classify_samples():
    assert classify_samples(["exact", "exact", "exact"]) == "exact"
    assert classify_samples(["exact", "regressed", "exact"]) == "nondeterministic"
    assert classify_samples([]) == "harness-error"
    assert classify_samples(["same"]) == "same"


# ---- roll-ups
def case(id, row, surface, signal, b, p, verdict="same"):
    return CaseResult(id, row, surface, signal, "ep", verdict, b, p)


def test_rollup_row_min_over_requests_and_exactness():
    rs = [case("r1a", "row1", "vl-native", "logs", FR(a=90.0), FR(a=100.0)),
          case("r1b", "row1", "vl-native", "logs", FR(a=100.0), FR(a=70.0)),
          case("r2", "row2", "vl-native", "logs", FR(a=100.0), FR(a=100.0))]
    rows = rollup_rows(rs)
    r1 = rows[("logs", "vl-native", "row1")]
    assert r1["pr_min"] == 70.0 and r1["base_min"] == 90.0 and r1["pr_mean"] == 85.0 and not r1["pr_exact"]
    st = rollup_surfaces(rs)[("logs", "vl-native")]
    assert st["rows"] == 2 and st["pr_exact_pct"] == 50.0 and st["base_exact_pct"] == 50.0
    assert st["pr_score"] == pytest.approx((70 + 100) / 2)


def test_signals_never_averaged_together():
    rs = [case("l", "l", "vl-native", "logs", FR(a=100.0), FR(a=100.0)),
          case("t", "t", "jaeger", "traces", FR(a=10.0), FR(a=20.0))]
    sig = rollup_signals(rs)
    assert set(sig) == {"logs", "traces"}  # no combined entry
    assert sig["logs"]["pr_score"] == 100.0 and sig["traces"]["pr_score"] == 20.0


def test_rollup_skips_verdicts_that_prove_nothing():
    rs = [case("b", "b", "vl-native", "logs", FR(a=0.0), FR(a=0.0), "blocked"),
          case("v", "v", "vl-native", "logs", FR(a=100.0), FR(a=100.0), "vacuous"),
          case("h", "h", "vl-native", "logs", None, None, "harness-error"),
          case("n", "n", "vl-native", "logs", FR(a=1.0), FR(a=1.0), "nondeterministic"),
          case("ok", "ok", "vl-native", "logs", FR(a=100.0), FR(a=100.0), "exact")]
    assert list(rollup_rows(rs)) == [("logs", "vl-native", "ok")]
    assert rollup_signals([]) == {}


def test_pct_text():
    assert pct_text(None) == "-" and pct_text(FR(a=100.0)) == "100" and pct_text(FR(a=99.99)) == "99.9"


def test_case_result_scores():
    c = case("x", "x", "vl-native", "logs", FR(a=60.0, b=90.0), FR(a=80.0))
    assert c.q_base == 60.0 and c.q_pr == 80.0 and c.delta == 20.0
    assert CaseResult("x", "x", "jaeger", "traces", "e", "harness-error").delta is None
