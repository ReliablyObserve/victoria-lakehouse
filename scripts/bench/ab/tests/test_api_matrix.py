"""api_matrix.py: scoring of requests sent to three fake targets."""

import json

import api_matrix
from fakes import Server

BODY_A = '{"values":[{"value":"INFO","hits":3},{"value":"ERROR","hits":2}]}'
BODY_B = '{"values":[{"value":"INFO","hits":3},{"value":"ERROR","hits":9}]}'


def req(**kw):
    r = {"id": "vl.select.x", "row": "vl.select.x", "registry_surface": "vl", "surface": "vl-native", "signal": "logs", "kind": "values", "method": "GET",
         "path": "/select/logsql/field_values", "params": {"query": "*"}, "headers": {}, "expect": "pass", "compare": "values-with-hits", "lh_only": False,
         "expect_error": False, "limit": None, "arbitrary": False}
    r.update(kw)
    return r


def servers(ref, base, pr, status=(200, 200, 200)):
    out = [Server(lambda m, p, h, b, body=body, st=st: (st, body, {})) for body, st in zip((ref, base, pr), status)]
    state = {"ports": {f"{t}-logs": s.port for t, s in zip(("ref", "base", "pr"), out)}}
    return out, state


def run(ref, base, pr, status=(200, 200, 200), **kw):
    ss, state = servers(ref, base, pr, status)
    try:
        return api_matrix.run_request(state, req(**kw), 3)
    finally:
        for s in ss:
            s.close()


def test_exact_when_both_builds_match_the_reference():
    r = run(BODY_A, BODY_A, BODY_A)
    assert r["verdict"] == "exact" and r["pr_score"] == 100 and not r["status_changed"]


def test_regressed_when_the_pr_moves_away_from_the_reference_and_fixed_when_it_moves_closer():
    assert run(BODY_A, BODY_A, BODY_B)["verdict"] == "regressed"
    assert run(BODY_A, BODY_B, BODY_A)["verdict"] == "fixed"
    assert run(BODY_A, BODY_B, BODY_B)["verdict"] == "same" and run(BODY_A, BODY_B, BODY_B)["pr_score"] < 100


def test_a_status_change_between_base_and_pr_fails_the_row():
    r = run(BODY_A, BODY_A, "boom", status=(200, 200, 500))
    assert r["status_changed"] and api_matrix.failing(r)


def test_a_request_every_target_rejects_is_unusable_not_a_pass_and_not_a_failure():
    r = run("bad", "bad", "bad", status=(400, 400, 400))
    assert r["verdict"] == "unusable" and not api_matrix.failing(r)
    ok = run("bad", "bad", "bad", status=(400, 400, 400), expect_error=True, kind=None, compare="error")
    assert ok["verdict"] in ("exact", "same")  # a row that expects the error is scored on status and error text


def test_an_arbitrary_subset_request_is_not_scored():
    r = run(BODY_A, BODY_B, BODY_B, arbitrary=True)
    assert r["verdict"] == "arbitrary" and not api_matrix.failing(r)
    r = run(BODY_A, BODY_B, BODY_B, limit=(2, True))
    assert r["verdict"] == "same"  # the stats answer has fewer rows than the limit: not truncated, so scored


def test_lakehouse_only_rows_are_scored_pr_against_base():
    ss, state = servers("unused", '{"a":1,"b":"x"}', '{"a":1,"b":"x"}')
    try:
        r = api_matrix.run_request(state, req(lh_only=True, kind="schema", surface="lh-logs", compare="schema"), 3)
        assert r["verdict"] == "exact" and "ref" not in r["status"]
    finally:
        for s in ss:
            s.close()


def test_status_only_rows_and_latency_verdict():
    r = run("x", "x", "x", kind=None, compare="status", expect_error=True)
    assert r["verdict"] in ("exact", "same") and r["pr_score"] == 100
    assert api_matrix.latency_verdict({"p50_ms": {"base": 10, "pr": 100}}) == "slower"
    assert api_matrix.latency_verdict({"p50_ms": {"base": 10, "pr": 25}}) == "same"
    assert api_matrix.latency_verdict({"p50_ms": {"base": 100, "pr": 10}}) == "faster"


def test_summary_counts_and_not_exercised_reasons():
    results = [dict(run(BODY_A, BODY_A, BODY_A), id="a"), dict(run(BODY_A, BODY_A, BODY_B), id="b")]
    s = api_matrix.summarize(results, [{"id": "x", "reason": "no HTTP request"}])
    assert s["verdicts"] == {"exact": 1, "regressed": 1} and s["failing"] == ["b"] and s["not_exercised"] == 1


def rows_body(*msgs):
    return "".join(json.dumps({"_msg": m}) + "\n" for m in msgs)


def limited(ref_rows, base_rows, pr_rows):
    """ref answers the unlimited query with ref_rows (and a limited one with its first two); base and pr answer base_rows / pr_rows."""
    def ref(method, path, headers, body):
        return 200, rows_body(*(ref_rows if "limit" not in path else ref_rows[:2])), {}
    ss = [Server(ref), Server(lambda *a: (200, rows_body(*base_rows), {})), Server(lambda *a: (200, rows_body(*pr_rows), {}))]
    state = {"ports": {f"{t}-logs": s.port for t, s in zip(("ref", "base", "pr"), ss)}}
    try:
        return api_matrix.run_request(state, req(kind="rows", compare="ndjson-multiset", path="/select/logsql/query",
                                                 params={"query": "* | copy a as b | limit 2"}, limit=(2, False)), 2)
    finally:
        for s in ss:
            s.close()


def test_a_limited_query_is_scored_by_membership_in_the_unlimited_reference_answer():
    universe = ["a", "b", "c", "d"]
    ok = limited(universe, ["c", "d"], ["a", "c"])  # other subsets than the reference's first two, all members
    assert ok["verdict"] == "exact" and ok["pr_score"] == 100
    bad = limited(universe, ["c", "d"], ["a", "zzz"])
    assert bad["verdict"] == "regressed" and bad["pr_worst"] == "membership" and api_matrix.failing(bad)
    gone = limited(universe, ["c", "d"], [])
    assert gone["verdict"] == "regressed" and gone["pr_worst"] == "emptiness"


def test_a_nondeterministic_row_fails_only_when_a_sample_had_the_pr_worse_than_the_base():
    assert api_matrix.failing({"verdict": "nondeterministic", "samples": ["same", "fixed", "exact"], "status_changed": False}) is False
    assert api_matrix.failing({"verdict": "nondeterministic", "samples": ["same", "regressed"], "status_changed": False}) is True
