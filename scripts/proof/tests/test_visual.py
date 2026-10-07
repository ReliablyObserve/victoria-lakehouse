"""Visual proof: panel states and transitions, frame conversion, page comparison, montage and the comment-less report."""
import json
import os

import pytest
from PIL import Image

from scripts.proof.visual import compare, frames, montage, states
from scripts.proof.visual.states import DATA, EMPTY, ERROR, UNSETTLED

OK_FRAMES = {"results": {"A": {"frames": [{"schema": {"fields": [{"name": "Time", "type": "time"}, {"name": "Line", "type": "string"}]},
                                            "data": {"values": [[1, 2], ["a", "b"]]}}]}}}
EMPTY_FRAMES = {"results": {"A": {"frames": [{"schema": {"fields": [{"name": "Time", "type": "time"}, {"name": "Line", "type": "string"}]},
                                              "data": {"values": [[], []]}}]}}}
QUERY_ERROR = {"results": {"A": {"error": "parse error: unexpected token", "status": 400}}}
UI_OK = {"noData": 0, "banners": [], "panelErrors": 0, "jaegerErrors": 0}


def rec(resp, status=200, url="/api/ds/query?x=1", request=None):
    return {"url": url, "status": status, "response": resp, "request": request}


def cap(records, ui=None, settled=True):
    return {"settled": settled, "records": records, "ui": ui or UI_OK}


# ---- data side and DOM side ----

def test_data_states():
    assert states.data_state([rec(OK_FRAMES)])["state"] == DATA
    assert states.data_state([rec(EMPTY_FRAMES)])["state"] == EMPTY
    assert states.data_state([])["state"] == "none"
    e = states.data_state([rec(QUERY_ERROR)])
    assert e["state"] == ERROR and e["kind"] == "query-row" and "parse error" in e["errors"][0]
    r = states.data_state([rec({"message": "x"}, status=502)])
    assert r["state"] == ERROR and r["kind"] == "request"
    notice = {"results": {"A": {"frames": [{"schema": {"fields": [], "meta": {"notices": [{"severity": "error", "text": "boom"}]}}, "data": {"values": []}}]}}}
    n = states.data_state([rec(notice)])
    assert n["state"] == ERROR and n["kind"] == "panel"
    assert states.data_state([rec({"values": [{"value": "a"}]}, url="/api/datasources/uid/x/resources/select/logsql/field_values")])["state"] == DATA
    assert states.data_state([rec({"values": []}, url="/x")])["state"] == EMPTY
    assert states.data_state([rec(["a"], url="/x")])["state"] == DATA
    assert states.data_state([rec("plain text", url="/x")])["state"] == EMPTY  # no rows to count


def test_dom_states():
    assert states.dom_state(None)["state"] == DATA
    assert states.dom_state({"banners": ["Plugin failed"]})["kind"] == "banner"
    assert states.dom_state({"panelErrors": 1})["kind"] == "panel"
    assert states.dom_state({"jaegerErrors": 2})["state"] == ERROR
    assert states.dom_state({"noData": 1, "noDataPanels": ["Logs"]}) == {"state": EMPTY, "kind": "", "detail": ["Logs"]}


def test_panel_state_combines_the_two_signals():
    assert states.panel_state(cap([rec(OK_FRAMES)]))["state"] == DATA
    assert states.panel_state(cap([rec(OK_FRAMES)], settled=False))["state"] == UNSETTLED
    # the data side shows the error although the DOM does not: the data side wins, with a warning
    s = states.panel_state(cap([rec(QUERY_ERROR)]))
    assert s["state"] == ERROR and "DOM does not" in s["warning"]
    # no request seen: only the DOM can say
    assert states.panel_state(cap([], {"noData": 1}))["state"] == EMPTY
    # the DOM decides an error the responses do not carry
    assert states.panel_state(cap([rec(OK_FRAMES)], {"banners": ["Bad Gateway"]}))["state"] == ERROR
    w = states.panel_state(cap([rec(OK_FRAMES)], {"noData": 1}))
    assert w["state"] == DATA and w["warning"]


@pytest.mark.parametrize("base,pr,ref,want", [
    (ERROR, EMPTY, EMPTY, "fixed"),          # the #669 / #682 case: a query error that now answers nothing, as the reference does
    (ERROR, DATA, DATA, "fixed"),
    (DATA, EMPTY, DATA, "regression"),
    (DATA, ERROR, DATA, "regression"),
    (EMPTY, ERROR, DATA, "regression"),
    (ERROR, ERROR, ERROR, "same-as-reference"),
    (ERROR, ERROR, DATA, "still-differs"),
    (EMPTY, DATA, DATA, "fixed"),
    (DATA, DATA, DATA, None),
    (EMPTY, EMPTY, EMPTY, None),
    (DATA, UNSETTLED, DATA, "unsettled"),
    (UNSETTLED, DATA, DATA, None),
    (DATA, DATA, None, None),
])
def test_transition_table(base, pr, ref, want):
    assert states.transition(base, pr, ref) == want


# ---- frames ----

def test_frames_metric_logs_and_trace_conversion():
    metric = {"results": {"A": {"frames": [{"schema": {"fields": [{"name": "Time", "type": "time"}, {"name": "Value", "type": "number", "labels": {"level": "INFO"}}]},
                                              "data": {"values": [[1000, 2000], [1, 2]]}}]}}}
    kind, body, _ = frames.answer_of_result(metric["results"]["A"])
    assert kind == "series_prom" and json.loads(body)["data"]["result"][0]["metric"] == {"level": "INFO"}
    kind, body, meta = frames.answer_of_result(OK_FRAMES["results"]["A"])
    assert kind == "rows" and meta["identity"] == ["Time", "Line"] and len(body.splitlines()) == 2
    trace = {"frames": [{"schema": {"fields": [{"name": "traceID"}, {"name": "spanID"}, {"name": "tags"}]}, "data": {"values": [["t"], ["s"], [[{"k": 1}]]]}}]}
    kind, body, meta = frames.answer_of_result(trace)
    assert meta["identity"] == ["traceID", "spanID"] and json.loads(body)["tags"] == '[{"k": 1}]'
    assert frames.answer_of_result({"error": "x"})[0] == "error"
    assert frames.answer_of_result({"frames": []})[0] == "rows"


def test_answers_key_the_same_question_alike_across_datasources():
    q = {"refId": "A", "queryType": "instant", "expr": "*", "datasource": {"uid": "vl-ref"}, "datasourceId": 1, "requestId": "r1", "intervalMs": 15000}
    q2 = {**q, "datasource": {"uid": "vl-pr"}, "datasourceId": 2, "requestId": "r2", "intervalMs": 30000}
    a = frames.answers_of([rec(OK_FRAMES, request={"queries": [q]})])
    b = frames.answers_of([rec(OK_FRAMES, request={"queries": [q2]})])
    assert set(a) == set(b) and len(a) == 1
    res = frames.answers_of([
        rec({"values": [{"value": "x", "hits": 1}]}, url="/api/datasources/uid/vl-ref/resources/select/logsql/field_values", request={"field": "f", "start": "1"}),
        rec({"data": ["svc"]}, url="/api/datasources/proxy/uid/jaeger-ref/api/services"),
        rec("<html>", url="/api/datasources/uid/x/resources/vmui"),
        rec({"hits": []}, url="/api/datasources/uid/x/resources/select/logsql/hits", request={}),
        rec({"data": []}, url="/api/datasources/proxy/uid/j/api/traces/0123abcd"),
    ])
    kinds = sorted(v["kind"] for v in res.values())
    assert kinds == ["series_hits", "trace_jaeger", "values", "values"]
    err = frames.answers_of([rec({}, status=500, request={"queries": [q]})])
    assert next(iter(err.values()))["kind"] == "error"
    qe = frames.answers_of([rec(QUERY_ERROR, request={"queries": [q]})])
    assert next(iter(qe.values()))["status"] == 500


# ---- compare on captured pages ----

def values_resp(vals):
    return {"values": [{"value": v, "hits": h} for v, h in vals]}


def page(vals, ui=None, settled=True, resp=None):
    r = rec(resp if resp is not None else values_resp(vals), url="/api/datasources/uid/vl-x/resources/select/logsql/field_values", request={"field": "http.method"})
    return cap([r], ui, settled)


def write_page(out, name, rng, sides):
    d = os.path.join(out, "data", name, rng)
    os.makedirs(d, exist_ok=True)
    for s, c in sides.items():
        json.dump(c, open(os.path.join(d, s + ".json"), "w"))


FULL = [("POST", 4), ("GET", 3)]


def test_compare_fixed_regression_same_and_match(tmp_path):
    out = str(tmp_path)
    write_page(out, "fixed", "cold", {"base": page([]), "pr": page(FULL), "ref": page(FULL)})
    write_page(out, "regress", "cold", {"base": page(FULL), "pr": page([("POST", 4)]), "ref": page(FULL)})
    write_page(out, "same", "cold", {"base": page([("POST", 4)]), "pr": page([("POST", 4)]), "ref": page(FULL)})
    write_page(out, "match", "cold", {"base": page(FULL), "pr": page(FULL), "ref": page(FULL)})
    full3 = FULL + [("PUT", 2)]
    write_page(out, "improved", "cold", {"base": page([("POST", 4)]), "pr": page(FULL), "ref": page(full3)})
    write_page(out, "noref", "cold", {"base": page(FULL), "pr": page(FULL)})
    write_page(out, "onlybase", "cold", {"base": page(FULL)})
    r = compare.compare_out(out)
    assert {k: v["verdict"] for k, v in r.items()} == {
        "fixed/cold": "fixed", "regress/cold": "regression", "same/cold": "same",
        "match/cold": "match", "improved/cold": "improved", "noref/cold": "undecided"}
    assert r["fixed/cold"]["pr_score"] == 100 and r["fixed/cold"]["base_score"] < 100
    assert r["regress/cold"]["pr_score"] < r["regress/cold"]["base_score"]
    assert "onlybase/cold" not in r


def test_669_682_sequence_a_base_query_error_that_now_answers_nothing_is_fixed(tmp_path):
    err = cap([rec(QUERY_ERROR, request={"queries": [{"refId": "A", "queryType": "range", "expr": "bad"}]})])
    empty = cap([rec(EMPTY_FRAMES, request={"queries": [{"refId": "A", "queryType": "range", "expr": "bad"}]})], {"noData": 1, "noDataPanels": ["Logs"]})
    write_page(str(tmp_path), "p", "1h", {"base": err, "pr": empty, "ref": empty})
    r = compare.compare_out(str(tmp_path))["p/1h"]
    assert r["states"]["base"]["state"] == ERROR and r["states"]["pr"]["state"] == EMPTY
    assert r["verdict"] == "fixed"


def test_state_regression_unsettled_and_unexpected_change(tmp_path):
    ok = page(FULL)
    write_page(str(tmp_path), "r", "1h", {"base": ok, "pr": page(FULL, {"banners": ["Bad Gateway"]}), "ref": ok})
    write_page(str(tmp_path), "u", "1h", {"base": ok, "pr": page(FULL, settled=False), "ref": ok})
    # base and PR differ from each other in the same way against the reference (a hit moved between values): unexpected
    a, b = page([("POST", 5), ("GET", 3)]), page([("POST", 3), ("GET", 5)])
    write_page(str(tmp_path), "x", "1h", {"base": a, "pr": b, "ref": page(FULL)})
    r = compare.compare_out(str(tmp_path))
    assert r["r/1h"]["verdict"] == "regression" and r["u/1h"]["verdict"] == "unsettled"
    assert r["x/1h"]["verdict"] in ("unexpected-change", "same", "regression", "improved")


def test_unasked_and_extra_requests_are_differences(tmp_path):
    ref = cap([rec(values_resp(FULL), url="/api/datasources/uid/a/resources/select/logsql/field_values", request={"field": "f1"}),
               rec(values_resp(FULL), url="/api/datasources/uid/a/resources/select/logsql/field_values", request={"field": "f2"})])
    side = cap([rec(values_resp(FULL), url="/api/datasources/uid/a/resources/select/logsql/field_values", request={"field": "f1"}),
                rec(values_resp(FULL), url="/api/datasources/uid/a/resources/select/logsql/field_values", request={"field": "f3"})])
    fr, notes = compare.score_side(frames.answers_of(ref["records"]), frames.answers_of(side["records"]))
    assert any("not asked" in n for n in notes) and any("extra request" in n for n in notes) and fr.score == 0


def test_kind_mismatch_and_undecodable_answers_are_scored_zero():
    ref = {"k": {"kind": "values", "body": json.dumps(values_resp(FULL)), "meta": {}, "status": 200}}
    side = {"k": {"kind": "rows", "body": "", "meta": {}, "status": 200}}
    fr, notes = compare.score_side(ref, side)
    assert fr.score == 0 and "kind" in notes[0]
    bad = {"k": {"kind": "values", "body": json.dumps({"nothing": 1}), "meta": {}, "status": 200}}
    fr, notes = compare.score_side(ref, bad)
    assert fr.score == 0 and "UnknownShape" in notes[0]


def test_markdown_and_main_exit_code(tmp_path, capsys):
    ok = page(FULL)
    write_page(str(tmp_path), "m", "1h", {"base": ok, "pr": ok, "ref": ok})
    assert compare.main([str(tmp_path)]) == 0
    assert os.path.exists(os.path.join(str(tmp_path), "compare.json"))
    assert "| `m/1h` | 100 | 100 |" in open(os.path.join(str(tmp_path), "compare.md")).read()
    write_page(str(tmp_path), "z", "1h", {"base": ok, "pr": page(FULL, {"banners": ["Bad Gateway"]}), "ref": ok})
    assert compare.main([str(tmp_path)]) == 1
    capsys.readouterr()


# ---- montage ----

def test_montage_side_by_side_and_pixel_diff(tmp_path):
    d = tmp_path / "shots" / "pg" / "cold"
    d.mkdir(parents=True)
    Image.new("RGB", (800, 600), (255, 255, 255)).save(d / "base.png")
    Image.new("RGB", (800, 600), (255, 255, 255)).save(d / "pr.png")
    scores = montage.make(str(tmp_path))
    assert scores == {"pg-cold": 0.0}
    m = tmp_path / "montage" / "pg-cold.png"
    assert m.exists() and m.stat().st_size <= montage.MAX_BYTES
    Image.new("RGB", (800, 600), (0, 0, 0)).save(d / "pr.png")
    Image.new("RGB", (800, 600), (255, 255, 255)).save(d / "ref.png")
    assert montage.make(str(tmp_path))["pg-cold"] == 1.0
    Image.new("RGB", (700, 600), (0, 0, 0)).save(d / "pr.png")
    assert montage.score(montage.load_image(str(d / "base.png")), montage.load_image(str(d / "pr.png"))) == 1.0


def test_montage_skips_a_page_without_both_sides(tmp_path):
    d = tmp_path / "shots" / "pg" / "cold"
    d.mkdir(parents=True)
    Image.new("RGB", (100, 100)).save(d / "base.png")
    assert montage.make(str(tmp_path)) == {}


# ---- the PR comment ----

def _report(results):
    return {"state": {}, "seed_equality": {}, "results": results}


def _res(rid, verdict, b, p, bf=None, pf=None, notes=None, form="numeric", layer="cold", surface="vl-native"):
    return {"id": f"{surface}/{rid}.{form}.{layer}", "row": rid, "surface": surface, "signal": "logs", "form": form, "layer": layer,
            "verdict": verdict, "base_score": b, "pr_score": p, "base_facets": bf or {}, "pr_facets": pf or {}, "pr_notes": notes or []}


def test_comment_lists_changes_remaining_differences_and_explanations(tmp_path):
    from scripts.proof import comment
    rep = _report([
        _res("a.exact", "exact", 100.0, 100.0),
        _res("b.fixed", "fixed", 0.0, 100.0, {"value_set": 0.0}, {"value_set": 100.0}),
        _res("c.same", "same", 71.0, 71.0, {"value_set": 71.0}, {"value_set": 71.0}, ["extra field account_id"]),
        _res("d.reg", "regressed", 100.0, 50.0, {"hits": 100.0}, {"hits": 50.0}),
        _res("e.vac", "vacuous", 100.0, 100.0),
    ])
    api = tmp_path / "api"
    api.mkdir()
    json.dump(rep, open(api / "report.json", "w"))
    exp = tmp_path / "e.json"
    json.dump({"c.": "known B1: injected schema fields"}, open(exp, "w"))
    out = tmp_path / "c.md"
    rc = comment.main(["--api", str(api), "--label", "PR 438", "--explain", str(exp), "--out", str(out)])
    text = out.read_text()
    assert rc == 1  # the regression row has no explanation
    assert "`b.fixed`" in text and "FIXED" in text and "REGRESSED" in text and "`a.exact`" not in text
    assert "1 requests match the reference exactly" in text
    assert "known B1" in text and "**unexplained**" in text
    assert "extra field account_id" in text


def test_comment_with_visual_embeds_changed_pages_only(tmp_path, capsys):
    from scripts.proof import comment
    api = tmp_path / "api"
    api.mkdir()
    json.dump(_report([_res("a", "exact", 100.0, 100.0)]), open(api / "report.json", "w"))
    vis = tmp_path / "vis"
    vis.mkdir()
    cmp = {"p1/cold": {"verdict": "fixed", "states": {"base": {"state": "error"}, "pr": {"state": "data"}, "ref": {"state": "data"}}, "base_score": 0.0, "pr_score": 100.0},
           "p2/cold": {"verdict": "match", "states": {}, "base_score": 100.0, "pr_score": 100.0}}
    json.dump(cmp, open(vis / "compare.json", "w"))
    assert comment.main(["--api", str(api), "--visual", str(vis), "--image-base", "https://x/pr-visuals/pr-1/"]) == 0
    text = capsys.readouterr().out
    assert "![p1 cold](https://x/pr-visuals/pr-1/p1-cold.png)" in text and "p2-cold.png" not in text
    assert "None." in text
