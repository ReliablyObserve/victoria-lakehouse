"""Visual proof: panel states and transitions, frame conversion, page comparison, montage and the comment-less report."""
import json
import os

import pytest
from PIL import Image

from scripts.proof.jsonio import dump_json, read_text
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
        dump_json(os.path.join(d, s + ".json"), c)


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
    assert any("not asked" in n for n in notes) and any("extra request" in n for n in notes)
    assert fr.facets["requests|asked"] == 50.0 and fr.facets["requests|extra"] == 100.0 * 2 / 3


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
    rc_ok = compare.main([str(tmp_path)])
    assert rc_ok == 0
    assert os.path.exists(os.path.join(str(tmp_path), "compare.json"))
    md = read_text(os.path.join(str(tmp_path), "compare.md"))
    assert "| `m/1h` | 100 | 100 |" in md
    write_page(str(tmp_path), "z", "1h", {"base": ok, "pr": page(FULL, {"banners": ["Bad Gateway"]}), "ref": ok})
    rc_bad = compare.main([str(tmp_path)])
    assert rc_bad == 1
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
    dump_json(str(api / "report.json"), rep)
    exp = tmp_path / "e.json"
    dump_json(str(exp), {"c.": "known B1: injected schema fields"})
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
    dump_json(str(api / "report.json"), _report([_res("a", "exact", 100.0, 100.0)]))
    vis = tmp_path / "vis"
    vis.mkdir()
    cmp = {"p1/cold": {"verdict": "fixed", "states": {"base": {"state": "error"}, "pr": {"state": "data"}, "ref": {"state": "data"}}, "base_score": 0.0, "pr_score": 100.0},
           "p2/cold": {"verdict": "match", "states": {}, "base_score": 100.0, "pr_score": 100.0}}
    dump_json(str(vis / "compare.json"), cmp)
    rc = comment.main(["--api", str(api), "--visual", str(vis), "--image-base", "https://x/pr-visuals/pr-1/"])
    text = capsys.readouterr().out
    assert rc == 0
    assert "![p1 cold](https://x/pr-visuals/pr-1/p1-cold.png)" in text and "p2-cold.png" not in text
    assert "None." in text and "1 page captures match" in text and "`p2/cold`" not in text


def test_montage_makes_a_zoom_montage_from_clip_images(tmp_path):
    d = tmp_path / "shots" / "pg" / "cold"
    d.mkdir(parents=True)
    for n in ("base", "pr", "ref"):
        Image.new("RGB", (800, 600), (255, 255, 255)).save(d / f"{n}.png")
        Image.new("RGB", (780, 480), (255, 255, 255) if n != "pr" else (0, 0, 0)).save(d / f"{n}-clip.png")
    scores = montage.make(str(tmp_path))
    assert scores == {"pg-cold": 0.0, "pg-cold-zoom": 1.0}
    assert (tmp_path / "montage" / "pg-cold-zoom.png").exists()


def test_vmui_query_rows_are_scored_with_key_order(tmp_path):
    def body(keys):
        return "\n".join(json.dumps({k: "v" if k != "_time" else "t1" for k in keys}) for _ in range(1))

    def cap_q(keys):
        r = rec(body(keys), url="/select/logsql/query?query=%2A", request=None)
        return cap([r])
    write_page(str(tmp_path), "sort", "cold", {"base": cap_q(["_time", "a", "b"]), "pr": cap_q(["_time", "b", "a"]), "ref": cap_q(["_time", "a", "b"])})
    r = compare.compare_out(str(tmp_path))["sort/cold"]
    assert r["base_score"] == 100.0 and r["pr_score"] == 0.0 and r["verdict"] == "regression"
    assert frames.resource_meta("/select/logsql/query?x=1") == {"key_order": True} and frames.resource_meta("/select/logsql/hits") == {}


# ---- publish: what is accepted, and where the token goes ----

def test_publish_accepts_png_files_only_by_their_signature(tmp_path):
    from scripts.proof.visual import publish
    good = tmp_path / "good"
    good.mkdir()
    (good / "page-cold.png").write_bytes(publish.PNG_SIGNATURE + b"data")
    assert publish.montages(str(good)) == ["page-cold.png"]
    bad = tmp_path / "bad"
    bad.mkdir()
    (bad / "page-cold.png").write_bytes(b"<html>not an image</html>")
    with pytest.raises(SystemExit) as e:
        publish.montages(str(bad))
    assert "not a PNG" in str(e.value)
    (bad / "page-cold.png").write_bytes(publish.PNG_SIGNATURE[:4])  # a truncated signature is not one either
    with pytest.raises(SystemExit):
        publish.montages(str(bad))


def test_publish_token_goes_through_the_environment_never_the_command_line(monkeypatch, tmp_path):
    from scripts.proof.visual import publish
    seen = {}

    def fake_run(cmd, check, capture_output, text, env):
        seen["cmd"], seen["env"] = cmd, env
        return type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()
    monkeypatch.setattr(publish.subprocess, "run", fake_run)
    publish.git(str(tmp_path), "fetch", auth="c2VjcmV0")
    assert "c2VjcmV0" not in " ".join(seen["cmd"]) and "extraheader" not in " ".join(seen["cmd"])
    assert seen["env"]["GIT_CONFIG_COUNT"] == "1" and seen["env"]["GIT_CONFIG_KEY_0"] == "http.extraheader"
    assert seen["env"]["GIT_CONFIG_VALUE_0"] == "AUTHORIZATION: basic c2VjcmV0"
    publish.git(str(tmp_path), "fetch")
    assert seen["env"] is None


# ---- behaviours a mutation run showed unpinned ----

def test_montage_tiles_are_base_then_pr_then_reference(tmp_path):
    d = tmp_path / "shots" / "pg" / "cold"
    d.mkdir(parents=True)
    colours = {"base": (220, 20, 20), "pr": (20, 200, 20), "ref": (20, 20, 220)}
    for n, c in colours.items():
        Image.new("RGB", (800, 600), c).save(d / f"{n}.png")
    montage.make(str(tmp_path))
    m = Image.open(tmp_path / "montage" / "pg-cold.png").convert("RGB")
    third = m.width // 3
    centres = [m.getpixel((third * i + third // 2, m.height // 2)) for i in range(3)]
    assert centres[0][0] > 150 and centres[1][1] > 150 and centres[2][2] > 150
    assert [montage.TITLES[i][0] for i in range(3)] == ["base", "pr", "ref"]


def test_a_page_where_every_side_is_empty_is_vacuous_and_never_a_match(tmp_path):
    empty = cap([rec(EMPTY_FRAMES, request={"queries": [{"refId": "A", "queryType": "range", "expr": "x"}]})])
    write_page(str(tmp_path), "e", "cold", {"base": empty, "pr": empty, "ref": empty})
    r = compare.compare_out(str(tmp_path))["e/cold"]
    assert r["verdict"] == "vacuous" and any("every side is empty" in w for w in r["warnings"])
    # no request at all is no proof of data either
    none = cap([])
    write_page(str(tmp_path), "n", "cold", {"base": none, "pr": none, "ref": none})
    r = compare.compare_out(str(tmp_path))["n/cold"]
    assert r["states"]["ref"]["state"] == EMPTY and r["verdict"] == "vacuous"
    assert "no backend request" in r["states"]["ref"]["warning"]


def test_a_target_panel_that_says_no_data_on_every_side_is_vacuous(tmp_path):
    ui = {"noData": 1, "noDataPanels": ["http_method"], "banners": [], "panelErrors": 0, "jaegerErrors": 0}
    other = {**ui, "noData": 0, "noDataPanels": []}
    for s_, u in (("base", ui), ("pr", ui), ("ref", ui)):
        pass
    write_page(str(tmp_path), "f", "cold", {s_: page(FULL, ui) for s_ in ("base", "pr", "ref")})
    plain = compare.compare_out(str(tmp_path))["f/cold"]
    assert plain["verdict"] == "match" and any("No data" in w for w in plain["warnings"])
    targeted = compare.compare_out(str(tmp_path), targets={"f": ["http_method"]})["f/cold"]
    assert targeted["verdict"] == "vacuous" and any("http_method" in w for w in targeted["warnings"])
    # a panel that shows data on one side is not vacuous
    write_page(str(tmp_path), "g", "cold", {"base": page(FULL, ui), "pr": page(FULL, ui), "ref": page(FULL, other)})
    assert compare.compare_out(str(tmp_path), targets={"g": ["http_method"]})["g/cold"]["verdict"] != "vacuous"


def test_vacuous_pages_are_listed_in_the_comment_and_warnings_are_shown(tmp_path):
    from scripts.proof import comment
    cmp_ = {"v/cold": {"verdict": "vacuous", "states": {"base": {"state": "empty"}, "pr": {"state": "empty"}, "ref": {"state": "empty"}},
                       "base_score": 100.0, "pr_score": 100.0, "warnings": ["every side is empty: x"]},
            "m/cold": {"verdict": "match", "states": {}, "base_score": 100.0, "pr_score": 100.0, "warnings": ["pr: the DOM shows 'No data'"]}}
    text = "\n".join(comment.visual_section(cmp_, "PR", None, {}))
    assert "`v/cold`" in text and "vacuous" in text and "every side is empty: x" in text
    assert "`m/cold` |" not in text and "1 of them have a warning" in text


def test_exact_count_of_the_comment_needs_both_sides_exact(tmp_path):
    from scripts.proof import comment
    rep = _report([_res("a", "exact", 100.0, 100.0), _res("b", "exact", 50.0, 100.0, {"x": 50.0}, {"x": 100.0})])
    lines, _ = comment.api_section(rep, "PR", {})
    assert any(l.startswith("1 requests match the reference exactly on both sides") for l in lines)


def test_the_seed_sentence_of_the_comment_comes_from_the_report():
    from scripts.proof import comment
    assert "skipped" in comment.seed_text({"seed_equality": {}})
    rep = {"seed_equality": {"logs/numeric/cold": {"counts": {"ref": 5, "base": 5, "pr": 5}}, "logs/alias/cold": {"counts": {"ref": 9, "base": 9, "pr": 9}}}}
    t = comment.seed_text(rep)
    assert "2 signal/tenant-form/layer cells" in t and "5 to 9 rows" in t and "same hash" in t


def test_base_and_pr_that_differ_while_neither_matches_the_reference_is_an_unexpected_change(tmp_path):
    a = page([("POST", 5), ("GET", 3)])   # POST hits wrong, GET right
    b = page([("POST", 4), ("GET", 5)])   # POST right, GET hits wrong: the same score against the reference
    write_page(str(tmp_path), "x", "1h", {"base": a, "pr": b, "ref": page(FULL)})
    r = compare.compare_out(str(tmp_path))["x/1h"]
    assert r["base_score"] == r["pr_score"] and r["verdict"] == "unexpected-change"
    same = {"base": a, "pr": a, "ref": page(FULL)}
    write_page(str(tmp_path), "y", "1h", same)
    assert compare.compare_out(str(tmp_path))["y/1h"]["verdict"] == "same"


def test_two_queries_with_one_refid_are_two_questions_and_tag_order_is_not_a_difference():
    q1 = {"refId": "A", "queryType": "range", "expr": "one"}
    q2 = {"refId": "A", "queryType": "range", "expr": "two"}
    keys = frames.answers_of([rec(OK_FRAMES, request={"queries": [q1]}), rec(OK_FRAMES, request={"queries": [q2]})])
    assert len(keys) == 2
    tags1 = [{"key": "a", "value": "1"}, {"key": "b", "value": "2"}]
    assert frames.canon(tags1) == frames.canon(list(reversed(tags1)))
    assert frames.canon({"x": [3, 1, 2]}) == {"x": [1, 2, 3]}


def test_every_non_200_status_is_an_error_state_and_known_issues_do_not_decide_a_page():
    assert states.data_state([rec({"message": "nope"}, status=404)])["state"] == ERROR
    assert states.data_state([rec({}, status=401)])["kind"] == "request"
    known = states.data_state([rec("not found", status=404, url="/select/buildinfo"), rec(OK_FRAMES)])
    assert known["state"] == DATA and known["known"] == ["/select/buildinfo: HTTP 404 (#463)"]
    # ... but the same failure on any other path still decides
    assert states.data_state([rec("not found", status=404, url="/select/other"), rec(OK_FRAMES)])["state"] == ERROR
    cap_ = cap([rec("not found", status=404, url="/select/buildinfo"), rec(OK_FRAMES)])
    st = states.panel_state(cap_)
    assert st["state"] == DATA and st["known"]
