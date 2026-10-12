"""selection.py: changed files -> registry items -> shape sets, and the --check gate."""
import copy

import abreg
import selection

SPEC = {
    "ranges": {"1h": 3600}, "layers": {"cold": {}},
    "areas": {"internal/compaction/": ["compaction"]},
    "sets": {
        "control-logs": {"layers": ["cold"], "ranges": ["1h"], "shapes": [
            {"name": "c1", "signal": "logs", "kind": "scalar", "smoke": True, "covers": ["lh.feature.q"]},
            {"name": "c2", "signal": "logs", "kind": "scalar", "covers": ["lh.feature.q"]}]},
        "control-traces": {"layers": ["cold"], "ranges": ["1h"], "shapes": [
            {"name": "t1", "signal": "traces", "kind": "scalar", "smoke": True, "covers": ["lh.feature.q"]}]},
        "enum": {"layers": ["cold"], "ranges": ["1h"], "shapes": [
            {"name": "e1", "signal": "logs", "kind": "values", "covers": ["vl.row.e", "lh.feature.e"]}]},
        "compaction": {"layers": ["cold"], "ranges": ["1h"], "shapes": [
            {"name": "k1", "signal": "logs", "kind": "scalar", "covers": ["vl.row.k"]}]},
    },
}
ROWS = [{"id": "vl.row.e", "refs": {"tests": ["internal/storage/enum/x_test.go#TestX"]}},
        {"id": "vl.row.k", "refs": {}}, {"id": "vl.row.other", "refs": {"tests": ["internal/other/y_test.go#TestY"]}}]
FEATS = [{"id": "lh.feature.q", "tests": ["internal/query/q_test.go#TestQ"], "rows": []},
         {"id": "lh.feature.e", "tests": ["internal/storage/enum/e_test.go#TestE"], "rows": ["vl.row.other"]}]


def sel(files, **kw):
    return selection.select(files, SPEC, selection.build_index(ROWS, FEATS), **kw)


def test_a_change_in_a_package_selects_the_shapes_whose_items_are_tested_there_plus_the_smoke_subset():
    r = sel(["internal/storage/enum/reader.go"])
    assert r["sets"] == {"control-logs": ["c1"], "control-traces": ["t1"], "enum": ["e1"]}
    assert "internal/storage/enum/reader.go" in r["why"]["enum/e1"]
    assert r["why"]["control-logs/c1"] == ["control smoke"]


def test_one_hop_through_the_rows_a_feature_lists():
    r = sel(["internal/other/y.go"])  # vl.row.other is listed by lh.feature.e, which e1 covers
    assert "enum" in r["sets"]


def test_a_query_package_change_selects_the_control_shapes_it_is_tested_in():
    r = sel(["internal/query/q.go"])
    assert r["sets"]["control-logs"] == ["c1", "c2"] and r["sets"]["control-traces"] == ["t1"]


def test_area_table_reaches_shapes_with_no_test_package():
    assert "compaction" in sel(["internal/compaction/planner.go"])["sets"]


def test_runtime_files_select_the_whole_control_sets():
    r = sel(["go.mod"])
    assert r["sets"] == {"control-logs": ["c1", "c2"], "control-traces": ["t1"]}
    assert sel(["Dockerfile.logs"])["sets"].keys() == {"control-logs", "control-traces"}


def test_harness_changes_run_the_smoke_subset():
    r = sel(["scripts/bench/ab/measure.py"])
    assert r["sets"] == {"control-logs": ["c1"], "control-traces": ["t1"]}


def test_docs_tests_ci_charts_and_registry_text_select_nothing():
    r = sel(["docs/x.md", "internal/storage/enum/reader_test.go", ".github/workflows/ci.yaml", "charts/a.yaml", "tests/conformance/registry/rows/vl/a.yaml",
             "README.md", "website/x.html", "CHANGELOG.md"])
    assert r["run"] is False and r["sets"] == {} and r["relevant"] == [] and len(r["ignored"]) == 8


def test_deps_trees_are_not_code():
    assert selection.classify("deps/VictoriaLogs/app/x.go") == "ignored"
    assert selection.classify("lakehouse-traces/deps/VictoriaTraces/app/x.go") == "ignored"
    assert selection.classify("lakehouse-traces/internal/storage/parquets3/x.go") == "code"
    assert selection.classify("patches/vl-logs/a.patch") == "code"


def test_check_passes_on_the_repository():
    assert selection.check(abreg.load_json(abreg.SHAPES), abreg.load_rows(), abreg.load_features()) == []


def problems(mutate):
    spec = copy.deepcopy(SPEC)
    mutate(spec)
    rows = ROWS + [{"id": "vl.row.x", "refs": {}}]
    return selection.check(spec, rows, FEATS)


def test_check_catches_each_kind_of_defect():
    assert any("unknown registry id" in p for p in problems(lambda s: s["sets"]["enum"]["shapes"][0].update(covers=["nope"])))
    assert any("covers nothing" in p for p in problems(lambda s: s["sets"]["enum"]["shapes"][0].update(covers=[])))
    assert any("smoke outside" in p for p in problems(lambda s: s["sets"]["enum"]["shapes"][0].update(smoke=True)))
    assert any("unknown kind" in p for p in problems(lambda s: s["sets"]["enum"]["shapes"][0].update(kind="mystery")))
    assert any("unknown layer" in p for p in problems(lambda s: s["sets"]["enum"]["shapes"][0].update(layers=["moon"])))
    assert any("unknown range" in p for p in problems(lambda s: s["sets"]["enum"]["shapes"][0].update(ranges=["3y"])))
    assert any("defined twice" in p for p in problems(lambda s: s["sets"]["enum"]["shapes"].append(dict(s["sets"]["enum"]["shapes"][0]))))
    assert any("must mark at least one" in p for p in problems(lambda s: s["sets"]["control-logs"]["shapes"][0].update(smoke=False)))
    assert any("control set 'control-traces' is missing" in p for p in problems(lambda s: s["sets"].pop("control-traces")))
    assert any("unknown set" in p for p in problems(lambda s: s["areas"].update({"x/": ["ghost"]})))
    assert any("cannot be selected" in p for p in problems(lambda s: s["sets"]["enum"]["shapes"][0].update(covers=["vl.row.x"])))
    assert any("unknown pick" in p for p in problems(lambda s: s["sets"]["enum"]["shapes"][0].update(pick="ghost")))
    assert any("no signal" in p for p in problems(lambda s: s["sets"]["enum"]["shapes"][0].pop("signal")))
