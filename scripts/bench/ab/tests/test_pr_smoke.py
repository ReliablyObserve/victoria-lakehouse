"""pr_smoke.py: the orchestration pieces that need no docker."""
import pr_smoke
import report
from helpers import cell_rows, raw

K = "control-logs|logs count_total|cold|1h|numeric"


def test_pins_are_read_from_the_makefile_and_the_e2e_compose_file():
    p = pr_smoke.pins()
    assert p["VictoriaLogs"].startswith("v") and p["VictoriaTraces"].startswith("v") and p["RustFS"] and p["toxiproxy"]


def test_select_all_covers_every_set():
    sel = pr_smoke.select_all({"sets": {"a": {"shapes": [{"name": "x"}]}, "b": {"shapes": [{"name": "y"}, {"name": "z"}]}}})
    assert sel["run"] and sel["sets"] == {"a": ["x"], "b": ["y", "z"]}


def test_merge_raw_adds_cpu_keeps_the_peak_rss_and_all_rows():
    a = {"rows": [1], "s3": {"c1": {}}, "process": {"base-logs": {"cpu_s": 1.0, "peak_rss_mb": 100}}, "elapsed_s": 2.0}
    b = {"rows": [2], "s3": {"c2": {}}, "process": {"base-logs": {"cpu_s": 0.5, "peak_rss_mb": 150}}, "elapsed_s": 3.0}
    m = pr_smoke.merge_raw([a, b])
    assert m["rows"] == [1, 2] and set(m["s3"]) == {"c1", "c2"} and m["process"]["base-logs"] == {"cpu_s": 1.5, "peak_rss_mb": 150} and m["elapsed_s"] == 5.0


def test_a_non_zero_latency_profile_runs_only_the_control_smoke_shapes_on_the_flushed_layers():
    def cell(set_, smoke, layer):
        return {"set": set_, "shape": {"smoke": smoke}, "layer": layer}
    cells = [cell("control-logs", True, "cold"), cell("control-logs", False, "cold"), cell("control-logs", True, "buffer"), cell("stats", True, "cold"),
             cell("control-traces", True, "compacted")]
    assert pr_smoke.profile_cells({}, cells) == [cells[0], cells[4]]


def test_confirm_measures_again_only_what_moved(monkeypatch):
    first = report.summarize(raw(cell_rows(K, 10, 25)), "t")
    calls = []

    def fake_run(state, spec, cells, runs):
        calls.append((len(cells), runs))
        return raw(cell_rows(K, 10, 10, runs=runs))
    monkeypatch.setattr(pr_smoke, "run_cells", fake_run)
    args = type("A", (), {"confirm_runs": 7, "noise": 0.15, "min_delta_ms": 3.0})()
    out = pr_smoke.confirm(first, {}, {}, {K: {"key": K}}, args)
    assert calls == [(1, 7)] and out["cells"][0]["verdict"] == "same" and out["cells"][0]["confirmed"]
    calls.clear()
    quiet = report.summarize(raw(cell_rows(K, 10, 10)), "t")
    assert pr_smoke.confirm(quiet, {}, {}, {K: {"key": K}}, args) is quiet and calls == []


def test_docs_only_pr_renders_skipped_comments_and_exits_zero(tmp_path, monkeypatch):
    monkeypatch.setattr(pr_smoke.selection, "changed_files", lambda b, h: ["docs/a.md"])
    monkeypatch.setattr(pr_smoke, "git", lambda *a: "a" * 40)
    rc = pr_smoke.main(["--out", str(tmp_path), "--base", "x", "--head", "y"])
    assert rc == 0 and "skipped" in (tmp_path / "perf-smoke.md").read_text() and "skipped" in (tmp_path / "api-matrix.md").read_text()
