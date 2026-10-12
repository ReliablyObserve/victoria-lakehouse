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

    def fake_run(state, spec, cells, runs, shift_s=0):
        calls.append((len(cells), runs, shift_s))
        return raw(cell_rows(K, 10, 10, runs=runs))
    monkeypatch.setattr(pr_smoke, "run_cells", fake_run)
    args = type("A", (), {"confirm_runs": 7, "noise": 0.15, "min_delta_ms": 3.0})()
    out = pr_smoke.confirm(first, {}, {}, {K: {"key": K}}, args)
    assert calls == [(1, 7, pr_smoke.measure.SHIFT_S)] and out["cells"][0]["verdict"] == "same" and out["cells"][0]["confirmed"]
    calls.clear()
    quiet = report.summarize(raw(cell_rows(K, 10, 10)), "t")
    assert pr_smoke.confirm(quiet, {}, {}, {K: {"key": K}}, args) is quiet and calls == []


def test_docs_only_pr_renders_skipped_comments_and_exits_zero(tmp_path, monkeypatch):
    monkeypatch.setattr(pr_smoke.selection, "changed_files", lambda b, h: ["docs/a.md"])
    monkeypatch.setattr(pr_smoke, "git", lambda *a: "a" * 40)
    rc = pr_smoke.main(["--out", str(tmp_path), "--base", "x", "--head", "y"])
    assert rc == 0 and "skipped" in (tmp_path / "perf-smoke.md").read_text() and "skipped" in (tmp_path / "api-matrix.md").read_text()


class Recorder:
    """Stands in for the docker-driving parts of stack.py and records what a run did to the project's stack, images and trees."""

    def __init__(self, monkeypatch, tmp_path, running, build_fails=False):
        self.calls = []
        st = pr_smoke.stack
        monkeypatch.setattr(pr_smoke.selection, "changed_files", lambda b, h: ["internal/x/y.go"])
        monkeypatch.setattr(pr_smoke, "git", lambda *a: "a" * 40)
        monkeypatch.setattr(st, "running", lambda: running)
        monkeypatch.setattr(st, "check_block", lambda: None)
        monkeypatch.setattr(st.P, "busy_ports", lambda ports: [])
        monkeypatch.setattr(st, "tree_add", lambda ref, parent, name: self.calls.append(("tree_add", name)) or str(tmp_path / name))
        monkeypatch.setattr(st, "tree_remove", lambda t: self.calls.append(("tree_remove", t)))
        monkeypatch.setattr(st, "cmd_build", lambda a: self.calls.append(("build",)) or (_ for _ in ()).throw(RuntimeError("docker build failed")) if build_fails else self.calls.append(("build",)))
        monkeypatch.setattr(st, "down_stack", lambda: self.calls.append(("down_stack",)))
        monkeypatch.setattr(st, "remove_images", lambda: self.calls.append(("remove_images",)))
        monkeypatch.setattr(pr_smoke, "measure_all", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("stopped for the test")))

    def names(self):
        return [c[0] for c in self.calls]


def test_n1_a_second_run_refuses_a_running_stack_before_building_and_touches_nothing(tmp_path, monkeypatch):
    """Another run's stack: no build (no retag of the lh-ab image tags), no tree, no `compose down`, no image removal, a distinct exit code."""
    rec = Recorder(monkeypatch, tmp_path, running=True)
    rc = pr_smoke.main(["--out", str(tmp_path / "out"), "--base", "x", "--head", "y"])
    assert rc == pr_smoke.stack.EXIT_BUSY == 4 and rec.calls == []
    assert "StackBusy" in (tmp_path / "out" / "perf-smoke.md").read_text()


def test_n1_a_reused_stack_is_never_torn_down_and_a_free_one_is_torn_down_with_its_images_when_the_build_fails(tmp_path, monkeypatch):
    rec = Recorder(monkeypatch, tmp_path / "a", running=True)
    (tmp_path / "a").mkdir()
    assert pr_smoke.main(["--out", str(tmp_path / "out-a"), "--base", "x", "--head", "y", "--reuse-stack"]) == 3
    assert "down_stack" not in rec.names() and "remove_images" not in rec.names()
    (tmp_path / "b").mkdir()
    rec = Recorder(monkeypatch, tmp_path / "b", running=False, build_fails=True)
    assert pr_smoke.main(["--out", str(tmp_path / "out-b"), "--base", "x", "--head", "y"]) == 3
    assert "down_stack" in rec.names() and "remove_images" in rec.names() and rec.names().count("tree_remove") == 2


def test_the_latency_check_covers_all_four_lakehouse_processes(monkeypatch):
    """Lakehouse's own S3 view must have risen on base-logs, base-traces, pr-logs and pr-traces, not on one of them."""
    monkeypatch.setattr(pr_smoke.latency, "set_latency", lambda api, ms: ms)
    monkeypatch.setattr(pr_smoke.latency, "probe", lambda d, p, ms: {"expect_ms": ms, "added_ms": ms, "ok": True})
    monkeypatch.setattr(pr_smoke.stack, "api_url", lambda: "http://x")
    zero = {"base-logs": 2.0, "base-traces": 2.0, "pr-logs": 2.0, "pr-traces": 2.0}
    monkeypatch.setattr(pr_smoke, "lh_means", lambda st: {"base-logs": 52.0, "base-traces": 52.0, "pr-logs": 52.0, "pr-traces": 52.0})
    assert pr_smoke.set_latency(50, {}, zero)["ok"] is True
    monkeypatch.setattr(pr_smoke, "lh_means", lambda st: {"base-logs": 52.0, "base-traces": 52.0, "pr-logs": 52.0, "pr-traces": 3.0})  # one process bypasses the proxy
    res = pr_smoke.set_latency(50, {}, zero)
    assert res["ok"] is False and res["lh_view"]["pr-traces"]["ok"] is False and res["lh_view"]["pr-logs"]["ok"] is True


def test_n2_unstable_cells_are_measured_again_too(monkeypatch):
    rows = cell_rows(K, 10, 10)
    for r in rows:
        if r["target"] == "pr" and r["run"] == 3:
            r.update(valid=False, reason="http 504", status=504, result="")
    first = report.summarize(raw(rows), "t")
    assert first["cells"][0]["verdict"] == "unstable"
    calls = []
    monkeypatch.setattr(pr_smoke, "run_cells", lambda st, sp, cells, runs, shift_s=0: calls.append(len(cells)) or raw(cell_rows(K, 10, 10, runs=runs)))
    args = type("A", (), {"confirm_runs": 7, "noise": 0.15, "min_delta_ms": 3.0})()
    out = pr_smoke.confirm(first, {}, {}, {K: {"key": K}}, args)
    assert calls == [1] and out["cells"][0]["flaky"] and out["flaky"] == [K]
