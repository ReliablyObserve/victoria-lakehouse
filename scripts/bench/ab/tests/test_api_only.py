"""pr_smoke.py --api-only: the API matrix alone, judged by the base registry, with the exit codes 0/1/2/3/4 of the check."""
import json

import abreg
import pr_smoke
from test_api_triage import aa, judged


class Fake:
    """Replaces the docker-driving parts of pr_smoke and records what the run did."""

    def __init__(self, monkeypatch, tmp_path, matrix=None, running=False, base_rows=None, boom=None):
        self.calls = []
        self.matrix = matrix
        st = pr_smoke.stack
        monkeypatch.setattr(pr_smoke, "git", lambda *a: "a" * 40)
        monkeypatch.setattr(st, "running", lambda: running)
        monkeypatch.setattr(st, "check_block", lambda: None)
        monkeypatch.setattr(st.P, "busy_ports", lambda ports: [])
        monkeypatch.setattr(st, "tree_add", lambda ref, parent, name: self.calls.append(("tree_add", name)) or str(tmp_path / name))
        monkeypatch.setattr(st, "tree_remove", lambda t: self.calls.append(("tree_remove", t)))
        monkeypatch.setattr(st, "cmd_build", lambda a: self.calls.append(("build",)))
        monkeypatch.setattr(st, "down_stack", lambda: self.calls.append(("down_stack",)))
        monkeypatch.setattr(st, "remove_images", lambda: self.calls.append(("remove_images",)))
        monkeypatch.setattr(pr_smoke.abreg, "registry_at", lambda ref, root=abreg.ROOT: self.calls.append(("registry_at", ref)) or (base_rows or [], {}))
        monkeypatch.setattr(pr_smoke.abreg, "load_rows", lambda *a, **k: base_rows or [])
        monkeypatch.setattr(pr_smoke, "prepare_stack", lambda a, out, meta: self.calls.append(("prepare_stack",)) or (None, {"H": "x", "layers": {}, "project": "p", "cold": {}}))
        if boom:
            monkeypatch.setattr(pr_smoke.api_matrix, "run_all", lambda *a, **k: (_ for _ in ()).throw(boom))
        else:
            monkeypatch.setattr(pr_smoke.api_matrix, "run_all", lambda state, rows, rounds, **k: self.calls.append(("run_all", rounds, k.get("confirm_rounds"), len(k.get("judge") or []))) or matrix)

    def names(self):
        return [c[0] for c in self.calls]


def run(tmp_path, *extra):
    return pr_smoke.main(["--api-only", "--out", str(tmp_path / "out"), "--base", "x", "--head", "y", *extra])


def passing():
    m, rows = aa()
    m["results"][0:0] = []
    for r in m["results"]:
        if r["id"] == "vl.pipe.extract.basic":
            r.update(samples=["regressed", "improved", "same"], verdict="nondeterministic")
    return judged(m, rows), rows


def test_a_passing_run_writes_only_the_api_comment_and_exits_zero(tmp_path, monkeypatch):
    m, rows = passing()
    fake = Fake(monkeypatch, tmp_path, matrix=m, base_rows=rows)
    assert run(tmp_path, "--rounds", "3", "--api-confirm-rounds", "5") == 0
    out = tmp_path / "out"
    assert (out / "api-matrix.md").read_text().startswith("<!-- lh-api-matrix -->\n")
    assert json.loads((out / "api-verdict.json").read_text())["exit"] == 0
    assert (out / "known-api.json").exists() and (out / "meta.json").exists() and (out / "api-matrix.json").exists()
    assert not (out / "perf-smoke.md").exists() and not (out / "perf-verdict.json").exists() and not (out / "summary-0ms.json").exists()
    assert ("run_all", 3, 5, len(rows)) in fake.calls  # the base rows judge; the confirming rounds are passed on
    assert ("registry_at", "a" * 40) in fake.calls
    assert fake.names().index("registry_at") < fake.names().index("build")  # a base that cannot be read stops the run before anything is built
    assert "down_stack" in fake.names() and "remove_images" in fake.names() and fake.names().count("tree_remove") == 2


def test_the_perf_verdict_never_feeds_the_exit_code_of_api_only(tmp_path, monkeypatch):
    m, rows = passing()
    Fake(monkeypatch, tmp_path, matrix=m, base_rows=rows)
    assert run(tmp_path) == 0
    assert not (tmp_path / "out" / "perf-verdict.json").exists()


def test_a_new_regression_exits_one(tmp_path, monkeypatch):
    m, rows = aa()
    for r in m["results"]:
        if r["id"] == "vt.select.logsql_hits.basic":
            r.update(verdict="regressed", samples=["regressed"] * 8, pr_score=5.0)
    Fake(monkeypatch, tmp_path, matrix=judged(m, rows), base_rows=rows)
    assert run(tmp_path) == 1
    assert "### RED" in (tmp_path / "out" / "api-matrix.md").read_text()


def test_a_run_that_dies_exits_three_with_a_did_not_complete_comment_and_tears_down(tmp_path, monkeypatch):
    fake = Fake(monkeypatch, tmp_path, boom=RuntimeError("docker exploded"), base_rows=[])
    assert run(tmp_path) == 3
    text = (tmp_path / "out" / "api-matrix.md").read_text()
    assert "did not complete" in text and "docker exploded" in text and "selected shape" not in text
    assert json.loads((tmp_path / "out" / "api-verdict.json").read_text())["exit"] == 3
    assert not (tmp_path / "out" / "perf-smoke.md").exists()
    assert "down_stack" in fake.names() and "remove_images" in fake.names()


def test_an_unreadable_base_registry_stops_before_the_build_and_never_falls_back(tmp_path, monkeypatch):
    fake = Fake(monkeypatch, tmp_path, base_rows=[])
    monkeypatch.setattr(pr_smoke.abreg, "registry_at", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("cannot read the registry at 'x'")))
    assert run(tmp_path) == 3
    assert "build" not in fake.names() and "run_all" not in fake.names()
    assert "cannot read the registry" in (tmp_path / "out" / "api-matrix.md").read_text()


def test_a_busy_project_exits_four_and_touches_nothing(tmp_path, monkeypatch):
    fake = Fake(monkeypatch, tmp_path, running=True, base_rows=[])
    assert run(tmp_path) == 4
    assert fake.names() == ["registry_at"] or fake.names() == []  # no build, no tree, no compose down, no image removal
    assert "down_stack" not in fake.names() and "remove_images" not in fake.names()


def test_the_tree_registry_is_available_for_a_local_run_and_is_named_in_the_meta(tmp_path, monkeypatch):
    m, rows = passing()
    fake = Fake(monkeypatch, tmp_path, matrix=m, base_rows=rows)
    assert run(tmp_path, "--registry-ref", "tree") == 0
    assert "registry_at" not in fake.names()
    assert json.loads((tmp_path / "out" / "meta.json").read_text())["registry"] == "working tree"


def test_the_meta_names_the_registry_that_judged(tmp_path, monkeypatch):
    m, rows = passing()
    Fake(monkeypatch, tmp_path, matrix=m, base_rows=rows)
    assert run(tmp_path) == 0
    assert json.loads((tmp_path / "out" / "meta.json").read_text())["registry"] == "base " + "a" * 10
