"""quality.py: static counts from a tree, the gates, the rendered comment (regression, no change, missing base), and the CI exclusions."""

import json
import os
import re
import subprocess

import pytest
import yaml

import quality
import quality_bundle as qb

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))


def tree(tmp_path, tests=3, fuzz=1):
    (tmp_path / "internal").mkdir()
    body = "package x\n" + "".join(f"func TestA{i}(t *testing.T) {{}}\n" for i in range(tests)) + "".join(f"func FuzzB{i}(f *testing.F) {{}}\n" for i in range(fuzz))
    (tmp_path / "internal" / "a_test.go").write_text(body)
    (tmp_path / "lakehouse-traces" / "internal").mkdir(parents=True)
    (tmp_path / "lakehouse-traces" / "internal" / "t_test.go").write_text("func TestT(t *testing.T) {}\n")
    (tmp_path / "deps" / "x").mkdir(parents=True)
    (tmp_path / "deps" / "x" / "v_test.go").write_text("func TestVendored(t *testing.T) {}\n")
    (tmp_path / ".claude" / "worktrees" / "w").mkdir(parents=True)
    (tmp_path / ".claude" / "worktrees" / "w" / "w_test.go").write_text("func TestWorktree(t *testing.T) {}\n")
    return str(tmp_path)


def test_go_counts_are_per_module_and_skip_deps_and_nested_worktrees(tmp_path):
    c = quality.go_counts(tree(tmp_path))
    assert c["root"]["tests"] == 3 and c["root"]["fuzz"] == 1 and c["lakehouse-traces"]["tests"] == 1
    assert c["root"]["fuzz_targets"] == ["internal:FuzzB0"]


def snap(tests=10, cov=None, expect=None, rows=100, lint=None, unexpected=None, pending=7, timing=None, parity=True):
    s = {"go": {"root": {"tests": tests, "fuzz": 1, "test_files": 1, "fuzz_targets": []}, "lakehouse-traces": {"tests": 5, "fuzz": 0, "test_files": 1, "fuzz_targets": []}},
         "registry": {"rows": rows, "by_expect": expect or {"pass": 90, "differ": 10}, "features": 5, "feature_status": {"shipped": 5}, "pending_rows": pending,
                      "known_differences": {"rows": 10, "with_issue": 4, "without_issue": 6, "without_issue_gap_id_only": 1}},
         "coverage": {"root": {"internal/a": 92.0, "internal/b": 95.0}} if cov is None else cov, "timing": timing or {}, "tests_failed": {}, "lint": lint,
         "parity": {"source": "run", "counts": {"pass": 5}, "by_signal": {}, "known_failures": 2, "known": [], "unexpected": unexpected or [], "aborted": 0, "min_pass": 5} if parity else None}
    return s


def test_gates_catch_each_regression_and_pass_a_clean_pr():
    assert quality.gates(snap(), snap()) == []
    assert any("below the 90% floor" in f for f in quality.gates(snap(), snap(cov={"root": {"internal/a": 88.0}})))
    assert any("fell from" in f for f in quality.gates(snap(cov={"root": {"internal/a": 95.0}}), snap(cov={"root": {"internal/a": 94.0}})))
    assert quality.gates(snap(cov={"root": {"internal/a": 95.0}}), snap(cov={"root": {"internal/a": 94.9}})) == []
    assert any("fewer Go tests" in f for f in quality.gates(snap(tests=10), snap(tests=9)))
    assert any("weakened row" in f for f in quality.gates(snap(), snap(expect={"pass": 89, "differ": 11})))
    assert quality.gates(snap(), snap(expect={"pass": 89, "differ": 11}, rows=101)) == []  # a new row that records a known difference
    assert any("outside the allowlist" in f for f in quality.gates(snap(), snap(unexpected=["p:TestX"])))
    assert any("new lint findings" in f for f in quality.gates(snap(), snap(lint={"root": {"new": 3, "findings": []}})))
    assert quality.gates(snap(), snap(lint={"root": {"new": 0, "findings": []}})) == []


def test_floor_follows_the_ci_coverage_step_exclusions():
    low = {"root": {"internal/testutil/x": 10.0, "internal/testutil": 10.0, "cmd/lakehouse-logs": 10.0}, "lakehouse-traces": {"internal/selectapi": 10.0, "internal/vtstorage_adapter": 10.0}}
    assert quality.gates(snap(), snap(cov=low)) == []  # excluded packages and packages outside internal/ are not held to the floor
    assert any("lakehouse-traces/internal/query" in f for f in quality.gates(snap(), snap(cov={"lakehouse-traces": {"internal/query": 10.0}})))


def test_a_package_new_in_the_pr_is_held_to_the_floor_but_has_no_drop():
    base = snap(cov={"root": {}})
    assert quality.gates(base, snap(cov={"root": {"internal/new": 91.0}})) == []
    assert quality.gates(base, snap(cov={"root": {"internal/new": 50.0}}))


def test_report_regression_lists_only_the_rows_that_moved():
    base = snap(cov={"root": {"internal/a": 95.0, "internal/b": 95.0, "internal/c": 95.0}})
    pr = snap(cov={"root": {"internal/a": 93.0, "internal/b": 95.0, "internal/c": 95.0}})
    text, v = quality.render(base, pr, {"root": {"measured": ["internal/a", "internal/b", "internal/c"], "left_out": []}}, None, None)
    assert text.splitlines()[0] == quality.MARKER and v["exit"] == 1 and "RED" in text
    assert "1 changed, 2 unchanged" in text and "`root/internal/a`" in text and "`root/internal/b`" not in text and "**dropped**" in text


def test_report_no_change_is_green_and_collapses_everything():
    text, v = quality.render(snap(), snap(), {"root": {"measured": ["internal/a", "internal/b"], "left_out": []}}, {"runs_seen": 3, "reran_runs": 0, "items": []}, None)
    assert v == {"failed": False, "exit": 0, "gates": []}
    assert "GREEN" in text and "0 changed, 2 unchanged" in text and "unchanged." in text
    assert "| package | base | PR |" not in text and "| | base | PR | change |" not in text
    assert "A job can show up here only after a re-run" in text


def test_report_missing_base_says_new_package_and_not_collected_never_zero():
    base = snap(cov={}, parity=False)
    pr = snap(cov={"root": {"internal/new": 93.5}})
    text, v = quality.render(base, pr, {"root": {"measured": ["internal/new"], "left_out": []}}, None, None)
    assert "new package" in text and "| - | 93.5% |" in text and v["exit"] == 0
    assert "**Lint**: not collected." in text and "**Flaky-test watch**: not collected." in text
    empty = snap(cov={})
    text, _ = quality.render(empty, empty, {"root": {"measured": [], "left_out": []}}, None, None)
    assert "**Coverage**: not collected (no Go package changed)." in text
    text, _ = quality.render(empty, empty, None, None, None)
    assert "**Coverage**: not collected." in text


def test_report_registry_changes_are_listed_and_pending_is_counted():
    text, _ = quality.render(snap(pending=7), snap(pending=9, rows=101), None, None, None)
    assert "| pending rows | 7 | 9 | +2 |" in text and "| rows | 100 | 101 | +1 |" in text


def test_report_untrusted_strings_cannot_mention_link_or_break_the_table():
    evil = "@everyone [x](http://evil.example) <img src=x onerror=1> www.evil.example #1 | `x"
    pr = snap(lint={"root": {"new": 1, "findings": [{"file": evil, "line": 1, "linter": evil, "text": evil}]}})
    pr["tests_failed"] = {"root": [evil]}
    pr["parity"]["known"] = [{"test": evil, "reason": evil, "issues": ["victoria-lakehouse#12"]}]
    flaky = {"runs_seen": 1, "reran_runs": 1, "items": [{"workflow": evil, "job": evil, "failed_attempt": 1, "passed_attempt": 2, "tests": [evil]}]}
    text, _ = quality.render(snap(), pr, {"root": {"measured": [evil], "left_out": [evil]}}, flaky, [evil])
    link = "[victoria-lakehouse#12](https://github.com/ReliablyObserve/victoria-lakehouse/issues/12)"
    body = re.sub(r"`[^`]*`", "", text.replace(link, ""))  # code spans render literally; the one link is ours
    for bad in ("@everyone", "<img", "](http", "www.evil", "http://evil"):
        assert bad not in body, bad
    assert link in text
    assert all(not ln.startswith("|") for ln in text.split("**New lint")[1].split("**Flaky")[0].splitlines())  # nothing in the lint list can start a table row


def test_report_is_capped_with_a_note_and_keeps_the_marker():
    pr = snap(lint={"root": {"new": 40, "findings": [{"file": f"f{i}.go", "line": i, "linter": "x", "text": "y" * 200} for i in range(40)]}})
    pr["timing"] = {"root": {f"internal/p{i}": {"seconds": 1.0, "status": "pass"} for i in range(100)}}
    changed = {"root": {"measured": [], "left_out": [f"internal/{'z' * 150}{i}" for i in range(500)]}}
    big = qb.cap_body("x\n" * 40000)
    assert len(big) <= qb.BODY_CAP and "cut to fit the size limit" in big
    text, _ = quality.render(snap(), pr, changed, None, None)
    assert len(text) <= qb.BODY_CAP and text.startswith(quality.MARKER)


def test_parity_static_and_run_summaries(tmp_path):
    (tmp_path / "tests" / "parity").mkdir(parents=True)
    (tmp_path / "tests" / "parity" / "known_failures.txt").write_text(
        "# floor\n#min-pass: 3\nTestParity_Filters/x  # tracked in ReliablyObserve/victoria-lakehouse#99 and nothing else\n")
    st = quality.parity_static(str(tmp_path))
    assert st["source"] == "static" and st["known_failures"] == 1 and st["known"][0]["issues"] == ["victoria-lakehouse#99"] and st["counts"] == {}
    results = tmp_path / "parity-results.json"
    results.write_text("\n".join(json.dumps(e) for e in (
        {"Action": "pass", "Package": "p", "Test": "TestParity_Filters/a"},
        {"Action": "pass", "Package": "p", "Test": "TestParity_Filters/b"},
        {"Action": "pass", "Package": "p", "Test": "TestParity_Traces_Jaeger/a"},
        {"Action": "skip", "Package": "p", "Test": "TestParity_Traces_Jaeger/b"},
        {"Action": "pass", "Package": "p", "Test": "TestHarness_JudgeCounts"},
    )) + "\n")
    parity = quality.parity_summary(str(results), str(tmp_path))
    assert parity["source"] == "run"
    assert parity["by_signal"]["logs"] == {"pass": 2, "fail": 0, "skip": 0}
    assert parity["by_signal"]["traces"] == {"pass": 1, "fail": 0, "skip": 1}
    assert parity["by_signal"]["unattributed"] == {"pass": 1, "fail": 0, "skip": 0}
    p = snap()
    p["parity"] = parity
    text, _ = quality.render(snap(), p)
    assert "| logs | 2 | 0 | 0 |" in text and "| traces | 1 | 0 | 1 |" in text and "| unattributed | 1 | 0 | 0 |" in text


def test_dot_directories_are_skipped_whatever_their_name(tmp_path):
    root = tree(tmp_path)
    (tmp_path / ".cache" / "x").mkdir(parents=True)
    (tmp_path / ".cache" / "x" / "c_test.go").write_text("func TestCached(t *testing.T) {}\n")
    assert quality.go_counts(root)["root"]["tests"] == 3


def test_collect_reads_gotest_and_lint_files(tmp_path):
    (tmp_path / "t").mkdir()
    t = tree(tmp_path / "t")
    (tmp_path / "t" / "tests" / "parity").mkdir(parents=True)
    gt = tmp_path / "go.json"
    gt.write_text("\n".join(json.dumps(e) for e in (
        {"Action": "output", "Package": "m/internal/a", "Output": "coverage: 91.5% of statements\n"},
        {"Action": "pass", "Package": "m/internal/a", "Elapsed": 2.5},
        {"Action": "fail", "Package": "m/internal/b", "Test": "TestBoom"},
        {"Action": "fail", "Package": "m/internal/b", "Elapsed": 1.0},
    )) + "\n")
    lint = tmp_path / "lint.json"
    lint.write_text(json.dumps({"Issues": [{"FromLinter": "errcheck", "Text": "unchecked", "Pos": {"Filename": "a.go", "Line": 4}}]}))
    s = quality.collect(t, {"root": str(gt)}, {"root": "m"}, "", {"root": str(lint)})
    assert s["coverage"] == {"root": {"internal/a": 91.5}}
    assert s["timing"]["root"]["internal/a"] == {"seconds": 2.5, "status": "pass"} and s["timing"]["root"]["internal/b"]["status"] == "fail"
    assert s["tests_failed"] == {"root": ["internal/b:TestBoom"]}
    assert s["lint"] == {"root": {"new": 1, "findings": [{"file": "a.go", "line": 4, "linter": "errcheck", "text": "unchecked"}]}}
    assert quality.lint_total(s) == 1 and s["parity"]["source"] == "static"


def test_cover_exclusions_equal_the_ci_coverage_steps():
    """COVER_EXCLUDE must be the `-x` arguments the CI test jobs pass to scripts/ci/check_coverage.sh."""
    wf = yaml.safe_load(open(os.path.join(ROOT, ".github", "workflows", "ci.yaml"), encoding="utf-8"))
    found = {}
    for job, module in (("test-logs", "root"), ("test-traces", "lakehouse-traces")):
        runs = [s["run"] for s in wf["jobs"][job]["steps"] if "check_coverage.sh" in s.get("run", "")]
        assert len(runs) == 1, job
        found[module] = tuple(re.findall(r"-x\s+(?:'([^']+)'|(\S+))", runs[0]))
        found[module] = tuple(a or b for a, b in found[module])
        assert "-t 90" in runs[0] and quality.COVER_FLOOR == 90.0
    assert {m: set(v) for m, v in found.items()} == {m: set(v) for m, v in quality.COVER_EXCLUDE.items()}


@pytest.mark.parametrize("module,rel,want", [("root", "internal/a", True), ("root", "internal/testutil", False), ("root", "internal/testutil/deep", False),
                                             ("root", "internal/storage/testutil", True), ("root", "cmd/x", False),
                                             ("lakehouse-traces", "internal/selectapi", False), ("lakehouse-traces", "internal/selectapi/sub", True)])
def test_floor_applies_matches_check_coverage_sh(module, rel, want, tmp_path):
    """The Python floor and scripts/ci/check_coverage.sh agree on which packages are held to the floor."""
    assert quality.floor_applies(module, rel) is want
    args = [x for pat in quality.COVER_EXCLUDE[module] for x in ("-x", pat)]
    if not rel.startswith("internal/"):
        return  # the shell script only reads packages below /internal/
    line = f"ok  \tgithub.com/x/y/{rel}\t0.1s\tcoverage: 10.0% of statements\n"
    rc = subprocess.run(["bash", os.path.join(ROOT, "scripts", "ci", "check_coverage.sh"), "-t", "90", *args], input=line, text=True, capture_output=True).returncode
    assert (rc == 1) is want


def test_packages_prints_nothing_for_an_empty_group_so_the_workflow_never_measures_the_module_root(tmp_path, capsys):
    """A blank line in the list became `./` in the workflow: the module root package, which has no Go files in the root module."""
    files = tmp_path / "files.txt"
    files.write_text("internal/a/x.go\n")
    for module, group, want in (("root", "short", "internal/a\n"), ("root", "full", ""), ("lakehouse-traces", "short", ""), ("lakehouse-traces", "full", "")):
        assert quality.main(["packages", "--files-from", str(files), "--module", module, "--group", group]) == 0
        assert capsys.readouterr().out == want, (module, group)
    assert quality.main(["packages", "--files-from", str(files), "--module", "lakehouse-traces"]) == 0
    assert capsys.readouterr().out == ""
    assert quality.main(["packages", "--files-from", str(files), "--module", "root", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == {"measured": ["internal/a"], "left_out": [], "dependency_change": False}
