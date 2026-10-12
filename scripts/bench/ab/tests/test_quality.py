"""quality.py: static counts from a tree, cover profiles, and the gates."""

import quality


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


def test_cover_percent_from_a_profile(tmp_path):
    p = tmp_path / "c.out"
    p.write_text("mode: set\na.go:1.1,2.2 4 1\na.go:3.1,4.2 6 0\n")
    assert quality.cover_percent(str(p)) == 40.0
    assert quality.cover_percent(str(tmp_path / "missing")) is None


def snap(tests=10, cov=92.0, expect=None, rows=100, lint=0, unexpected=None):
    return {"go": {"root": {"tests": tests, "fuzz": 1, "fuzz_targets": []}, "lakehouse-traces": {"tests": 5, "fuzz": 0, "fuzz_targets": []}},
            "registry": {"rows": rows, "by_expect": expect or {"pass": 90, "differ": 10}, "features": 5, "feature_status": {"shipped": 5}, "pending_rows": 0,
                         "known_differences": {"rows": 10, "with_issue": 4, "without_issue": 6, "without_issue_gap_id_only": 1}},
            "coverage": {"root": cov, "lakehouse-traces": 91.0}, "lint": lint,
            "parity": {"counts": {"pass": 5}, "known_failures": 2, "known": [], "unexpected": unexpected or []}}


def test_gates_catch_each_regression_and_pass_a_clean_pr():
    assert quality.gates(snap(), snap()) == []
    assert any("below the 90% floor" in f for f in quality.gates(snap(), snap(cov=88.0)))
    assert any("fell from" in f for f in quality.gates(snap(cov=95.0), snap(cov=94.0)))
    assert quality.gates(snap(cov=95.0), snap(cov=94.9)) == []
    assert any("fewer Go tests" in f for f in quality.gates(snap(tests=10), snap(tests=9)))
    assert any("weakened row" in f for f in quality.gates(snap(), snap(expect={"pass": 89, "differ": 11})))
    assert quality.gates(snap(), snap(expect={"pass": 89, "differ": 11}, rows=101)) == []  # a new row that records a known difference
    assert any("outside the allowlist" in f for f in quality.gates(snap(), snap(unexpected=["p:TestX"])))
    assert any("new lint" in f for f in quality.gates(snap(lint=1), snap(lint=3)))


def test_report_says_not_collected_instead_of_zero():
    b, p = snap(), snap()
    p["coverage"], p["lint"], p["parity"] = {}, None, None
    text, v = quality.render(b, p)
    assert text.splitlines()[0] == quality.MARKER and "not collected" in text and v["exit"] == 0
    assert "Parity suite**: not collected" in text
    text, v = quality.render(snap(), snap(tests=9))
    assert v["exit"] == 1 and "RED" in text
