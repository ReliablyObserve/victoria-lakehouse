"""quality_inputs.py: go test -json, the changed packages, golangci-lint JSON and the flaky-test watch, on fixtures."""

import json

import quality_inputs as qi

MOD = "github.com/ReliablyObserve/victoria-lakehouse"


def ev(**kw):
    return json.dumps(kw)


def test_parse_gotest_reads_the_cover_line_time_status_and_failures():
    lines = [
        "not json at all", "",
        ev(Action="output", Package=f"{MOD}/internal/a", Output="coverage: 91.5% of statements\n"),
        ev(Action="output", Package=f"{MOD}/internal/a", Test="TestX", Output="coverage: 12.0% of statements\n"),  # a test's own output is not the package total
        ev(Action="pass", Package=f"{MOD}/internal/a", Elapsed=2.5),
        ev(Action="output", Package=f"{MOD}/internal/nostmt", Output="coverage: [no statements]\n"),
        ev(Action="pass", Package=f"{MOD}/internal/nostmt", Elapsed=0.1),
        ev(Action="fail", Package=f"{MOD}/internal/b", Test="TestBoom"),
        ev(Action="fail", Package=f"{MOD}/internal/b", Elapsed=1.0),
        ev(Action="pass", Package=MOD, Elapsed=0.2),
    ]
    got = qi.parse_gotest(lines, MOD)
    assert got["coverage"] == {"internal/a": 91.5}
    assert got["seconds"]["internal/a"] == 2.5 and got["status"]["internal/b"] == "fail" and got["status"]["."] == "pass"
    assert got["failed_tests"] == ["internal/b:TestBoom"]


def test_rel_pkg_without_a_module_path_keeps_the_import_path():
    assert qi.rel_pkg("a/b/internal/c", "") == "a/b/internal/c" and qi.rel_pkg(MOD, MOD) == "." and qi.rel_pkg("other/x", MOD) == "other/x"


def test_changed_packages_per_module_with_a_cap():
    files = ["internal/a/x.go", "internal/a/y_test.go", "internal/b/z.go", "internal/b/testdata/q.go", "cmd/lakehouse-logs/main.go", "main.go", "tests/e2e/x_test.go",
             "internal/a/notes.md", "deps/VictoriaLogs/x.go", "lakehouse-traces/internal/t/t.go", "lakehouse-traces/main.go", "lakehouse-traces/deps/q/q.go", "scripts/ci/gen/gen.go"]
    assert qi.changed_packages(files, "root") == (["cmd/lakehouse-logs", "internal/a", "internal/b"], [])
    assert qi.changed_packages(files, "lakehouse-traces") == ([".", "internal/t"], [])
    many = [f"internal/p{i:02d}/x.go" for i in range(20)] + ["internal/p05/y.go"]
    measured, left = qi.changed_packages(many, "root")
    assert len(measured) == qi.MAX_PACKAGES and len(left) == 20 - qi.MAX_PACKAGES and "internal/p05" in measured  # the busiest first


def test_has_go_files_and_short_flags(tmp_path):
    (tmp_path / "internal" / "a").mkdir(parents=True)
    (tmp_path / "internal" / "a" / "a.go").write_text("package a\n")
    (tmp_path / "internal" / "e").mkdir()
    assert qi.has_go_files(str(tmp_path), "root", "internal/a") and not qi.has_go_files(str(tmp_path), "root", "internal/e")
    assert not qi.has_go_files(str(tmp_path), "root", "internal/gone")
    assert qi.go_test_flags("internal/compaction") == [] and qi.go_test_flags("internal/storage/parquets3") == ["-short"]


def test_parse_lint_counts_all_and_keeps_a_bounded_list():
    issues = [{"FromLinter": "errcheck", "Text": "t" * 500, "Pos": {"Filename": f"f{i}.go", "Line": i}} for i in range(60)]
    got = qi.parse_lint(json.dumps({"Issues": issues}))
    assert got["new"] == 60 and len(got["findings"]) == qi.LINT_MAX_FINDINGS and len(got["findings"][0]["text"]) == 200
    assert qi.parse_lint(json.dumps({"Issues": None})) == {"new": 0, "findings": []} and qi.parse_lint("") == {"new": 0, "findings": []}


class FakeGh:
    """gh api answers by path; counts the calls."""

    def __init__(self, runs, jobs, logs=None):
        self.runs, self.jobs, self.logs, self.calls = runs, jobs, logs or {}, []

    def __call__(self, path, jq=None):
        self.calls.append(path)
        if "/actions/runs?head_sha=" in path:
            return "\n".join(json.dumps(r) for r in self.runs)
        if "/attempts/" in path:
            run, attempt = path.split("/runs/")[1].split("/attempts/")[0], int(path.split("/attempts/")[1].split("/")[0])
            return "\n".join(json.dumps(j) for j in self.jobs[(int(run), attempt)])
        if "/jobs/" in path and path.endswith("/logs"):
            jid = int(path.split("/jobs/")[1].split("/")[0])
            if jid not in self.logs:
                raise RuntimeError("gone")
            return self.logs[jid]
        raise AssertionError(path)


def test_flaky_failed_then_passed_on_a_rerun_is_listed_with_test_names():
    runs = [{"id": 1, "name": "CI", "run_attempt": 2, "event": "pull_request"}, {"id": 2, "name": "Parity Tests", "run_attempt": 1, "event": "pull_request"}]
    jobs = {(1, 1): [{"id": 11, "name": "test-logs", "conclusion": "failure"}, {"id": 12, "name": "lint-logs", "conclusion": "success"},
                     {"id": 13, "name": "build", "conclusion": "failure"}, {"id": 14, "name": "cancelled-one", "conclusion": "cancelled"}],
            (1, 2): [{"id": 21, "name": "test-logs", "conclusion": "success"}, {"id": 22, "name": "lint-logs", "conclusion": "success"},
                     {"id": 23, "name": "build", "conclusion": "failure"}, {"id": 24, "name": "cancelled-one", "conclusion": "success"}]}
    gh = FakeGh(runs, jobs, {11: "=== RUN\n    --- FAIL: TestFlaky (0.1s)\n--- FAIL: TestFlaky/sub (0.1s)\nFAIL\n"})
    got = qi.collect_flaky("o/r", "a" * 40, gh)
    assert got["runs_seen"] == 2 and got["reran_runs"] == 1
    assert got["items"] == [{"workflow": "CI", "job": "test-logs", "failed_attempt": 1, "passed_attempt": 2, "tests": ["TestFlaky", "TestFlaky/sub"]}]  # build failed twice (a real failure), cancelled is not a failure
    assert not any("/attempts/" in c and c.startswith("repos/o/r/actions/runs/2/") for c in gh.calls)  # a run never re-run costs one call, no job listing


def test_flaky_with_no_rerun_and_with_unreadable_logs():
    got = qi.collect_flaky("o/r", "a" * 40, FakeGh([{"id": 1, "name": "CI", "run_attempt": 1, "event": "pull_request"}], {}))
    assert got == {"runs_seen": 1, "reran_runs": 0, "items": []}
    jobs = {(1, 1): [{"id": 11, "name": "j", "conclusion": "timed_out"}], (1, 2): [{"id": 21, "name": "j", "conclusion": "success"}]}
    got = qi.collect_flaky("o/r", "a" * 40, FakeGh([{"id": 1, "name": "CI", "run_attempt": 2, "event": "x"}], jobs))
    assert got["items"][0]["tests"] == [] and got["items"][0]["job"] == "j"


def test_failed_test_names_are_bounded():
    log = "".join(f"--- FAIL: TestN{i} (0.0s)\n" for i in range(50))
    assert len(qi.failed_test_names(log)) == 10
