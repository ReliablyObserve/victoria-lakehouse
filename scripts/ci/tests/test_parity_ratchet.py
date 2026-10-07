import io
import json
import os
import tempfile
import unittest
import unittest.mock
from contextlib import redirect_stderr, redirect_stdout

from scripts.ci.parity_ratchet import (
    Allowlist,
    GoTestRun,
    Verdict,
    count_by_action,
    evaluate,
    exact_equivalent,
    lock_failures,
    lock_tests,
    parse_lock_cells,
    main,
    parse_allowlist,
    parse_go_test_json,
    render_summary,
    stale_entries,
    unexpected_failures,
    unexplained_package_failures,
)

PKG = "example.com/parity"


def events(*pairs, package=PKG):
    """Build `go test -json` lines from (test, action) pairs."""
    out = []
    for test, action in pairs:
        out.append(json.dumps({"Action": "run", "Test": test, "Package": package}))
        out.append(json.dumps({"Action": action, "Test": test, "Package": package}))
    return out


def event(action, test=None, package=PKG, output=None):
    e = {"Action": action, "Package": package}
    if test is not None:
        e["Test"] = test
    if output is not None:
        e["Output"] = output
    return json.dumps(e)


def results(**by_test):
    """{(PKG, test): action} from keyword arguments; '__' stands for '/'."""
    return {(PKG, name.replace("__", "/")): action for name, action in by_test.items()}


# Event streams recorded from `go test -json` (go1.26) against small
# packages that time out, panic and exit mid-run, with the per-line output
# events trimmed to the ones the ratchet reads.
TIMEOUT_STREAM = [
    event("start"),
    event("run", "TestFast"),
    event("output", "TestFast", output="=== RUN   TestFast\n"),
    event("pass", "TestFast"),
    event("run", "TestParent"),
    event("run", "TestParent/done"),
    event("pass", "TestParent/done"),
    event("run", "TestParent/hangs"),
    event("output", "TestParent/hangs", output="panic: test timed out after 2s\n"),
    event("output", "TestParent/hangs", output="\trunning tests:\n"),
    event("output", output="FAIL\texample.com/parity\t2.271s\n"),
    event("fail"),
]

GOROUTINE_PANIC_STREAM = [
    event("start"),
    event("run", "TestOK"),
    event("pass", "TestOK"),
    event("run", "TestParent"),
    event("run", "TestParent/crashes"),
    event("output", "TestParent/crashes", output="panic: boom in goroutine\n"),
    event("output", output="FAIL\texample.com/parity\t0.396s\n"),
    event("fail"),
]

# A panic on the test goroutine is reported as a `fail` of the panicking test
# and its parents; the tests after it never start.
TEST_PANIC_STREAM = [
    event("start"),
    event("run", "TestOK"),
    event("pass", "TestOK"),
    event("run", "TestParent"),
    event("run", "TestParent/panics"),
    event("output", "TestParent/panics", output="--- FAIL: TestParent/panics (0.00s)\n"),
    event("fail", "TestParent/panics"),
    event("fail", "TestParent"),
    event(
        "output",
        "TestParent",
        output="panic: boom in test goroutine [recovered, repanicked]\n",
    ),
    event("output", output="FAIL\texample.com/parity\t0.271s\n"),
    event("fail"),
]

OS_EXIT_STREAM = [
    event("start"),
    event("run", "TestOK"),
    event("pass", "TestOK"),
    event("run", "TestExits"),
    event("output", output="FAIL\texample.com/parity\t0.263s\n"),
    event("fail"),
]


class ParseAllowlistTests(unittest.TestCase):
    def test_parses_entries_and_reasons(self):
        al = parse_allowlist(
            "# header\n"
            "TestA  # B1: junk columns\n"
            "TestB/sub # B2: field_names from Parquet only\n"
        )
        self.assertEqual(
            al.entries,
            {"TestA": "B1: junk columns", "TestB/sub": "B2: field_names from Parquet only"},
        )
        self.assertIsNone(al.min_pass)

    def test_parses_min_pass_directive(self):
        al = parse_allowlist("# min-pass: 341\nTestA  # B1: reason\n")
        self.assertEqual(al.min_pass, 341)

    def test_ignores_blank_and_comment_lines(self):
        al = parse_allowlist("\n\n# just a comment\n# fewer than `min-pass` tests\n\n")
        self.assertEqual(al.entries, {})
        self.assertIsNone(al.min_pass)

    def test_rejects_entry_without_reason(self):
        with self.assertRaises(ValueError) as ctx:
            parse_allowlist("TestA\n")
        self.assertIn("no ' # reason'", str(ctx.exception))

    def test_rejects_entry_with_empty_reason(self):
        with self.assertRaises(ValueError):
            parse_allowlist("TestA  #\n")

    def test_rejects_duplicate_entry(self):
        with self.assertRaises(ValueError) as ctx:
            parse_allowlist("TestA # B1: x\nTestA # B1: y\n")
        self.assertIn("duplicate", str(ctx.exception))

    def test_rejects_invalid_min_pass(self):
        with self.assertRaises(ValueError):
            parse_allowlist("# min-pass: many\n")

    def test_rejects_negative_min_pass(self):
        with self.assertRaises(ValueError) as ctx:
            parse_allowlist("# min-pass: -1\n")
        self.assertIn("negative", str(ctx.exception))

    def test_rejects_duplicate_min_pass_directive(self):
        with self.assertRaises(ValueError) as ctx:
            parse_allowlist("# min-pass: 300\nTestA  # B1: x\n# min-pass: 310\n")
        message = str(ctx.exception)
        self.assertIn("line 3", message)
        self.assertIn("duplicate min-pass", message)
        self.assertIn("line 1", message)

    def test_hash_inside_test_path_is_part_of_the_path(self):
        # go test names the second `t.Run("sub", ...)` of a parent "sub#01".
        al = parse_allowlist("TestX/sub#01  # B1: duplicate subtest name\n")
        self.assertEqual(al.entries, {"TestX/sub#01": "B1: duplicate subtest name"})

    def test_hash_inside_reason_stays_in_the_reason(self):
        al = parse_allowlist("TestA  # B1: tracked in issue # 12 and #13\n")
        self.assertEqual(al.entries, {"TestA": "B1: tracked in issue # 12 and #13"})

    def test_tab_separated_reason_is_accepted(self):
        al = parse_allowlist("TestA\t#\tB3: map filter\n")
        self.assertEqual(al.entries, {"TestA": "B3: map filter"})

    def test_hash_without_whitespace_is_not_a_separator(self):
        with self.assertRaises(ValueError) as ctx:
            parse_allowlist("TestA# B1: glued to the path\n")
        self.assertIn("whitespace", str(ctx.exception))


class ParseGoTestJSONTests(unittest.TestCase):
    def test_keeps_only_terminal_actions(self):
        run = parse_go_test_json(events(("TestA", "pass"), ("TestB", "fail")))
        self.assertEqual(run.results, results(TestA="pass", TestB="fail"))

    def test_package_level_events_are_not_test_results(self):
        run = parse_go_test_json([event("fail"), event("pass", package="other")])
        self.assertEqual(run.results, {})
        self.assertEqual(run.failed_packages, {PKG})

    def test_ignores_non_json_noise(self):
        lines = ["Creating parity-tests ... done", ""] + events(("TestA", "skip"))
        self.assertEqual(parse_go_test_json(lines).results, results(TestA="skip"))

    def test_ignores_malformed_json(self):
        lines = ['{"Action": "fail", ', "[1, 2]", *events(("TestA", "pass"))]
        self.assertEqual(parse_go_test_json(lines).results, results(TestA="pass"))

    def test_last_terminal_action_wins(self):
        lines = events(("TestA", "pass")) + events(("TestA", "fail"))
        self.assertEqual(parse_go_test_json(lines).results, results(TestA="fail"))

    def test_results_are_keyed_by_package_and_test(self):
        lines = events(("TestShared", "fail"), package="a") + events(
            ("TestShared", "pass"), package="b"
        )
        run = parse_go_test_json(lines)
        self.assertEqual(
            run.results, {("a", "TestShared"): "fail", ("b", "TestShared"): "pass"}
        )

    def test_pause_and_cont_do_not_finish_a_test(self):
        lines = [
            event("run", "TestP"),
            event("pause", "TestP"),
            event("cont", "TestP"),
        ]
        self.assertEqual(parse_go_test_json(lines).aborted(), [(PKG, "TestP")])

    def test_timeout_leaves_running_tests_aborted(self):
        run = parse_go_test_json(TIMEOUT_STREAM)
        self.assertEqual(run.aborted(), [(PKG, "TestParent"), (PKG, "TestParent/hangs")])
        self.assertEqual(run.results, results(TestFast="pass", TestParent__done="pass"))
        self.assertEqual(
            run.panics, {PKG: ("TestParent/hangs", "panic: test timed out after 2s")}
        )
        self.assertEqual(run.failed_packages, {PKG})

    def test_goroutine_panic_leaves_running_tests_aborted(self):
        run = parse_go_test_json(GOROUTINE_PANIC_STREAM)
        self.assertEqual(
            run.aborted(), [(PKG, "TestParent"), (PKG, "TestParent/crashes")]
        )
        self.assertIn(PKG, run.panics)

    def test_os_exit_leaves_the_test_aborted_without_a_panic(self):
        run = parse_go_test_json(OS_EXIT_STREAM)
        self.assertEqual(run.aborted(), [(PKG, "TestExits")])
        self.assertEqual(run.panics, {})

    def test_panic_on_the_test_goroutine_is_recorded(self):
        run = parse_go_test_json(TEST_PANIC_STREAM)
        self.assertEqual(run.aborted(), [])
        self.assertEqual(
            run.panics[PKG],
            ("TestParent", "panic: boom in test goroutine [recovered, repanicked]"),
        )

    def test_indented_panic_text_is_not_a_crash(self):
        lines = [
            event("run", "TestA"),
            event("output", "TestA", output="    a_test.go:9: panic: in a log line\n"),
            event("pass", "TestA"),
        ]
        self.assertEqual(parse_go_test_json(lines).panics, {})


class UnexpectedFailureTests(unittest.TestCase):
    def test_allowlisted_failure_is_accepted(self):
        self.assertEqual(unexpected_failures(results(TestA="fail"), {"TestA"}), [])

    def test_unlisted_failure_is_reported(self):
        self.assertEqual(
            unexpected_failures(results(TestA="fail"), set()), [(PKG, "TestA")]
        )

    def test_parent_excused_when_all_failing_children_allowlisted(self):
        r = results(TestP="fail", TestP__one="fail", TestP__two="pass")
        self.assertEqual(unexpected_failures(r, {"TestP/one"}), [])

    def test_parent_reported_when_a_child_is_not_allowlisted(self):
        r = results(TestP="fail", TestP__one="fail", TestP__two="fail")
        self.assertEqual(
            unexpected_failures(r, {"TestP/one"}),
            [(PKG, "TestP"), (PKG, "TestP/two")],
        )

    def test_grandparent_chain_is_excused(self):
        r = results(TestP="fail", TestP__mid="fail", TestP__mid__leaf="fail")
        self.assertEqual(unexpected_failures(r, {"TestP/mid/leaf"}), [])

    def test_parent_failing_without_failing_children_is_reported(self):
        # A t.Fatalf in the parent body itself, not in a subtest.
        r = results(TestP="fail", TestP__one="pass")
        self.assertEqual(unexpected_failures(r, set()), [(PKG, "TestP")])

    def test_passing_tests_are_never_reported(self):
        self.assertEqual(
            unexpected_failures(results(TestA="pass", TestB="skip"), set()), []
        )

    def test_children_in_another_package_do_not_excuse_a_parent(self):
        r = {("a", "TestP"): "fail", ("b", "TestP/one"): "fail"}
        self.assertEqual(unexpected_failures(r, {"TestP/one"}), [("a", "TestP")])

    def test_allowlist_entry_covers_the_path_in_every_package(self):
        r = {("a", "TestShared"): "fail", ("b", "TestShared"): "fail"}
        self.assertEqual(unexpected_failures(r, {"TestShared"}), [])


class StaleEntryTests(unittest.TestCase):
    def test_entry_that_still_fails_is_not_stale(self):
        al = Allowlist(entries={"TestA": "B1"})
        self.assertEqual(stale_entries(results(TestA="fail"), al), [])

    def test_entry_that_passes_is_stale(self):
        al = Allowlist(entries={"TestA": "B1"})
        self.assertEqual(stale_entries(results(TestA="pass"), al), [("TestA", "now passes")])

    def test_entry_that_skips_is_stale(self):
        al = Allowlist(entries={"TestA": "B1"})
        self.assertEqual(stale_entries(results(TestA="skip"), al), [("TestA", "now skips")])

    def test_entry_that_vanished_is_stale(self):
        al = Allowlist(entries={"TestGone": "B1"})
        self.assertEqual(
            stale_entries(results(TestA="pass"), al), [("TestGone", "did not run")]
        )

    def test_entry_failing_in_one_package_is_live(self):
        al = Allowlist(entries={"TestShared": "B1"})
        r = {("a", "TestShared"): "pass", ("b", "TestShared"): "fail"}
        self.assertEqual(stale_entries(r, al), [])

    def test_aborted_entry_is_left_to_the_aborted_report(self):
        al = Allowlist(entries={"TestHangs": "B1"})
        self.assertEqual(stale_entries({}, al, aborted=[(PKG, "TestHangs")]), [])


class UnexplainedPackageFailureTests(unittest.TestCase):
    def test_package_failure_explained_by_a_failing_test(self):
        run = GoTestRun(results=results(TestA="fail"), failed_packages={PKG})
        self.assertEqual(unexplained_package_failures(run), [])

    def test_package_failure_explained_by_an_aborted_test(self):
        run = parse_go_test_json(OS_EXIT_STREAM)
        self.assertEqual(unexplained_package_failures(run), [])

    def test_package_failure_with_only_passing_tests_is_reported(self):
        # e.g. TestMain calling os.Exit(1) after m.Run() returned.
        run = GoTestRun(results=results(TestA="pass"), failed_packages={PKG})
        self.assertEqual(unexplained_package_failures(run), [PKG])

    def test_build_failure_without_tests_is_reported(self):
        run = parse_go_test_json([event("start"), event("fail")])
        self.assertEqual(unexplained_package_failures(run), [PKG])


class EvaluateTests(unittest.TestCase):
    def test_clean_run_against_matching_allowlist(self):
        run = GoTestRun(results=results(TestA="fail", TestB="pass", TestC="pass"))
        al = Allowlist(entries={"TestA": "B1: junk columns"}, min_pass=2)
        verdict = evaluate(run, al)
        self.assertFalse(verdict.failed)
        self.assertEqual(verdict, Verdict())

    def test_pass_count_regression_is_reported(self):
        run = GoTestRun(results=results(TestA="fail", TestB="pass"))
        al = Allowlist(entries={"TestA": "B1: junk columns"}, min_pass=2)
        verdict = evaluate(run, al)
        self.assertTrue(verdict.failed)
        self.assertIn("Only 1 tests passed", verdict.pass_regression)

    def test_no_min_pass_means_no_regression_check(self):
        verdict = evaluate(GoTestRun(results=results(TestB="pass")), Allowlist())
        self.assertIsNone(verdict.pass_regression)

    def test_extra_passes_do_not_regress(self):
        run = GoTestRun(
            results=results(TestA="fail", TestB="pass", TestC="pass", TestD="pass")
        )
        verdict = evaluate(run, Allowlist(entries={"TestA": "B1"}, min_pass=2))
        self.assertFalse(verdict.failed)

    def test_timeout_fails_even_when_every_finished_result_is_known(self):
        # Before aborted tests were tracked, this run passed the gate: its only
        # results are two passes and the package-level fail was ignored.
        verdict = evaluate(parse_go_test_json(TIMEOUT_STREAM), Allowlist(min_pass=2))
        self.assertTrue(verdict.failed)
        self.assertEqual(
            verdict.aborted, [(PKG, "TestParent"), (PKG, "TestParent/hangs")]
        )
        self.assertEqual(verdict.unexpected, [])
        self.assertIsNone(verdict.pass_regression)

    def test_os_exit_fails_the_gate(self):
        verdict = evaluate(parse_go_test_json(OS_EXIT_STREAM), Allowlist(min_pass=1))
        self.assertTrue(verdict.failed)
        self.assertEqual(verdict.aborted, [(PKG, "TestExits")])

    def test_aborted_test_is_not_covered_by_its_allowlist_entry(self):
        al = Allowlist(entries={"TestParent/hangs": "B1: known failure"})
        verdict = evaluate(parse_go_test_json(TIMEOUT_STREAM), al)
        self.assertTrue(verdict.failed)
        self.assertIn((PKG, "TestParent/hangs"), verdict.aborted)
        self.assertEqual(verdict.stale, [])

    def test_known_failure_that_panics_fails_the_gate(self):
        al = Allowlist(entries={"TestParent/panics": "B1: known failure"}, min_pass=1)
        verdict = evaluate(parse_go_test_json(TEST_PANIC_STREAM), al)
        self.assertEqual(verdict.unexpected, [])
        self.assertEqual(verdict.aborted, [])
        self.assertTrue(verdict.failed)
        self.assertIn(PKG, verdict.panics)

    def test_package_crash_without_a_failing_test_fails_the_gate(self):
        run = GoTestRun(results=results(TestA="pass"), failed_packages={PKG})
        verdict = evaluate(run, Allowlist(min_pass=1))
        self.assertTrue(verdict.failed)
        self.assertEqual(verdict.crashed_packages, [PKG])

    def test_same_name_in_two_packages_is_counted_twice(self):
        lines = events(("TestShared", "pass"), package="a") + events(
            ("TestShared", "pass"), package="b"
        )
        verdict = evaluate(parse_go_test_json(lines), Allowlist(min_pass=2))
        self.assertFalse(verdict.failed)


class CountByActionTests(unittest.TestCase):
    def test_counts_each_action(self):
        r = results(a="pass", b="pass", c="fail", d="skip")
        self.assertEqual(count_by_action(r), {"pass": 2, "fail": 1, "skip": 1})

    def test_counts_zero_for_missing_actions(self):
        self.assertEqual(count_by_action({}), {"pass": 0, "fail": 0, "skip": 0})


class RenderSummaryTests(unittest.TestCase):
    def test_clean_summary_lists_known_failures(self):
        run = GoTestRun(results=results(TestA="fail", TestB="pass"))
        al = Allowlist(entries={"TestA": "B1: junk columns"}, min_pass=1)
        out = render_summary(run, al, evaluate(run, al))
        self.assertIn("## Parity Test Results", out)
        self.assertIn("| Passed | 1 |", out)
        self.assertIn("| Aborted (started, never finished) | 0 |", out)
        self.assertIn("every allowlist entry is still live", out)
        self.assertIn("B1: junk columns", out)

    def test_summary_lists_unexpected_failures(self):
        run = GoTestRun(results=results(TestX="fail"))
        out = render_summary(run, Allowlist(), evaluate(run, Allowlist()))
        self.assertIn("### Unexpected failures", out)
        self.assertIn("- `TestX`", out)
        self.assertNotIn("every allowlist entry is still live", out)

    def test_summary_lists_stale_entries(self):
        run = GoTestRun(results=results(TestA="pass"))
        al = Allowlist(entries={"TestA": "B1"})
        out = render_summary(run, al, evaluate(run, al))
        self.assertIn("### Stale allowlist entries", out)
        self.assertIn("now passes", out)

    def test_summary_reports_pass_regression(self):
        verdict = Verdict(pass_regression="coverage went backwards")
        out = render_summary(GoTestRun(), Allowlist(), verdict)
        self.assertIn("### Pass-count regression", out)
        self.assertIn("coverage went backwards", out)

    def test_summary_reports_aborted_tests_and_the_panic(self):
        run = parse_go_test_json(TIMEOUT_STREAM)
        out = render_summary(run, Allowlist(), evaluate(run, Allowlist()))
        self.assertIn("### Crashed or aborted", out)
        self.assertIn("| Aborted (started, never finished) | 2 |", out)
        self.assertIn("`TestParent/hangs` — aborted (crash or timeout)", out)
        self.assertIn("panicked while `TestParent/hangs` was running", out)
        self.assertIn("panic: test timed out after 2s", out)

    def test_summary_reports_unexplained_package_failure(self):
        run = GoTestRun(results=results(TestA="pass"), failed_packages={PKG})
        out = render_summary(run, Allowlist(), evaluate(run, Allowlist()))
        self.assertIn(f"package `{PKG}` failed with no failing or aborted test", out)

    def test_names_are_package_qualified_only_when_packages_differ(self):
        run = GoTestRun(results={("a", "TestX"): "fail", ("b", "TestY"): "pass"})
        out = render_summary(run, Allowlist(), evaluate(run, Allowlist()))
        self.assertIn("- `a: TestX`", out)


class MainTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)

    def write(self, name, text):
        path = os.path.join(self.dir.name, name)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
        return path

    def run_main(self, results_lines, allowlist_text, summary=True):
        argv = [
            "--results",
            self.write("results.json", "\n".join(results_lines) + "\n"),
            "--allowlist",
            self.write("known_failures.txt", allowlist_text),
        ]
        summary_path = os.path.join(self.dir.name, "summary.md")
        if summary:
            argv += ["--summary-file", summary_path]
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = main(argv)
        return code, out.getvalue(), err.getvalue(), summary_path

    def test_exit_zero_when_every_failure_is_known(self):
        lines = events(("TestA", "fail"), ("TestB", "pass")) + [event("fail")]
        code, out, _, summary_path = self.run_main(
            lines, "# min-pass: 1\nTestA  # B1: junk columns\n"
        )
        self.assertEqual(code, 0)
        self.assertIn("every allowlist entry is still live", out)
        with open(summary_path, encoding="utf-8") as fh:
            self.assertIn("## Parity Test Results", fh.read())

    def test_exit_one_when_the_suite_timed_out(self):
        code, out, _, _ = self.run_main(TIMEOUT_STREAM, "# min-pass: 2\n")
        self.assertEqual(code, 1)
        self.assertIn("aborted (crash or timeout)", out)

    def test_exit_one_when_nothing_was_parsed(self):
        code, _, err, _ = self.run_main(["not json"], "", summary=False)
        self.assertEqual(code, 1)
        self.assertIn("no test results parsed", err)

    def test_invalid_allowlist_raises(self):
        with self.assertRaises(ValueError):
            self.run_main(events(("TestA", "pass")), "# min-pass: 1\n# min-pass: 2\n")


ROWS = """
- {id: lh.lock.exact, title: a, expect: pass, compare: {type: exact-json}, refs: {tests: [tests/parity/a_test.go#TestLockA, tests/parity/a_test.go]}}
- {id: lh.lock.vwh, title: b, expect: pass, compare: {type: values-with-hits, options: {hits_tolerance: "0"}}, refs: {tests: ["tests/parity/b_test.go#TestLockB/sub", "internal/x/x_test.go#TestUnit"]}}
- {id: lh.lock.pending, title: c, expect: pass, pending: true, compare: {type: exact-json}, refs: {tests: [tests/parity/a_test.go#TestPending]}}
- {id: lh.not.differ, title: d, expect: differ, compare: {type: exact-json}, refs: {tests: [tests/parity/a_test.go#TestDiffer]}}
- {id: lh.not.exact, title: e, expect: pass, compare: {type: values-with-hits, options: {hits_tolerance: "0.5"}}, refs: {tests: [tests/parity/a_test.go#TestLoose]}}
- {id: lh.second, title: f, expect: pass, compare: {type: count}, refs: {tests: [tests/parity/a_test.go#TestLockA]}}
- {id: lh.bare, title: g, expect: pass, compare: {type: trace}, refs: {tests: [tests/parity/bare_test.go, tests/parity/sub/deep_test.go, "tests/parity/sub/deep_test.go#TestDeep"]}}
"""


NONCE = "n0nce-abc123"
HELPER_LINE = 27


def cell_text(test, n, nonce=NONCE, line=HELPER_LINE):
    return f"    lock_cells_test.go:{line}: lock-cells {nonce} {test.split('/')[0]} {n}\n"


def cell_event(test, n, package=PKG, text=None):
    out = text if text is not None else cell_text(test, n)
    return json.dumps({"Action": "output", "Package": package, "Test": test, "Output": out})


class RegistryLockTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.root = self.dir.name
        rows = os.path.join(self.root, "tests", "conformance", "registry", "rows", "lh")
        os.makedirs(rows)
        self.rows = os.path.join(self.root, "tests", "conformance", "registry", "rows")
        with open(os.path.join(rows, "rows.yaml"), "w", encoding="utf-8") as fh:
            fh.write(ROWS)
        with open(os.path.join(rows, "ignored.txt"), "w", encoding="utf-8") as fh:
            fh.write("- {id: x, expect: pass, compare: {type: exact-json}, refs: {tests: [tests/parity/z_test.go#TestIgnored]}}")
        os.makedirs(os.path.join(self.root, "tests", "parity"))
        with open(os.path.join(self.root, "tests", "parity", "bare_test.go"), "w", encoding="utf-8") as fh:
            fh.write("package parity\n\nfunc TestBareOne(t *testing.T) {}\nfunc TestBareTwo(t *testing.T) {}\nfunc helper() {}\nfunc Testify() {}\n")

    def test_every_exact_pass_row_with_a_parity_test_is_a_lock_pending_included(self):
        got = lock_tests(self.rows, self.root)
        self.assertEqual({k: sorted(v) for k, v in got.items()},
                         {"TestLockA": ["lh.lock.exact", "lh.second"], "TestPending": ["lh.lock.pending"]})
        # a bare reference names no test: it is reported instead, once per row and file
        bare = []
        lock_tests(self.rows, self.root, bare)
        self.assertEqual(bare, ["lh.bare -> tests/parity/bare_test.go"])

    def test_a_bare_file_reference_needs_the_repo_root(self):
        self.assertNotIn("TestBareOne", lock_tests(self.rows))

    def test_exact_equivalent_compares(self):
        for c, want in (({"type": "exact-json"}, True), ({"type": "count"}, True), ({"type": "trace"}, True),
                        ({"type": "ndjson-multiset"}, True), ({"type": "status"}, False), ({"type": "schema"}, False),
                        ({"type": "values-with-hits", "options": {"hits_tolerance": "0"}}, True),
                        ({"type": "values-with-hits", "options": {"hits_tolerance": "0.0"}}, True),
                        ({"type": "values-with-hits", "options": {"hits_tolerance": "0.1"}}, False),
                        ({"type": "values-with-hits", "options": {"hits_tolerance": "x"}}, False),
                        ({"type": "values-with-hits"}, False),
                        ({"type": "series", "options": {"rel_tolerance": "0"}}, True),
                        ({"type": "series", "options": {"rel_tolerance": "0.02"}}, False), ({"type": "series"}, False),
                        ("exact-json", False), (None, False)):
            self.assertEqual(exact_equivalent(c), want, c)

    def test_registry_files_must_be_one_document_with_real_booleans(self):
        rows = os.path.join(self.rows, "lh", "rows.yaml")
        with open(rows, "a", encoding="utf-8") as fh:
            fh.write("---\n- {id: second, expect: pass}\n")
        with self.assertRaises(ValueError):
            lock_tests(self.rows, self.root)
        with open(rows, "w", encoding="utf-8") as fh:
            fh.write("- {id: yes-row, expect: pass, pending: yes, compare: {type: exact-json}, refs: {tests: [tests/parity/a_test.go#T]}}\n")
        with self.assertRaises(ValueError):
            lock_tests(self.rows, self.root)

    def test_lock_failures(self):
        locks = {"TestLockA": ["lh.lock.exact"], "TestLockC": ["lh.c"]}
        floors = {"TestLockA": 10, "TestLockC": 1, "TestOnlyFloor": 3}

        def run(results, cells):
            r = GoTestRun(results=results)
            r.cells = cells
            return r

        ok = run({(PKG, "TestLockA"): "pass", (PKG, "TestLockC"): "pass", (PKG, "TestOnlyFloor"): "pass"},
                 {(PKG, "TestLockA"): 10, (PKG, "TestLockC"): 4, (PKG, "TestOnlyFloor"): 3})
        self.assertEqual(lock_failures(ok, locks, floors), [])
        # X3: an early return passes with no cells
        early = run({(PKG, "TestLockA"): "pass", (PKG, "TestLockC"): "pass", (PKG, "TestOnlyFloor"): "pass"},
                    {(PKG, "TestLockC"): 4, (PKG, "TestOnlyFloor"): 3})
        self.assertIn("compared 0 cells, the floor in lock_cells.txt is 10", lock_failures(early, locks, floors)[0])
        # one cell short
        short = run(ok.results, {(PKG, "TestLockA"): 9, (PKG, "TestLockC"): 1, (PKG, "TestOnlyFloor"): 3})
        self.assertIn("compared 9 cells", lock_failures(short, locks, floors)[0])
        # X4: skipped, failed, missing (TestMain os.Exit(0), renamed file, windows suffix)
        skipped = run({**ok.results, (PKG, "TestLockA"): "skip"}, ok.cells)
        self.assertIn("was skipped", lock_failures(skipped, locks, floors)[0])
        failed = run({**ok.results, (PKG, "TestLockA"): "fail"}, ok.cells)
        self.assertIn("failed", lock_failures(failed, locks, floors)[0])
        gone = run({(PKG, "TestLockC"): "pass", (PKG, "TestOnlyFloor"): "pass"}, ok.cells)
        self.assertIn("did not report a result", lock_failures(gone, locks, floors)[0])
        # a test that is only in the baseline is held too
        self.assertIn("TestOnlyFloor did not report", lock_failures(run({(PKG, "TestLockA"): "pass", (PKG, "TestLockC"): "pass"}, ok.cells), locks, floors)[0])
        # a subtest result is not the top-level test
        sub = run({(PKG, "TestLockA/sub"): "pass", (PKG, "TestLockC"): "pass", (PKG, "TestOnlyFloor"): "pass"}, ok.cells)
        self.assertIn("TestLockA did not report", lock_failures(sub, locks, floors)[0])
        # a name in two packages: one failing result is enough
        both = run({("a", "TestLockA"): "pass", ("b", "TestLockA"): "fail", (PKG, "TestLockC"): "pass", (PKG, "TestOnlyFloor"): "pass"}, ok.cells)
        self.assertIn("failed", lock_failures(both, locks, floors)[0])
        # no floor for a registry lock: nothing to compare against (the ratchet's main refuses it for named refs)
        self.assertEqual(lock_failures(run({(PKG, "TestLockA"): "pass"}, {}), {"TestLockA": ["r"]}, {}), [])

    def test_cells_are_summed_and_only_the_helpers_line_counts(self):
        stream = [
            json.dumps({"Action": "run", "Package": PKG, "Test": "TestLockA"}),
            cell_event("TestLockA", 3),
            json.dumps({"Action": "run", "Package": PKG, "Test": "TestLockA/sub"}),
            cell_event("TestLockA/sub", 4),
            json.dumps({"Action": "pass", "Package": PKG, "Test": "TestLockA/sub"}),
            json.dumps({"Action": "pass", "Package": PKG, "Test": "TestLockA"}),
            # forged: printed by the test file itself
            cell_event("TestLockA", 0, text="    sort_all_columns_parity_test.go:60: lock-cells TestLockA 999\n"),
            # forged: bare text
            cell_event("TestLockA", 0, text="lock-cells TestLockA 999\n"),
            # forged: reported for another test
            cell_event("TestOther", 0, text=cell_text("TestLockA", 999)),
            # forged: a different file whose name merely ends the same way
            cell_event("TestLockA", 0, text="    zz_lock_cells_test.go:27: lock-cells " + NONCE + " TestLockA 999\n"),
            # forged: the right file with the wrong line (the helper's t.Logf is on line 27)
            cell_event("TestLockA", 0, text=cell_text("TestLockA", 999, line=28)),
            # forged: the right line with a guessed, default or missing nonce
            cell_event("TestLockA", 0, text=cell_text("TestLockA", 999, nonce="guess")),
            cell_event("TestLockA", 0, text=cell_text("TestLockA", 999, nonce="-")),
            cell_event("TestLockA", 0, text="    lock_cells_test.go:27: lock-cells TestLockA 999\n"),
        ]
        run = parse_go_test_json(stream, NONCE, HELPER_LINE)
        self.assertEqual(run.cells, {(PKG, "TestLockA"): 7})

    def test_parse_lock_cells(self):
        self.assertEqual(parse_lock_cells("# c\nTestA  12  # why\n\nTestB 3\n"), {"TestA": 12, "TestB": 3})
        for bad in ("TestA\n", "TestA 0\n", "TestA x\n", "notatest 3\n", "TestA 3\nTestA 4\n", "TestA 3 4\n"):
            with self.assertRaises(ValueError, msg=bad):
                parse_lock_cells(bad)

    def test_evaluate_fails_the_run_and_the_summary_names_the_lock(self):
        run = GoTestRun(results={(PKG, "TestLockA"): "skip", (PKG, "TestOther"): "pass"})
        verdict = evaluate(run, Allowlist(), {"TestLockA": ["lh.lock.exact"]}, {"TestLockA": 5})
        self.assertTrue(verdict.failed)
        out = render_summary(run, Allowlist(), verdict)
        self.assertIn("Registry locks that did not pass", out)
        self.assertIn("TestLockA was skipped (lock for lh.lock.exact)", out)
        self.assertFalse(evaluate(run, Allowlist()).failed, "without a registry or floors the check is off")

    def test_the_summary_tabulates_observed_cells_against_the_floors(self):
        run = GoTestRun(results={(PKG, "TestLockA"): "pass"})
        run.cells = {(PKG, "TestLockA"): 12}
        out = render_summary(run, Allowlist(), evaluate(run, Allowlist()), {"TestLockA": 10, "TestGone": 3})
        self.assertIn("| TestLockA | 12 | 10 |", out)
        self.assertIn("| TestGone | 0 | 3 |", out)
        self.assertNotIn("Lock cells", render_summary(run, Allowlist(), evaluate(run, Allowlist())))

    def write(self, name, text):
        path = os.path.join(self.root, name)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
        return path

    HELPER = "package parity\n" + "\n" * (HELPER_LINE - 2) + '\tt.Logf("lock-cells %s %s %d", nonce, root, n)\n'

    def run_main(self, events_lines, floors, extra=(), nonce=NONCE, helper=None):
        results = self.write("r.json", "\n".join(events_lines) + "\n")
        allow = self.write("a.txt", "# min-pass: 1\n")
        args = ["--results", results, "--allowlist", allow, *extra]
        if floors is not None:
            args += ["--lock-cells", self.write("lock_cells.txt", floors)]
            self.write("lock_cells_test.go", self.HELPER if helper is None else helper)
        out, err = io.StringIO(), io.StringIO()
        env = {"PARITY_LOCK_NONCE": nonce} if nonce is not None else {}
        with unittest.mock.patch.dict(os.environ, env, clear=False), redirect_stdout(out), redirect_stderr(err):
            if nonce is None:
                os.environ.pop("PARITY_LOCK_NONCE", None)
            code = main(args)
        return code, out.getvalue(), err.getvalue()

    def test_a_lock_row_naming_a_test_file_is_refused_by_main(self):
        good = [json.dumps({"Action": "pass", "Package": PKG, "Test": "TestLockA"})]
        code, _, err = self.run_main(good, "TestLockA 1\n", ["--registry", self.rows])
        self.assertEqual(code, 1)
        self.assertIn("name a whole tests/parity test file instead of its tests", err)
        self.assertIn("lh.bare -> tests/parity/bare_test.go", err)

    def test_a_bare_reference_to_a_helper_file_is_ignored(self):
        with open(os.path.join(self.root, "tests", "parity", "helpers_test.go"), "w", encoding="utf-8") as fh:
            fh.write("package parity\n\nfunc helper() {}\n")
        bare = []
        lock_tests(self.rows, self.root, bare)
        with open(os.path.join(self.rows, "lh", "rows.yaml"), "a", encoding="utf-8") as fh:
            fh.write("- {id: lh.helpers, title: h, expect: pass, compare: {type: exact-json}, refs: {tests: [tests/parity/helpers_test.go]}}\n")
        bare2 = []
        lock_tests(self.rows, self.root, bare2)
        self.assertEqual(sorted(bare2), sorted(bare), "a file without tests names no test")

    def use_named_rows(self):
        rows = os.path.join(self.rows, "lh", "rows.yaml")
        with open(rows, encoding="utf-8") as fh:
            text = fh.read()
        text = text.replace("tests/parity/bare_test.go, tests/parity/sub/deep_test.go", "tests/parity/bare_test.go#TestBareOne, tests/parity/bare_test.go#TestBareTwo")
        with open(rows, "w", encoding="utf-8") as fh:
            fh.write(text)

    def test_main_with_floors_and_registry(self):
        self.use_named_rows()
        good = [
            json.dumps({"Action": "run", "Package": PKG, "Test": "TestLockA"}), cell_event("TestLockA", 12),
            json.dumps({"Action": "pass", "Package": PKG, "Test": "TestLockA"}),
            json.dumps({"Action": "run", "Package": PKG, "Test": "TestPending"}), cell_event("TestPending", 2),
            json.dumps({"Action": "pass", "Package": PKG, "Test": "TestPending"}),
        ]
        for t in ("TestBareOne", "TestBareTwo"):
            good += [json.dumps({"Action": "run", "Package": PKG, "Test": t}), json.dumps({"Action": "pass", "Package": PKG, "Test": t})]
        floors = "TestLockA 10\nTestPending 2\nTestBareOne 2\nTestBareTwo 2\n"
        good = good + [cell_event("TestBareOne", 2), cell_event("TestBareTwo", 2)]
        code, out, err = self.run_main(good, floors, ["--registry", self.rows])
        self.assertEqual((code, err), (0, ""), out)
        # the early return: no cells
        early = [l for l in good if f"lock-cells {NONCE} TestLockA" not in l]
        code, out, _ = self.run_main(early, floors, ["--registry", self.rows])
        self.assertEqual(code, 1)
        self.assertIn("compared 0 cells", out)
        # a named registry lock without a floor is refused outright
        code, _, err = self.run_main(good, "TestLockA 10\nTestBareOne 2\nTestBareTwo 2\n", ["--registry", self.rows])
        self.assertEqual(code, 1)
        self.assertIn("without a floor in lock_cells.txt: TestPending", err)
        # a named lock that did not run
        no_bare = [l for l in good if "TestBareTwo" not in l and "TestBareTwo" not in l]
        code, out, _ = self.run_main(no_bare, floors, ["--registry", self.rows])
        self.assertEqual(code, 1)
        self.assertIn("TestBareTwo did not report a result", out)
        # the run's nonce is mandatory when floors are, and the helper must hold its Logf
        code, _, err = self.run_main(good, floors, ["--registry", self.rows], nonce=None)
        self.assertEqual(code, 1)
        self.assertIn("PARITY_LOCK_NONCE is not set", err)
        code, _, err = self.run_main(good, floors, ["--registry", self.rows], helper="package parity\n")
        self.assertEqual(code, 1)
        self.assertIn("no t.Logf", err)
        # a line printed with another nonce counts for nothing
        code, out, _ = self.run_main(good, floors, ["--registry", self.rows], nonce="a-different-run")
        self.assertEqual(code, 1)
        self.assertIn("compared 0 cells", out)
        # the registry option is optional
        self.assertEqual(self.run_main(early, floors)[0], 1, "floors alone still hold TestLockA")
        self.assertEqual(self.run_main(early, None)[0], 0)


if __name__ == "__main__":
    unittest.main()
