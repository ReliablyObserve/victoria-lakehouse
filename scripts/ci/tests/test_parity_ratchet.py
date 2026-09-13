import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

from scripts.ci.parity_ratchet import (
    Allowlist,
    GoTestRun,
    Verdict,
    count_by_action,
    evaluate,
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


if __name__ == "__main__":
    unittest.main()
