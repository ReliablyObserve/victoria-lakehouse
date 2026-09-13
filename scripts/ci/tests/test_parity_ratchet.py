import json
import unittest

from scripts.ci.parity_ratchet import (
    Allowlist,
    count_by_action,
    evaluate,
    parse_allowlist,
    parse_go_test_json,
    render_summary,
    stale_entries,
    unexpected_failures,
)


def events(*pairs):
    """Build `go test -json` lines from (test, action) pairs."""
    out = []
    for test, action in pairs:
        out.append(json.dumps({"Action": "run", "Test": test, "Package": "p"}))
        out.append(json.dumps({"Action": action, "Test": test, "Package": "p"}))
    return out


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
        al = parse_allowlist("\n\n# just a comment\n\n")
        self.assertEqual(al.entries, {})

    def test_rejects_entry_without_reason(self):
        with self.assertRaises(ValueError) as ctx:
            parse_allowlist("TestA\n")
        self.assertIn("no '# reason'", str(ctx.exception))

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


class ParseGoTestJSONTests(unittest.TestCase):
    def test_keeps_only_terminal_actions(self):
        results = parse_go_test_json(events(("TestA", "pass"), ("TestB", "fail")))
        self.assertEqual(results, {"TestA": "pass", "TestB": "fail"})

    def test_ignores_package_level_events(self):
        lines = [json.dumps({"Action": "fail", "Package": "p"})]
        self.assertEqual(parse_go_test_json(lines), {})

    def test_ignores_non_json_noise(self):
        lines = ["Creating parity-tests ... done", ""] + events(("TestA", "skip"))
        self.assertEqual(parse_go_test_json(lines), {"TestA": "skip"})

    def test_ignores_malformed_json(self):
        lines = ['{"Action": "fail", ', *events(("TestA", "pass"))]
        self.assertEqual(parse_go_test_json(lines), {"TestA": "pass"})

    def test_last_terminal_action_wins(self):
        lines = events(("TestA", "pass")) + events(("TestA", "fail"))
        self.assertEqual(parse_go_test_json(lines), {"TestA": "fail"})


class UnexpectedFailureTests(unittest.TestCase):
    def test_allowlisted_failure_is_accepted(self):
        results = {"TestA": "fail"}
        self.assertEqual(unexpected_failures(results, {"TestA"}), [])

    def test_unlisted_failure_is_reported(self):
        results = {"TestA": "fail"}
        self.assertEqual(unexpected_failures(results, set()), ["TestA"])

    def test_parent_excused_when_all_failing_children_allowlisted(self):
        results = {
            "TestP": "fail",
            "TestP/one": "fail",
            "TestP/two": "pass",
        }
        self.assertEqual(unexpected_failures(results, {"TestP/one"}), [])

    def test_parent_reported_when_a_child_is_not_allowlisted(self):
        results = {"TestP": "fail", "TestP/one": "fail", "TestP/two": "fail"}
        self.assertEqual(
            unexpected_failures(results, {"TestP/one"}), ["TestP", "TestP/two"]
        )

    def test_grandparent_chain_is_excused(self):
        results = {
            "TestP": "fail",
            "TestP/mid": "fail",
            "TestP/mid/leaf": "fail",
        }
        self.assertEqual(unexpected_failures(results, {"TestP/mid/leaf"}), [])

    def test_parent_failing_without_failing_children_is_reported(self):
        # A t.Fatalf in the parent body itself, not in a subtest.
        results = {"TestP": "fail", "TestP/one": "pass"}
        self.assertEqual(unexpected_failures(results, set()), ["TestP"])

    def test_passing_tests_are_never_reported(self):
        results = {"TestA": "pass", "TestB": "skip"}
        self.assertEqual(unexpected_failures(results, set()), [])


class StaleEntryTests(unittest.TestCase):
    def test_entry_that_still_fails_is_not_stale(self):
        al = Allowlist(entries={"TestA": "B1"})
        self.assertEqual(stale_entries({"TestA": "fail"}, al), [])

    def test_entry_that_passes_is_stale(self):
        al = Allowlist(entries={"TestA": "B1"})
        self.assertEqual(stale_entries({"TestA": "pass"}, al), [("TestA", "now passes")])

    def test_entry_that_skips_is_stale(self):
        al = Allowlist(entries={"TestA": "B1"})
        self.assertEqual(stale_entries({"TestA": "skip"}, al), [("TestA", "now skips")])

    def test_entry_that_vanished_is_stale(self):
        al = Allowlist(entries={"TestGone": "B1"})
        self.assertEqual(stale_entries({"TestA": "pass"}, al), [("TestGone", "did not run")])


class EvaluateTests(unittest.TestCase):
    def test_clean_run_against_matching_allowlist(self):
        results = {"TestA": "fail", "TestB": "pass", "TestC": "pass"}
        al = Allowlist(entries={"TestA": "B1: junk columns"}, min_pass=2)
        unexpected, stale, regression = evaluate(results, al)
        self.assertEqual((unexpected, stale, regression), ([], [], None))

    def test_pass_count_regression_is_reported(self):
        results = {"TestA": "fail", "TestB": "pass"}
        al = Allowlist(entries={"TestA": "B1: junk columns"}, min_pass=2)
        _, _, regression = evaluate(results, al)
        self.assertIsNotNone(regression)
        self.assertIn("Only 1 tests passed", regression)

    def test_no_min_pass_means_no_regression_check(self):
        results = {"TestB": "pass"}
        _, _, regression = evaluate(results, Allowlist())
        self.assertIsNone(regression)

    def test_extra_passes_do_not_regress(self):
        results = {"TestA": "fail", "TestB": "pass", "TestC": "pass", "TestD": "pass"}
        al = Allowlist(entries={"TestA": "B1"}, min_pass=2)
        _, _, regression = evaluate(results, al)
        self.assertIsNone(regression)


class CountByActionTests(unittest.TestCase):
    def test_counts_each_action(self):
        results = {"a": "pass", "b": "pass", "c": "fail", "d": "skip"}
        self.assertEqual(count_by_action(results), {"pass": 2, "fail": 1, "skip": 1})

    def test_counts_zero_for_missing_actions(self):
        self.assertEqual(count_by_action({}), {"pass": 0, "fail": 0, "skip": 0})


class RenderSummaryTests(unittest.TestCase):
    def test_clean_summary_lists_known_failures(self):
        results = {"TestA": "fail", "TestB": "pass"}
        al = Allowlist(entries={"TestA": "B1: junk columns"}, min_pass=1)
        out = render_summary(results, al, [], [], None)
        self.assertIn("## Parity Test Results", out)
        self.assertIn("| Passed | 1 |", out)
        self.assertIn("every allowlist entry is still live", out)
        self.assertIn("B1: junk columns", out)

    def test_summary_lists_unexpected_failures(self):
        out = render_summary({"TestX": "fail"}, Allowlist(), ["TestX"], [], None)
        self.assertIn("### Unexpected failures", out)
        self.assertIn("`TestX`", out)

    def test_summary_lists_stale_entries(self):
        out = render_summary(
            {"TestA": "pass"},
            Allowlist(entries={"TestA": "B1"}),
            [],
            [("TestA", "now passes")],
            None,
        )
        self.assertIn("### Stale allowlist entries", out)
        self.assertIn("now passes", out)

    def test_summary_reports_pass_regression(self):
        out = render_summary({}, Allowlist(), [], [], "coverage went backwards")
        self.assertIn("### Pass-count regression", out)
        self.assertIn("coverage went backwards", out)


if __name__ == "__main__":
    unittest.main()
