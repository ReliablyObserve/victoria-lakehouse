import io
import json
import os
import tempfile
import unittest

from scripts.ci.gotest_report import main

PKG = "github.com/x/y/internal/storage/parquets3"


def ev(action, test=None, elapsed=None, output=None, pkg=PKG):
    e = {"Action": action, "Package": pkg}
    if test:
        e["Test"] = test
    if elapsed is not None:
        e["Elapsed"] = elapsed
    if output is not None:
        e["Output"] = output
    return json.dumps(e)


def run(lines, *extra):
    out = io.StringIO()
    with tempfile.TemporaryDirectory() as d:
        summary = os.path.join(d, "summary.md")
        code = main(["--budget-seconds", "100", "--summary", summary, *extra],
                    stdin=io.StringIO("\n".join(lines) + "\n"), stdout=out)
        text = ""
        if os.path.exists(summary):
            with open(summary) as f:
                text = f.read()
    return code, out.getvalue(), text


class GoTestReportTest(unittest.TestCase):
    def test_fast_package_passes_quietly_and_lists_slowest(self):
        code, out, summary = run([
            ev("output", "TestA", output="=== RUN TestA\n"),
            ev("pass", "TestA", 1.5),
            ev("pass", "TestB", 3.0),
            ev("pass", elapsed=10.0),
        ])
        self.assertEqual(code, 0)
        self.assertNotIn("::warning", out)
        self.assertNotIn("=== RUN", out)  # passing tests' chatter is dropped
        self.assertLess(summary.index("TestB"), summary.index("TestA"))  # slowest first

    def test_warns_at_seventy_percent(self):
        code, out, summary = run([ev("pass", "TestA", 1.0), ev("pass", elapsed=75.0)])
        self.assertEqual(code, 0)
        self.assertIn("::warning title=Test time budget::", out)
        self.assertIn("**WARN**", summary)

    def test_fails_at_ninety_percent(self):
        code, out, summary = run([ev("pass", "TestA", 1.0), ev("pass", elapsed=91.0)])
        self.assertEqual(code, 1)
        self.assertIn("::error title=Test time budget::", out)
        self.assertIn("91s = 91% of its 100s timeout", out)
        self.assertIn("**FAIL**", summary)

    def test_thresholds_are_configurable(self):
        code, _, _ = run([ev("pass", elapsed=60.0)], "--fail", "0.5")
        self.assertEqual(code, 1)

    def test_failing_test_prints_its_output_and_fails(self):
        code, out, _ = run([
            ev("output", "TestBad", output="    x_test.go:9: boom\n"),
            ev("fail", "TestBad", 0.1),
            ev("fail", elapsed=1.0),
        ])
        self.assertEqual(code, 1)
        self.assertIn("x_test.go:9: boom", out)

    def test_package_level_output_is_kept(self):
        _, out, _ = run([
            ev("output", output="panic: test timed out after 10m0s\n"),
            ev("fail", elapsed=600.0),
        ])
        self.assertIn("panic: test timed out", out)

    def test_non_json_lines_pass_through(self):
        _, out, _ = run(["# pkg\nfoo.go:1: undefined: bar", ev("fail", elapsed=0.0)])
        self.assertIn("undefined: bar", out)

    def test_benchmarks_are_listed_slowest_first_and_written_for_benchstat(self):
        with tempfile.TemporaryDirectory() as d:
            bench_out = os.path.join(d, "bench.txt")
            code, out, summary = run([
                ev("output", output="goos: linux\n"),
                ev("output", output="pkg: github.com/x/y\n"),
                ev("output", "BenchmarkFast", output="BenchmarkFast-2 \t 1000\t 100.0 ns/op\t 8 B/op\n"),
                ev("output", "BenchmarkSlow", output="BenchmarkSlow-2 \t 10\t 5000000000 ns/op\n"),
                ev("output", "BenchmarkSlow", output="=== RUN   BenchmarkSlow\n"),
                ev("pass", elapsed=60.0),
            ], "--bench-out", bench_out)
            with open(bench_out) as f:
                text = f.read()
        self.assertEqual(code, 0)
        self.assertLess(summary.index("BenchmarkSlow"), summary.index("BenchmarkFast"))
        self.assertIn("50.0", summary)  # 10 iterations x 5s
        self.assertIn("goos: linux", text)
        self.assertIn("BenchmarkFast-2", text)
        self.assertNotIn("=== RUN", text)

    def test_empty_stream_is_an_error_not_a_pass(self):
        code, out, _ = run(["not json"])
        self.assertEqual(code, 1)
        self.assertIn("no package results", out)


if __name__ == "__main__":
    unittest.main()
