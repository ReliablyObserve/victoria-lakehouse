import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import shard_tests as st  # noqa: E402

LIST_OUT = """TestA
TestB
FuzzC
ExampleD
BenchmarkE
TestMain
not a test line
Test_under
ok  	github.com/x/y	0.012s
"""


def names(n):
    return [f"TestN{i}" for i in range(n)]


class AssignTest(unittest.TestCase):
    def test_parse_list_keeps_only_tests_fuzz_examples(self):
        self.assertEqual(st.parse_list(LIST_OUT),
                         ["ExampleD", "FuzzC", "TestA", "TestB", "TestMain", "Test_under"])

    def test_union_equals_list_and_no_duplicates(self):
        for n in (1, 2, 3, 5):
            shards = st.assign(names(200), n)
            flat = [t for s in shards for t in s]
            self.assertEqual(sorted(flat), sorted(names(200)))
            self.assertEqual(len(flat), len(set(flat)))
            st.guard_partition(names(200), shards)

    def test_assignment_is_stable_across_calls_and_input_order(self):
        a = st.assign(names(100), 3)
        b = st.assign(list(reversed(names(100))), 3)
        self.assertEqual(a, b)

    def test_new_test_lands_in_exactly_one_shard_and_moves_no_other(self):
        before = st.assign(names(100), 3)
        after = st.assign(names(100) + ["TestBrandNew"], 3)
        homes = [i for i, s in enumerate(after) if "TestBrandNew" in s]
        self.assertEqual(len(homes), 1)
        for i in range(3):
            self.assertEqual([t for t in after[i] if t != "TestBrandNew"], before[i])

    def test_removed_test_is_fine_and_moves_no_other(self):
        before = st.assign(names(100), 3)
        gone = "TestN7"
        after = st.assign([t for t in names(100) if t != gone], 3)
        st.guard_partition([t for t in names(100) if t != gone], after)
        for i in range(3):
            self.assertEqual(after[i], [t for t in before[i] if t != gone])

    def test_roughly_balanced(self):
        sizes = [len(s) for s in st.assign(names(900), 3)]
        self.assertLess(max(sizes) - min(sizes), 90)


class GuardTest(unittest.TestCase):
    def test_duplicate_detected(self):
        with self.assertRaisesRegex(ValueError, "TestA is in shards 0 and 1"):
            st.guard_partition(["TestA", "TestB"], [["TestA"], ["TestA", "TestB"]])

    def test_missing_detected(self):
        with self.assertRaisesRegex(ValueError, "missing"):
            st.guard_partition(["TestA", "TestB"], [["TestA"], ["TestC"]])

    def test_empty_shard_detected(self):
        with self.assertRaisesRegex(ValueError, "empty"):
            st.guard_partition(["TestA"], [["TestA"], []])

    def test_regex_matches_exact_names_only(self):
        self.assertEqual(st.regex_for(["TestA", "TestB"]), "^(TestA|TestB)$")


class FilterTest(unittest.TestCase):
    """--run-filter: shard and verify only the selected (heavy) tests."""

    def test_filter_keeps_only_matching_names(self):
        all_names = names(20) + ["TestHeavyA", "TestHeavyB", "TestHeavyC"]
        self.assertEqual(st.filter_tests(all_names, "^(TestHeavyA|TestHeavyC)$"), ["TestHeavyA", "TestHeavyC"])
        self.assertEqual(st.filter_tests(all_names, ""), all_names)

    def test_filter_matching_nothing_is_an_error(self):
        with self.assertRaises(SystemExit):
            st.filter_tests(names(5), "^(TestNope)$")

    def test_filtered_list_is_partitioned_and_verified_over_the_filtered_names_only(self):
        heavy = ["TestHeavy%d" % i for i in range(8)]
        everything = names(30) + heavy
        kept = st.filter_tests(everything, "^(" + "|".join(heavy) + ")$")
        self.assertEqual(sorted(kept), sorted(heavy))
        shards = st.assign(kept, 2)
        st.guard_partition(kept, shards)
        with tempfile.TemporaryDirectory() as d:
            files = [write_json(d, "s%d.json" % i, s) for i, s in enumerate(shards)]
            self.assertEqual(st.verify(kept, 2, files), [])
            # a heavy test that silently did not run is still caught
            lost = [list(x) for x in shards]
            gone = lost[0].pop()
            files = [write_json(d, "l%d.json" % i, s) for i, s in enumerate(lost)]
            problems = st.verify(kept, 2, files)
            self.assertEqual(len(problems), 1)
            self.assertIn(gone, problems[0])

    def test_cli_regex_passes_the_filter_through(self):
        out = io.StringIO()
        with unittest.mock.patch.object(st, "list_tests", return_value=names(10) + ["TestHeavyA", "TestHeavyB"]):
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(st.main(["regex", "--pkg", "p", "--shards", "1", "--index", "0",
                                          "--run-filter", "^(TestHeavyA|TestHeavyB)$"]), 0)
        self.assertEqual(out.getvalue().strip(), "^(TestHeavyA|TestHeavyB)$")


def write_json(d, name, tests):
    p = os.path.join(d, name)
    with open(p, "w") as f:
        for t in tests:
            f.write(json.dumps({"Action": "run", "Test": t}) + "\n")
            f.write(json.dumps({"Action": "pass", "Test": t + "/sub"}) + "\n")
            f.write(json.dumps({"Action": "pass", "Test": t, "Elapsed": 0.1}) + "\n")
        f.write(json.dumps({"Action": "pass", "Package": "p"}) + "\n")
    return p


class VerifyTest(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.TemporaryDirectory()
        self.addCleanup(self.d.cleanup)
        self.names = names(30)
        self.shards = st.assign(self.names, 3)

    def files(self, shards):
        return [write_json(self.d.name, f"s{i}.json", s) for i, s in enumerate(shards)]

    def test_clean_run_passes(self):
        self.assertEqual(st.verify(self.names, 3, self.files(self.shards)), [])

    def test_test_that_ran_nowhere_is_reported(self):
        s = [list(x) for x in self.shards]
        lost = s[1].pop()
        problems = st.verify(self.names, 3, self.files(s))
        self.assertEqual(len(problems), 1)
        self.assertIn(lost, problems[0])
        self.assertIn("no shard", problems[0])

    def test_test_in_two_shards_is_reported(self):
        s = [list(x) for x in self.shards]
        s[0].append(s[2][0])
        problems = st.verify(self.names, 3, self.files(s))
        self.assertEqual(len(problems), 1)
        self.assertIn(s[2][0], problems[0])

    def test_new_unlisted_test_in_list_but_not_run_is_reported(self):
        problems = st.verify(self.names + ["TestNew"], 3, self.files(self.shards))
        self.assertTrue(any("TestNew" in p for p in problems))

    def test_wrong_file_count(self):
        self.assertTrue(st.verify(self.names, 3, self.files(self.shards)[:2]))

    def test_removed_test_still_fine(self):
        names = self.names[:-1]
        shards = st.assign(names, 3)
        self.assertEqual(st.verify(names, 3, self.files(shards)), [])


class CoverTest(unittest.TestCase):
    def prof(self, d, name, lines, mode="set"):
        p = os.path.join(d, name)
        with open(p, "w") as f:
            f.write(f"mode: {mode}\n" + "".join(l + "\n" for l in lines))
        return p

    def test_merge_is_union_and_matches_unsharded_percent(self):
        with tempfile.TemporaryDirectory() as d:
            a = self.prof(d, "a", ["f.go:1.1,2.2 4 1", "f.go:3.1,4.2 6 0", "g.go:1.1,2.2 10 0"])
            b = self.prof(d, "b", ["f.go:1.1,2.2 4 0", "f.go:3.1,4.2 6 1", "g.go:1.1,2.2 10 0"])
            mode, merged = st.merge_profiles([a, b])
            self.assertEqual(mode, "set")
            self.assertAlmostEqual(st.percent(merged), 50.0)  # 10 of 20 statements
            self.assertAlmostEqual(st.percent(st.merge_profiles([a])[1]), 20.0)

    def test_mode_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            a = self.prof(d, "a", ["f.go:1.1,2.2 1 1"], "set")
            b = self.prof(d, "b", ["f.go:1.1,2.2 1 1"], "atomic")
            with self.assertRaises(ValueError):
                st.merge_profiles([a, b])

    def test_cli_writes_merged_profile_and_prints_coverage_line(self):
        with tempfile.TemporaryDirectory() as d:
            a = self.prof(d, "a", ["f.go:1.1,2.2 1 1"])
            out = os.path.join(d, "all")
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                self.assertEqual(st.main(["cover-merge", out, a]), 0)
            self.assertEqual(buf.getvalue().strip(), "coverage: 100.0% of statements")
            with open(out) as f:
                self.assertTrue(f.read().startswith("mode: set\n"))


@unittest.skipUnless(shutil.which("go"), "go toolchain not available")
class ListFlagsTest(unittest.TestCase):
    """The list must be built with the run's flags, or build-tagged tests drift."""

    def setUp(self):
        self.d = tempfile.TemporaryDirectory()
        self.addCleanup(self.d.cleanup)
        files = {
            "go.mod": "module example.com/p\n\ngo 1.21\n",
            "p.go": "package p\n",
            "a_test.go": 'package p\n\nimport "testing"\n\nfunc TestEverywhere(t *testing.T) {}\n',
            "on_test.go": '//go:build race\n\npackage p\n\nimport "testing"\n\nfunc TestOnlyRace(t *testing.T) {}\n',
            "off_test.go": '//go:build !race\n\npackage p\n\nimport "testing"\n\nfunc TestNeverRace(t *testing.T) {}\n',
        }
        for n, b in files.items():
            with open(os.path.join(self.d.name, n), "w") as f:
                f.write(b)
        self.cwd = os.getcwd()
        os.chdir(self.d.name)
        self.addCleanup(os.chdir, self.cwd)
        self.env = unittest.mock.patch.dict(os.environ, {"GOWORK": "off", "GOFLAGS": ""})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_list_without_race_sees_the_not_race_test(self):
        self.assertEqual(st.list_tests("./"), ["TestEverywhere", "TestNeverRace"])

    def test_list_with_race_matches_what_a_race_run_executes(self):
        self.assertEqual(st.list_tests("./", ["-race"]), ["TestEverywhere", "TestOnlyRace"])


if __name__ == "__main__":
    unittest.main()
