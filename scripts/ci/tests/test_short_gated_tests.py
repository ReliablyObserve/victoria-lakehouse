import os
import subprocess
import tempfile
import unittest

SCRIPT = os.path.join(os.path.dirname(__file__), "..", "short_gated_tests.sh")

GATED = '''package p

import "testing"

func TestHeavy(t *testing.T) {
	if testing.Short() {
		t.Skip("big")
	}
}

func TestPlain(t *testing.T) {}

func helper(t *testing.T) {
	if testing.Short() {
		t.Skip("a helper is not a test")
	}
}

func BenchmarkB(b *testing.B) {
	if testing.Short() {
		b.Skip("benchmarks are ignored")
	}
}

func TestAfterHelper(t *testing.T) {
	t.Log("no gate here")
}
'''

OTHER = '''package p

import "testing"

func TestAlsoHeavy_Sub(t *testing.T) {
	t.Run("x", func(t *testing.T) {
		if testing.Short() {
			t.Skip()
		}
	})
}
'''


def run(files):
    with tempfile.TemporaryDirectory() as d:
        for name, body in files.items():
            with open(os.path.join(d, name), "w") as f:
                f.write(body)
        return subprocess.run(["bash", SCRIPT, d], capture_output=True, text=True)


class ShortGatedTestsTest(unittest.TestCase):
    def test_lists_only_gated_test_functions(self):
        r = run({"a_test.go": GATED, "b_test.go": OTHER})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), "^(TestAlsoHeavy_Sub|TestHeavy)$")

    def test_fails_loudly_when_nothing_is_gated(self):
        r = run({"a_test.go": "package p\n\nimport \"testing\"\n\nfunc TestPlain(t *testing.T) {}\n"})
        self.assertEqual(r.returncode, 1)
        self.assertIn("no testing.Short()-gated tests", r.stderr)


if __name__ == "__main__":
    unittest.main()
