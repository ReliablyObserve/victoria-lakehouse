"""The go.mod files of both binaries keep the dependency floors that close known vulnerabilities.

A dependency bot, a `go get` of an older module or a merge of a stale branch can lower a `require`
line without any compile error, and the Security workflow (govulncheck, Trivy) then goes red on
every pull request. This test fails first, in the unit tests, and names the advisory.

Add a row to FLOORS when a vulnerability is closed by raising a floor; never lower or remove one.

Run: python3 -I -m unittest discover -s scripts/ci/tests -p test_dependency_floors.py -v
"""

import pathlib
import re
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
GO_MODS = ("go.mod", "lakehouse-traces/go.mod")

# module -> (minimum version, advisory it closes)
FLOORS = {
    "github.com/andybalholm/brotli": ("v1.2.7", "GO-2026-6888"),
    "github.com/klauspost/compress": ("v1.20.2", "GO-2026-6865"),
    "golang.org/x/net": ("v0.62.0", "CVE-2026-78661"),
}

REQUIRE = re.compile(r"^\s*(?P<mod>[^\s]+)\s+(?P<ver>v\d+\.\d+\.\d+)\b")


def parse(version: str):
    return tuple(int(p) for p in version[1:].split("."))


def required_versions(path: pathlib.Path):
    """module -> version of every line inside a `require (...)` block or a one-line `require`."""
    found = {}
    in_block = False
    for raw in path.read_text(encoding="utf-8").split("\n"):
        line = raw.split("//", 1)[0].rstrip()
        if line.startswith("require ("):
            in_block = True
            continue
        if in_block and line.strip() == ")":
            in_block = False
            continue
        if line.startswith("require "):
            line = line[len("require "):]
        elif not in_block:
            continue
        m = REQUIRE.match(line)
        if m:
            found[m.group("mod")] = m.group("ver")
    return found


class DependencyFloors(unittest.TestCase):
    def test_floors_hold_in_every_go_mod(self):
        for rel in GO_MODS:
            versions = required_versions(ROOT / rel)
            for module, (floor, advisory) in FLOORS.items():
                with self.subTest(go_mod=rel, module=module, advisory=advisory):
                    self.assertIn(module, versions, f"{rel} no longer requires {module} (floor {floor}, {advisory})")
                    self.assertGreaterEqual(
                        parse(versions[module]),
                        parse(floor),
                        f"{rel} requires {module} {versions[module]}, below {floor} which closes {advisory}",
                    )

    def test_parser_reads_block_and_single_line_requires(self):
        with tempfile.TemporaryDirectory() as d:
            tmp = pathlib.Path(d) / "go.mod"
            tmp.write_text(
                "module m\n\nrequire a/b v1.2.3\n\nrequire (\n\tc/d v0.4.5 // indirect\n)\n",
                encoding="utf-8",
            )
            self.assertEqual(required_versions(tmp), {"a/b": "v1.2.3", "c/d": "v0.4.5"})


if __name__ == "__main__":
    unittest.main()
