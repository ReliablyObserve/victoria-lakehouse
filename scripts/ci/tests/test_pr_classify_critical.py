"""Drift test for CRITICAL_MODULE_PREFIXES (scripts/ci/pr_classify.py).

A module whose bump changes what the product stores or answers must never be bumped under a
dependency-only or toolchain-only exemption. The list is hand-maintained, so this test derives
the modules the storage-shaping code imports and fails when one is missing from it: the stream-id
and trace-index hashes (xxhash was missing once), the schema and every Parquet writer/reader file.
"""
import glob
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(HERE, ".."))
import pr_classify as pc  # noqa: E402

SELF_MODULE = "github.com/ReliablyObserve/victoria-lakehouse"

# Files and trees whose imports decide stored bytes: row keys, trace index, schema, Parquet I/O.
SCOPE_FILES = (
    "internal/vlstorage/stream_id.go",
    "lakehouse-traces/internal/vlstorage/stream_id.go",
    "internal/storage/parquets3/trace_index.go",
    "internal/compaction/ownership.go",
)
SCOPE_GLOBS = ("internal/schema/**/*.go", "internal/traceindex/**/*.go", "lakehouse-traces/internal/**/parquet*.go",
               "internal/**/parquet*.go", "internal/**/parquet*/**/*.go")
# Third-party modules these files import that decide nothing stored (none today).
NOT_STORAGE_CRITICAL: frozenset = frozenset()


def go_imports(src: str) -> list:
    block = re.search(r"^import \((.*?)^\)", src, re.M | re.S)
    if block:
        return re.findall(r'"([^"]+)"', block.group(1))
    return re.findall(r'^import\s+(?:\w+\s+)?"([^"]+)"', src, re.M)


def module_of(imp: str, modules: list) -> str:
    best = ""
    for m in modules:
        if (imp == m or imp.startswith(m + "/")) and len(m) > len(best):
            best = m
    return best


class CriticalModuleDrift(unittest.TestCase):
    def test_every_module_the_storage_code_imports_is_critical(self):
        with open(os.path.join(ROOT, "go.mod"), encoding="utf-8") as fh:
            reqs, _ = pc.parse_go_mod(fh.read())
        with open(os.path.join(ROOT, "lakehouse-traces", "go.mod"), encoding="utf-8") as fh:
            reqs2, _ = pc.parse_go_mod(fh.read())
        modules = sorted(set(reqs) | set(reqs2))
        files = set()
        for f in SCOPE_FILES:
            if os.path.exists(os.path.join(ROOT, f)):
                files.add(f)
        for g in SCOPE_GLOBS:
            files.update(os.path.relpath(p, ROOT) for p in glob.glob(os.path.join(ROOT, g), recursive=True))
        files = {f for f in files if f.endswith(".go") and not f.endswith("_test.go")}
        self.assertTrue(files, "the scope matched no file: the drift test checks nothing")
        missing = {}
        for f in sorted(files):
            with open(os.path.join(ROOT, f), encoding="utf-8") as fh:
                imps = go_imports(fh.read())
            for imp in imps:
                if "." not in imp.split("/")[0] or imp.startswith(SELF_MODULE):
                    continue  # standard library or this repository
                mod = module_of(imp, modules)
                self.assertTrue(mod, f"{f} imports {imp}, which no go.mod requires")
                if mod.startswith(pc.CRITICAL_MODULE_PREFIXES) or mod in NOT_STORAGE_CRITICAL:
                    continue
                missing.setdefault(mod, []).append(f)
        self.assertEqual(missing, {}, "add these modules to CRITICAL_MODULE_PREFIXES in scripts/ci/pr_classify.py "
                                      "(or to NOT_STORAGE_CRITICAL here with the reason): " + repr(missing))

    def test_the_stream_id_and_trace_index_hash_is_critical(self):
        self.assertTrue("github.com/cespare/xxhash/v2".startswith(pc.CRITICAL_MODULE_PREFIXES))

    def test_the_drift_test_would_notice_a_missing_prefix(self):
        keep = pc.CRITICAL_MODULE_PREFIXES
        try:
            pc.CRITICAL_MODULE_PREFIXES = tuple(p for p in keep if "xxhash" not in p)
            with self.assertRaises(AssertionError):
                self.test_every_module_the_storage_code_imports_is_critical()
        finally:
            pc.CRITICAL_MODULE_PREFIXES = keep


if __name__ == "__main__":
    unittest.main()
