"""Drift tests for the module classification of the registry gate (scripts/ci/pr_classify.py).

A module whose bump changes what the product stores or answers must never be bumped under a
dependency-only or toolchain-only exemption. The gate used to LIST the critical modules and was
one module short more than once (xxhash, then valyala/fastjson, quicktemplate, yaml.v3 ...), so the
list is inverted: a module is critical unless INERT_MODULES names it, with the reason. These tests keep
that table honest:

  * every module in either go.mod is classified on purpose (inert, or in KNOWN_CRITICAL below), so a
    new dependency forces a decision here;
  * no stale entry: every inert entry and every known-critical entry is still required by a go.mod;
  * no inert module is imported by the storage-shaping code (stream-id and trace-index hashes, schema,
    every Parquet reader and writer), and the inert modules the product imports directly are exactly the
    ones listed in INERT_DIRECT_IMPORTS, with the packages that import them;
  * golang.org/x/net, the one inert module that looks risky, has no direct importer in this repository.
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

# Product trees whose non-test imports are scanned for inert modules.
PRODUCT_GLOBS = ("internal/**/*.go", "cmd/**/*.go", "lakehouse-traces/*.go", "lakehouse-traces/internal/**/*.go",
                 "lakehouse-traces/cmd/**/*.go")

# Inert modules (or prefixes) the product imports directly, and the directories allowed to import them.
# A new importer of an inert module must be justified here: "inert" was decided for the importers below.
INERT_DIRECT_IMPORTS = {
    "golang.org/x/sync": ("internal/s3reader",),
    # spans around the select handlers and the storage wrappers, exported out of band by internal/telemetry
    "go.opentelemetry.io/otel": ("internal/telemetry", "internal/selectapi", "lakehouse-traces/internal/selectapi",
                                 "lakehouse-traces/internal/vlstorage"),
}

# Every module either go.mod requires that is CRITICAL, reviewed by hand. A module missing from both
# this set and INERT_MODULES fails test_every_module_is_classified: decide it, then add it to one list.
KNOWN_CRITICAL = frozenset("""
github.com/VictoriaMetrics/VictoriaLogs
github.com/VictoriaMetrics/VictoriaMetrics
github.com/VictoriaMetrics/VictoriaTraces
github.com/VictoriaMetrics/easyproto
github.com/VictoriaMetrics/metrics
github.com/VictoriaMetrics/metricsql
github.com/andybalholm/brotli
github.com/aws/aws-sdk-go-v2
github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream
github.com/aws/aws-sdk-go-v2/config
github.com/aws/aws-sdk-go-v2/credentials
github.com/aws/aws-sdk-go-v2/feature/ec2/imds
github.com/aws/aws-sdk-go-v2/internal/configsources
github.com/aws/aws-sdk-go-v2/internal/endpoints/v2
github.com/aws/aws-sdk-go-v2/internal/v4a
github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding
github.com/aws/aws-sdk-go-v2/service/internal/checksum
github.com/aws/aws-sdk-go-v2/service/internal/presigned-url
github.com/aws/aws-sdk-go-v2/service/internal/s3shared
github.com/aws/aws-sdk-go-v2/service/s3
github.com/aws/aws-sdk-go-v2/service/signin
github.com/aws/aws-sdk-go-v2/service/sso
github.com/aws/aws-sdk-go-v2/service/ssooidc
github.com/aws/aws-sdk-go-v2/service/sts
github.com/aws/smithy-go
github.com/cespare/xxhash/v2
github.com/felixge/httpsnoop
github.com/golang/snappy
github.com/google/uuid
github.com/grpc-ecosystem/grpc-gateway/v2
github.com/klauspost/compress
github.com/parquet-go/bitpack
github.com/parquet-go/jsonlite
github.com/parquet-go/parquet-go
github.com/pierrec/lz4/v4
github.com/twpayne/go-geom
github.com/valyala/bytebufferpool
github.com/valyala/fastjson
github.com/valyala/fastrand
github.com/valyala/fasttemplate
github.com/valyala/gozstd
github.com/valyala/histogram
github.com/valyala/quicktemplate
go.opentelemetry.io/auto/sdk
go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp
go.opentelemetry.io/proto/otlp
golang.org/x/text
google.golang.org/genproto/googleapis/api
google.golang.org/genproto/googleapis/rpc
google.golang.org/grpc
google.golang.org/protobuf
gopkg.in/yaml.v3
""".split())


def go_imports(src: str) -> list:
    block = re.search(r"^import \((.*?)^\)", src, re.M | re.S)
    out = re.findall(r'"([^"]+)"', block.group(1)) if block else []
    return out + re.findall(r'^import\s+(?:[\w.]+\s+)?"([^"]+)"', src, re.M)


def module_of(imp: str, modules: list) -> str:
    best = ""
    for m in modules:
        if (imp == m or imp.startswith(m + "/")) and len(m) > len(best):
            best = m
    return best


def read(path: str) -> str:
    with open(os.path.join(ROOT, path), encoding="utf-8") as fh:
        return fh.read()


def go_mod_modules() -> list:
    reqs, _ = pc.parse_go_mod(read("go.mod"))
    reqs2, _ = pc.parse_go_mod(read("lakehouse-traces/go.mod"))
    return sorted((set(reqs) | set(reqs2)) - {SELF_MODULE})


def product_files(globs, tests: bool = False) -> list:
    files = set()
    for g in globs:
        files.update(os.path.relpath(p, ROOT) for p in glob.glob(os.path.join(ROOT, g), recursive=True))
    return sorted(f for f in files if f.endswith(".go") and (tests or not f.endswith("_test.go")) and "/testdata/" not in f)


def imported_modules(f: str, modules: list):
    for imp in go_imports(read(f)):
        if "." not in imp.split("/")[0] or imp.startswith(SELF_MODULE):
            continue  # standard library or this repository
        yield imp, module_of(imp, modules)


class InvertedModuleList(unittest.TestCase):
    def test_every_module_is_classified(self):
        unclassified = [m for m in go_mod_modules() if pc.is_inert(m) is False and m not in KNOWN_CRITICAL]
        self.assertEqual(unclassified, [], "decide these modules: add each to INERT_MODULES in scripts/ci/pr_classify.py "
                                           "(with the reason it cannot change a stored or answered byte) or to KNOWN_CRITICAL here")

    def test_the_drift_test_would_notice_an_unclassified_module(self):
        keep = KNOWN_CRITICAL
        try:
            globals()["KNOWN_CRITICAL"] = frozenset(m for m in KNOWN_CRITICAL if m != "github.com/valyala/fastjson")
            with self.assertRaises(AssertionError):
                self.test_every_module_is_classified()
        finally:
            globals()["KNOWN_CRITICAL"] = keep

    def test_no_module_is_on_both_lists(self):
        both = sorted(m for m in KNOWN_CRITICAL if pc.is_inert(m))
        self.assertEqual(both, [])

    def test_no_stale_entries(self):
        modules = set(go_mod_modules())
        self.assertEqual(sorted(m for m in KNOWN_CRITICAL if m not in modules), [], "KNOWN_CRITICAL names a module no go.mod requires")
        stale = [m for m in pc.INERT_MODULES if m not in modules]
        self.assertEqual(stale, [], "INERT_MODULES names a module no go.mod requires")
        for prefix in pc.INERT_MODULE_PREFIXES:
            self.assertTrue(any(m.startswith(prefix) for m in modules), f"no module under {prefix}")

    def test_every_inert_entry_has_a_reason(self):
        for m, why in pc.INERT_MODULES.items():
            self.assertTrue(len(why.split()) >= 3, f"{m}: say why a bump of it cannot change a stored or answered byte")

    def test_unknown_and_review_found_modules_are_critical(self):
        for m in ("github.com/valyala/fastjson", "github.com/valyala/quicktemplate", "github.com/valyala/histogram",
                  "github.com/valyala/fastrand", "github.com/valyala/fasttemplate", "github.com/valyala/bytebufferpool",
                  "gopkg.in/yaml.v3", "github.com/google/uuid", "golang.org/x/text",
                  "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp", "go.opentelemetry.io/proto/otlp",
                  "github.com/felixge/httpsnoop", "github.com/never/heard-of-it", "github.com/klauspost/compress",
                  "github.com/cespare/xxhash/v2", "golang.org/x/crypto", "go.opentelemetry.io/otelx"):
            with self.subTest(m):
                self.assertTrue(pc.is_critical(m))

    def test_inert_modules(self):
        for m in ("golang.org/x/net", "golang.org/x/sys", "golang.org/x/sync", "golang.org/x/oauth2", "go.uber.org/goleak",
                  "go.opentelemetry.io/otel", "go.opentelemetry.io/otel/sdk", "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"):
            with self.subTest(m):
                self.assertFalse(pc.is_critical(m))

    def test_matching_is_by_whole_module_path(self):
        # a family prefix ends at a path separator, an exact entry matches exactly
        self.assertTrue(pc.is_critical("golang.org/x/net/evil"))
        self.assertTrue(pc.is_critical("golang.org/x/netx"))
        self.assertTrue(pc.is_critical("go.opentelemetry.io/otel-evil"))
        self.assertTrue(pc.is_critical("go.uber.org/goleak2"))


class StorageCodeStaysCritical(unittest.TestCase):
    def test_no_inert_module_is_imported_by_the_storage_code(self):
        modules = go_mod_modules()
        files = {f for f in SCOPE_FILES if os.path.exists(os.path.join(ROOT, f))}
        files.update(product_files(SCOPE_GLOBS))
        self.assertTrue(files, "the scope matched no file: the drift test checks nothing")
        bad = {}
        for f in sorted(files):
            for imp, mod in imported_modules(f, modules):
                self.assertTrue(mod, f"{f} imports {imp}, which no go.mod requires")
                if pc.is_inert(mod):
                    bad.setdefault(mod, []).append(f)
        self.assertEqual(bad, {}, "the storage-shaping code imports these modules, so they cannot be inert: "
                                  "remove them from INERT_MODULES in scripts/ci/pr_classify.py")

    def test_the_inert_modules_the_product_imports_are_the_documented_ones(self):
        modules = go_mod_modules()
        seen = {}
        for f in product_files(PRODUCT_GLOBS):
            for imp, mod in imported_modules(f, modules):
                if mod and pc.is_inert(mod):
                    key = "go.opentelemetry.io/otel" if mod.startswith("go.opentelemetry.io/otel") else mod
                    seen.setdefault(key, set()).add(os.path.dirname(f))
        for mod, dirs in sorted(seen.items()):
            allowed = INERT_DIRECT_IMPORTS.get(mod)
            self.assertIsNotNone(allowed, f"{mod} is inert and the product imports it ({sorted(dirs)}): document the importer in INERT_DIRECT_IMPORTS")
            extra = sorted(d for d in dirs if d not in allowed)
            self.assertEqual(extra, [], f"{mod} is inert only for {allowed}: {extra} also import it, so decide again")

    def test_x_net_has_no_direct_importer(self):
        # golang.org/x/net is reached through google.golang.org/grpc (the OTLP trace exporter client) only;
        # nothing in this repository imports it, and neither does the checked-out upstream source when present
        hits = []
        for top in ("internal", "cmd", "lakehouse-traces", "tests", "deps"):
            for root, dirs, files in os.walk(os.path.join(ROOT, top)):
                dirs[:] = [d for d in dirs if d not in ("vendor", "node_modules", ".git")]
                for name in files:
                    if name.endswith(".go"):
                        path = os.path.join(root, name)
                        with open(path, encoding="utf-8", errors="replace") as fh:
                            if re.search(r'^\s*(?:import\s+)?(?:[\w.]+\s+)?"golang\.org/x/net[/"]', fh.read(), re.M):
                                hits.append(os.path.relpath(path, ROOT))
        self.assertEqual(hits, [], "golang.org/x/net is inert on the strength of having no direct importer; decide again")

    def test_the_drift_test_would_notice_an_inert_storage_module(self):
        keep = dict(pc.INERT_MODULES)
        try:
            pc.INERT_MODULES["github.com/parquet-go/parquet-go"] = "wrongly declared inert for this test"
            with self.assertRaises(AssertionError):
                self.test_no_inert_module_is_imported_by_the_storage_code()
        finally:
            pc.INERT_MODULES.clear()
            pc.INERT_MODULES.update(keep)


class ClassifiedBumps(unittest.TestCase):
    GO_MOD = "module x\n\ngo 1.27.2\n\nrequire (\n\t{a} {va}\n\tgolang.org/x/net v0.59.0 // indirect\n)\n"

    def test_a_bump_of_a_review_found_module_is_not_dependency_only(self):
        for mod in ("github.com/valyala/fastjson", "github.com/valyala/quicktemplate", "gopkg.in/yaml.v3", "golang.org/x/text",
                    "github.com/google/uuid", "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"):
            base = self.GO_MOD.format(a=mod, va="v1.0.0")
            head = self.GO_MOD.format(a=mod, va="v1.0.1")
            with self.subTest(mod):
                self.assertFalse(pc.go_mod_dependency_only(base, head))
                self.assertFalse(pc.go_mod_toolchain_only(base, head))

    def test_an_x_net_bump_stays_dependency_and_toolchain_only(self):
        # PR #491
        base = self.GO_MOD.format(a="github.com/a/b", va="v1.0.0")
        head = base.replace("x/net v0.59.0", "x/net v0.60.0").replace("go 1.27.2", "go 1.27.2")
        self.assertTrue(pc.go_mod_dependency_only(base, head))
        self.assertTrue(pc.go_mod_toolchain_only(base, head))

    def test_go_sum_lines_of_critical_modules_are_refused(self):
        base = "github.com/a/b v1.0.0 h1:aaa=\n"
        self.assertTrue(pc.go_sum_toolchain_only(base, base + "golang.org/x/net v0.60.0 h1:ccc=\n"))
        for mod in ("github.com/valyala/fastjson", "gopkg.in/yaml.v3", "golang.org/x/text", "github.com/never/heard-of-it"):
            with self.subTest(mod):
                self.assertFalse(pc.go_sum_toolchain_only(base, base + f"{mod} v1.0.1 h1:ccc=\n"))
                self.assertFalse(pc.go_sum_toolchain_only(base, base + f"{mod} v1.0.1/go.mod h1:ccc=\n"))


if __name__ == "__main__":
    unittest.main()
