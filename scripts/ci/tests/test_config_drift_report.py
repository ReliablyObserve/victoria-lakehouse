import contextlib
import io
import json
import os
import shutil
import tempfile
import textwrap
import unittest

from scripts.ci import config_drift_report as cdr


def key(name, typ, default, file="set", flags=None, effective=None):
    k = {"key": name, "type": typ, "default": default, "file": file}
    if effective is not None:
        k["effective"] = effective
    if flags:
        k["flags"] = flags
    return k


def flag(name, typ, default, usage, keys, effect="set"):
    return {"name": name, "type": typ, "default": default, "usage": usage, "keys": keys, "effect": effect}


SHARED_KEYS = [
    key("cache.memory_limit", "string", "512MB", flags=["lakehouse.cache.memory-mb"]),
    key("compaction.enabled", "bool", True, "enable-only", ["lakehouse.compaction.enabled"]),
    key("insert.ack_mode", "string", "buffer"),
    key("insert.buffer_engine", "string", "buffer"),
    key("insert.flush_interval", "duration", "1m"),
    key("logs.bloom_columns", "[]string", ["service.name", "trace_id"]),
    key("mode", "string", "", "ignored"),
    key("profile", "string", ""),
    key("query.file_workers", "int", 64, flags=["lakehouse.query.file-workers"]),
    key("query.max_files_per_query", "int", 0, "ignored"),
    key("shutdown.persist_timeout", "duration", "10s"),
    key("tenant.overrides", "map[string]object", {}),
    key("traces.jaeger_enabled", "bool", True, "enable-only"),
]
SHARED_FLAGS = [
    flag("lakehouse.cache.memory-mb", "int", "0", "L1 memory cache size in MB (default: 512)", ["cache.memory_limit"]),
    flag("lakehouse.compaction.enabled", "bool", "false", "Enable compaction", ["compaction.enabled"], "enable-only"),
    flag("lakehouse.config", "string", "", "Path to YAML config file", [], "none"),
    flag("lakehouse.query.file-workers", "int", "0", "Parallel file workers (default: 64)", ["query.file_workers"]),
    flag("lakehouse.s3.whole-file-threshold-bytes", "int", "0", "Threshold (default: 5MB logs / 8MB traces)",
         ["query.file_workers"]),
]
PROFILES = {
    "balanced": {},
    "dev": {"compaction.enabled": False, "query.file_workers": 2},
    "max-cost-savings": {"compaction.enabled": False},
    "max-durability": {},
    "max-performance": {"query.file_workers": 16},
}


def surface(binary, mode):
    keys = json.loads(json.dumps(SHARED_KEYS))
    flags = json.loads(json.dumps(SHARED_FLAGS))
    for k in keys:
        if k["key"] == "mode":
            k["effective"] = mode
    if mode == "logs":
        flags.append(flag("lakehouse.logs.bloom-columns", "string", "", "Bloom columns (default: service.name,trace_id)",
                          ["logs.bloom_columns"]))
        next(k for k in keys if k["key"] == "logs.bloom_columns")["flags"] = ["lakehouse.logs.bloom-columns"]
    else:
        flags.append(flag("lakehouse.traces.jaeger-enabled", "bool", "true", "Enable Jaeger", ["traces.jaeger_enabled"],
                          "enable-only"))
        next(k for k in keys if k["key"] == "traces.jaeger_enabled")["flags"] = ["lakehouse.traces.jaeger-enabled"]
    flags.sort(key=lambda f: f["name"])
    return {"binary": binary, "mode": mode, "keys": keys, "profiles": PROFILES, "flags": flags,
            "profile_flag_gaps": {"balanced": [], "dev": ["compaction.enabled", "query.file_workers"]}}


FIELD_DOCS = {
    "keys": {
        "cache.memory_limit": {"name": "MemoryLimit", "doc": "MemoryLimit is the L1 cache size. Sizes like 512MB."},
        "compaction.enabled": {"name": "Enabled", "doc": "Enabled turns on the compaction scheduler."},
        "query.file_workers": {"name": "FileWorkers", "doc": "FileWorkers bounds concurrent file readers | per pod."},
        "insert.ack_mode": {"name": "AckMode", "doc": "AckMode selects when a write is acknowledged."},
        "insert.buffer_engine": {"name": "BufferEngine", "doc": "BufferEngine selects the insert buffer."},
    },
    "sections": {"query": {"name": "QueryConfig", "doc": "QueryConfig bounds query cost.\n\nMore detail."}},
}

VALUES = """\
lakehouseConfig:
  profile: ""
  query:
    file_workers: 64
    max_files_per_query: 0
  compaction:
    enabled: true
  insert:
    flush_interval: 60s
  cache:
    memory_limit: 512MiB
  shutdown:
    persist_timeout: 10s
logs:
  config:
    bloom_columns: [service.name, trace_id]
traces:
  config:
    jaeger_enabled: true
"""

SCHEMA = {"properties": {"lakehouseConfig": {"properties": {
    "query": {"properties": {"file_workers": {"type": "integer", "default": 64}}}}}}}

CONFIG_DOC = """\
# Configuration

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.query.file-workers` | `64` | Parallel file workers |
| `query.max_files_per_query` | `0` | Cap |

<!-- BEGIN GENERATED: config-reference -->
<!-- END GENERATED: config-reference -->

<!-- BEGIN GENERATED: config-flags -->
<!-- END GENERATED: config-flags -->

<!-- BEGIN GENERATED: config-profiles -->
<!-- END GENERATED: config-profiles -->

<!-- BEGIN GENERATED: config-profile-flag-gaps -->
<!-- END GENERATED: config-profile-flag-gaps -->

```yaml
lakehouse:
  query:
    file_workers: 32
```

```bash
lakehouse-logs -lakehouse.query.file-workers=16
```
"""

GO_CONFIG = """\
package config

func (c *InsertConfig) BufferEngineLogstore() bool { return c.BufferEngine == "logstore" }

func (c *Config) usesLogstore() bool { return c.Insert.BufferEngineLogstore() }

func (c *Config) Buffered() bool { return c.usesLogstore() }

func mergeConfig(base, overlay *Config) *Config {
	base.Insert.AckMode = overlay.Insert.AckMode
	return base
}

func (c *Config) Validate() error {
	_ = c.Insert.AckMode
	return nil
}
"""

GO_MAIN = """\
package main

func run(cfg *config.Config) {
	_ = cfg.Buffered()
	_ = cfg.Query.FileWorkers
	_ = cfg.Compaction.Enabled
	_ = cfg.Cache.MemoryLimit
}
"""

README = """\
# Readme

<!-- BEGIN GENERATED: config-profile-summary -->
<!-- END GENERATED: config-profile-summary -->
"""


class FixtureRepo:
    """A minimal repository with every surface the report reads."""

    def __init__(self):
        self.root = tempfile.mkdtemp(prefix="config-drift-")
        self.write("cmd/lakehouse-logs/testdata/config-surface.json", json.dumps(surface("lakehouse-logs", "logs")))
        self.write("lakehouse-traces/testdata/config-surface.json", json.dumps(surface("lakehouse-traces", "traces")))
        self.write("internal/config/testdata/field-docs.json", json.dumps(FIELD_DOCS))
        self.write("charts/victoria-lakehouse/values.yaml", VALUES)
        self.write("charts/victoria-lakehouse/values.schema.json", json.dumps(SCHEMA))
        self.write("charts/victoria-lakehouse/templates/configmaps.yaml",
                   "{{- $cfg := $.Values.lakehouseConfig }}\n")
        self.write("charts/victoria-lakehouse/templates/services.yaml",
                   '{{- if (dig "jaeger_enabled" true $signalVals.config) }}\n'
                   '  port: {{ dig "jaeger_grpc_port" 16685 $signalVals.config }}\n'
                   '  sleep {{ default "10s" $.Values.lakehouseConfig.shutdown.persist_timeout }}\n')
        self.write("docs/configuration.md", CONFIG_DOC)
        self.write("README.md", README)
        self.write("deployment/docker/lakehouse-e2e-config.yml", "lakehouse:\n  query:\n    file_workers: 16\n")
        self.write("deployment/docker/docker-compose-e2e.yml",
                   'command:\n  - "-lakehouse.query.file-workers=16"\n  # - "-lakehouse.gone=1"\n')
        self.write("scripts/ci/helm-drift-allowlist.txt", "# coverage allowlist\ncache.memory_limit\n")
        self.write("internal/config/config.go", GO_CONFIG)
        self.write("internal/config/surface.go", "package config\nfunc describe(c *Config) { _ = c.Insert.AckMode }\n")
        self.write("cmd/lakehouse-logs/main.go", GO_MAIN)
        self.write("lakehouse-traces/deps/VictoriaLogs/lib.go", "package lib\nfunc x(c *T) { _ = c.AckMode }\n")
        cdr.write_docs(self.root, self.truth())

    def write(self, rel, text):
        path = os.path.join(self.root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)

    def read(self, rel):
        with open(os.path.join(self.root, rel), encoding="utf-8") as fh:
            return fh.read()

    def replace(self, rel, old, new):
        text = self.read(rel)
        assert old in text, f"{old!r} not in {rel}"
        self.write(rel, text.replace(old, new))

    def allow(self, line):
        self.write("scripts/ci/helm-drift-allowlist.txt", self.read("scripts/ci/helm-drift-allowlist.txt") + line + "\n")

    def truth(self):
        return cdr.load_truth(self.root)

    def report(self):
        return cdr.build_report(self.root, self.truth(),
                                cdr.load_overrides(os.path.join(self.root, cdr.ALLOWLIST)))

    def check(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cdr.main(["--repo", self.root, "--check"])
        return code, out.getvalue(), err.getvalue()

    def cleanup(self):
        shutil.rmtree(self.root)


class ConfigDriftReportTests(unittest.TestCase):
    def setUp(self):
        self.repo = FixtureRepo()
        self.addCleanup(self.repo.cleanup)

    def kinds(self, report):
        return sorted((f.surface, f.kind, f.key) for f in report.findings)

    # -- matching ---------------------------------------------------------

    def test_matching_repo_passes_the_gate(self):
        report = self.repo.report()
        self.assertEqual(self.kinds(report), [])
        self.assertEqual(report.stale, [])
        self.assertEqual(report.stale_docs, [])
        code, out, err = self.repo.check()
        self.assertEqual(code, 0, out + err)
        self.assertIn("no unexplained drift", out)

    # -- drifting ---------------------------------------------------------

    def test_helm_value_drift_fails_with_an_actionable_message(self):
        self.repo.replace("charts/victoria-lakehouse/values.yaml", "file_workers: 64", "file_workers: 8")
        self.assertEqual(self.kinds(self.repo.report()), [("helm-values", "value", "query.file_workers")])
        code, out, err = self.repo.check()
        self.assertEqual(code, 1)
        self.assertIn("| helm-values | value | `query.file_workers` | `64` | `8` |", out)
        self.assertIn("override <surface>:<key> <value> (code <value>) — <reason>", err)

    def test_helm_unknown_ignored_and_type_findings(self):
        self.repo.replace("charts/victoria-lakehouse/values.yaml", "    max_files_per_query: 0",
                          "    max_files_per_query: 500\n  circuit_breaker:\n    threshold: 5")
        self.repo.replace("charts/victoria-lakehouse/values.yaml", "enabled: true\n  insert",
                          'enabled: "yes"\n  insert')
        findings = {(f.kind, f.key): f for f in self.repo.report().findings}
        self.assertIn(("unknown-key", "circuit_breaker.threshold"), findings)
        self.assertIn("ignore this key", findings[("value", "query.max_files_per_query")].note)
        self.assertIn(("type", "compaction.enabled"), findings)

    def test_unrendered_section_is_reported(self):
        self.repo.write("charts/victoria-lakehouse/templates/configmaps.yaml",
                        '{{- $cfg := omit $.Values.lakehouseConfig "shutdown" }}\n')
        self.assertEqual(self.kinds(self.repo.report()),
                         [("helm-values", "not-rendered", "shutdown.persist_timeout")])

    def test_schema_default_drift(self):
        self.repo.replace("charts/victoria-lakehouse/values.schema.json", '"default": 64', '"default": 8')
        self.assertEqual(self.kinds(self.repo.report()), [("helm-schema", "value", "query.file_workers")])

    def test_template_fallback_drift_names_every_location(self):
        self.repo.replace("charts/victoria-lakehouse/templates/services.yaml", '"jaeger_enabled" true',
                          '"jaeger_enabled" false')
        self.repo.write("charts/victoria-lakehouse/templates/networkpolicy.yaml",
                        '{{- if (dig "jaeger_enabled" false .Values.traces.config) }}\n')
        findings = self.repo.report().findings
        self.assertEqual(sorted(f.where for f in findings), [
            "charts/victoria-lakehouse/templates/networkpolicy.yaml:1",
            "charts/victoria-lakehouse/templates/services.yaml:1",
        ])
        self.assertTrue(all(f.key == "traces.jaeger_enabled" for f in findings))

    def test_flag_usage_hint_drift(self):
        for binary in ("cmd/lakehouse-logs", "lakehouse-traces"):
            self.repo.replace(f"{binary}/testdata/config-surface.json", "(default: 512)", "(default: 256)")
        [finding] = self.repo.report().findings
        self.assertEqual((finding.surface, finding.key, finding.found), ("flags", "cache.memory_limit", "256"))
        self.assertIn("lakehouse-logs, lakehouse-traces", finding.where)

    def test_docs_drift(self):
        self.repo.replace("docs/configuration.md", "| `64` | Parallel", "| `8` | Parallel")
        self.repo.replace("docs/configuration.md", "| `0` | Cap |", "| (unlimited) | Cap |")
        self.repo.replace("docs/configuration.md", "file_workers: 32", "file_workers: lots\n  mode: logs\n  gone: 1")
        self.repo.replace("docs/configuration.md", "file-workers=16", "file-workers=16 --lakehouse.cache.memory-limit=1GB")
        self.repo.write("docs/topic.md", "| Config | Default |\n|---|---|\n| `query.nope` | `1` |\n"
                                         "| `cache.go` | not a key |\n")
        self.assertEqual(self.kinds(self.repo.report()), [
            ("docs", "ignored", "mode"),
            ("docs", "type", "query.file_workers"),
            ("docs", "unknown-flag", "lakehouse.cache.memory-limit"),
            ("docs", "unknown-key", "gone"),
            ("docs", "unknown-key", "query.nope"),
            ("docs", "unverifiable", "query.max_files_per_query"),
            ("docs", "value", "query.file_workers"),
        ])

    def test_yaml_fragments_rooted_at_a_section(self):
        self.repo.write("docs/topic.md", textwrap.dedent("""\
            ```yaml
            compaction:
              leaderElection: auto
            query:
              file_workers: 8
            ```

            ```yaml
            select:
              replicaCount: 2
            ```

            ```yaml
            unrelated: {query: 1}
            ```
            """))
        self.assertEqual(self.kinds(self.repo.report()), [("docs", "unknown-key", "compaction.leaderElection")])

    def test_ignored_keys_in_docs_need_the_marker(self):
        self.repo.write("docs/topic.md", textwrap.dedent("""\
            Intro.

            ```yaml
            lakehouse:
              query:
                max_files_per_query: 500
                file_workers: 32   # not read from the config file in this release
            ```

            ```yaml
            query:
              max_files_per_query: 500   # not read from the config file in this release
            ```
            """))
        findings = sorted((f.kind, f.key, f.where) for f in self.repo.report().findings)
        self.assertEqual(findings, [
            ("ignored", "query.max_files_per_query", "docs/topic.md:6"),
            ("stale-marker", "query.file_workers", "docs/topic.md:7"),
        ])

    def test_docker_surfaces(self):
        self.repo.write("deployment/docker/lakehouse-benchmark-config.yml", "profile: max-performance\n")
        self.repo.replace("deployment/docker/docker-compose-e2e.yml", "file-workers=16", "file-workers=16\n"
                          '  - "--lakehouse.insert.ack-mode=buffer"')
        self.assertEqual(self.kinds(self.repo.report()), [
            ("docker", "missing-root", "deployment/docker/lakehouse-benchmark-config.yml"),
            ("docker", "unknown-flag", "lakehouse.insert.ack-mode"),
        ])

    # -- allowlisted ------------------------------------------------------

    def test_allowlisted_override_passes(self):
        self.repo.replace("charts/victoria-lakehouse/values.yaml", "file_workers: 64", "file_workers: 8")
        self.repo.allow("override helm-values:query.file_workers 8 (code 64) — small default pods")
        report = self.repo.report()
        self.assertEqual(report.findings, [])
        self.assertEqual([(f.key, o.reason) for f, o in report.allowlisted],
                         [("query.file_workers", "small default pods")])
        code, out, _ = self.repo.check()
        self.assertEqual(code, 0)
        self.assertIn("helm-values:query.file_workers = 8 (code 64) — small default pods", out)

    def test_override_values_compare_normalized(self):
        self.repo.replace("charts/victoria-lakehouse/values.yaml", "memory_limit: 512MiB", "memory_limit: 1GB")
        self.repo.allow('override helm-values:cache.memory_limit "1024MB" (code "512MiB") — bigger pods')
        self.assertEqual(self.repo.report().findings, [])

    # -- stale allowlist entries ------------------------------------------

    def test_override_without_drift_is_stale(self):
        self.repo.allow("override helm-values:query.file_workers 8 (code 64) — small default pods")
        report = self.repo.report()
        self.assertEqual([o.key for o in report.stale], ["query.file_workers"])
        code, out, _ = self.repo.check()
        self.assertEqual(code, 1)
        self.assertIn("STALE OVERRIDES (1)", out)

    def test_override_goes_stale_when_the_code_default_moves(self):
        self.repo.replace("charts/victoria-lakehouse/values.yaml", "file_workers: 64", "file_workers: 8")
        self.repo.allow("override helm-values:query.file_workers 8 (code 32) — small default pods")
        report = self.repo.report()
        self.assertEqual([f.key for f in report.findings], ["query.file_workers"])
        self.assertEqual([o.line for o in report.stale], [3])

    # -- generated docs ---------------------------------------------------

    def test_stale_generated_docs_fail_and_are_rewritten(self):
        for binary in ("cmd/lakehouse-logs", "lakehouse-traces"):
            self.repo.replace(f"{binary}/testdata/config-surface.json", '"default": 64', '"default": 48')
        self.repo.replace("charts/victoria-lakehouse/values.yaml", "file_workers: 64", "file_workers: 48")
        self.repo.replace("charts/victoria-lakehouse/values.schema.json", '"default": 64', '"default": 48')
        self.repo.replace("docs/configuration.md", "| `64` | Parallel", "| `48` | Parallel")
        self.repo.replace("docs/configuration.md", "Parallel file workers (default: 64)", "Parallel file workers (default: 48)")
        for binary in ("cmd/lakehouse-logs", "lakehouse-traces"):
            self.repo.replace(f"{binary}/testdata/config-surface.json", "(default: 64)", "(default: 48)")
        report = self.repo.report()
        self.assertEqual(report.findings, [])
        self.assertEqual(report.stale_docs, ["docs/configuration.md"])
        code, _, err = self.repo.check()
        self.assertEqual(code, 1)
        self.assertIn("make config-docs", err)

        self.assertEqual(cdr.write_docs(self.repo.root, self.repo.truth()), ["docs/configuration.md"])
        self.assertEqual(cdr.write_docs(self.repo.root, self.repo.truth()), [])
        self.assertIn("| `query.file_workers` | int | `48` | set |", self.repo.read("docs/configuration.md"))

    def test_generated_blocks_content(self):
        doc = self.repo.read("docs/configuration.md")
        self.assertIn("### `query`\n\nBounds query cost.", doc)
        self.assertIn("| `query.file_workers` | int | `64` | set | `-lakehouse.query.file-workers` | "
                      "max-performance: `16`; dev: `2` | Bounds concurrent file readers \\| per pod. |", doc)
        self.assertIn("`-lakehouse.compaction.enabled` (enable-only)", doc)
        self.assertIn("| `-lakehouse.logs.bloom-columns` | logs |", doc)
        self.assertIn("| `compaction.enabled` | `true` |  |  | `false` | `false` |", doc)
        self.assertIn("| `dev` | `compaction.enabled`, `query.file_workers` | 2 |", doc)
        readme = self.repo.read("README.md")
        self.assertIn("| Profile | Ack mode (not read) | Flush interval | Cache memory | Compaction |", readme)
        self.assertIn("| `max-cost-savings` | buffer | 1m | 512MB | off |", readme)
        self.assertIn("<!-- Generated by `make config-docs` from the code; do not edit. -->", readme)

    def test_config_keys_block_renders_the_listed_keys(self):
        self.repo.write("docs/topic.md", "# Topic\n\n<!-- BEGIN GENERATED: config-keys query.file_workers insert.ack_mode -->\n"
                                         "stale\n<!-- END GENERATED: config-keys -->\n")
        self.assertEqual(self.repo.report().stale_docs, ["docs/topic.md"])
        cdr.write_docs(self.repo.root, self.repo.truth())
        topic = self.repo.read("docs/topic.md")
        self.assertIn("| `query.file_workers` | `64` | set | `-lakehouse.query.file-workers` | "
                      "Bounds concurrent file readers \\| per pod. |\n| `insert.ack_mode` | `buffer` | set |  | "
                      "**Not read.** Selects when a write is acknowledged. |", topic)
        self.assertNotIn("stale", topic)
        truth = self.repo.truth()
        with self.assertRaises(ValueError):
            cdr.gen_keys(truth, [])
        with self.assertRaises(ValueError):
            cdr.gen_keys(truth, ["query.nope"])

    def test_inventory_lists_every_key_and_flag(self):
        self.repo.replace("charts/victoria-lakehouse/values.yaml", "file_workers: 64", "file_workers: 8")
        out = os.path.join(self.repo.root, "inventory.md")
        with contextlib.redirect_stdout(io.StringIO()):
            cdr.main(["--repo", self.repo.root, "--inventory", out])
        with open(out, encoding="utf-8") as fh:
            inventory = fh.read()
        self.assertIn("| `query.file_workers` | int | `64` | set | yes | -lakehouse.query.file-workers | `16` |  |  | `2` | "
                      "`8` | `64` |  | docs/configuration.md:5=64 | lakehouse-e2e-config.yml=16 | yes |", inventory)
        self.assertIn("| `traces.jaeger_enabled` | bool | `true` | enable-only | yes | -lakehouse.traces.jaeger-enabled "
                      "|  |  |  |  | `true` |  | services.yaml:1=true |", inventory)
        self.assertIn("| `shutdown.persist_timeout` | duration | `10s` | set | yes |  |  |  |  |  | `10s` |  | "
                      "services.yaml:3=10s |", inventory)
        self.assertIn("| `insert.ack_mode` | string | `buffer` | set | no |", inventory)
        self.assertIn("| `-lakehouse.traces.jaeger-enabled` | traces | `traces.jaeger_enabled` | enable-only |", inventory)
        keys, flags = 13, 5 + 2  # shared flags plus one per binary
        self.assertEqual(inventory.count("\n| `"), keys + flags)

    def test_unread_keys_follow_reader_methods(self):
        # BufferEngine is read through BufferEngineLogstore -> usesLogstore ->
        # Buffered, which main calls. AckMode is only merged, validated,
        # described and read by vendored code: no binary reads it.
        self.assertEqual(self.repo.truth().unread, {"insert.ack_mode"})
        doc = self.repo.read("docs/configuration.md")
        self.assertIn("| `insert.ack_mode` | string | `buffer` | set |  |  | **Not read.** Selects when a write is acknowledged. |", doc)
        self.assertIn("**Not read.** marks a key that neither binary reads", doc)
        self.assertIn("| `insert.buffer_engine` | string | `buffer` | set |  |  | Selects the insert buffer. |", doc)
        code, out, _ = self.repo.check()
        self.assertEqual(code, 0, out)
        self.assertIn("keys no binary reads (1, informational): insert.ack_mode", out)

        self.repo.replace("cmd/lakehouse-logs/main.go", "_ = cfg.Buffered()", "_ = cfg.Insert.AckMode")
        self.assertEqual(self.repo.truth().unread, {"insert.buffer_engine"})

    def test_binaries_must_agree_on_defaults(self):
        self.repo.replace("lakehouse-traces/testdata/config-surface.json", '"default": 64', '"default": 65')
        with self.assertRaises(ValueError):
            self.repo.truth()


class ParsingTests(unittest.TestCase):
    def test_normalize(self):
        self.assertTrue(cdr.same("512MB", 536870912, "int"))
        self.assertTrue(cdr.same("512MB", "512MiB", "string"))
        self.assertFalse(cdr.same("512MB", "500MB", "string"))
        self.assertTrue(cdr.same("1h", "60m", "duration"))
        self.assertTrue(cdr.same("0s", None, "duration"))
        self.assertTrue(cdr.same("90d", "2160h", "string"))
        self.assertTrue(cdr.same("true", True, "bool"))
        self.assertTrue(cdr.same(None, False, "bool"))
        self.assertTrue(cdr.same("service.name,trace_id", ["service.name", "trace_id"], "[]string"))
        self.assertTrue(cdr.same("[3, 7, 11]", [3, 7, 11], "[]int"))
        self.assertTrue(cdr.same(None, [], "[]int"))
        self.assertFalse(cdr.same("[3, 7", [3, 7], "[]int"))
        self.assertFalse(cdr.same(7, [7], "[]int"))
        self.assertTrue(cdr.same({"b": 1, "a": "2"}, {"a": 2, "b": 1.0}, "map[string]float"))
        self.assertFalse(cdr.same([1], {}, "map[string]float"))
        self.assertTrue(cdr.same({"x": [1]}, {"x": [1]}, "object"))
        self.assertTrue(cdr.same("", 0, "int"))
        self.assertFalse(cdr.same(True, 1, "int"))
        self.assertFalse(cdr.same("lots", 1, "float"))
        self.assertFalse(cdr.same("soon", "1s", "duration"))
        self.assertTrue(cdr.same(None, "", "string"))
        self.assertTrue(cdr.same(False, "false", "string"))

    def test_type_ok(self):
        self.assertTrue(cdr.type_ok(None, "int"))
        self.assertTrue(cdr.type_ok(["a"], "[]string"))
        self.assertFalse(cdr.type_ok("a,b", "[]string"))
        self.assertTrue(cdr.type_ok({"a": 1.5}, "map[string]float"))
        self.assertFalse(cdr.type_ok([], "object"))
        self.assertFalse(cdr.type_ok("yes", "bool"))
        self.assertFalse(cdr.type_ok(True, "int"))
        self.assertTrue(cdr.type_ok(3, "float"))
        self.assertTrue(cdr.type_ok("90s", "duration"))
        self.assertTrue(cdr.type_ok(5, "duration"))
        self.assertFalse(cdr.type_ok("7d", "duration"))
        self.assertTrue(cdr.type_ok("7d", "string"))
        self.assertFalse(cdr.type_ok(False, "string"))

    def test_parse_helpers(self):
        self.assertEqual(cdr.parse_size("128KiB"), 128 * 1024)
        self.assertEqual(cdr.parse_size("100B"), 100)
        self.assertIsNone(cdr.parse_size(5))
        self.assertEqual(cdr.parse_duration("1h30m"), 5400.0)
        self.assertEqual(cdr.parse_duration("0"), 0.0)
        self.assertIsNone(cdr.parse_duration("5 minutes"))
        self.assertIsNone(cdr.parse_duration(True))

    def test_usage_default(self):
        self.assertEqual(cdr.usage_default("workers (default: 64)"), "64")
        self.assertEqual(cdr.usage_default("request (always-reserved baseline; default: 4)"), "4")
        self.assertEqual(cdr.usage_default("cap (default: 0 = unlimited)"), "0")
        self.assertEqual(cdr.usage_default("prefix (default: /delete/logsql)"), "/delete/logsql")
        self.assertEqual(cdr.usage_default("tpl (default: {AccountID}/{ProjectID}/)"), "{AccountID}/{ProjectID}/")
        self.assertIsNone(cdr.usage_default("size (default: 5MB logs / 8MB traces)"))
        self.assertIsNone(cdr.usage_default("threshold (0 = default 50000)"))
        self.assertIsNone(cdr.usage_default("mode: az-local (default), global"))

    def test_render(self):
        self.assertEqual(cdr.render(None), '""')
        self.assertEqual(cdr.render(False), "false")
        self.assertEqual(cdr.render(64.0), "64")
        self.assertEqual(cdr.render([3, "a"]), "[3, a]")
        self.assertEqual(cdr.render({}), "{}")
        self.assertEqual(cdr.render({"b": 1, "a": 2}), '{"a": 2, "b": 1}')

    def test_summarize_doc(self):
        self.assertEqual(cdr.summarize_doc("FileWorkers is the pool size. It grows.", "FileWorkers"), "The pool size.")
        self.assertEqual(cdr.summarize_doc("Enabled turns on the catalog, e.g. dropdowns.", "Enabled"),
                         "Turns on the catalog, e.g. dropdowns.")
        self.assertEqual(cdr.summarize_doc("Size of the cache.\n\nDetails.", "Other"), "Size of the cache.")
        self.assertEqual(cdr.summarize_doc(""), "")

    def test_load_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "allow.txt")
            self.assertEqual(cdr.load_overrides(path), [])
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(textwrap.dedent("""\
                    # comment
                    cache.bloom_ttl
                    override helm-values:logs.bloom_columns ["service.name"] (code ["service.name","trace_id"]) — why
                    override helm-values:tenant.orgid_header "X-Scope OrgID" (code "X-Scope-OrgID") — spaces ok
                    """))
            overrides = cdr.load_overrides(path)
            self.assertEqual([(o.key, o.found, o.line) for o in overrides],
                             [("logs.bloom_columns", ["service.name"], 3), ("tenant.orgid_header", "X-Scope OrgID", 4)])
            for bad in ("override helm-values:x 1 (code 2)\n", "override helm-values:x nope (code 2) — why\n"):
                with open(path, "w", encoding="utf-8") as fh:
                    fh.write(bad)
                with self.assertRaises(ValueError):
                    cdr.load_overrides(path)

    def test_splice_blocks_errors(self):
        truth = cdr.Truth({}, {}, {}, {}, {}, {})
        with self.assertRaises(ValueError):
            cdr.splice_blocks("<!-- BEGIN GENERATED: nope -->\n<!-- END GENERATED: nope -->", truth)
        with self.assertRaises(ValueError):
            cdr.splice_blocks("<!-- BEGIN GENERATED: config-flags -->\n", truth)

    def test_markdown_escaping(self):
        self.assertEqual(cdr.esc("a|b <c>\nd"), "a\\|b &lt;c&gt; d")
        self.assertEqual(cdr.code("x|y"), "`x\\|y`")


if __name__ == "__main__":
    unittest.main()
