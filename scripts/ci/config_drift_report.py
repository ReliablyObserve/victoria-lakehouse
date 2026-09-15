#!/usr/bin/env python3
"""Config drift report: the binaries' code defaults are the single source of truth.

A Victoria Lakehouse setting is spelled on several surfaces. The code is the
truth; every other surface is generated from it or checked against it:

  code          internal/config defaults, profiles, config-file merge rules and
                flags, as printed by `lakehouse-logs print-default-config` and
                `lakehouse-traces print-default-config` and pinned in
                cmd/lakehouse-logs/testdata/config-surface.json and
                lakehouse-traces/testdata/config-surface.json
  helm-values   charts/victoria-lakehouse/values.yaml (lakehouseConfig, <signal>.config)
  helm-schema   `default` values in charts/victoria-lakehouse/values.schema.json
  helm-template literal fallbacks in charts/victoria-lakehouse/templates/*
  docs          flag/key tables and YAML examples in docs/**/*.md and README.md
  flags         "(default: X)" hints in the flag usage strings
  docker        deployment/docker/lakehouse-*.yml and -lakehouse.* flags in compose files

A chart value that differs from the code default on purpose is an "override"
line in scripts/ci/helm-drift-allowlist.txt with a reason. An override whose
chart value or code default no longer matches is stale and fails the gate, so
every deviation is re-justified when either side changes.

Usage:
    python scripts/ci/config_drift_report.py                    # report
    python scripts/ci/config_drift_report.py --check            # CI gate
    python scripts/ci/config_drift_report.py --write-docs       # regenerate docs blocks
    python scripts/ci/config_drift_report.py --inventory FILE   # full markdown inventory
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Iterable

import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SURFACE_FILES = {
    "lakehouse-logs": "cmd/lakehouse-logs/testdata/config-surface.json",
    "lakehouse-traces": "lakehouse-traces/testdata/config-surface.json",
}
FIELD_DOCS = "internal/config/testdata/field-docs.json"
VALUES = "charts/victoria-lakehouse/values.yaml"
SCHEMA = "charts/victoria-lakehouse/values.schema.json"
TEMPLATES = "charts/victoria-lakehouse/templates"
ALLOWLIST = "scripts/ci/helm-drift-allowlist.txt"
CONFIG_DOC = "docs/configuration.md"
DOCKER_CONFIGS = "deployment/docker/lakehouse-*.yml"
COMPOSE_FILES = "deployment/docker/docker-compose*.yml"

REGENERATE = "make config-docs"

PROFILE_ORDER = ["balanced", "max-performance", "max-durability", "max-cost-savings", "dev"]

# ---------------------------------------------------------------------------
# value normalization
# ---------------------------------------------------------------------------

DURATION_TOKEN = re.compile(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h|d|w|y)")
DURATION_SECONDS = {
    "ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0,
    "h": 3600.0, "d": 86400.0, "w": 604800.0, "y": 31536000.0,
}
# config.ParseSizeBytes treats KB/MB/GB/TB as binary units.
SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*(k|m|g|t)?(i)?b$", re.IGNORECASE)
SIZE_POWER = {"": 0, "k": 1, "m": 2, "g": 3, "t": 4}


def parse_duration(text: Any) -> float | None:
    """Seconds for a Go/VictoriaMetrics duration ("1h30m", "200ms", "7d"); None otherwise."""
    if isinstance(text, bool) or not isinstance(text, str):
        return None
    text = text.strip()
    if text == "0":
        return 0.0
    tokens = DURATION_TOKEN.findall(text)
    if not tokens or DURATION_TOKEN.sub("", text):
        return None
    return sum(float(n) * DURATION_SECONDS[u] for n, u in tokens)


def parse_size(text: Any) -> int | None:
    """Bytes for a size string ("512MB", "128KiB", "100B"); None otherwise."""
    if not isinstance(text, str):
        return None
    m = SIZE_RE.match(text.strip())
    if not m:
        return None
    return int(float(m.group(1)) * 1024 ** SIZE_POWER[(m.group(2) or "").lower()])


def normalize(value: Any, kind: str) -> Any:
    """Reduce a value from any surface to a canonical, comparable form."""
    if kind.startswith("[]"):
        if value is None or value == "":
            return []
        if isinstance(value, str) and value.strip().startswith("["):
            try:
                value = yaml.safe_load(value)
            except yaml.YAMLError:
                return ("invalid", value)
        if isinstance(value, str):
            value = [p.strip() for p in value.split(",") if p.strip()]
        if not isinstance(value, list):
            return ("invalid", value)
        return [normalize(v, kind[2:]) for v in value]
    if kind.startswith("map["):
        if value is None:
            return {}
        if not isinstance(value, dict):
            return ("invalid", value)
        elem = kind[kind.index("]") + 1:]
        return {str(k): normalize(v, elem) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if kind == "object":
        return json.loads(json.dumps(value, sort_keys=True))
    if kind == "bool":
        if value is None:
            return False
        if isinstance(value, str) and value.strip().lower() in ("true", "false"):
            return value.strip().lower() == "true"
        return value
    if kind in ("int", "float"):
        if value is None or value == "":
            return 0.0
        if isinstance(value, bool):
            return ("invalid", value)
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).replace("_", "").strip()
        size = parse_size(text)
        if size is not None:
            return float(size)
        try:
            return float(text)
        except ValueError:
            return ("invalid", value)
    if kind == "duration":
        if value is None or value == "" or value == 0:
            return ("duration", 0.0)
        secs = parse_duration(value)
        return ("duration", secs) if secs is not None else ("invalid", value)
    # string: sizes and durations compare by magnitude, everything else verbatim
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value)
    size = parse_size(text)
    if size is not None:
        return ("size", size)
    secs = parse_duration(text)
    if secs is not None:
        return ("duration", secs)
    return text


def same(a: Any, b: Any, kind: str) -> bool:
    return normalize(a, kind) == normalize(b, kind)


def type_ok(value: Any, kind: str) -> bool:
    """Would the Go YAML decoder accept value for a key of this type?"""
    if value is None:
        return True
    if kind.startswith("[]"):
        return isinstance(value, list) and all(type_ok(v, kind[2:]) for v in value)
    if kind.startswith("map["):
        elem = kind[kind.index("]") + 1:]
        return isinstance(value, dict) and all(type_ok(v, elem) for v in value.values())
    if kind == "object":
        return isinstance(value, dict)
    if kind == "bool":
        return isinstance(value, bool)
    if kind == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if kind == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if kind == "duration":
        # yaml.v3 decodes a Duration from an integer (nanoseconds) or from a
        # time.ParseDuration string, which has no d/w/y units.
        if isinstance(value, int) and not isinstance(value, bool):
            return True
        return isinstance(value, str) and parse_duration(value) is not None and not re.search(r"\d[dwy]\b", value)
    return isinstance(value, (str, int, float)) and not isinstance(value, bool)


def render(value: Any) -> str:
    """Spell a value the way the docs and the chart do."""
    if value is None or value == "":
        return '""'
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, list):
        return "[" + ", ".join(render(v) for v in value) + "]"
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, separators=(", ", ": ")) if value else "{}"
    return str(value)


# ---------------------------------------------------------------------------
# the code side
# ---------------------------------------------------------------------------


@dataclass
class Key:
    key: str
    type: str
    default: Any
    file: str
    flags: dict[str, list[str]] = field(default_factory=dict)  # binary -> flag names
    doc: str = ""
    ident: str = ""


@dataclass
class Flag:
    name: str
    type: str
    default: str
    usage: str
    keys: list[str]
    effect: str
    binaries: list[str] = field(default_factory=list)


@dataclass
class Truth:
    keys: dict[str, Key]
    flags: dict[str, Flag]
    profiles: dict[str, dict[str, Any]]
    gaps: dict[str, dict[str, list[str]]]  # binary -> profile -> keys
    sections: dict[str, dict[str, str]]  # section -> {"name", "doc"}
    effective: dict[str, dict[str, Any]]  # binary -> key -> value
    unread: set[str] = field(default_factory=set)

    def sections_of(self) -> set[str]:
        return {k.split(".")[0] for k in self.keys if "." in k}


def load_truth(repo: str) -> Truth:
    keys: dict[str, Key] = {}
    flags: dict[str, Flag] = {}
    profiles: dict[str, dict[str, Any]] = {}
    gaps: dict[str, dict[str, list[str]]] = {}
    effective: dict[str, dict[str, Any]] = {}
    for binary, rel in SURFACE_FILES.items():
        with open(os.path.join(repo, rel), encoding="utf-8") as fh:
            surface = json.load(fh)
        effective[binary] = {}
        for k in surface["keys"]:
            known = keys.get(k["key"])
            if known is None:
                known = keys[k["key"]] = Key(k["key"], k["type"], k["default"], k["file"])
            elif json.dumps(known.default, sort_keys=True) != json.dumps(k["default"], sort_keys=True):
                raise ValueError(f"{k['key']}: the two binaries disagree on the default")
            known.flags[binary] = k.get("flags") or []
            if "effective" in k:
                effective[binary][k["key"]] = k["effective"]
        for f in surface["flags"]:
            known = flags.get(f["name"])
            if known is None:
                known = flags[f["name"]] = Flag(f["name"], f["type"], f["default"], f["usage"], f["keys"], f["effect"])
            known.binaries.append(binary)
        if not profiles:
            profiles = surface["profiles"]
        gaps[binary] = surface.get("profile_flag_gaps") or {}
    with open(os.path.join(repo, FIELD_DOCS), encoding="utf-8") as fh:
        docs = json.load(fh)
    for name, entry in docs["keys"].items():
        if name in keys:
            keys[name].doc, keys[name].ident = entry["doc"], entry["name"]
    truth = Truth(keys, flags, profiles, gaps, docs.get("sections", {}), effective)
    truth.unread = unread_keys(repo, truth)
    return truth


# ---------------------------------------------------------------------------
# findings and the allowlist
# ---------------------------------------------------------------------------


@dataclass
class Finding:
    surface: str
    kind: str  # value | unknown-key | unknown-flag | type | ignored | not-rendered | missing-root
    key: str
    code: Any
    found: Any
    where: str
    note: str = ""

    def ident(self) -> str:
        return f"{self.surface}:{self.key}"


@dataclass
class Override:
    surface: str
    key: str
    found: Any
    code: Any
    reason: str
    line: int

    def ident(self) -> str:
        return f"{self.surface}:{self.key}"


OVERRIDE_RE = re.compile(r"^override\s+(\S+?):(\S+)\s+(.+?)\s+\(code\s+(.+?)\)\s+—\s+(\S.*)$")


def load_overrides(path: str) -> list[Override]:
    """Parse `override <surface>:<key> <value> (code <value>) — <reason>` lines."""
    out: list[Override] = []
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line.startswith("override "):
                continue
            m = OVERRIDE_RE.match(line)
            if not m:
                raise ValueError(
                    f"{path}:{lineno}: want `override <surface>:<key> <value> (code <value>) — <reason>`"
                )
            try:
                found, code = json.loads(m.group(3)), json.loads(m.group(4))
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{lineno}: values must be JSON: {exc}") from exc
            out.append(Override(m.group(1), m.group(2), found, code, m.group(5).strip(), lineno))
    return out


def override_matches(o: Override, f: Finding, truth: Truth) -> bool:
    if o.ident() != f.ident():
        return False
    kind = truth.keys[f.key].type if f.key in truth.keys else "object"
    return same(o.found, f.found, kind) and same(o.code, f.code, kind)


# ---------------------------------------------------------------------------
# surfaces
# ---------------------------------------------------------------------------


def flatten_config(node: Any, truth: Truth, prefix: str = "") -> dict[str, Any]:
    """Flatten a config mapping into key paths, stopping at the config's leaves
    (so map-typed keys such as tenant.overrides stay whole)."""
    out: dict[str, Any] = {}
    if not isinstance(node, dict):
        return out
    for name, value in node.items():
        path = f"{prefix}.{name}" if prefix else str(name)
        if path in truth.keys or not isinstance(value, dict):
            out[path] = value
        else:
            out.update(flatten_config(value, truth, path))
    return out


def load_yaml(path: str) -> Any:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def chart_config(repo: str, truth: Truth) -> dict[str, Any]:
    doc = load_yaml(os.path.join(repo, VALUES)) or {}
    out = flatten_config(doc.get("lakehouseConfig") or {}, truth)
    for signal in ("logs", "traces"):
        for name, value in flatten_config((doc.get(signal) or {}).get("config") or {}, truth).items():
            out[f"{signal}.{name}"] = value
    return out


OMIT_RE = re.compile(r"omit\s+\$\.Values\.lakehouseConfig((?:\s+\"[\w.]+\")+)")


def unrendered_sections(repo: str) -> set[str]:
    path = os.path.join(repo, TEMPLATES, "configmaps.yaml")
    if not os.path.exists(path):
        return set()
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    return {name for m in OMIT_RE.finditer(text) for name in re.findall(r'"([\w.]+)"', m.group(1))}


def scan_helm_values(repo: str, truth: Truth) -> list[Finding]:
    findings: list[Finding] = []
    omitted = unrendered_sections(repo)
    for key, value in sorted(chart_config(repo, truth).items()):
        k = truth.keys.get(key)
        if k is None:
            findings.append(Finding("helm-values", "unknown-key", key, None, value, VALUES,
                                    "not a config key: the binaries ignore it"))
            continue
        if key.split(".")[0] in omitted:
            findings.append(Finding("helm-values", "not-rendered", key, k.default, value, VALUES,
                                    f"templates/configmaps.yaml omits `{key.split('.')[0]}` from the rendered config"))
        if not type_ok(value, k.type):
            findings.append(Finding("helm-values", "type", key, k.default, value, VALUES, f"expected {k.type}"))
            continue
        if not same(value, k.default, k.type):
            note = "the binaries ignore this key in the config file" if k.file == "ignored" else ""
            findings.append(Finding("helm-values", "value", key, k.default, value, VALUES, note))
    return findings


def scan_helm_schema(repo: str, truth: Truth) -> list[Finding]:
    with open(os.path.join(repo, SCHEMA), encoding="utf-8") as fh:
        schema = json.load(fh)
    findings: list[Finding] = []

    def walk(node: dict, prefix: str) -> None:
        for name, prop in (node.get("properties") or {}).items():
            path = f"{prefix}.{name}" if prefix else name
            if "properties" in prop:
                walk(prop, path)
            if "default" not in prop:
                continue
            k = truth.keys.get(path)
            if k is None:
                findings.append(Finding("helm-schema", "unknown-key", path, None, prop["default"], SCHEMA))
            elif not same(prop["default"], k.default, k.type):
                findings.append(Finding("helm-schema", "value", path, k.default, prop["default"], SCHEMA))

    walk((schema.get("properties") or {}).get("lakehouseConfig") or {}, "")
    return findings


TEMPLATE_PATTERNS = [
    # dig "jaeger_enabled" false $signalVals.config  /  .Values.traces.config
    (re.compile(r'dig\s+"(\w+)"\s+("[^"]*"|true|false|-?\d+(?:\.\d+)?)\s+(?:\$signalVals|\$?\.Values\.(logs|traces))\.config\b'), "signal"),
    # dig "profile" "" $.Values.lakehouseConfig
    (re.compile(r'dig\s+"(\w+)"\s+("[^"]*"|true|false|-?\d+(?:\.\d+)?)\s+\$\.Values\.lakehouseConfig\b(?!\.)'), "root"),
    # default "5s" $.Values.lakehouseConfig.shutdown.delay
    (re.compile(r'default\s+("[^"]*"|true|false|-?\d+(?:\.\d+)?)\s+\$\.Values\.lakehouseConfig\.([\w.]+)'), "path"),
]


def template_literal(text: str) -> Any:
    return json.loads(text)


def scan_helm_templates(repo: str, truth: Truth) -> tuple[list[Finding], dict[str, list[str]]]:
    """Drifting template fallbacks, plus every fallback literal per key."""
    findings: list[Finding] = []
    values: dict[str, list[str]] = {}
    seen: set[tuple[str, str]] = set()
    for path in sorted(glob.glob(os.path.join(repo, TEMPLATES, "*"))):
        rel = os.path.relpath(path, repo)
        with open(path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        for lineno, line in enumerate(lines, start=1):
            for regex, shape in TEMPLATE_PATTERNS:
                for m in regex.finditer(line):
                    if shape == "signal":
                        name, literal, signal = m.group(1), m.group(2), m.group(3)
                        candidates = [f"{signal}.{name}"] if signal else [f"logs.{name}", f"traces.{name}"]
                        key = next((c for c in candidates if c in truth.keys), None)
                    elif shape == "root":
                        name, literal = m.group(1), m.group(2)
                        key = name if name in truth.keys else None
                    else:
                        literal, key = m.group(1), m.group(2)
                        key = key if key in truth.keys else None
                    if key is None:
                        continue  # a chart-only input, not a config key
                    value = template_literal(literal)
                    k = truth.keys[key]
                    where = f"{rel}:{lineno}"
                    if (key, where) in seen:
                        continue
                    seen.add((key, where))
                    values.setdefault(key, []).append(f"{os.path.basename(rel)}:{lineno}={render(value)}")
                    if same(value, k.default, k.type):
                        continue
                    findings.append(Finding("helm-template", "value", key, k.default, value, where,
                                            "fallback used when the value is unset"))
    return findings, values


HINT_RE = re.compile(r"default:\s*([^);]+)[);]")


def usage_default(usage: str) -> str | None:
    """The single value a usage string claims as default, or None."""
    m = HINT_RE.search(usage)
    if not m:
        return None
    text = m.group(1).strip()
    if "/" in text and not text.startswith(("/", "{")):
        return None  # per-signal: "5MB logs / 8MB traces"
    return text.split()[0] if text else None


def flag_value_as_key(flag: Flag, value: str) -> str:
    """-lakehouse.cache.memory-mb=512 writes cache.memory_limit="512MB"."""
    if flag.name.endswith("-mb") and re.fullmatch(r"\d+", value):
        return f"{value}MB"
    return value


def scan_flag_usage(truth: Truth) -> list[Finding]:
    findings: list[Finding] = []
    for name, flag in sorted(truth.flags.items()):
        hint = usage_default(flag.usage)
        if hint is None or not flag.keys:
            continue
        k = truth.keys[flag.keys[0]]
        if not same(flag_value_as_key(flag, hint), k.default, k.type):
            findings.append(Finding("flags", "value", k.key, k.default, hint,
                                    f"-{name} ({', '.join(flag.binaries)})", "usage string claims a different default"))
    return findings


def markdown_files(repo: str) -> list[str]:
    files = glob.glob(os.path.join(repo, "docs", "**", "*.md"), recursive=True)
    readme = os.path.join(repo, "README.md")
    if os.path.exists(readme):
        files.append(readme)
    return sorted(files)


BEGIN_RE = re.compile(r"<!-- BEGIN GENERATED: ([\w-]+)((?: [\w.]+)*) -->")
END_RE = re.compile(r"<!-- END GENERATED: ([\w-]+) -->")
CODE_SPAN_RE = re.compile(r"`([^`]+)`")
KEY_PATH_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z0-9_]+)+$")
FILE_SUFFIXES = (".go", ".md", ".yaml", ".yml", ".json", ".sh", ".py", ".txt", ".proto", ".parquet", ".tpl", ".bin")
FENCE_RE = re.compile(r"^```(\w*)\s*$")
# -lakehouse.x / --lakehouse.x as typed on a command line; not part of a URL or a path.
FLAG_ARG_RE = re.compile(r"(?<![\w/.-])--?(lakehouse\.[a-z0-9][a-z0-9.\-]*[a-z0-9])")


def split_row(line: str) -> list[str]:
    cells = [c.strip() for c in re.split(r"(?<!\\)\|", line.strip())]
    return cells[1:-1] if len(cells) >= 2 else []


def doc_default_cell(cell: str) -> tuple[bool, Any]:
    """(verifiable, value) for a Default table cell: the value is its leading code span."""
    cell = cell.strip()
    if not cell.startswith("`"):
        return False, None
    value = CODE_SPAN_RE.match(cell).group(1).strip()
    if value in ('""', "''"):
        return True, ""
    return True, value


def unknown_flags(text: str, truth: Truth, where: str) -> list[Finding]:
    return [Finding("docs", "unknown-flag", m.group(1), None, m.group(0), where, "no such flag in either binary")
            for m in FLAG_ARG_RE.finditer(text) if m.group(1) not in truth.flags]


def scan_markdown(repo: str, truth: Truth) -> tuple[list[Finding], dict[str, list[str]]]:
    """Findings plus, per key, the docs locations and values that document it."""
    findings: list[Finding] = []
    mentions: dict[str, list[str]] = {}
    sections = truth.sections_of()
    for path in markdown_files(repo):
        rel = os.path.relpath(path, repo)
        with open(path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        generated = None
        header: list[str] | None = None
        fence: str | None = None
        block: list[str] = []
        block_start = 0
        for lineno, line in enumerate(lines, start=1):
            if generated:
                if END_RE.search(line):
                    generated = None
                continue
            m = BEGIN_RE.search(line)
            if m:
                generated = m.group(1)
                continue
            if fence is not None:
                if line.strip() == "```":
                    if fence in ("yaml", "yml"):
                        findings.extend(scan_yaml_example("\n".join(block), truth, f"{rel}:{block_start}"))
                    fence, block = None, []
                else:
                    block.append(line)
                    findings.extend(unknown_flags(line, truth, f"{rel}:{lineno}"))
                continue
            fm = FENCE_RE.match(line.strip())
            if fm:
                fence, block, block_start = fm.group(1).lower(), [], lineno + 1
                continue
            if not line.lstrip().startswith("|"):
                header = None
                continue
            cells = split_row(line)
            if header is None:
                header = [c.lower() for c in cells]
                continue
            if all(set(c) <= set(":- ") for c in cells):
                continue
            findings.extend(unknown_flags(line, truth, f"{rel}:{lineno}"))
            findings.extend(scan_table_row(cells, header, truth, sections, f"{rel}:{lineno}", mentions))
    return findings, mentions


def scan_table_row(cells: list[str], header: list[str], truth: Truth, sections: set[str], where: str,
                   mentions: dict[str, list[str]]) -> list[Finding]:
    findings: list[Finding] = []
    if not cells:
        return findings
    first = CODE_SPAN_RE.match(cells[0])
    subject = first.group(1).strip() if first else ""
    default_col = next((i for i, h in enumerate(header) if h.startswith("default")), None)
    key: str | None = None
    flag: Flag | None = None
    name = subject.lstrip("-")
    if name.startswith("lakehouse.") and FLAG_ARG_RE.fullmatch("-" + name):
        flag = truth.flags.get(name)
        if flag is None or not flag.keys:
            return findings  # unknown flags are reported by unknown_flags
        key = flag.keys[0]
    elif (KEY_PATH_RE.match(subject) and subject.split(".")[0] in sections
          and not subject.endswith(FILE_SUFFIXES)):
        if subject not in truth.keys:
            findings.append(Finding("docs", "unknown-key", subject, None, subject, where, "no such config key"))
            return findings
        key = subject
    if key is None or default_col is None or default_col >= len(cells):
        return findings
    k = truth.keys[key]
    verifiable, value = doc_default_cell(cells[default_col])
    if not verifiable:
        findings.append(Finding("docs", "unverifiable", key, k.default, cells[default_col], where,
                                "spell the default as a code span so it can be checked"))
        return findings
    mentions.setdefault(key, []).append(f"{where}={value}")
    shown = flag_value_as_key(flag, value) if flag else value
    if not same(shown, k.default, k.type):
        findings.append(Finding("docs", "value", key, k.default, value, where))
    return findings


# Top-level names that are both config sections and Helm chart structure
# (`select:` replicas, `logs:` signal toggles): a bare block of them is chart YAML.
CHART_STRUCTURE = {"insert", "select", "logs", "traces"}


# A docs YAML line may set a key the loader ignores only if it says so.
NOT_READ_MARKER = "not read from the config file in this release"


def yaml_key_lines(text: str, root_key: str | None, truth: Truth) -> dict[str, int]:
    """0-based line of every config key path in a YAML document."""
    out: dict[str, int] = {}

    def walk(node: Any, prefix: str) -> None:
        if not isinstance(node, yaml.MappingNode):
            return
        for key_node, value_node in node.value:
            path = f"{prefix}.{key_node.value}" if prefix else str(key_node.value)
            out[path] = key_node.start_mark.line
            if path not in truth.keys:
                walk(value_node, path)

    root = yaml.compose(text)
    if root_key is not None and isinstance(root, yaml.MappingNode):
        root = next((v for k, v in root.value if k.value == root_key), None)
    walk(root, "")
    return out


def scan_yaml_example(text: str, truth: Truth, where: str) -> list[Finding]:
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError:
        return []
    if not isinstance(doc, dict) or not doc:
        return []
    root_key = "lakehouse" if "lakehouse" in doc else "lakehouseConfig" if "lakehouseConfig" in doc else None
    if root_key is not None:
        root = doc.get(root_key)
    elif set(doc) <= truth.sections_of() - CHART_STRUCTURE:
        root = doc  # a fragment of the lakehouse: document, e.g. `pmeta:` or `compaction:`
    else:
        return []
    if not isinstance(root, dict):
        return []
    file, start = where.rsplit(":", 1)
    lines = text.splitlines()
    key_lines = yaml_key_lines(text, root_key, truth)
    findings = []
    for f in check_config_mapping(root, truth, "docs", where):
        line = key_lines.get(f.key)
        if line is not None:
            f.where = f"{file}:{int(start) + line}"
        if f.kind == "ignored" and line is not None and NOT_READ_MARKER in lines[line]:
            continue
        findings.append(f)
    for path, line in sorted(key_lines.items()):
        k = truth.keys.get(path)
        if NOT_READ_MARKER in lines[line] and k is not None and k.file != "ignored":
            findings.append(Finding("docs", "stale-marker", path, k.default, NOT_READ_MARKER,
                                    f"{file}:{int(start) + line}", "the binaries now read this key from the config file"))
    return findings


def check_config_mapping(root: dict, truth: Truth, surface: str, where: str) -> list[Finding]:
    findings: list[Finding] = []
    for key, value in sorted(flatten_config(root, truth).items()):
        k = truth.keys.get(key)
        if k is None:
            findings.append(Finding(surface, "unknown-key", key, None, value, where, "no such config key"))
        elif not type_ok(value, k.type):
            findings.append(Finding(surface, "type", key, k.default, value, where, f"the binaries expect {k.type}"))
        elif k.file == "ignored":
            findings.append(Finding(surface, "ignored", key, k.default, value, where,
                                    f"the binaries ignore this key in the config file; say so with `# {NOT_READ_MARKER}`"
                                    if surface == "docs" else "the binaries ignore this key in the config file"))
    return findings


def scan_docker(repo: str, truth: Truth) -> tuple[list[Finding], dict[str, list[str]]]:
    findings: list[Finding] = []
    values: dict[str, list[str]] = {}
    for path in sorted(glob.glob(os.path.join(repo, DOCKER_CONFIGS))):
        rel = os.path.relpath(path, repo)
        doc = load_yaml(path) or {}
        root = doc.get("lakehouse") if isinstance(doc, dict) else None
        if not isinstance(root, dict):
            findings.append(Finding("docker", "missing-root", rel, None, sorted(doc) if isinstance(doc, dict) else doc,
                                    rel, "no `lakehouse:` root: the binaries ignore every key in this file"))
            continue
        findings.extend(check_config_mapping(root, truth, "docker", rel))
        for key, value in flatten_config(root, truth).items():
            values.setdefault(key, []).append(f"{os.path.basename(rel)}={render(value)}")
    for path in sorted(glob.glob(os.path.join(repo, COMPOSE_FILES))):
        rel = os.path.relpath(path, repo)
        with open(path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        for lineno, line in enumerate(lines, start=1):
            if line.lstrip().startswith("#"):
                continue
            for m in FLAG_ARG_RE.finditer(line):
                if m.group(1) not in truth.flags:
                    findings.append(Finding("docker", "unknown-flag", m.group(1), None, m.group(0), f"{rel}:{lineno}",
                                            "no such flag in either binary"))
    return findings, values


# ---------------------------------------------------------------------------
# which keys the binaries actually read
# ---------------------------------------------------------------------------

GO_SOURCE_ROOTS = ("cmd", "internal", "lakehouse-traces")
CONFIG_PACKAGE = "internal/config"
# internal/config functions that touch every field without using it: building
# defaults and profiles, merging, validating, and describing the surface.
NON_READERS = re.compile(
    r"^(Default|mergeConfig|MergeConfigs|ProfileConfig|balancedConfig|maxPerformanceConfig|maxDurabilityConfig|"
    r"maxCostSavingsConfig|devConfig|Validate\w*|validate\w*)$"
)
GO_FUNC_RE = re.compile(r"^func (?:\([^)]*\) )?(\w+)\(", re.MULTILINE)
SELECTOR_RE = re.compile(r"\.([A-Z]\w*)\b")


def go_sources(repo: str, config_package: bool) -> list[str]:
    out = []
    for root in GO_SOURCE_ROOTS:
        for dirpath, dirnames, filenames in os.walk(os.path.join(repo, root)):
            dirnames[:] = sorted(d for d in dirnames if d not in ("deps", "testdata"))
            rel = os.path.relpath(dirpath, repo)
            in_config = rel == CONFIG_PACKAGE or rel.startswith(CONFIG_PACKAGE + os.sep)
            if in_config != config_package:
                continue
            for name in sorted(filenames):
                if name.endswith(".go") and not name.endswith("_test.go") and name != "surface.go":
                    with open(os.path.join(dirpath, name), encoding="utf-8") as fh:
                        out.append(fh.read())
    return out


def unread_keys(repo: str, truth: Truth) -> set[str]:
    """Keys no binary reads: their Go field is never selected outside
    internal/config, and no internal/config function that reads it (directly
    or through another function) is called from outside. The scan can only
    err towards "read" (a field name shared by several structs), never towards
    "unread"."""
    outside = "\n".join(go_sources(repo, config_package=False))
    outside_selectors = set(SELECTOR_RE.findall(outside))
    bodies: dict[str, str] = {}
    for text in go_sources(repo, config_package=True):
        starts = list(GO_FUNC_RE.finditer(text))
        for i, m in enumerate(starts):
            end = starts[i + 1].start() if i + 1 < len(starts) else len(text)
            if not NON_READERS.match(m.group(1)):
                bodies[m.group(1)] = bodies.get(m.group(1), "") + text[m.start():end]
    reads = {name: set(SELECTOR_RE.findall(body)) for name, body in bodies.items()}
    calls = {name: {c for c in re.findall(r"\b(\w+)\(", body) if c in bodies and c != name}
             for name, body in bodies.items()}
    changed = True
    while changed:
        changed = False
        for name in bodies:
            for callee in calls[name]:
                if not reads[callee] <= reads[name]:
                    reads[name] |= reads[callee]
                    changed = True
    via_methods: set[str] = set()
    for name in bodies:
        if re.search(rf"\.{re.escape(name)}\(", outside):
            via_methods |= reads[name]
    return {k.key for k in truth.keys.values()
            if k.ident and k.ident not in outside_selectors and k.ident not in via_methods}


# ---------------------------------------------------------------------------
# the report
# ---------------------------------------------------------------------------


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)
    allowlisted: list[tuple[Finding, Override]] = field(default_factory=list)
    stale: list[Override] = field(default_factory=list)
    stale_docs: list[str] = field(default_factory=list)
    docs_mentions: dict[str, list[str]] = field(default_factory=dict)
    docker_values: dict[str, list[str]] = field(default_factory=dict)
    template_values: dict[str, list[str]] = field(default_factory=dict)
    unread: set[str] = field(default_factory=set)

    def ok(self) -> bool:
        return not self.findings and not self.stale and not self.stale_docs


def build_report(repo: str, truth: Truth, overrides: list[Override]) -> Report:
    report = Report()
    raw: list[Finding] = []
    raw += scan_helm_values(repo, truth)
    raw += scan_helm_schema(repo, truth)
    template_findings, report.template_values = scan_helm_templates(repo, truth)
    raw += template_findings
    raw += scan_flag_usage(truth)
    md_findings, report.docs_mentions = scan_markdown(repo, truth)
    raw += md_findings
    docker_findings, report.docker_values = scan_docker(repo, truth)
    raw += docker_findings

    used: set[int] = set()
    for f in raw:
        match = next((o for o in overrides if override_matches(o, f, truth)), None)
        if match is None:
            report.findings.append(f)
        else:
            used.add(id(match))
            report.allowlisted.append((f, match))
    report.stale = [o for o in overrides if id(o) not in used]
    report.unread = truth.unread
    report.stale_docs = stale_generated_blocks(repo, truth)
    return report


def md_cell(text: Any) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def format_findings(report: Report) -> str:
    lines: list[str] = []
    if report.findings:
        lines.append(f"UNEXPLAINED DRIFT ({len(report.findings)}):")
        lines.append("")
        lines.append("| Surface | Kind | Key | Code (truth) | Surface value | Where | Note |")
        lines.append("|---|---|---|---|---|---|---|")
        for f in sorted(report.findings, key=lambda f: (f.surface, f.key, f.where)):
            lines.append(
                f"| {f.surface} | {f.kind} | `{md_cell(f.key)}` | `{md_cell(render(f.code))}` | "
                f"`{md_cell(render(f.found))}` | {md_cell(f.where)} | {md_cell(f.note)} |"
            )
        lines.append("")
    else:
        lines.append("no unexplained drift")
    if report.allowlisted:
        lines.append(f"deliberate overrides ({len(report.allowlisted)}):")
        for f, o in sorted(report.allowlisted, key=lambda p: p[0].ident()):
            lines.append(f"  {f.ident()} = {render(f.found)} (code {render(f.code)}) — {o.reason}")
    if report.stale:
        lines.append(f"STALE OVERRIDES ({len(report.stale)}) — the chart value or the code default changed; "
                     f"re-justify or delete them in {ALLOWLIST}:")
        for o in report.stale:
            lines.append(f"  line {o.line}: {o.ident()} = {render(o.found)} (code {render(o.code)})")
    if report.stale_docs:
        lines.append(f"STALE GENERATED DOCS — run `{REGENERATE}`: " + ", ".join(report.stale_docs))
    if report.unread:
        lines.append(f"keys no binary reads ({len(report.unread)}, informational): " + ", ".join(sorted(report.unread)))
    return "\n".join(lines).rstrip() + "\n"


def summary_counts(report: Report) -> dict[str, int]:
    counts: dict[str, int] = {}
    for f in report.findings:
        counts[f.surface] = counts.get(f.surface, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# inventory
# ---------------------------------------------------------------------------


def build_inventory(repo: str, truth: Truth, report: Report) -> str:
    chart = chart_config(repo, truth)
    schema_defaults: dict[str, Any] = {}
    with open(os.path.join(repo, SCHEMA), encoding="utf-8") as fh:
        schema = json.load(fh)

    def walk(node: dict, prefix: str) -> None:
        for name, prop in (node.get("properties") or {}).items():
            path = f"{prefix}.{name}" if prefix else name
            if "default" in prop:
                schema_defaults[path] = prop["default"]
            walk(prop, path)

    walk((schema.get("properties") or {}).get("lakehouseConfig") or {}, "")
    drifting = {f.key for f in report.findings} | {f.key for f, _ in report.allowlisted}
    profile_names = [p for p in PROFILE_ORDER if p in truth.profiles and p != "balanced"]

    out: list[str] = []
    out.append("### Keys")
    out.append("")
    head = ["Key", "Type", "Code default", "Config file", "Read by a binary", "Flags"] + profile_names + [
        "Helm values", "Helm schema default", "Helm template fallback", "Docs", "deployment/docker", "Drift"]
    out.append("| " + " | ".join(head) + " |")
    out.append("|" + "---|" * len(head))
    for name in sorted(truth.keys):
        k = truth.keys[name]
        flags = sorted({f for fl in k.flags.values() for f in fl})
        row = [f"`{name}`", k.type, f"`{md_cell(render(k.default))}`", k.file,
               "no" if name in report.unread else "yes", md_cell(", ".join(f"-{f}" for f in flags))]
        for p in profile_names:
            v = truth.profiles[p].get(name, None)
            row.append(f"`{md_cell(render(v))}`" if name in truth.profiles[p] else "")
        row.append(f"`{md_cell(render(chart[name]))}`" if name in chart else "")
        row.append(f"`{md_cell(render(schema_defaults[name]))}`" if name in schema_defaults else "")
        row.append(md_cell("; ".join(report.template_values.get(name, []))))
        row.append(md_cell("; ".join(report.docs_mentions.get(name, []))))
        row.append(md_cell("; ".join(report.docker_values.get(name, []))))
        row.append("yes" if name in drifting else "")
        out.append("| " + " | ".join(row) + " |")
    out.append("")
    out.append("### Flags")
    out.append("")
    out.append("| Flag | Binaries | Key | Effect | Flag default | Usage default hint | Code default of the key | Drift |")
    out.append("|---|---|---|---|---|---|---|---|")
    flag_drift = {f.where.split(" ")[0].lstrip("-") for f in report.findings if f.surface == "flags"}
    for name in sorted(truth.flags):
        f = truth.flags[name]
        key = f.keys[0] if f.keys else ""
        code = render(truth.keys[key].default) if key else ""
        binaries = "both" if len(f.binaries) == 2 else f.binaries[0].replace("lakehouse-", "")
        out.append(
            f"| `-{name}` | {binaries} | {'`' + key + '`' if key else ''} | {f.effect} | `{md_cell(f.default)}` | "
            f"{md_cell(usage_default(f.usage) or '')} | {'`' + md_cell(code) + '`' if key else ''} | "
            f"{'yes' if name in flag_drift else ''} |"
        )
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# generated documentation
# ---------------------------------------------------------------------------

UNREAD_NOTE = "**Not read.**"

FILE_MERGE_LEGEND = {
    "set": "a non-zero, non-empty value replaces the profile value; `0` and empty values are treated as unset",
    "enable-only": "`true` replaces the profile value; `false` is treated as unset, so it cannot turn off a key the profile enables",
    "disable-only": "`false`, or leaving the key out of a config file, sets `false`; `true` keeps the profile value",
    "replace": "the file value always replaces the profile value",
    "reset": "loading any config file resets the key to its zero value",
    "ignored": "the value in the config file is ignored; the profile value always applies",
}
FLAG_EFFECT_LEGEND = {
    "set": "a non-zero value overrides the key; the zero value leaves the loaded value alone",
    "enable-only": "`true` sets the key; `false` leaves it unchanged",
    "disable-only": "`false` sets the key; `true` leaves it unchanged",
    "authoritative": "the flag value, including its default, always replaces the loaded value",
    "none": "writes no config key",
}

def summarize_doc(doc: str, ident: str = "") -> str:
    """First sentence of a Go doc comment. A comment that opens with its Go
    identifier ("ReadAheadMaxBytes is the ceiling ...") loses it
    ("The ceiling ...")."""
    if not doc:
        return ""
    para = doc.split("\n\n")[0].strip()
    sentence = re.split(r"(?<=[.!?])\s+(?=[A-Z`(\"])", para, maxsplit=1)[0].strip()
    if ident:
        m = re.match(rf"^{re.escape(ident)}\s+(?:is\s+|are\s+)?(\S.*)$", sentence)
        if m:
            sentence = m.group(1)
    return sentence[:1].upper() + sentence[1:]


def esc(text: str) -> str:
    return (text.replace("|", "\\|").replace("<", "&lt;").replace(">", "&gt;").replace("\n", " "))


def code(value: Any) -> str:
    text = render(value).replace("|", "\\|")
    return f"`{text}`"


def flag_label(flag: Flag) -> str:
    who = "" if len(flag.binaries) == 2 else f" ({flag.binaries[0].replace('lakehouse-', '')} only)"
    effect = "" if flag.effect == "set" else f" ({flag.effect})"
    return f"`-{flag.name}`{effect}{who}"


def gen_reference(truth: Truth) -> str:
    lines = [
        # Explicit +: two adjacent literals concatenate implicitly, which reads
        # exactly like a list whose comma was forgotten.
        ("The **Config file** column says how a value written in the `--lakehouse.config` file is merged over the "
         + "profile the file selects:"),
        "",
    ]
    for name, text in FILE_MERGE_LEGEND.items():
        if any(k.file == name for k in truth.keys.values()):
            lines.append(f"- `{name}` — {text}.")
    if truth.unread:
        lines += ["", f"{UNREAD_NOTE} marks a key that neither binary reads in this release: setting it has no effect."]
    lines.append("")
    groups: dict[str, list[Key]] = {}
    for name in sorted(truth.keys):
        section = name.split(".")[0] if "." in name else ""
        groups.setdefault(section, []).append(truth.keys[name])
    profile_names = [p for p in PROFILE_ORDER if p != "balanced"]
    for section in sorted(groups, key=lambda s: (s != "", s)):
        title = f"`{section}`" if section else "Top-level keys"
        lines.append(f"### {title}")
        lines.append("")
        entry = truth.sections.get(section) or {}
        doc = summarize_doc(entry.get("doc", ""), entry.get("name", ""))
        if doc:
            lines.append(esc(doc))
            lines.append("")
        lines.append("| Key | Type | Default | Config file | Flags | Profile overrides | Description |")
        lines.append("|---|---|---|---|---|---|---|")
        for k in groups[section]:
            flags = sorted({f for fl in k.flags.values() for f in fl})
            flag_cells = ", ".join(flag_label(truth.flags[f]) for f in flags)
            profiles = "; ".join(
                f"{p}: {code(truth.profiles[p][k.key])}" for p in profile_names if k.key in truth.profiles.get(p, {})
            )
            description = esc(summarize_doc(k.doc, k.ident))
            if k.key in truth.unread:
                description = f"{UNREAD_NOTE} {description}".rstrip()
            lines.append(
                f"| `{k.key}` | {esc(k.type)} | {code(k.default)} | {k.file} | {flag_cells} | {profiles} | "
                f"{description} |"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def gen_flags(truth: Truth) -> str:
    lines = ["The **Effect** column says what a flag does to the config key it writes:", ""]
    for name, text in FLAG_EFFECT_LEGEND.items():
        if any(f.effect == name for f in truth.flags.values()):
            lines.append(f"- `{name}` — {text}.")
    lines += [
        "",
        "| Flag | Binaries | Sets | Effect | Flag default | Description |",
        "|---|---|---|---|---|---|",
    ]
    for name in sorted(truth.flags):
        f = truth.flags[name]
        binaries = "both" if len(f.binaries) == 2 else f.binaries[0].replace("lakehouse-", "")
        sets = ", ".join(f"`{k}`" for k in f.keys)
        lines.append(f"| `-{name}` | {binaries} | {sets} | {f.effect} | `{esc(f.default) or ' '}` | {esc(f.usage)} |")
    return "\n".join(lines) + "\n"


def gen_profiles(truth: Truth) -> str:
    names = [p for p in PROFILE_ORDER if p != "balanced" and p in truth.profiles]
    keys = sorted({k for p in names for k in truth.profiles[p]})
    lines = [
        # Explicit +, same reason as gen_reference above.
        ("`balanced` is the built-in defaults and overrides nothing. Every other profile is exactly the set of keys "
         + "below; an empty cell means the profile keeps the default."),
        "",
        "| Key | Default | " + " | ".join(f"`{p}`" for p in names) + " |",
        "|---|---|" + "---|" * len(names),
    ]
    for key in keys:
        cells = [code(truth.profiles[p][key]) if key in truth.profiles[p] else "" for p in names]
        label = f"`{key}` (not read)" if key in truth.unread else f"`{key}`"
        lines.append(f"| {label} | {code(truth.keys[key].default)} | " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def gen_profile_flag_gaps(truth: Truth) -> str:
    per_profile: dict[str, list[str]] = {}
    for binary in sorted(truth.gaps):
        for profile, keys in truth.gaps[binary].items():
            known = per_profile.setdefault(profile, [])
            for k in keys:
                if k not in known:
                    known.append(k)
    lines = [
        "| Profile | Keys not applied by `--lakehouse.profile` | Count |",
        "|---|---|---|",
    ]
    for profile in PROFILE_ORDER:
        if profile not in per_profile:
            continue
        keys = sorted(per_profile[profile])
        lines.append(f"| `{profile}` | {', '.join(f'`{k}`' for k in keys) or '—'} | {len(keys)} |")
    return "\n".join(lines) + "\n"


SUMMARY_COLUMNS = [
    ("insert.ack_mode", "Ack mode"),
    ("insert.flush_interval", "Flush interval"),
    ("insert.compression_level", "zstd level"),
    ("cache.memory_limit", "Cache memory"),
    ("cache.disk_limit", "Cache disk"),
    ("compaction.enabled", "Compaction"),
    ("gc.enabled", "GC"),
    ("retention.enabled", "Retention"),
    ("stats.enabled", "Stats"),
    ("cross_signal.enabled", "Cross-signal"),
]


def gen_profile_summary(truth: Truth) -> str:
    columns = [(key, label + (" (not read)" if key in truth.unread else "")) for key, label in SUMMARY_COLUMNS
               if key in truth.keys]
    lines = [
        "| Profile | " + " | ".join(label for _, label in columns) + " |",
        "|---|" + "---|" * len(columns),
    ]
    for profile in PROFILE_ORDER:
        if profile not in truth.profiles:
            continue
        cells = []
        for key, _ in columns:
            value = truth.profiles[profile].get(key, truth.keys[key].default)
            if isinstance(value, bool):
                cells.append("on" if value else "off")
            else:
                cells.append(render(value))
        lines.append(f"| `{profile}` | " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def gen_keys(truth: Truth, keys: list[str]) -> str:
    """A reference table for the listed keys, for topic pages."""
    if not keys:
        raise ValueError("config-keys needs at least one key")
    unknown = [k for k in keys if k not in truth.keys]
    if unknown:
        raise ValueError(f"config-keys lists unknown keys: {', '.join(unknown)}")
    lines = [
        "| Key | Default | Config file | Flags | Description |",
        "|---|---|---|---|---|",
    ]
    for name in keys:
        k = truth.keys[name]
        flags = sorted({f for fl in k.flags.values() for f in fl})
        description = esc(summarize_doc(k.doc, k.ident))
        if name in truth.unread:
            description = f"{UNREAD_NOTE} {description}".rstrip()
        lines.append(f"| `{name}` | {code(k.default)} | {k.file} | "
                     f"{', '.join(flag_label(truth.flags[f]) for f in flags)} | {description} |")
    return "\n".join(lines) + "\n"


GENERATORS = {
    "config-reference": lambda truth, args: gen_reference(truth),
    "config-flags": lambda truth, args: gen_flags(truth),
    "config-profiles": lambda truth, args: gen_profiles(truth),
    "config-profile-flag-gaps": lambda truth, args: gen_profile_flag_gaps(truth),
    "config-profile-summary": lambda truth, args: gen_profile_summary(truth),
    "config-keys": gen_keys,
}


def splice_blocks(text: str, truth: Truth) -> str:
    """Rewrite every generated block in a markdown document."""
    out: list[str] = []
    pos = 0
    for m in BEGIN_RE.finditer(text):
        if m.start() < pos:
            continue
        name = m.group(1)
        if name not in GENERATORS:
            raise ValueError(f"unknown generated block {name!r}")
        end = re.compile(rf"<!-- END GENERATED: {re.escape(name)} -->").search(text, m.end())
        if end is None:
            raise ValueError(f"generated block {name!r} has no END marker")
        out.append(text[pos:m.end()])
        out.append(f"\n<!-- Generated by `{REGENERATE}` from the code; do not edit. -->\n\n")
        out.append(GENERATORS[name](truth, m.group(2).split()).rstrip() + "\n\n")
        pos = end.start()
    out.append(text[pos:])
    return "".join(out)


def generated_docs(repo: str) -> list[str]:
    out = []
    for path in markdown_files(repo):
        with open(path, encoding="utf-8") as fh:
            if BEGIN_RE.search(fh.read()):
                out.append(path)
    return out


def stale_generated_blocks(repo: str, truth: Truth) -> list[str]:
    stale = []
    for path in generated_docs(repo):
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        if splice_blocks(text, truth) != text:
            stale.append(os.path.relpath(path, repo))
    return stale


def write_docs(repo: str, truth: Truth) -> list[str]:
    changed = []
    for path in generated_docs(repo):
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        updated = splice_blocks(text, truth)
        if updated != text:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(updated)
            changed.append(os.path.relpath(path, repo))
    return changed


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", default=REPO_ROOT, help="repository root")
    parser.add_argument("--allowlist", default=None, help=f"override file (default: {ALLOWLIST})")
    parser.add_argument("--check", action="store_true", help="exit 1 on unexplained drift or stale generated docs")
    parser.add_argument("--write-docs", action="store_true", help="regenerate the generated docs blocks")
    parser.add_argument("--inventory", metavar="FILE", help="write the full markdown inventory to FILE")
    args = parser.parse_args(list(argv) if argv is not None else None)

    truth = load_truth(args.repo)
    overrides = load_overrides(args.allowlist or os.path.join(args.repo, ALLOWLIST))

    if args.write_docs:
        for rel in write_docs(args.repo, truth):
            print(f"config_drift_report: regenerated {rel}")

    report = build_report(args.repo, truth, overrides)
    sys.stdout.write(format_findings(report))

    if args.inventory:
        with open(args.inventory, "w", encoding="utf-8") as fh:
            fh.write(build_inventory(args.repo, truth, report))

    if not args.check or report.ok():
        return 0
    if report.findings or report.stale:
        print(
            "::error::config drift: the code defaults (print-default-config) are the source of truth. "
            f"Fix the surface value, or record a deliberate chart override with a reason in {ALLOWLIST} "
            "(`override <surface>:<key> <value> (code <value>) — <reason>`). "
            "See docs/configuration.md#configuration-drift-gate.",
            file=sys.stderr,
        )
    if report.stale_docs:
        print(f"::error::generated configuration docs are stale — run `{REGENERATE}` and commit the result.",
              file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
