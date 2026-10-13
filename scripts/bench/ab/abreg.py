"""Shared helpers of the A/B harness: repository paths, the conformance registry as data, small JSON helpers.

The registry is the single source of truth (tests/conformance/registry): rows (one per endpoint or feature
the verification machine checks) and features (what the product ships, with the tests and docs that prove it).
This module reads those YAML files as they are; nothing here keeps a second copy of a row.
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
REGISTRY = os.path.join(ROOT, "tests", "conformance", "registry")
SHAPES = os.path.join(HERE, "shapes.json")
GAPS_DOC = "docs/parity-and-gaps.md"

if ROOT not in sys.path:  # scripts.proof is imported as a package from the repository root
    sys.path.insert(0, ROOT)
if HERE not in sys.path:
    sys.path.insert(0, HERE)


def need_yaml():
    try:
        import yaml  # noqa: PLC0415 - optional until the registry is read
    except ImportError as e:  # pragma: no cover - environment
        raise SystemExit("PyYAML is required to read the conformance registry: python3 -m pip install pyyaml") from e
    return yaml


def _load_list(path: str) -> list:
    yaml = need_yaml()
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if doc is None:
        return []
    if not isinstance(doc, list):
        raise ValueError(f"{path}: a registry file is a list of entries")
    return doc


def load_rows(root: str = REGISTRY) -> list[dict]:
    """Every registry row, each with `_file` (path relative to the repository)."""
    rows = []
    for path in sorted(glob.glob(os.path.join(root, "rows", "**", "*.yaml"), recursive=True)):
        rel = os.path.relpath(path, os.path.dirname(os.path.dirname(os.path.dirname(root))))
        for r in _load_list(path):
            r["_file"] = rel
            rows.append(r)
    return rows


def load_features(root: str = REGISTRY) -> list[dict]:
    feats = []
    for path in sorted(glob.glob(os.path.join(root, "features", "*.yaml"))):
        rel = os.path.relpath(path, os.path.dirname(os.path.dirname(os.path.dirname(root))))
        for f in _load_list(path):
            f["_file"] = rel
            feats.append(f)
    return feats


def load_json(path: str):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def dump_json(path: str, obj, indent: int | None = 1) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=indent, sort_keys=False, default=str)
        f.write("\n")


def write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


ORG = "ReliablyObserve"
# What counts as an issue that tracks a difference: the full URL of an issue of a repository of this organisation, or the repository-prefixed
# reference `ReliablyObserve/victoria-lakehouse#123`. A bare `#123` (it may be an upstream VictoriaLogs issue), a pull request link and an
# issue of another organisation do not count.
ISSUE_URL_RE = re.compile(r"github\.com/ReliablyObserve/([\w.-]+)/issues/(\d+)")
ISSUE_REF_RE = re.compile(r"(?<![\w/.-])ReliablyObserve/([\w.-]+)#(\d+)\b")
GAP_ID_RE = re.compile(r"(?<![\w/])#([A-Z]\d+)\b")


def issue_refs(text: str) -> list[str]:
    """Issues of ReliablyObserve repositories cited in `text`, as `repo#number` (in the order they appear)."""
    found = []
    for m in sorted(list(ISSUE_URL_RE.finditer(text or "")) + list(ISSUE_REF_RE.finditer(text or "")), key=lambda m: m.start()):
        ref = f"{m.group(1)}#{m.group(2)}"
        if ref not in found:
            found.append(ref)
    return found


def gap_ids(text: str) -> list[str]:
    return sorted(set(GAP_ID_RE.findall(text or "")))


def row_text(row: dict) -> str:
    """The prose of a row that can cite an issue or a gap."""
    refs = row.get("refs") or {}
    return " ".join(str(x) for x in (row.get("differ_note"), row.get("notes"), row.get("title"), json.dumps(refs, default=str)) if x)


def gaps_doc_issues(root: str = ROOT) -> dict[str, list[str]]:
    """Gap id (`A6`, `B2`) -> issue references (`repo#number`) cited in the section of docs/parity-and-gaps.md that names it.

    A section starts at a line that mentions the id (heading, table row or list item) and runs to the next
    line that mentions a different gap id; this is deliberately loose, it only has to find the issue links.
    """
    path = os.path.join(root, GAPS_DOC)
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        lines = f.read().splitlines()
    ids_re = re.compile(r"(?<![\w/#])([A-Z]\d{1,2})(?![\w])")
    out: dict[str, list[str]] = {}
    for i, line in enumerate(lines):
        for gid in set(ids_re.findall(line)):
            if not line.lstrip().startswith(("#", "|", "-", "*", "1", "2", "3", "4", "5", "6", "7", "8", "9")):
                continue
            nums = issue_refs(line)
            for j in range(i + 1, min(i + 6, len(lines))):
                if ids_re.search(lines[j]) and set(ids_re.findall(lines[j])) - {gid}:
                    break
                nums += issue_refs(lines[j])
            out.setdefault(gid, [])
            for n in nums:
                if n not in out[gid]:
                    out[gid].append(n)
    return out
