#!/usr/bin/env python3
"""Summarise a Semgrep SARIF file as Markdown (the job summary of .github/workflows/semgrep.yaml).

Usage: semgrep_sarif_summary.py <semgrep.sarif>

Prints finding counts by level and the most frequent rules and paths. It never fails on findings:
the Semgrep job is report-only until the baseline is triaged. A missing or unreadable file is an
error (exit 2), so a scan that produced nothing is not reported as "0 findings".
"""
import collections
import json
import sys

LEVELS = ("error", "warning", "note", "none")
TOP = 10


def summarise(doc: dict) -> str:
    levels = collections.Counter()
    rules = collections.Counter()
    paths = collections.Counter()
    for run in doc.get("runs", []):
        for result in run.get("results", []):
            levels[result.get("level", "warning")] += 1
            rules[result.get("ruleId", "unknown")] += 1
            for loc in result.get("locations", [])[:1]:
                uri = loc.get("physicalLocation", {}).get("artifactLocation", {}).get("uri", "")
                paths["/".join(uri.split("/")[:2]) or "unknown"] += 1
    total = sum(levels.values())
    out = ["## Semgrep findings (report only)", "", f"Total findings: **{total}**", "", "| Level | Findings |", "| --- | ---: |"]
    for level in LEVELS:
        out.append(f"| {level} | {levels.get(level, 0)} |")
    for level in sorted(set(levels) - set(LEVELS)):
        out.append(f"| {level} | {levels[level]} |")
    if rules:
        out += ["", f"### Top {TOP} rules", "", "| Rule | Findings |", "| --- | ---: |"]
        out += [f"| `{rule}` | {count} |" for rule, count in rules.most_common(TOP)]
        out += ["", f"### Top {TOP} paths (first two components)", "", "| Path | Findings |", "| --- | ---: |"]
        out += [f"| `{path}` | {count} |" for path, count in paths.most_common(TOP)]
    return "\n".join(out) + "\n"


def main(argv: list) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        with open(argv[1], encoding="utf-8") as handle:
            doc = json.load(handle)
    except (OSError, ValueError) as err:
        print(f"cannot read SARIF {argv[1]}: {err}", file=sys.stderr)
        return 2
    sys.stdout.write(summarise(doc))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
