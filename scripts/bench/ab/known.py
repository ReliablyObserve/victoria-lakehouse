#!/usr/bin/env python3
"""Known differences and gaps: the differences a run found, joined with the registry.

Every difference the run finds (a perf cell whose validated answer differs from hot VictoriaLogs/VictoriaTraces,
an API row that does not match the reference) is either

  (a) pre-existing on the base build, and then it must map to a registry row that records it (`expect: differ`,
      `unsupported` or `absent`) with its GitHub issue and/or its docs/parity-and-gaps.md entry, or
  (b) NEW, introduced by this PR (the PR's answer differs from the reference and from the base's), which fails
      the check, or
  (c) UNTRACKED: a difference nobody recorded (the covered rows say `expect: pass`, or there is none). It is
      printed as UNTRACKED and fails the check: owner rule (skill 2b), no silent problems, every difference has
      an issue, a registry row and a line in the PR comment.

A difference the PR fixes is listed too: the registry row it maps to can flip to `expect: pass` with a lock.
`--require-issue` additionally fails a tracked difference whose registry row cites no GitHub issue.

  known.py --perf summary.json --api api-matrix.json [--out known.json] [--require-issue]
"""
from __future__ import annotations

import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import abreg  # noqa: E402

KNOWN_EXPECT = ("differ", "unsupported", "absent")
NOT_A_DIFFERENCE = ("exact", "vacuous", "blocked", "arbitrary", "unusable", "harness-error", "nondeterministic")


def row_tracking(row: dict, gaps: dict[str, list[int]]) -> dict:
    """What a registry row says about its difference: expect, issues (own text + the gap entry it names), gap ids, docs."""
    text = abreg.row_text(row)
    gids = abreg.gap_ids(text)
    issues = abreg.issue_numbers(text)
    for g in gids:
        for n in gaps.get(g, []):
            if n not in issues:
                issues.append(n)
    refs = row.get("refs") or {}
    return {"row": row["id"], "expect": row["expect"], "issues": issues, "gap_ids": gids, "doc": refs.get("doc"),
            "note": (row.get("differ_note") or "")[:200]}


def registry_gap_stats(rows: list[dict], gaps: dict[str, list[int]]) -> dict:
    """Static counts: how many rows record a known difference, and how many of those cite a GitHub issue."""
    known = [row_tracking(r, gaps) for r in rows if r["expect"] in KNOWN_EXPECT]
    no_issue = [k for k in known if not k["issues"]]
    return {"rows": len(known), "with_issue": len(known) - len(no_issue), "without_issue": len(no_issue),
            "without_issue_gap_id_only": sum(1 for k in no_issue if k["gap_ids"]),
            "without_issue_ids": [k["row"] for k in no_issue]}


def observed_from_perf(summary: dict, spec: dict) -> list[dict]:
    """One observation per (shape, difference class) from the perf summary's cells."""
    covers = {s["name"]: s.get("covers", []) for ss in spec["sets"].values() for s in ss["shapes"]}
    groups: dict[tuple, dict] = {}
    for c in summary.get("cells", []):
        if c["difference"] == "none":
            continue
        key = (c["shape"], c["difference"])
        g = groups.setdefault(key, {"source": "perf", "what": c["shape"], "status": c["difference"], "covers": covers.get(c["shape"], []),
                                    "where": [], "signal": c["signal"]})
        g["where"].append(f"{c['layer']}/{c['range']}/{c['form']}")
    return list(groups.values())


def observed_from_api(matrix: dict) -> list[dict]:
    out = []
    for r in matrix.get("results", []):
        v = r["verdict"]
        if v in NOT_A_DIFFERENCE:
            continue  # proves nothing (arbitrary, vacuous, unusable, blocked) or is a failure of the run, not a difference
        if v == "regressed":
            status = "new"
        elif v == "fixed":
            status = "fixed"
        elif (r.get("pr_score") or 0) >= 100 and (r.get("base_score") or 0) >= 100:
            continue
        else:
            status = "pre-existing" if v in ("same", "improved") else "new"
        out.append({"source": "api", "what": f"{r['surface']} {r['path']}", "status": status, "covers": [r["row"]], "where": [r["id"]],
                    "signal": r["signal"], "detail": (r.get("notes") or [""])[0][:140], "verdict": v})
    return out


def join(observed: list[dict], rows: list[dict], gaps: dict[str, list[int]], require_issue: bool = False) -> dict:
    by_id = {r["id"]: r for r in rows}
    items = []
    for o in observed:
        covered = [by_id[c] for c in o["covers"] if c in by_id]
        known = [row_tracking(r, gaps) for r in covered if r["expect"] in KNOWN_EXPECT]
        item = dict(o)
        item["tracked_by"] = known
        if o["status"] == "fixed":
            item["tracking"] = "fixed"
            item["flip"] = [k["row"] for k in known]  # rows that can flip to pass with a lock
        elif o["status"] == "new":
            item["tracking"] = "new"  # introduced by the PR: fails whether or not a row exists for the base's difference
        elif known:
            item["tracking"] = "tracked" if all(k["issues"] for k in known) or not require_issue else "tracked-no-issue"
            item["no_issue"] = [k["row"] for k in known if not k["issues"]]
        else:
            item["tracking"] = "untracked"
        items.append(item)
    counts: dict[str, int] = {}
    for it in items:
        counts[it["tracking"]] = counts.get(it["tracking"], 0) + 1
    failing = [it for it in items if it["tracking"] in ("new", "untracked", "tracked-no-issue")]
    return {"items": items, "counts": counts, "failed": bool(failing), "failing": len(failing)}


def stats_line(stats: dict) -> str:
    return (f"The registry records {stats['rows']} known differences (`expect: differ`, `unsupported`, `absent`); "
            f"{stats['with_issue']} cite a GitHub issue, {stats['without_issue']} do not "
            f"({stats['without_issue_gap_id_only']} of those name a gap id of docs/parity-and-gaps.md only).")


def issue_links(numbers: list[int], repo: str = "ReliablyObserve/victoria-lakehouse") -> str:
    return ", ".join(f"[#{n}](https://github.com/{repo}/issues/{n})" for n in numbers)


def tracked_text(it: dict) -> str:
    parts = []
    for k in it["tracked_by"]:
        s = f"`{k['row']}` (`expect: {k['expect']}`)"
        if k["issues"]:
            s += " " + issue_links(k["issues"])
        elif k["gap_ids"]:
            s += " gap " + ", ".join(k["gap_ids"]) + " (no issue)"
        else:
            s += " (no issue linked)"
        parts.append(s)
    return "; ".join(parts)


def render(known: dict, stats: dict | None = None, max_rows: int = 40) -> list[str]:
    """Markdown lines of the 'Known differences and gaps' section."""
    lines = ["#### Known differences and gaps", ""]
    items = known["items"]
    if not items:
        lines += ["No difference from hot VictoriaLogs / VictoriaTraces in the cells and rows of this run."]
    else:
        order = {"new": 0, "untracked": 1, "tracked-no-issue": 2, "tracked": 3, "fixed": 4}
        items = sorted(items, key=lambda i: (order.get(i["tracking"], 9), i["source"], i["what"]))
        lines += ["| | where | difference | status | tracked as |", "|---|---|---|---|---|"]
        for it in items[:max_rows]:
            icon = {"new": "NEW", "untracked": "UNTRACKED", "tracked-no-issue": "NO ISSUE", "tracked": "known", "fixed": "fixed"}[it["tracking"]]
            where = f"{it['source']} {it['signal']}"
            n = len(it["where"])
            cells = f" ({n} cell{'s' if n != 1 else ''})" if it["source"] == "perf" else ""
            if it["tracking"] == "new":
                st = "**NEW, introduced by this PR (fails the check)**"
                tr = tracked_text(it) or "no registry row records a difference here"
            elif it["tracking"] == "untracked":
                st = "**UNTRACKED (fails the check)**"
                tr = "no registry `differ`/`unsupported`/`absent` row and no issue: open an issue and record the row"
            elif it["tracking"] == "tracked-no-issue":
                st = "**tracked without an issue (fails with --require-issue)**"
                tr = tracked_text(it)
            elif it["tracking"] == "fixed":
                st = "fixed by this PR"
                tr = ("row can flip to `expect: pass` with a lock: " + ", ".join(f"`{x}`" for x in it.get("flip", []))) if it.get("flip") else "no row recorded it"
            else:
                st = "pre-existing on base"
                tr = tracked_text(it)
            lines.append(f"| {icon} | {where} | `{it['what']}`{cells}{(': ' + it['detail']) if it.get('detail') else ''} | {st} | {tr} |")
        if len(items) > max_rows:
            lines.append(f"| | | {len(items) - max_rows} more in the JSON artifact | | |")
    counts = known["counts"]
    lines += ["", "Differences found: " + (", ".join(f"{n} {k}" for k, n in sorted(counts.items())) if counts else "none") + "."]
    if stats:
        lines += ["", stats_line(stats)]
    return lines


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--perf", default="")
    ap.add_argument("--api", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--require-issue", action="store_true")
    a = ap.parse_args(argv)
    rows = abreg.load_rows()
    gaps = abreg.gaps_doc_issues()
    observed = []
    if a.perf:
        observed += observed_from_perf(abreg.load_json(a.perf), abreg.load_json(abreg.SHAPES))
    if a.api:
        observed += observed_from_api(abreg.load_json(a.api))
    known = join(observed, rows, gaps, a.require_issue)
    known["registry"] = registry_gap_stats(rows, gaps)
    if a.out:
        abreg.dump_json(a.out, known)
    print("\n".join(render(known, known["registry"])))
    return 1 if known["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
