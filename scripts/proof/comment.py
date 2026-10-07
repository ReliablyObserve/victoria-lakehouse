#!/usr/bin/env python3
"""The PR comment of the API data proof and the visual proof, as Markdown.

  python3 -m scripts.proof.comment --api OUT/api [--visual OUT/visual] [--image-base URL] [--label "PR 438"]
         [--explain explanations.json] [--out comment.md]

The first line is the verdict. Rows that changed (fixed, improved, regressed) and rows that still differ
from the reference are listed; rows that match on both sides are counted. `explanations.json` maps a
request-id prefix to the reason a difference remains, and every remaining difference without one is
listed as unexplained. All percentages come from scripts/proof/metrics; nothing is rounded up to 100.
"""
from __future__ import annotations

import argparse
import os
import re
import sys

from .metrics.common import display_pct
from .jsonio import load_json, write_text

ICON = {"exact": "match", "same": "same", "fixed": "FIXED", "improved": "IMPROVED", "regressed": "REGRESSED",
        "not-reproduced-on-base": "not reproduced on base", "vacuous": "vacuous", "blocked": "blocked",
        "nondeterministic": "NONDETERMINISTIC", "harness-error": "HARNESS ERROR"}


def pct(v) -> str:
    return "-" if v is None else display_pct(v)


def _worst(facets: dict) -> str:
    if not facets:
        return "-"
    k = min(facets, key=lambda x: (facets[x], x))
    return "exact" if facets[k] >= 100 else f"{k} {pct(facets[k])}"


def explanation(rid: str, explain: dict) -> str | None:
    best = None
    for prefix, text in explain.items():
        if rid.startswith(prefix) and (best is None or len(prefix) > len(best[0])):
            best = (prefix, text)
    return best[1] if best else None


def _group(rs: list[dict]) -> list[dict]:
    """Merge requests that differ only in the tenant form (identical numbers and verdict) into one row."""
    out: dict[tuple, dict] = {}
    for r in rs:
        rid = re.sub(rf"\.{r['form']}\.{r['layer']}$", "", r["id"].split("/", 1)[1])
        key = (r["surface"], rid, r["layer"], r["verdict"], pct(r["base_score"]), pct(r["pr_score"]), _worst(r["base_facets"]), _worst(r["pr_facets"]))
        g = out.setdefault(key, {**r, "rid": rid, "forms": []})
        g["forms"].append(r["form"])
    return sorted(out.values(), key=lambda g: (g["surface"], g["rid"], g["layer"]))


def api_section(rep: dict, label: str, explain: dict) -> tuple[list[str], dict]:
    rs = rep["results"]
    counts: dict[str, int] = {}
    for r in rs:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    remaining = [r for r in rs if r["verdict"] not in ("fixed", "vacuous", "blocked", "harness-error") and (r["pr_score"] or 0) < 100]
    lines = [f"### API data proof: base (main) vs {label} vs hot VictoriaLogs / VictoriaTraces", ""]
    lines.append(f"{len(rs)} requests, " + ", ".join(f"{n} {v}" for v, n in sorted(counts.items())) + ". "
                 "Seed equality (rows per tenant form and layer) held on ref, base and PR before any comparison. "
                 "Percentages are the worst quality facet of each answer against the reference (measured; 100 only when exact).")
    lines += ["", f"| request | form | layer | base % | {label} % | worst facet base -> {label} | verdict |", "|---|---|---|--:|--:|---|---|"]
    for r in _group([r for r in rs if not (r["verdict"] == "exact" and (r["base_score"] or 0) >= 100)]):
        lines.append(f"| `{r['rid']}` | {' + '.join(r['forms'])} | {r['layer']} | {pct(r['base_score'])} | {pct(r['pr_score'])} | "
                     f"{_worst(r['base_facets'])} -> {_worst(r['pr_facets'])} | {ICON[r['verdict']]} |")
    exact = sum(1 for r in rs if r["verdict"] == "exact" and (r["base_score"] or 0) >= 100)
    lines += ["", f"{exact} requests match the reference exactly on both sides (not listed)."]
    lines += ["", "#### Remaining differences from the reference", ""]
    if not remaining:
        lines.append("None.")
    else:
        lines += ["| request | form / layer | base % | PR % | what differs | why |", "|---|---|--:|--:|---|---|"]
        for r in _group(remaining):
            notes = (r.get("pr_notes") or [])[:1]
            why = explanation(r["id"].split("/", 1)[1], explain) or "**unexplained**"
            lines.append(f"| `{r['rid']}` | {' + '.join(r['forms'])} / {r['layer']} | {pct(r['base_score'])} | {pct(r['pr_score'])} | "
                         f"{_worst(r['pr_facets'])}{(': ' + notes[0][:90]) if notes else ''} | {why} |")
    return lines, {"counts": counts, "remaining": len(remaining),
                   "unexplained": sum(1 for r in remaining if not explanation(r['id'].split('/', 1)[1], explain))}


def visual_section(cmp: dict, label: str, image_base: str | None, explain: dict) -> list[str]:
    lines = ["", f"### Visual proof: Grafana, VMUI, VTUI and the Jaeger UI, base | {label} | reference", ""]
    lines += [f"| page / range | base % | {label} % | panel state base / {label} / reference | verdict |", "|---|--:|--:|---|---|"]
    shown = {k: r for k, r in cmp.items() if not (r["verdict"] == "match" and (r["pr_score"] or 0) >= 100)}
    for k, r in sorted(shown.items()):
        st = r["states"]
        s = " / ".join(st.get(x, {}).get("state", "-") for x in ("base", "pr", "ref"))
        lines.append(f"| `{k}` | {pct(r['base_score'])} | {pct(r['pr_score'])} | {s} | {r['verdict']} |")
    lines += ["", f"{len(cmp) - len(shown)} page captures match the reference on every question on both sides (not listed)."]
    detail = [(k, r) for k, r in sorted(cmp.items()) if r["verdict"] not in ("match", "same-as-reference") and r.get("questions")]
    if detail:
        lines += ["", f"Questions the pages asked that differ from the reference (worst facet of each answer, base % -> {label} %):", ""]
        lines += [f"| page / range | question | base % | {label} % | facet |", "|---|---|--:|--:|---|"]
        for k, r in detail:
            qb, qp = r["questions"]["base"], r["questions"]["pr"]
            for q in sorted(set(qb) | set(qp)):
                b, p = qb.get(q), qp.get(q)
                if (b and b[0] < 100) or (p and p[0] < 100):
                    lines.append(f"| `{k}` | `{q}` | {pct(b[0]) if b else '-'} | {pct(p[0]) if p else '-'} | {(p or b)[1]} |")
    if image_base:
        lines.append("")
        for k in sorted(cmp):
            if cmp[k]["verdict"] in ("match", "same-as-reference"):
                continue
            page, rng = k.split("/")
            lines.append(f"**{page} ({rng})**\n\n![{page} {rng}]({image_base.rstrip('/')}/{page}-{rng}.png)\n")
    return lines


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", required=True)
    ap.add_argument("--visual")
    ap.add_argument("--image-base")
    ap.add_argument("--label", default="PR")
    ap.add_argument("--explain")
    ap.add_argument("--out")
    a = ap.parse_args(argv)
    explain = load_json(a.explain) if a.explain else {}
    rep = load_json(os.path.join(a.api, "report.json"))
    lines, info = api_section(rep, a.label, explain)
    if a.visual:
        lines += visual_section(load_json(os.path.join(a.visual, "compare.json")), a.label, a.image_base, explain)
    text = "\n".join(lines) + "\n"
    if a.out:
        write_text(a.out, text)
    else:
        sys.stdout.write(text)
    return 1 if info["unexplained"] else 0


if __name__ == "__main__":
    sys.exit(main())
