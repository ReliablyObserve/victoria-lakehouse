"""Markdown and JSON reports of a runner output: the table is row, form and layer, base %, PR %, verdict."""
from __future__ import annotations

import json
import os

from ..metrics.common import SURFACES, display_pct
from ..metrics.report import ICON, render_table
from ..metrics.verdict import pct_text

FIRST_DIFF = 12


def _worst(side) -> str:
    if side is None:
        return "-"
    if side.exact:
        return "exact"
    n, s = side.worst
    return f"{n} {display_pct(s)}"


def _facets(side) -> dict:
    return {} if side is None else {k: round(v, 3) for k, v in side.facets.items()}


def markdown(items, state: dict, seed: dict, label: str = "PR", seconds: float = 0.0) -> str:
    order = {s: i for i, s in enumerate(SURFACES)}
    results = sorted((r for _, r in items), key=lambda r: (order.get(r.surface, 99), r.id))
    lines = [f"| surface | request | base % | {label} % | worst facet base / {label} | verdict |", "|---|---|--:|--:|---|---|"]
    for r in results:
        lines.append(f"| {r.surface} | `{r.id.split('/', 1)[1]}` | {pct_text(r.base)} | {pct_text(r.pr)} | "
                     f"{_worst(r.base)} / {_worst(r.pr)} | {ICON[r.verdict]} |")
    counts: dict[str, int] = {}
    for r in results:
        counts[r.verdict] = counts.get(r.verdict, 0) + 1
    lines += ["", "verdicts: " + ", ".join(f"{v}={n}" for v, n in sorted(counts.items())),
              f"run: {len(results)} requests in {seconds:.0f}s; seed equality (rows per tenant form and layer, the same on ref, base and PR): "
              + ("checked" if seed else "skipped")]
    return "\n".join(lines)


def write_reports(out: str, items, state: dict, seed: dict, label: str = "PR", seconds: float = 0.0) -> None:
    results = [r for _, r in items]
    open(os.path.join(out, "report.txt"), "w").write(render_table(results) + "\n")
    open(os.path.join(out, "report.md"), "w").write(markdown(items, state, seed, label, seconds) + "\n")
    rows = []
    for meta, r in items:
        rows.append({"id": r.id, "row": r.row, "surface": r.surface, "signal": r.signal, "form": meta.get("form"),
                     "layer": meta.get("layer"), "endpoint": r.endpoint, "verdict": r.verdict, "samples": r.samples,
                     "base_score": None if r.base is None else r.base.score, "pr_score": None if r.pr is None else r.pr.score,
                     "base_facets": _facets(r.base), "pr_facets": _facets(r.pr),
                     "base_notes": [] if r.base is None else r.base.notes[:FIRST_DIFF],
                     "pr_notes": [] if r.pr is None else r.pr.notes[:FIRST_DIFF],
                     "base_details": {} if r.base is None else r.base.details, "pr_details": {} if r.pr is None else r.pr.details,
                     "latency": r.latency})
    json.dump({"state": state, "seed_equality": seed, "seconds": round(seconds, 1), "results": rows},
              open(os.path.join(out, "report.json"), "w"), indent=1, default=str)
