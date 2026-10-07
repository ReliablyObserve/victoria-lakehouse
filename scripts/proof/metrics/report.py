"""The compact table: endpoint x base% x PR% x delta x verdict."""
from __future__ import annotations

from .common import SURFACES, display_pct
from .verdict import CaseResult, FAILING, pct_text, rollup_signals, rollup_surfaces

ICON = {
    "exact": "=", "same": "~", "fixed": "FIXED", "improved": "IMPROVED", "regressed": "REGRESSED",
    "not-reproduced-on-base": "NOT-REPRODUCED", "vacuous": "VACUOUS", "blocked": "BLOCKED",
    "nondeterministic": "NONDETERMINISTIC", "harness-error": "HARNESS-ERROR",
}


def _delta(r: CaseResult) -> str:
    d = r.delta
    if d is None:
        return "-"
    return "0" if abs(d) < 0.05 else f"{d:+.1f}"


def _worst(r: CaseResult) -> str:
    """Worst facet, base -> PR, so a fix reads as a movement of one named facet."""
    if r.pr is None or r.base is None:
        return "-"
    if r.pr.exact and r.base.exact:
        return "-"
    bn, bs = r.base.worst
    pn, ps = r.pr.worst
    left = "exact" if r.base.exact else f"{bn} {display_pct(bs)}"
    right = "exact" if r.pr.exact else f"{pn} {display_pct(ps)}"
    return f"{left} -> {right}"


def render_table(results: list[CaseResult]) -> str:
    order = {s: i for i, s in enumerate(SURFACES)}
    results = sorted(results, key=lambda r: (order.get(r.surface, 99), r.id))
    head = ("surface", "request", "base%", "PR%", "delta", "worst facet (base -> PR)", "verdict")
    rows = [head]
    for r in results:
        rows.append((r.surface, r.id, pct_text(r.base), pct_text(r.pr), _delta(r), _worst(r), ICON[r.verdict]))
    widths = [max(len(row[i]) for row in rows) for i in range(len(head))]
    lines = []
    for i, row in enumerate(rows):
        lines.append("  ".join(c.ljust(widths[j]) for j, c in enumerate(row)).rstrip())
        if i == 0:
            lines.append("  ".join("-" * w for w in widths))
    lines.append("")
    lines.append("signal rollup (logs and traces are never averaged together)")
    for sig, st in sorted(rollup_signals(results).items()):
        lines.append(f"  {sig}: {st['rows']} rows, exact rows base {display_pct(st['base_exact_pct'])}% -> PR {display_pct(st['pr_exact_pct'])}%, "
                     f"mean score base {display_pct(st['base_score'])}% -> PR {display_pct(st['pr_score'])}%")
    for (sig, surf), st in sorted(rollup_surfaces(results).items(), key=lambda kv: (kv[0][0], order.get(kv[0][1], 99))):
        lines.append(f"    {sig}/{surf}: {st['rows']} rows, exact {display_pct(st['base_exact_pct'])}% -> {display_pct(st['pr_exact_pct'])}%, "
                     f"score {display_pct(st['base_score'])}% -> {display_pct(st['pr_score'])}%")
    counts: dict[str, int] = {}
    for r in results:
        counts[r.verdict] = counts.get(r.verdict, 0) + 1
    lines.append("verdicts: " + ", ".join(f"{v}={n}" for v, n in sorted(counts.items())))
    bad = [r.id for r in results if r.verdict in FAILING]
    if bad:
        lines.append("failing: " + ", ".join(bad))
    return "\n".join(lines)
