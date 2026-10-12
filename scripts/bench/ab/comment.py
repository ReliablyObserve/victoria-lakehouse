#!/usr/bin/env python3
"""The sticky PR comments of the perf smoke, as Markdown and as a verdict (the check).

  comment.py perf SUMMARY.json --selection selection.json [--meta meta.json] [--known known.json] [--components components.json] \\
             [--out perf-smoke.md] [--verdict perf-verdict.json]
  comment.py api  API.json     --selection selection.json [--meta meta.json] [--known known.json] [--out api-matrix.md] [--verdict ...]

`perf` renders `<!-- lh-perf-smoke -->`, `api` renders `<!-- lh-api-matrix -->`. Both put the verdict first, show only
rows that changed beyond noise, differ from hot VictoriaLogs/VictoriaTraces or regressed, collapse everything else
into a count, and end with the "Known differences and gaps" section (known.py).

Exit status of the check: 0 passed (or skipped); 1 a confirmed regression, a broken cell, a new or untracked
difference, a status change, or more S3 GETs/bytes on a control shape; 2 the run is invalid (a cell with no valid
answer, a cold run whose cache was not cleared); 3 the run did not complete.
"""
from __future__ import annotations

import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import abreg  # noqa: E402
import known as knownmod  # noqa: E402

PERF_MARKER = "<!-- lh-perf-smoke -->"
API_MARKER = "<!-- lh-api-matrix -->"
REF_INVALID_SHARE = 0.2  # more than this share of the cells (and more than 2) without a hot answer: the reference stack is broken
ICON = {"broken": "BROKEN", "unstable": "UNSTABLE", "slower": "SLOWER", "fixed": "fixed", "faster": "faster", "same": "same", "n/a": "n/a"}
ORDER = {"broken": 0, "unstable": 1, "slower": 2, "fixed": 3, "faster": 4, "same": 5, "n/a": 6}
LAYER_ORDER = {"cold": 0, "compacted": 1, "buffer": 2, "after-restart": 3}


def ms(v, status=None):
    """Milliseconds to two significant digits; a bold status when the answer was an error."""
    if status and status not in ("200", "200/200"):
        return f"**{status}**"
    if v is None:
        return "-"
    if v >= 1000:
        return f"{v / 1000:.1f} s"
    if v >= 100:
        return f"{round(v, -1):.0f} ms"
    return f"{v:.2g} ms" if v < 10 else f"{v:.0f} ms"


def cell(text) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def ratio(p, r):
    if p is None or not r:
        return "-"
    x = p / r
    return f"{x:.1f}x" if x < 10 else f"{x:.0f}x"


def s3_pair(c: dict, key: str, phase: str) -> tuple[float | None, float | None]:
    s3 = c.get("s3") or {}
    b, p = (s3.get("base") or {}).get(phase), (s3.get("pr") or {}).get(phase)
    return (None if not b else b.get(key)), (None if not p else p.get(key))


def fmt_pair(b, p, scale=1.0, digits=0) -> str:
    if b is None or p is None:
        return "-"
    f = f"{{:.{digits}f}}"
    bs, ps = f.format(b / scale), f.format(p / scale)
    return bs if bs == ps else f"{bs} -> {ps}"


def result_text(c: dict) -> str:
    pr = c["vs_ref"]["pr"]
    d = c["difference"]
    if pr == "same":
        return "same" + (" (was different on base)" if d == "fixed" else "")
    if pr == "differs":
        return {"pre-existing": "differs (as base)", "new": "**differs, new in this PR**"}.get(d, "differs")
    return pr


def failing_cells(summary: dict, gate_s3_all: bool = False) -> dict:
    """The cells that fail the check. More S3 GETs or bytes fail on a control shape; with gate_s3_all (opt-in) on any shape."""
    cells = summary["cells"]
    return {
        "broken": [c for c in cells if c["verdict"] == "broken"],
        "unstable": [c for c in cells if c["verdict"] == "unstable"],
        "slower": [c for c in cells if c["verdict"] == "slower"],
        "new_difference": [c for c in cells if c["difference"] == "new"],
        "s3_more": [c for c in cells if (c.get("control") or gate_s3_all) and "more" in c["s3_verdict"].values()],
    }


def s3_flag(c: dict) -> str:
    """What S3 cost changed and in which phase, e.g. 'more bytes (cold)', or '-'."""
    parts = []
    for phase in ("cold", "warm"):
        for metric, how in (c.get("s3_why", {}).get(phase) or {}).items():
            parts.append(f"{how} {'GETs' if metric == 'gets' else metric} ({phase})")
    return "; ".join(parts) or "-"


def perf_table(cells: list[dict]) -> list[str]:
    head = ("| | signal | layer | shape | range | base p50 | PR p50 | change | VL/VT p50 | PR / VL/VT | S3 GETs cold | S3 GETs warm | S3 MB cold | S3 cost change | result vs VL/VT |")
    out = [head, "|---|---|---|---|---|--:|--:|--:|--:|--:|--:|--:|--:|---|---|"]
    for c in cells:
        p50 = c["p50"]
        gb, gp = s3_pair(c, "gets", "cold")
        wb, wp = s3_pair(c, "gets", "warm_per_query")
        bb, bp = s3_pair(c, "bytes", "cold")
        mark = " ¹" if c.get("confirmed") else ""
        change = f"{c['change_pct']:+.0f}%" if c.get("change_pct") is not None else "-"
        out.append(f"| {ICON.get(c['verdict'], c['verdict'])} | {c['signal']} | {c['layer']} | {cell(c['shape'])} | {c['range']} | "
                   f"{ms(p50.get('base'), c['status'].get('base'))} | {ms(p50.get('pr'), c['status'].get('pr'))}{mark} | {change} | "
                   f"{ms(p50.get('ref'), c['status'].get('ref'))} | {ratio(p50.get('pr'), p50.get('ref'))} | "
                   f"{fmt_pair(gb, gp, digits=0)} | {fmt_pair(wb, wp, digits=1)} | {fmt_pair(bb, bp, 1e6, 2)} | {s3_flag(c)} | {result_text(c)} |")
    return out


def cold_table(cells: list[dict]) -> list[str]:
    out = ["| signal | layer | shape | range | base cold | PR cold | VL/VT cold | PR / VL/VT cold |", "|---|---|---|---|--:|--:|--:|--:|"]
    for c in cells:
        cd = c["cold"]
        out.append(f"| {c['signal']} | {c['layer']} | {cell(c['shape'])} | {c['range']} | {ms(cd.get('base'))} | {ms(cd.get('pr'))} | "
                   f"{ms(cd.get('ref'))} | {ratio(cd.get('pr'), cd.get('ref'))} |")
    return out


def component_lines(comp: dict | None) -> list[str]:
    if not comp:
        return ["Component figures were not measured in this run."]
    lines = ["| component | signal | VL/VT | base | PR | unit | note |", "|---|---|--:|--:|--:|---|---|"]
    for row in comp.get("rows", []):
        lines.append(f"| {cell(row['component'])} | {row['signal']} | {row.get('ref', '-')} | {row.get('base', '-')} | {row.get('pr', '-')} | "
                     f"{row.get('unit', '')} | {cell(row.get('note', ''))} |")
    return lines


def process_lines(proc: dict) -> list[str]:
    lines = ["| Lakehouse process | base CPU-s | PR CPU-s | base peak RSS | PR peak RSS |", "|---|--:|--:|--:|--:|"]
    for sig in ("logs", "traces"):
        b, p = proc.get(f"base-{sig}") or {}, proc.get(f"pr-{sig}") or {}
        lines.append(f"| {sig} | {b.get('cpu_s', '-')} | {p.get('cpu_s', '-')} | {b.get('peak_rss_mb', '-')} MB | {p.get('peak_rss_mb', '-')} MB |")
    return lines


def meta_stamp(meta: dict, summary: dict | None = None) -> str:
    bits = [f"base `{str(meta.get('base', '?'))[:10]}` -> PR `{str(meta.get('head', '?'))[:10]}`"]
    if meta.get("versions"):
        bits.append("hot " + ", ".join(f"{k} {v}" for k, v in meta["versions"].items()))
    if meta.get("latency"):
        bits.append("S3 latency " + ", ".join(
            f"{x['expect_ms']:g} ms (probe +{x['added_ms']:g} ms"
            + (", Lakehouse's own S3 mean " + "/".join(f"{v:.0f}" for v in x["lh_mean_ms"].values() if v is not None) + " ms (base-logs/base-traces/pr-logs/pr-traces order may vary)"
               if isinstance(x.get("lh_mean_ms"), dict) else "")
            + f", {'ok' if x['ok'] else 'FAILED'})" for x in meta["latency"]))
    if summary:
        bits.append(f"{summary.get('runs')} runs per target (first cold), warm p50; noise max({int(summary['noise'] * 100)}%, {summary['min_delta_ms']:g} ms)")
    if meta.get("data"):
        bits.append(f"data: {meta['data']}")
    if meta.get("elapsed_s"):
        bits.append(f"{meta['elapsed_s'] / 60:.1f} min")
    if meta.get("host_load"):
        bits.append(f"host load average {meta['host_load']} at the start" + (f", {meta['host_load_end']} at the end" if meta.get("host_load_end") else ""))
    return " · ".join(bits)


def skipped(selection: dict, marker: str, title: str) -> tuple[str, dict]:
    lines = [marker, f"### {title}: skipped", "",
             f"No runtime change in this pull request ({selection.get('changed', 0)} file(s): docs, CI, tests, charts or registry text), so nothing was run."]
    return "\n".join(lines) + "\n", {"state": "skipped", "failed": False, "exit": 0}


def errored(selection: dict, meta: dict, message: str, marker: str, title: str) -> tuple[str, dict]:
    shapes = sum(len(v) for v in selection.get("sets", {}).values())
    lines = [marker, f"### {title}: did not complete", "",
             f"The run for {shapes} selected shape(s) stopped before a result: `{message[:300]}`. The job log and the uploaded artifact have the details.", "",
             f"<sub>{meta_stamp(meta)}</sub>"]
    return "\n".join(lines) + "\n", {"state": "error", "failed": True, "exit": 3}


def why_lines(selection: dict) -> list[str]:
    why = selection.get("why", {})
    if not why:
        return []
    by_reason: dict[str, list[str]] = {}
    for key, reasons in why.items():
        for reason in reasons:
            by_reason.setdefault(reason, []).append(key)
    lines = ["", "<details><summary>Why these shapes (touched-area selection)</summary>", ""]
    for reason, keys in sorted(by_reason.items(), key=lambda kv: (-len(kv[1]), kv[0]))[:25]:
        lines.append(f"- `{reason}`: {len(keys)} shape(s), e.g. " + ", ".join(cell(k) for k in sorted(keys)[:4]))
    return lines + ["", "</details>"]


def with_profile(summary: dict, profile: str) -> list[dict]:
    return [dict(c, profile=profile) for c in summary["cells"]]


def where(c: dict) -> str:
    return f"`{c['shape']}` ({c['layer']}/{c['range']}" + (f", {c['profile']}" if c.get("profile") else "") + ")"


def render_perf(summary: dict | None, selection: dict, meta: dict, known: dict | None = None, components: dict | None = None,
                extra: dict | None = None, gate_s3_all: bool = False) -> tuple[str, dict]:
    """`extra` holds the summaries of the other S3 latency profiles ({"50": summary}): their cells fail the check like the 0 ms ones."""
    title = "Performance A/B vs main, against VictoriaLogs / VictoriaTraces"
    if summary is None:
        return skipped(selection, PERF_MARKER, title)
    cells = summary["cells"]
    counts = summary["verdicts"]
    fails = failing_cells(summary, gate_s3_all)
    other = [c for p, sm in (extra or {}).items() for c in with_profile(sm, f"{p} ms")]
    for key, items in failing_cells({"cells": other}, gate_s3_all).items():
        fails[key] = fails[key] + items
    invalid_cells = [c for c in cells + other if c["verdict"] == "n/a" and not c["n_valid"].get("base") and not c["n_valid"].get("pr") and not c.get("vacuous")]
    ref_invalid = list(summary.get("ref_invalid", [])) + [c["cell"] for c in other if c.get("ref_invalid")]
    # a few cells without a hot answer are listed (base and PR are compared with each other there); only a broken reference stack, where
    # many cells have none, invalidates the run
    ref_broken = len(ref_invalid) > max(2, REF_INVALID_SHARE * len(cells + other))
    vacuous = list(summary.get("vacuous", [])) + [c["cell"] for c in other if c.get("vacuous")]
    flaky = list(summary.get("flaky", [])) + [c["cell"] for c in other if c.get("flaky")]
    uncleared = list(summary.get("cold_not_cleared", [])) + [x for sm in (extra or {}).values() for x in sm.get("cold_not_cleared", [])]
    probe_failed = [x for x in meta.get("latency", []) if not x.get("ok")]
    invalid = bool(invalid_cells or uncleared or probe_failed or ref_broken)
    known_failed = bool(known and known.get("failed"))
    failed = any(fails.values()) or known_failed
    diff_new = len(fails["new_difference"])
    if invalid:
        head, state = "WARNING Performance A/B: invalid run, re-run the job", "invalid"
    elif failed:
        head, state = "RED Performance A/B: regression or new difference", "failed"
    elif counts.get("faster") or counts.get("fixed"):
        head, state = "GREEN Performance A/B: faster, no regression", "passed"
    else:
        head, state = "NEUTRAL Performance A/B: no change beyond noise", "passed"
    diffs = summary["differences"]
    same_ref = sum(1 for c in cells if c["vs_ref"]["pr"] == "same")
    bits = [f"{counts[k]} {k}" for k in ("broken", "unstable", "slower", "fixed", "faster", "same", "n/a") if counts.get(k)]
    lines = [PERF_MARKER, f"### {head}", "",
             f"**{len(cells)} cells** (shape x layer x range x tenant form): " + ", ".join(bits) +
             f". Results vs VL/VT: {same_ref} same, {diffs.get('new', 0)} new difference(s), {diffs.get('pre-existing', 0)} pre-existing, {diffs.get('fixed', 0)} fixed. "
             f"S3 cost: {summary['s3_flags']['more']} cell(s) need more GETs or bytes, {summary['s3_flags']['less']} fewer.", ""]
    for text, items in (("A confirmed regression", fails["slower"]), ("A cell that broke", fails["broken"]),
                        ("A cell where the PR gave an error, empty or flapping answer the base did not", fails["unstable"]),
                        ("A new difference from hot, or from the base where hot cannot be compared", fails["new_difference"]),
                        ("More S3 GETs or bytes" + ("" if gate_s3_all else " on a control shape"), fails["s3_more"])):
        if items:
            lines.append(f"**{text}**: " + ", ".join(where(c) for c in items[:8]) + (" ..." if len(items) > 8 else ""))
    if invalid_cells:
        lines.append("**No valid answer on base or PR** (a cell that proves nothing): " + ", ".join(f"`{c['cell']}`" for c in invalid_cells[:5]))
    if uncleared:
        lines.append("**The Lakehouse cache could not be cleared before a cold run** (the cold figure would be warm): " + ", ".join(f"`{x}`" for x in uncleared[:5]))
    if ref_invalid:
        lines.append(("**Hot VictoriaLogs/VictoriaTraces gave no valid answer in most cells: the reference stack is broken**" if ref_broken else
                      "**Hot VictoriaLogs/VictoriaTraces gave no valid answer** (asked twice; base and PR are compared with each other for these cells)")
                     + ": " + ", ".join(f"`{x}`" for x in ref_invalid[:5]) + (" ..." if len(ref_invalid) > 5 else ""))
    if flaky:
        lines.append("**Flaky, not failing** (a problem of the PR's answers that did not reproduce on the re-measure, a finding to look at): " + ", ".join(f"`{x}`" for x in flaky[:5]))
    if vacuous:
        lines.append(f"{len(vacuous)} cell(s) are empty on every target (a shape the seeded data does not feed): they measure nothing.")
    if probe_failed:
        lines.append("**The S3 latency probe failed**: the injected delay was not measured: " + ", ".join(f"{x['expect_ms']:g} ms (measured +{x['added_ms']:g} ms)" for x in probe_failed))
    if any(lines[-1:]):
        lines.append("")
    moved = [c for c in cells if c["verdict"] not in ("same", "n/a") or c["difference"] in ("new", "fixed") or "more" in c["s3_verdict"].values()
             or "less" in c["s3_verdict"].values()]
    moved.sort(key=lambda c: (ORDER.get(c["verdict"], 9), c["signal"], LAYER_ORDER.get(c["layer"], 9), c["shape"]))
    rest = [c for c in cells if c not in moved]
    if moved:
        lines += perf_table(moved)
        if any(c.get("confirmed") for c in moved):
            lines += ["", f"¹ re-measured with {meta.get('confirm_runs', '?')} runs after the first pass moved beyond noise; only a move that holds is reported."]
    else:
        lines.append(f"Every cell is within noise of the base build (max({int(summary['noise'] * 100)}%, {summary['min_delta_ms']:g} ms)), and no answer changed.")
    if rest:
        lines += ["", f"<details><summary>{len(rest)} unchanged cells (within noise)</summary>", ""] + perf_table(rest[:120])
        if len(rest) > 120:
            lines.append(f"\n{len(rest) - 120} more in the JSON artifact.")
        lines += ["", "</details>"]
    lines += ["", "<details><summary>Cold (first-run) timings: what a user opening a panel sees</summary>", ""]
    lines += cold_table(sorted(cells, key=lambda c: (c["signal"], LAYER_ORDER.get(c["layer"], 9), c["shape"]))[:120])
    lines += ["", "VL/VT cannot clear its caches, so its cold column is a first request, not a cold one.", "", "</details>"]
    lines += ["", "<details><summary>Component summary: ingest, compaction, CPU and memory, base vs PR</summary>", ""]
    lines += component_lines(components) + [""] + process_lines(summary.get("process", {})) + ["", "</details>"]
    lines += why_lines(selection)
    if known is not None:
        lines += [""] + knownmod.render(known, meta.get("registry_gap_stats") or known.get("registry"))
    lines += ["", "<sub>" + meta_stamp(meta, summary) + ". One fresh stack, targets interleaved per request, every answer validated "
              "(an invalid answer is never a latency). How to read and reproduce: `docs/pr-perf-smoke.md`.</sub>"]
    exit_code = 2 if invalid else (1 if failed else 0)
    verdict = {"state": state, "failed": failed or invalid, "exit": exit_code, "counts": counts, "differences": diffs,
               "s3_flags": summary["s3_flags"], "new_differences": diff_new, "cells": len(cells),
               "known": (known or {}).get("counts", {}), "invalid_cells": [c["cell"] for c in invalid_cells], "ref_invalid": ref_invalid, "flaky": flaky, "vacuous": vacuous,
               "failing": {k: len(v) for k, v in fails.items()}, "extra_profiles": {p: sm["verdicts"] for p, sm in (extra or {}).items()}}
    return "\n".join(lines) + "\n", verdict


# ---------------------------------------------------------------------------------------------------- api matrix
def score(v) -> str:
    return "-" if v is None else (f"{v:.0f}%" if v < 99.995 else "100%")


def render_api(matrix: dict | None, selection: dict, meta: dict, known: dict | None = None) -> tuple[str, dict]:
    title = "API endpoints: Lakehouse base vs PR vs VictoriaLogs / VictoriaTraces"
    if matrix is None:
        return skipped(selection, API_MARKER, title)
    results = matrix["results"]
    s = matrix["summary"]
    counts = s["verdicts"]
    failing = [r for r in results if r["id"] in set(s["failing"])]
    known_failed = bool(known and known.get("failed"))
    failed = bool(failing) or known_failed
    if failed:
        head, state = "RED API matrix: a regression, a status change or a new difference", "failed"
    elif counts.get("fixed") or counts.get("improved"):
        head, state = "GREEN API matrix: fixed or improved, no regression", "passed"
    else:
        head, state = "NEUTRAL API matrix: no change in any endpoint", "passed"
    exact = [r for r in results if r["verdict"] == "exact"]
    bits = [f"{n} {k}" for k, n in sorted(counts.items(), key=lambda kv: -kv[1])]
    lines = [API_MARKER, f"### {head}", "",
             f"**{len(results)} registry rows exercised** against base, PR and hot VL/VT (read-only requests, {matrix.get('rounds', '?')} rounds each): " + ", ".join(bits) +
             f". **{s['not_exercised']} rows not exercised** (listed below by reason).", ""]
    shown = [r for r in results if r["verdict"] not in ("exact", "arbitrary", "vacuous", "unusable") or r["status_changed"] or r.get("latency") == "slower"]
    shown.sort(key=lambda r: (0 if r["id"] in s["failing"] else 1, r["surface"], r["id"]))
    if shown:
        lines += ["| surface | row | base status | PR status | VL/VT status | base % | PR % | worst facet | base p50 | PR p50 | VL/VT p50 | verdict |",
                  "|---|---|---|---|---|--:|--:|---|--:|--:|--:|---|"]
        for r in shown[:80]:
            st, lat = r["status"], r["p50_ms"]
            lines.append(f"| {r['surface']} | `{r['id']}` | {st.get('base', '-')} | {st.get('pr', '-')} | {st.get('ref', '-')} | {score(r['base_score'])} | {score(r['pr_score'])} | "
                         f"{r['pr_worst'] or '-'} | {ms(lat.get('base'))} | {ms(lat.get('pr'))} | {ms(lat.get('ref'))} | {r['verdict']}"
                         f"{' (status changed)' if r['status_changed'] else ''}{' (slower)' if r.get('latency') == 'slower' else ''} |")
        if len(shown) > 80:
            lines.append(f"| | {len(shown) - 80} more rows in the JSON artifact | | | | | | | | | | |")
    else:
        lines.append("Every exercised row answers like the base build and like hot VL/VT where it did before.")
    lines += ["", f"{len(exact)} rows match the reference exactly on base and PR (not listed)."]
    not_proving = [r for r in results if r["verdict"] in ("arbitrary", "vacuous", "unusable")]
    if not_proving:
        lines += ["", f"<details><summary>{len(not_proving)} rows that prove nothing about this PR (not scored)</summary>", ""]
        reasons = {"arbitrary": "the request asks for an arbitrary subset (`limit` without `sort`, facets, tag samples): two correct systems answer different rows",
                   "vacuous": "empty on every target (the registry's seed vocabulary names services the seeded data does not have)",
                   "unusable": "hot VictoriaLogs/VictoriaTraces itself rejects the request the registry row declares: the row needs fixing"}
        for kind in ("arbitrary", "vacuous", "unusable"):
            ids = [r["id"] for r in not_proving if r["verdict"] == kind]
            if ids:
                lines.append(f"- **{kind}** ({len(ids)}): {reasons[kind]}. " + ", ".join(f"`{i}`" for i in ids[:12]) + (" ..." if len(ids) > 12 else ""))
        lines += ["", "</details>"]
    lines += ["", f"<details><summary>{s['not_exercised']} registry rows not exercised, by reason</summary>", ""]
    for reason, n in s["not_exercised_reasons"].items():
        lines.append(f"- {n}: {reason}")
    lines += ["", "</details>"]
    lines += why_lines(selection)
    if known is not None:
        lines += [""] + knownmod.render(known, meta.get("registry_gap_stats") or known.get("registry"))
    stamp = meta_stamp(meta)
    lines += ["", "<sub>" + stamp + ". Verdicts and percentages are scripts/proof's (status, error text, rows, values, series, traces, schema; 100% only when exact). "
              "How to read and reproduce: `docs/pr-perf-smoke.md`.</sub>"]
    verdict = {"state": state, "failed": failed, "exit": 1 if failed else 0, "counts": counts, "failing": s["failing"],
               "status_changes": s["status_changes"], "known": (known or {}).get("counts", {}), "rows": len(results),
               "not_exercised": s["not_exercised"]}
    return "\n".join(lines) + "\n", verdict


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("kind", choices=("perf", "api"))
    ap.add_argument("input")
    ap.add_argument("--selection", required=True)
    ap.add_argument("--meta", default="")
    ap.add_argument("--known", default="")
    ap.add_argument("--components", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--verdict", default="")
    a = ap.parse_args(argv)
    meta = abreg.load_json(a.meta) if a.meta else {}
    known = abreg.load_json(a.known) if a.known else None
    sel = abreg.load_json(a.selection)
    data = abreg.load_json(a.input)
    if a.kind == "perf":
        text, verdict = render_perf(data, sel, meta, known, abreg.load_json(a.components) if a.components else None)
    else:
        text, verdict = render_api(data, sel, meta, known)
    if a.out:
        abreg.write_text(a.out, text)
    if a.verdict:
        abreg.dump_json(a.verdict, verdict)
    print(text)
    return verdict["exit"]


if __name__ == "__main__":
    sys.exit(main())
