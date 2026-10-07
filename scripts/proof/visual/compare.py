#!/usr/bin/env python3
# adapted from loki-vl-proxy/bench/visual/compare.py@429f15b9: request matching by key and the volatile-field rule
# are kept; scoring is delegated to scripts/proof/metrics, the sides are base | PR | reference.
"""Compare the backend traffic captured by tests/playwright/proof/capture.spec.ts.

  compare.py OUT

For every page and range the questions a page asked (Grafana /api/ds/query frames, datasource resource
calls, the own API of VMUI, VTUI and the Jaeger UI) are matched across base, PR and reference by request
key. Each matched pair is scored with scripts/proof/metrics (values, rows, series, traces), so a page row
shows base % and PR % against the reference, in the same numbers as the API proof. The panel state of every
side (data, empty, error, unsettled) is decided from the data and the DOM (states.py); a state change
decides the verdict by itself when the table of states says so. Writes OUT/compare.json and OUT/compare.md.
"""
from __future__ import annotations

import glob
import json
import os
import sys

from ..metrics.common import FacetResult, display_pct
from ..metrics.evaluate import evaluate_body
from ..metrics.verdict import classify
from .frames import answers_of
from .states import panel_state, transition
from .vio import dump_json, load_json, write_text

SIDES = ("base", "pr", "ref")


def _load(d: str, side: str) -> dict | None:
    p = os.path.join(d, f"{side}.json")
    return load_json(p) if os.path.exists(p) else None


def score_side(ref_answers: dict, side_answers: dict) -> tuple[FacetResult, list[str]]:
    """Facets of one side against the reference, one set per matched question plus request_match facets."""
    res = FacetResult()
    notes: list[str] = []
    for key, ra in sorted(ref_answers.items()):
        sa = side_answers.get(key)
        tag = key[:60]
        if sa is None:
            res.facets[f"{tag}|asked"] = 0.0
            notes.append(f"not asked: {key[:100]}")
            continue
        meta = {"kind": ra["kind"] if ra["kind"] not in ("error",) else "json", **ra["meta"], **{k: v for k, v in sa["meta"].items() if k not in ra["meta"]}}
        if ra["kind"] != sa["kind"]:
            res.facets[f"{tag}|kind"] = 0.0
            notes.append(f"answer kind {sa['kind']} vs reference {ra['kind']}: {key[:80]}")
            continue
        try:
            fr = evaluate_body(meta, {"status": ra["status"], "body": ra["body"]}, {"status": sa["status"], "body": sa["body"]})
        except Exception as e:  # noqa: BLE001 - an unknown shape is a harness problem, shown, never a pass
            res.facets[f"{tag}|undecodable"] = 0.0
            notes.append(f"{type(e).__name__}: {e}")
            continue
        for f, v in fr.facets.items():
            res.facets[f"{tag}|{f}"] = v
        res.notes += [f"{tag}: {n}" for n in fr.notes[:4]]
    for key in sorted(set(side_answers) - set(ref_answers)):
        res.facets[f"{key[:60]}|extra"] = 0.0
        notes.append(f"extra request, reference did not ask: {key[:100]}")
    res.notes += notes
    return res, notes


def page_verdict(states: dict, base_fr: FacetResult, pr_fr: FacetResult, claimed: bool, digests: dict | None = None) -> str:
    t = transition(states["base"]["state"], states["pr"]["state"], states.get("ref", {}).get("state"))
    if t in ("regression", "unsettled", "fixed"):
        return t
    v = classify(base_fr, pr_fr, claimed=claimed)
    if v == "regressed":
        return "regression"
    if v == "same" and digests and digests["base"] != digests["pr"]:
        return "unexpected-change"
    if t == "same-as-reference" and v in ("same", "exact"):
        return "same-as-reference"
    return {"exact": "match", "same": "same"}.get(v, v)


def compare_page(d: str, claimed: bool = False) -> dict | None:
    caps = {s: _load(d, s) for s in SIDES}
    if not caps["base"] or not caps["pr"]:
        return None
    ans = {s: answers_of(c["records"]) if c else {} for s, c in caps.items()}
    states = {s: panel_state(c) for s, c in caps.items() if c}
    if caps["ref"]:
        base_fr, base_notes = score_side(ans["ref"], ans["base"])
        pr_fr, pr_notes = score_side(ans["ref"], ans["pr"])
    else:
        base_fr = pr_fr = FacetResult()
        base_notes = pr_notes = ["no reference captured"]
    dig = {s: json.dumps({k: v["body"] for k, v in sorted(ans[s].items())}, sort_keys=True) for s in ("base", "pr")}
    verdict = page_verdict(states, base_fr, pr_fr, claimed, dig) if caps["ref"] else "undecided"
    return {
        "verdict": verdict, "states": states,
        "base_score": base_fr.score, "pr_score": pr_fr.score,
        "base_worst": base_fr.worst, "pr_worst": pr_fr.worst,
        "base_notes": base_notes[:8], "pr_notes": pr_notes[:8],
        "requests": {s: len(ans[s]) for s in SIDES},
        "ui": {s: (caps[s] or {}).get("ui") for s in SIDES},
        "console_errors": {s: len((caps[s] or {}).get("errors") or []) for s in SIDES},
        "settle_ms": {s: (caps[s] or {}).get("settle_ms") for s in SIDES},
    }


def compare_out(out: str, fixes: dict[str, list[str]] | None = None) -> dict:
    results = {}
    for d in sorted(glob.glob(os.path.join(out, "data", "*", "*"))):
        page, rng = d.split(os.sep)[-2:]
        r = compare_page(d, claimed=bool((fixes or {}).get(page)))
        if r:
            results[f"{page}/{rng}"] = r
    return results


def pct(v: float | None) -> str:
    return "-" if v is None else display_pct(v)


def markdown(results: dict, label: str = "PR") -> str:
    lines = [f"| page / range | base % | {label} % | state base / {label} / ref | verdict | worst facet {label} |", "|---|--:|--:|---|---|---|"]
    for k, r in sorted(results.items()):
        st = r["states"]
        s = " / ".join(st.get(x, {}).get("state", "-") for x in SIDES)
        lines.append(f"| `{k}` | {pct(r['base_score'])} | {pct(r['pr_score'])} | {s} | {r['verdict']} | {r['pr_worst'][0].split('|')[-1] if r['pr_score'] < 100 else '-'} |")
    return "\n".join(lines)


def main(argv=None) -> int:
    out = (argv or sys.argv[1:])[0]
    results = compare_out(out)
    dump_json(os.path.join(out, "compare.json"), results)
    write_text(os.path.join(out, "compare.md"), markdown(results) + "\n")
    print(markdown(results))
    return 1 if any(r["verdict"] in ("regression", "unsettled", "unexpected-change") for r in results.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
