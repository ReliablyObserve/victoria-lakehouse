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


def questions(ref_answers: dict, fr: FacetResult) -> dict[str, list]:
    """Per question: [score, worst facet], so a page row can name the request a difference sits in."""
    out: dict[str, list] = {}
    for key, a in ref_answers.items():
        tag = key[:60] + "|"
        facets = {k[len(tag):]: v for k, v in fr.facets.items() if k.startswith(tag)}
        if facets:
            n = min(facets, key=lambda x: (facets[x], x))
            out[a.get("label", key)] = [facets[n], n]
    for k, v in fr.facets.items():
        if k.endswith("|extra"):
            out[f"extra: {k[:-6][:50]}"] = [v, "extra"]
    return out


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
        meta["surface"] = "vt-native" if "spanID" in (meta.get("identity") or []) else "vl-native"  # picks the default row identity
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


def page_verdict(states: dict, base_fr: FacetResult, pr_fr: FacetResult, claimed: bool, differ: bool = False) -> str:
    t = transition(states["base"]["state"], states["pr"]["state"], states.get("ref", {}).get("state"))
    if t in ("regression", "unsettled", "fixed"):
        return t
    v = classify(base_fr, pr_fr, claimed=claimed)
    if v == "regressed":
        return "regression"
    if v == "same" and differ:
        return "unexpected-change"
    if t == "same-as-reference" and v in ("same", "exact"):
        return "same-as-reference"
    return {"exact": "match", "same": "same"}.get(v, v)


def _short(w: tuple[str, float]) -> list:
    """The worst facet with the question it belongs to: 'field_names hits_equality'."""
    key, facet = (w[0].rsplit("|", 1) + [""])[:2] if "|" in w[0] else ("", w[0])
    m = key.split("/")[-1].split(":")[0] if key.startswith("r:") else key.split(":")[1] if key.startswith("q:") else key
    return [f"{m} {facet}".strip(), w[1]]


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
    # base and PR answering differently while neither matches the reference is an unexpected change
    diff = score_side(ans["base"], ans["pr"])[0] if caps["ref"] else FacetResult()
    verdict = page_verdict(states, base_fr, pr_fr, claimed, not diff.exact) if caps["ref"] else "undecided"
    return {
        "verdict": verdict, "states": states,
        "base_score": base_fr.score, "pr_score": pr_fr.score,
        "base_worst": _short(base_fr.worst), "pr_worst": _short(pr_fr.worst),
        "base_notes": base_notes[:8], "pr_notes": pr_notes[:8],
        "requests": {s: len(ans[s]) for s in SIDES},
        "questions": {"base": questions(ans["ref"], base_fr), "pr": questions(ans["ref"], pr_fr)} if caps["ref"] else {},
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
        lines.append(f"| `{k}` | {pct(r['base_score'])} | {pct(r['pr_score'])} | {s} | {r['verdict']} | {r['pr_worst'][0] if r['pr_score'] < 100 else '-'} |")
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
