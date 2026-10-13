#!/usr/bin/env python3
"""Reduce measure.py output to a comparable summary: one row per cell with base / PR / reference timings, S3 cost
and result parity, and a verdict.

  report.py summarize RAW.json --label pr518-control [--save OUT.json] [--noise 0.15 --min-delta-ms 3]

Timings are the warm p50 (runs after the first, valid answers only); the first run is `cold`. An answer that is
not valid (status, parse, empty, flapping: validate.py) never counts as a latency. A cell is `slower` or
`faster` only when the PR's p50 moves against the base's by more than max(noise x base, min-delta-ms).

Verdicts: broken (the base answered, the PR does not), unstable (the PR gave an error, empty or flapping answer the base did not),
fixed (the reverse of broken), slower, faster, same, n/a.
Result parity compares the validated result of the PR (and of the base) with the reference's on the same window:
`same`, `differs`, `invalid`. A difference the base already had, with the same answer, is pre-existing; a PR
answer that differs from the reference and from the base's is a new difference and fails the check.
S3: GETs and bytes per query, cold and warm, from the Lakehouse /metrics delta (measure.py); a rise beyond
max(S3_REL, floor) on a control shape fails the check even when the latency is flat.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import abreg  # noqa: E402

NOISE_REL = 0.15
NOISE_ABS_MS = 3.0
S3_REL = 0.10
S3_FLOOR = {"gets": 2.0, "requests": 2.0, "bytes": 64 * 1024.0}
CONTROL_SETS = ("control-logs", "control-traces")
TARGETS = ("ref", "base", "pr")
MOVED = ("slower", "faster", "unstable")  # first-pass verdicts that are measured again before they count


def pct(values, p):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def timing_verdict(base, pr, noise=NOISE_REL, min_delta=NOISE_ABS_MS):
    if base is None or pr is None:
        return "n/a"
    if pr > base * (1 + noise) and pr - base > min_delta:
        return "slower"
    if pr < base / (1 + noise) and base - pr > min_delta:
        return "faster"
    return "same"


def s3_changes(base, pr) -> dict:
    """Per metric (gets, requests, bytes): 'more' when the PR needs more than the base beyond noise, 'less' for the reverse."""
    out = {}
    for k, floor in S3_FLOOR.items():
        b, p = (base or {}).get(k), (pr or {}).get(k)
        if b is None or p is None:
            continue
        if p > b * (1 + S3_REL) and p - b >= floor:
            out[k] = "more"
        elif p < b / (1 + S3_REL) and b - p >= floor:
            out[k] = "less"
    return out


def s3_verdict(base, pr):
    """'more' when the PR needs more GETs, requests or bytes than the base beyond noise, 'less' for the reverse, else 'same'."""
    if not base or not pr:
        return "n/a"
    ch = s3_changes(base, pr)
    return "more" if "more" in ch.values() else ("less" if "less" in ch.values() else "same")


def parity(res_t, res_ref):
    if res_t is None or res_ref is None:
        return "invalid" if res_t is None else "n/a"
    return "same" if res_t == res_ref else "differs"


def cell_summary(key: str, rows: list[dict], s3: dict | None, noise: float, min_delta: float, ref_skip: frozenset = frozenset()) -> dict:
    first = rows[0]
    out = {"cell": key, "set": first["set"], "shape": first["shape"], "signal": first["signal"], "layer": first["layer"],
           "range": first["range"], "form": first["form"], "window": first["window"], "p50": {}, "p95": {}, "cold": {}, "n_valid": {},
           "n_invalid": {}, "invalid_reasons": {}, "status": {}, "result": {}, "cold_cleared": {}}
    for t in TARGETS:
        rs = sorted((r for r in rows if r["target"] == t), key=lambda r: r["run"])
        if not rs:
            continue
        warm = [r["ms"] for r in rs if r["valid"] and not r["cold"]]
        cold = [r["ms"] for r in rs if r["valid"] and r["cold"]]
        out["p50"][t] = round(pct(warm, 0.5), 2) if warm else None
        out["p95"][t] = round(pct(warm, 0.95), 2) if warm else None
        out["cold"][t] = cold[0] if cold else None
        out["n_valid"][t] = sum(1 for r in rs if r["valid"])
        out["n_invalid"][t] = sum(1 for r in rs if not r["valid"])
        out["invalid_reasons"][t] = sorted({r["reason"] for r in rs if not r["valid"]})
        out["status"][t] = "/".join(sorted({str(r["status"]) for r in rs}))
        results = sorted({r["result"] for r in rs if r["valid"]})
        out["result"][t] = results[0] if len(results) == 1 else (None if not results else "unstable")
        cc = [r.get("cold_cleared") for r in rs if r["cold"] and "cold_cleared" in r]
        if cc:
            out["cold_cleared"][t] = all(cc)
    out["s3"] = s3 or {}
    ref_res = out["result"].get("ref")
    # every target answered empty: a shape the seeded data does not feed, not a broken run (nothing is measured, nothing fails)
    out["vacuous"] = all(out["n_valid"].get(t, 0) == 0 and out["invalid_reasons"].get(t) == ["empty result"] for t in TARGETS if t in out["n_valid"])
    ref_ok = out["n_valid"].get("ref", 0) > 0
    skip = first["shape"] in ref_skip
    out["ref_invalid"] = not skip and not ref_ok and not out["vacuous"]
    # a shape that is not compared with hot, or whose hot answer is invalid, is compared base against PR
    out["vs_ref"] = {t: ("n/a" if skip or not ref_ok else parity(out["result"].get(t), ref_res)) for t in ("base", "pr")}
    b_ok, p_ok = out["n_valid"].get("base", 0) > 0, out["n_valid"].get("pr", 0) > 0
    pr_worse = out["n_invalid"].get("pr", 0) > out["n_invalid"].get("base", 0)
    if b_ok and not p_ok:
        verdict = "broken"
    elif p_ok and not b_ok:
        verdict = "fixed"
    elif not b_ok and not p_ok:
        verdict = "n/a"
    elif pr_worse or (out["p50"].get("pr") is None and out["p50"].get("base") is not None):
        verdict = "unstable"  # the PR gave answers the base did not (an error, an empty or a flapping answer): a latency of the rest proves nothing
    else:
        verdict = timing_verdict(out["p50"].get("base"), out["p50"].get("pr"), noise, min_delta)
    out["verdict"] = verdict
    out["change_pct"] = (round((out["p50"]["pr"] - out["p50"]["base"]) / out["p50"]["base"] * 100, 1)
                         if out["p50"].get("base") and out["p50"].get("pr") is not None else None)
    br, pr_ = out["vs_ref"]["base"], out["vs_ref"]["pr"]
    same_answer = out["result"].get("base") == out["result"].get("pr")
    if pr_ == "differs" and br == "differs" and same_answer:
        diff = "pre-existing"
    elif pr_ == "differs":
        diff = "new"
    elif br == "differs" and pr_ == "same":
        diff = "fixed"
    else:
        diff = "none"
    if (skip or not ref_ok) and not same_answer and p_ok and b_ok:
        diff = "new"  # not compared with hot, but the PR answers differently from the base
    out["difference"] = diff
    sc, sw = (s3 or {}).get("base", {}), (s3 or {}).get("pr", {})
    out["s3_verdict"] = {"cold": s3_verdict(sc.get("cold"), sw.get("cold")), "warm": s3_verdict(sc.get("warm_per_query"), sw.get("warm_per_query"))}
    out["s3_why"] = {"cold": s3_changes(sc.get("cold"), sw.get("cold")), "warm": s3_changes(sc.get("warm_per_query"), sw.get("warm_per_query"))}
    out["control"] = first["set"] in CONTROL_SETS
    return out


def ref_skip_from(spec: dict) -> frozenset:
    """Shape names that are not compared with hot VL/VT (`vs_ref: false`)."""
    return frozenset(sh["name"] for s in spec.get("sets", {}).values() for sh in s["shapes"] if sh.get("vs_ref") is False)


def recount(summary: dict) -> dict:
    """Totals of a summary from its cells; called after every change to the cells so the header never disagrees with the table."""
    cells = summary["cells"]
    for key, field in (("verdicts", "verdict"), ("differences", "difference")):
        counts: dict[str, int] = {}
        for c in cells:
            counts[c[field]] = counts.get(c[field], 0) + 1
        summary[key] = counts
    summary["s3_flags"] = {"more": sum(1 for c in cells if "more" in c["s3_verdict"].values()),
                           "less": sum(1 for c in cells if "less" in c["s3_verdict"].values())}
    summary["cold_not_cleared"] = sorted({c["cell"] for c in cells if c["cold_cleared"] and not all(c["cold_cleared"].values())})
    summary["ref_invalid"] = sorted(c["cell"] for c in cells if c.get("ref_invalid"))
    summary["vacuous"] = sorted(c["cell"] for c in cells if c.get("vacuous"))
    summary["flaky"] = sorted(c["cell"] for c in cells if c.get("flaky_scope") == "both")           # on base and PR: a finding, does not fail
    summary["intermittent"] = sorted(c["cell"] for c in cells if c.get("intermittent"))            # only on the PR: fails
    return summary


def summarize(raw: dict, label: str, noise: float = NOISE_REL, min_delta: float = NOISE_ABS_MS, ref_skip: frozenset = frozenset()) -> dict:
    by: dict[str, list[dict]] = {}
    for r in raw["rows"]:
        by.setdefault(r["cell"], []).append(r)
    cells = [cell_summary(k, rs, (raw.get("s3") or {}).get(k), noise, min_delta, ref_skip) for k, rs in by.items()]
    summary = {"label": label, "date": dt.date.today().isoformat(), "runs": raw.get("runs"), "sets": raw.get("sets"), "noise": noise,
               "min_delta_ms": min_delta, "cells": cells, "requests": len(raw["rows"]),
               "invalid_requests": sum(r["valid"] is False for r in raw["rows"]), "process": raw.get("process", {}),
               "elapsed_s": raw.get("elapsed_s"), "state": raw.get("state")}
    return recount(summary)


def apply_ref_skip(summary: dict, ref_skip: frozenset) -> dict:
    """Re-derive the result-vs-hot fields of a saved summary for shapes that are compared base against PR only (and not at all for samples)."""
    for c in summary["cells"]:
        if c["shape"] in ref_skip:
            c["vs_ref"] = {"base": "n/a", "pr": "n/a"}
            same_answer = c["result"].get("base") == c["result"].get("pr")
            c["difference"] = "none" if same_answer or not (c["n_valid"].get("base") and c["n_valid"].get("pr")) else "new"
    return recount(summary)


def merge_confirm(summary: dict, again: dict) -> dict:
    """Replace the cells the first pass called slower, faster or unstable by the re-measure's cells (marked `confirmed`). A new
    difference from the reference that either pass saw stays: re-measuring decides timing, it never clears a wrong answer. An
    `unstable` cell whose problem does not reproduce is kept as `flaky`: listed, not failing (one transient error is not a regression);
    one that reproduces stays `unstable` and fails; one that does not reproduce stays `unstable` too (an intermittent failure the PR
    introduced) unless the base had failures in either pass, and then it is listed as flaky on both."""
    redo = {c["cell"]: c for c in again["cells"]}
    moved = MOVED
    for i, c in enumerate(summary["cells"]):
        if c["cell"] in redo and c["verdict"] in moved:
            new = dict(redo[c["cell"]], confirmed=True, first_pass=c["verdict"])
            if c["verdict"] == "unstable" and new["verdict"] != "unstable":
                # the problem did not reproduce. A check that fails and then passes on retry is a finding, not noise: when the base was clean in
                # both passes the intermittent failure is the PR's and stays a failure; when the base had failures too it is host noise, listed
                new["flaky"] = True
                if c["n_invalid"].get("base", 0) > 0 or new["n_invalid"].get("base", 0) > 0:
                    new["flaky_scope"] = "both"
                else:
                    new["flaky_scope"], new["intermittent"], new["verdict"] = "pr", True, "unstable"
            if c["difference"] == "new" and new["difference"] != "new":
                new["difference"], new["first_pass_difference"] = "new", "new"
            summary["cells"][i] = new
    return recount(summary)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("summarize")
    s.add_argument("raw")
    s.add_argument("--label", required=True)
    s.add_argument("--save", default="")
    s.add_argument("--noise", type=float, default=NOISE_REL)
    s.add_argument("--min-delta-ms", type=float, default=NOISE_ABS_MS)
    a = ap.parse_args(argv)
    spec = abreg.load_json(abreg.SHAPES)
    summary = summarize(abreg.load_json(a.raw), a.label, a.noise, a.min_delta_ms, ref_skip_from(spec))
    if a.save:
        abreg.dump_json(a.save, summary)
    print(f"{len(summary['cells'])} cells: {summary['verdicts']}; differences {summary['differences']}; invalid requests {summary['invalid_requests']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
