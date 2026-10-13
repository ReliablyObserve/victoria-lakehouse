#!/usr/bin/env python3
"""The API matrix: every read-only registry row, sent to hot VictoriaLogs/VictoriaTraces, Lakehouse (base) and
Lakehouse (PR), scored by scripts.proof.metrics.

  api_matrix.py --state OUT/state.json --out OUT/api-matrix.json [--rounds 3] [--only REGEXP]

For each row of tests/conformance/registry (api_rows.py) the request is sent `rounds` times to every target,
the targets in a rotating order. The first round is scored (status, error text, rows, values, series, traces or
JSON schema, whichever the row's comparator names); the other rounds only classify (a verdict that flips is
`nondeterministic`) and give the latency p50. Lakehouse-only rows (no `hot` target, or the /lakehouse API) are
scored PR against base. Rows that cannot be exercised read-only are listed with the reason.

Verdicts are scripts.proof.metrics': exact, same, fixed, improved, regressed, nondeterministic, vacuous, blocked,
harness-error. A row whose query ends in `limit N` keeps an arbitrary subset, so it is scored by membership: every row an
answer returns must be a row of the unlimited reference answer (`evaluate_limited`). `facets` is reported as `arbitrary`; Tempo's tag list and tag values (samples) are scored by status, shape and non-emptiness. The check fails on regressed and harness-error, on a nondeterministic row only when one sample had the PR worse than the base
(a flip the other way is listed, not failing), and on a status change base -> PR.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import statistics
import sys

import os
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import abreg  # noqa: E402
import api_rows  # noqa: E402
from scripts.proof.metrics.cases import evaluate_case
from scripts.proof.metrics.common import UnknownShape
from scripts.proof.metrics.decode import decode_values  # noqa: E402
from scripts.proof.metrics.common import FacetResult, SURFACE_SIGNAL  # noqa: E402
from scripts.proof.metrics.generic import evaluate_status  # noqa: E402
from scripts.proof.metrics.verdict import FAILING, CaseResult, classify, classify_samples  # noqa: E402
from scripts.proof.runner import client  # noqa: E402

TARGETS = ("ref", "base", "pr")
STABLE = ("exact", "same")
TIMEOUT_S = 60.0
PEER_KEY = "proof-peer-key"
LATENCY_REL = 1.0  # a row is "slower" when the PR's p50 is more than 2x the base's and 20 ms above it (3 samples: only a gross change)
LATENCY_ABS_MS = 20.0


def port(state: dict, target: str, signal: str) -> str:
    return f"http://127.0.0.1:{state['ports'][f'{target}-{signal}']}"


def send(state: dict, req: dict, target: str) -> dict:
    headers = dict(req["headers"])
    if req["lh_only"]:
        headers.setdefault("Authorization", f"Bearer {PEER_KEY}")  # the admin and internal endpoints of Lakehouse need the peer key
    return client.send(req["method"], port(state, target, req["signal"]) + req["path"], req["params"], headers, timeout=TIMEOUT_S)


def matrix_window(state: dict) -> dict:
    """The window of {{seed.start}}..{{seed.cold_end}}: the last cold hour. Row-returning rows (`query=*`) answer every
    row of it, and scoring a whole six-hour window row by row in Python costs seconds per request."""
    end = state["cold"]["end"]
    h = dt.datetime.strptime(end, "%Y-%m-%dT%H:%M:%SZ") - dt.timedelta(hours=1)
    return {"start": h.strftime("%Y-%m-%dT%H:%M:%SZ"), "end": end}


def pick_values(state: dict) -> dict[str, str]:
    """{{seed.trace_id}} and {{seed.stream_id}}: the first of each in the matrix window, taken from the reference."""
    out = {}
    win = matrix_window(state)
    hdr = {"AccountID": "0", "ProjectID": "0"}
    env = client.send("POST", port(state, "ref", "traces") + "/select/logsql/query",
                      {"query": "span_id:* | sort by (trace_id) | fields trace_id | limit 1", "start": win["start"], "end": win["end"],
                       "disable_latency_offset": "true"}, hdr, timeout=TIMEOUT_S)
    try:
        out["seed.trace_id"] = json.loads(env["body"].strip().splitlines()[0])["trace_id"]
    except (ValueError, KeyError, IndexError):
        pass  # no trace on the reference: the pick stays unset and the rows that need it are listed as not exercised
    env = client.send("POST", port(state, "ref", "logs") + "/select/logsql/query",
                      {"query": "* | sort by (_stream_id) | fields _stream_id | limit 1", "start": win["start"], "end": win["end"]}, hdr, timeout=TIMEOUT_S)
    try:
        out["seed.stream_id"] = json.loads(env["body"].strip().splitlines()[0])["_stream_id"]
    except (ValueError, KeyError, IndexError):
        pass  # no stream on the reference: likewise
    return out


def status_only(answers: dict[str, dict], lh_only: bool) -> tuple[FacetResult, FacetResult]:
    ref = answers["base"] if lh_only else answers["ref"]
    return evaluate_status(ref, answers["base"]), evaluate_status(ref, answers["pr"])


def evaluate(req: dict, answers: dict[str, dict], resamples: list[dict[str, dict]]) -> CaseResult:
    """Score one request. Status-only rows are scored on status (and error text) alone."""
    meta = {"id": f"{req['surface']}/{req['id']}", "row": req["row"], "surface": req["surface"], "kind": req["kind"] or "json",
            "endpoint": req["path"], "may_be_empty": True, "lh_only": req["lh_only"]}
    if req["expect_error"]:
        meta["expect_error"] = True
    if req["kind"] is not None:
        return evaluate_case(meta, answers, resamples)
    res = CaseResult(id=meta["id"], row=req["row"], surface=req["surface"], signal=SURFACE_SIGNAL[req["surface"]], endpoint=req["path"],
                     verdict="harness-error")
    verdicts = []
    for ans in [answers] + resamples:
        b, p = status_only(ans, req["lh_only"])
        verdicts.append(classify(b, p))
        if ans is answers:
            res.base, res.pr = b, p
    res.samples = verdicts
    res.verdict = classify_samples(verdicts)
    return res


LIMIT_TAIL = re.compile(r"\s*\|\s*limit\s+\d+\s*$")


def canonical_rows(body: str) -> list[str]:
    out = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            try:
                out.append(json.dumps(json.loads(line), sort_keys=True))
            except ValueError:
                out.append(line)
    return out


def limited_facets(universe: set[str], total: int, ans: dict, ref_count: int) -> FacetResult:
    """A limited query keeps an arbitrary subset, so two correct systems answer different rows. What must hold is that every
    row an answer returns belongs to the unlimited reference answer (the pipes did the same thing to it), that an answer is
    empty exactly when the reference is, and that it returns as many rows as the reference's limited answer (a limit that keeps
    10 of 5000 rows must keep 10, not 1)."""
    res = FacetResult()
    rows = canonical_rows(ans["body"]) if (ans.get("status") or 0) == 200 else []
    res.facets["status"] = 100.0 if ans.get("status") == 200 else 0.0
    inside = sum(1 for r in rows if r in universe)
    res.facets["membership"] = 100.0 if not rows else 100.0 * inside / len(rows)
    res.facets["emptiness"] = 100.0 if bool(rows) == bool(total) else 0.0
    res.facets["cardinality"] = 100.0 if len(rows) == ref_count else 100.0 * min(len(rows), ref_count) / max(len(rows), ref_count)
    if len(rows) != ref_count:
        res.notes.append(f"{len(rows)} rows where the reference's limited answer has {ref_count}")
    if rows and inside < len(rows):
        res.notes.append(f"{len(rows) - inside} of {len(rows)} rows are not in the unlimited reference answer")
    res.details["rows"] = len(rows)
    return res


def evaluate_limited(state: dict, req: dict, answers: dict[str, dict], resamples: list[dict[str, dict]]) -> CaseResult:
    unlimited = dict(req, params=dict(req["params"], query=LIMIT_TAIL.sub("", req["params"]["query"])))
    env = send(state, unlimited, "ref")
    universe = set(canonical_rows(env["body"])) if env["status"] == 200 else set()
    total = len(universe)
    res = CaseResult(id=f"{req['surface']}/{req['id']}", row=req["row"], surface=req["surface"], signal=SURFACE_SIGNAL[req["surface"]], endpoint=req["path"],
                     verdict="harness-error")
    if env["status"] != 200:
        res.latency["error"] = f"the unlimited reference answer is {env['status']}"
        return res
    verdicts = []
    for ans in [answers] + resamples:
        ref_count = len(canonical_rows(ans["ref"]["body"])) if (ans["ref"].get("status") or 0) == 200 else 0
        b, p = limited_facets(universe, total, ans["base"], ref_count), limited_facets(universe, total, ans["pr"], ref_count)
        verdicts.append(classify(b, p))
        if ans is answers:
            res.base, res.pr = b, p
    res.samples = verdicts
    res.verdict = classify_samples(verdicts)
    return res


def value_count(ans: dict) -> int | None:
    """How many tags or values a Tempo tag answer lists; None when it is not a 200 or not a tag/value list."""
    if (ans.get("status") or 0) != 200:
        return None
    try:
        return len(decode_values(json.loads(ans["body"])))
    except (ValueError, UnknownShape, AttributeError, TypeError):
        return None


def shape_facets(ref: dict, ans: dict) -> FacetResult:
    """Scored by status, shape and non-emptiness: the content of a sampled answer proves nothing."""
    res = FacetResult()
    res.facets["status"] = 100.0 if ans.get("status") == ref.get("status") else 0.0
    n, r = value_count(ans), value_count(ref)
    res.facets["shape"] = 100.0 if (n is None) == (r is None) else 0.0
    res.facets["non_empty"] = 100.0 if bool(n) == bool(r) else 0.0
    if res.facets["non_empty"] < 100:
        res.notes.append(f"{n} entries where the reference lists {r}")
    return res


def evaluate_shape_only(req: dict, answers: dict[str, dict], resamples: list[dict[str, dict]]) -> CaseResult:
    res = CaseResult(id=f"{req['surface']}/{req['id']}", row=req["row"], surface=req["surface"], signal=SURFACE_SIGNAL[req["surface"]], endpoint=req["path"],
                     verdict="harness-error")
    verdicts = []
    for ans in [answers] + resamples:
        b, p = shape_facets(ans["ref"], ans["base"]), shape_facets(ans["ref"], ans["pr"])
        verdicts.append(classify(b, p))
        if ans is answers:
            res.base, res.pr = b, p
    res.samples = verdicts
    res.verdict = classify_samples(verdicts)
    return res


def p50(values: list[float]) -> float | None:
    return round(statistics.median(values), 2) if values else None


def run_request(state: dict, req: dict, rounds: int) -> dict:
    targets = [t for t in TARGETS if not (req["lh_only"] and t == "ref")]
    envs: list[dict[str, dict]] = []
    for rnd in range(rounds):
        order = targets[rnd % len(targets):] + targets[:rnd % len(targets)]
        envs.append({t: send(state, req, t) for t in order})
    first, rest = envs[0], envs[1:]
    lat = {t: p50([e[t]["latency_ms"] for e in envs if e[t].get("status")]) for t in targets}
    status = {t: "/".join(sorted({str(e[t]["status"]) for e in envs})) for t in targets}
    out = {"id": req["id"], "row": req["row"], "registry_surface": req["registry_surface"], "surface": req["surface"], "signal": req["signal"],
           "method": req["method"], "path": req["path"], "compare": req["compare"], "expect": req["expect"], "lh_only": req["lh_only"],
           "status": status, "p50_ms": lat, "status_changed": status.get("base") != status.get("pr"), "samples": [], "notes": [],
           "base_score": None, "pr_score": None, "base_worst": None, "pr_worst": None, "error": None}
    sides = [first[t] for t in targets]
    if all(400 <= (e.get("status") or 0) < 500 for e in sides) and not req["expect_error"]:
        # hot VictoriaLogs/VictoriaTraces itself rejects the request the registry declares: the row needs fixing, not the PR
        return dict(out, verdict="unusable", error=f"every target answers {status} to the request the registry row declares", status_changed=False)
    lim = req.get("limit")
    if lim is not None and req["kind"] == "rows" and not req["lh_only"] and not req.get("arbitrary"):
        result = evaluate_limited(state, req, first, rest)  # scored by membership in the unlimited reference answer
    elif req.get("shape_only") and not req["lh_only"]:
        result = evaluate_shape_only(req, first, rest)  # a sampled answer: status, shape and non-emptiness only
    else:
        result = evaluate(req, first, rest)
    verdict = result.verdict
    ref_env = first.get("ref") or first["base"]
    truncated = lim is not None and req["kind"] not in (None, "rows") and (not lim[1] or sum(1 for ln in ref_env["body"].splitlines() if ln.strip()) >= lim[0])
    if (req.get("arbitrary") or truncated) and verdict not in ("harness-error", "blocked"):
        verdict = "arbitrary"  # not scored: the request asks for an arbitrary subset (facets, tag samples, limited lakehouse-only rows)
    return dict(out, verdict=verdict, samples=result.samples, notes=(result.pr.notes if result.pr else [])[:3],
                base_score=None if result.base is None else result.base.score, pr_score=None if result.pr is None else result.pr.score,
                base_worst=None if result.base is None else result.base.worst[0], pr_worst=None if result.pr is None else result.pr.worst[0],
                error=result.latency.get("error") if result.latency else None)


def latency_verdict(row: dict) -> str:
    b, p = row["p50_ms"].get("base"), row["p50_ms"].get("pr")
    if b is None or p is None:
        return "n/a"
    if p > b * (1 + LATENCY_REL) and p - b > LATENCY_ABS_MS:
        return "slower"
    if b > p * (1 + LATENCY_REL) and b - p > LATENCY_ABS_MS:
        return "faster"
    return "same"


NOT_PROVING = ("vacuous", "blocked", "arbitrary", "unusable")


def failing(row: dict) -> bool:
    """regressed and harness-error fail; so does a status change from base to PR. A request whose verdict flips between samples fails only
    when one of the samples had the PR worse than the base: a flip the other way is the base being unstable, not the PR."""
    if row["verdict"] == "nondeterministic":
        return "regressed" in row["samples"]
    return row["verdict"] in FAILING or (row["status_changed"] and row["verdict"] not in NOT_PROVING)


def summarize(results: list[dict], skipped: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for r in results:
        r["latency"] = latency_verdict(r)
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    return {"requests": len(results), "verdicts": counts, "failing": [r["id"] for r in results if failing(r)],
            "status_changes": [r["id"] for r in results if r["status_changed"]],
            "slower": [r["id"] for r in results if r["latency"] == "slower"],
            "not_exercised": len(skipped), "not_exercised_reasons": api_rows.skip_counts(skipped)}


def run_all(state: dict, rows: list[dict], rounds: int = 3, only: str = "", log=lambda m: None) -> dict:
    values = api_rows.window_values(matrix_window(state))
    picks = pick_values(state)
    reqs, skipped = api_rows.translate(rows, values, picks)
    if only:
        reqs = [r for r in reqs if re.search(only, r["id"])]
    results = []
    for i, req in enumerate(reqs):
        results.append(run_request(state, req, rounds))
        if i % 25 == 0:
            log(f"api matrix {i + 1}/{len(reqs)}")
    return {"results": results, "skipped": skipped, "summary": summarize(results, skipped), "rounds": rounds,
            "window": matrix_window(state), "state": {k: state[k] for k in ("H", "cold", "project")}}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--only", default="")
    a = ap.parse_args(argv)
    state = abreg.load_json(a.state)
    res = run_all(state, abreg.load_rows(), a.rounds, a.only, log=lambda m: print(m, file=sys.stderr, flush=True))
    abreg.dump_json(a.out, res)
    s = res["summary"]
    print(f"{s['requests']} requests: {s['verdicts']}; failing {len(s['failing'])}; not exercised {s['not_exercised']}")
    return 1 if s["failing"] else 0


if __name__ == "__main__":
    sys.exit(main())
