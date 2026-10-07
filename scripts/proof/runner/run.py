#!/usr/bin/env python3
"""API data-proof runner: send each row's requests to hot VictoriaLogs/VictoriaTraces (ref), Lakehouse
built from main (base) and Lakehouse built from the PR (pr), and score the answers.

  python3 -m scripts.proof.runner.run --state OUT/state.json --out OUT/api --tier core [--tier field-values]

Per request the three targets are called one after the other in the order ref, base, pr (the same
moment, the same data). The answers are written as case directories (meta.json, ref.json, base.json,
pr.json, resample-N/) that scripts/proof/metrics scores; report.md, report.json and bodies.jsonl.gz
are written next to them. A request whose verdict is neither `exact` nor `same` is sampled twice more
(all three targets, fresh connections): re-sampling only classifies, a flipping verdict is
`nondeterministic`, never a retry into a pass.

Exit codes: 0 done, 1 a failing verdict (regressed, nondeterministic, harness-error),
2 the run did not complete (seed equality failed, a target is down).
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import sys
import time

from ..metrics.cases import evaluate_case, run_dir
from ..metrics.verdict import FAILING
from . import client
from .report import write_reports
from .rows import expand, load_tier, tenant_headers, window_of
from ..jsonio import dump_json, load_json, read_text

TARGETS = ("ref", "base", "pr")
RESAMPLES = 2
STABLE = ("exact", "same")


class Incomplete(SystemExit):
    """The run did not complete (exit 2). The reason goes to stderr: a SystemExit with an integer code prints nothing."""

    def __init__(self, msg: str):
        super().__init__(2)
        self.reason = f"incomplete run: {msg}"
        sys.stderr.write(self.reason + "\n")

    def __str__(self) -> str:
        return self.reason


def target_url(state: dict, target: str, signal: str) -> str:
    return f"http://127.0.0.1:{state['ports'][f'{target}-{signal}']}"


SEED_FORMS = ("numeric", "numeric1001", "alias")
SEED_LAYERS = ("cold", "buffer", "all")
COUNT_ALL = "* | stats count() c"
COUNT_SPANS = "span_id:* | stats count() c"
# Identity of a row for the content hash: what one row is, not what a Lakehouse adds to it.
IDENTITY_QUERY = {"logs": "* | fields _time, _msg, trace_id, span_id | sort by (_time, _msg, span_id)",
                  "traces": "span_id:* | fields trace_id, span_id, name | sort by (trace_id, span_id)"}


# the key-order fixture is logs only
FORM_SIGNALS = {"keyorder": ("logs",)}


def signals_of(form: str) -> tuple:
    return FORM_SIGNALS.get(form, ("logs", "traces"))


def _query(state: dict, target: str, signal: str, form: str, layer: str, query: str) -> dict:
    win = window_of(state, layer)
    return client.send("POST", target_url(state, target, signal) + "/select/logsql/query",
                       {"query": query, "start": win["start"], "end": win["end"]}, tenant_headers(form, target))


def count_rows(state: dict, target: str, signal: str, form: str, layer: str, query: str = "") -> int | None:
    env = _query(state, target, signal, form, layer, query or (COUNT_ALL if signal == "logs" else COUNT_SPANS))
    if env["status"] != 200:
        return None
    try:
        return int(json.loads(env["body"].strip().splitlines()[-1])["c"])
    except (ValueError, KeyError, IndexError):
        return None


def content_hash(state: dict, target: str, signal: str, form: str, layer: str) -> str | None:
    """sha256 of the canonical identity rows of the window: equal counts with different rows are not equal data."""
    env = _query(state, target, signal, form, layer, IDENTITY_QUERY[signal])
    if env["status"] != 200:
        return None
    try:
        rows = sorted(json.dumps(json.loads(x), sort_keys=True) for x in env["body"].splitlines() if x.strip())
    except ValueError:
        return None
    return hashlib.sha256("\n".join(rows).encode()).hexdigest()[:16]


def snapshot(state: dict, forms, layers) -> dict:
    """Counts of `*` (and of span rows for traces) per signal, form, layer and target."""
    out = {}
    for form in forms:
        for signal in signals_of(form):
            for layer in layers:
                for t in TARGETS:
                    out[f"{signal}/{form}/{layer}/{t}/all"] = count_rows(state, t, signal, form, layer, COUNT_ALL)
                    if signal == "traces":
                        out[f"{signal}/{form}/{layer}/{t}/spans"] = count_rows(state, t, signal, form, layer, COUNT_SPANS)
    return out


def wait_stable(state: dict, forms=SEED_FORMS, layers=SEED_LAYERS, timeout: float = 120, step: float = 3) -> None:
    """Rows reach Lakehouse's insert buffer late (trace-index rows included): wait until the count of `*` is the same
    on two consecutive reads for every signal, form, layer and target. A count that keeps moving fails the run."""
    deadline = time.time() + timeout
    last = None
    while True:
        cur = snapshot(state, forms, layers)
        if cur == last and None not in cur.values():
            return
        if time.time() > deadline:
            moving = sorted(k for k in cur if last is None or cur[k] != last.get(k))
            raise Incomplete(f"counts still moving after {timeout:.0f}s: {moving[:6]}")
        last = cur
        time.sleep(step)


def seed_equality(state: dict, forms=SEED_FORMS, layers=SEED_LAYERS) -> dict:
    """The same data on ref, base and PR for every tenant form, layer and signal, before any comparison:
    the same count of span rows (logs: of all rows) and the same hash of the row identities. Zero rows on all three
    is an error: every tenant, window and layer is seeded. Trace counts of `*` differ by design (hot's trace-index
    rows, #458), so for traces `*` is only reported, and has to be stable (wait_stable)."""
    seen = {}
    for form in forms:
        for signal in signals_of(form):
            for layer in layers:
                key = f"{signal}/{form}/{layer}"
                counts = {t: count_rows(state, t, signal, form, layer) for t in TARGETS}
                hashes = {t: content_hash(state, t, signal, form, layer) for t in TARGETS}
                seen[key] = {"counts": counts, "hashes": hashes,
                             "all": {t: count_rows(state, t, signal, form, layer, COUNT_ALL) for t in TARGETS}}
                if None in counts.values() or len(set(counts.values())) != 1:
                    raise Incomplete(f"seed equality failed for {key}: {counts}")
                if set(counts.values()) == {0}:
                    raise Incomplete(f"seed equality: no rows at all for {key} (every tenant and window is seeded)")
                if None in hashes.values() or len(set(hashes.values())) != 1:
                    raise Incomplete(f"seed equality: same counts, different rows for {key}: {hashes}")
    return seen


def buffered_rows(state: dict, target: str, signal: str, form: str, layer: str) -> int | None:
    """Rows of the tenant held unflushed in the insert buffer in the layer's window (reported, never absorbed)."""
    if target == "ref" or layer != "buffer":
        return None
    from ..stack import buffered_rows as br
    acct = {"alias": "1001", "numeric1001": "1001", "keyorder": "7"}.get(form, "0")
    try:
        return br(state["ports"][f"{target}-{signal}"], signal, state[layer]["start"], state[layer]["end"], acct, "0")
    except Exception:  # noqa: BLE001 - informational only
        return None


def resolve_picks(row: dict, state: dict, layer: str = "cold", form: str = "numeric") -> dict:
    """A value taken once from the reference (first row of a sorted query in the request's window and tenant
    form), used on every target of that request."""
    out = {}
    win = window_of(state, "cold" if layer == "all" else layer)
    for name, spec in (row.get("pick") or {}).items():
        sig = "traces" if spec.get("signal") == "traces" else "logs"
        env = client.send("POST", target_url(state, "ref", sig) + "/select/logsql/query",
                          {"query": spec["query"], "start": win["start"], "end": win["end"], "limit": "1"},
                          tenant_headers(form, "ref"))
        try:
            out[name] = json.loads(env["body"].strip().splitlines()[0])[spec["field"]]
        except (ValueError, KeyError, IndexError):
            raise Incomplete(f"pick {name!r} of row {row['id']} found nothing on the reference: {env['status']} {env['body'][:120]}")
    return out


def case_id(req: dict) -> str:
    r = req["row"]
    return f"{r['surface']}/{r['id']}.{req['form']}.{req['layer']}"


def sample(req: dict, state: dict) -> dict[str, dict]:
    ans = {}
    for t in TARGETS:
        env = client.send(req["method"], target_url(state, t, req["signal"]) + req["path"], req["params"],
                          tenant_headers(req["form"], t))
        env.update(target=t, tenant_form=req["form"], layer=req["layer"])
        b = buffered_rows(state, t, req["signal"], req["form"], req["layer"])
        if b is not None:
            env["buffer_unflushed_rows"] = b
        ans[t] = env
    return ans


def meta_of(req: dict) -> dict:
    r = req["row"]
    m = {"id": case_id(req), "row": r.get("row", r["id"]), "surface": r["surface"], "kind": r["kind"],
         "endpoint": req["path"], "form": req["form"], "layer": req["layer"], "request_id": r["id"]}
    if r.get("claimed_gap"):
        m["claimed_gap"] = r["claimed_gap"]
    if r.get("may_be_empty"):
        m["may_be_empty"] = True
    if r.get("expect_error"):
        m["expect_error"] = True
    if r.get("limit_arbitrary"):
        m["limit_arbitrary"] = True
    m.update(r.get("meta", {}))
    return m


def write_case(root: str, meta: dict, ans: dict, resamples: list[dict]) -> None:
    d = os.path.join(root, "cases", meta["id"])
    os.makedirs(d, exist_ok=True)
    dump_json(os.path.join(d, "meta.json"), meta, indent=1, sort_keys=True)
    for n, e in ans.items():
        dump_json(os.path.join(d, n + ".json"), e, indent=1)
    for i, rs in enumerate(resamples, 1):
        sub = os.path.join(d, f"resample-{i}")
        os.makedirs(sub, exist_ok=True)
        for n, e in rs.items():
            dump_json(os.path.join(sub, n + ".json"), e, indent=1)


def universe_of(req: dict, state: dict) -> list[str]:
    """The reference answer of a limited request without its limit: the values a cut list may keep."""
    params = {**req["params"], "limit": "1000000"}
    env = client.send(req["method"], target_url(state, "ref", req["signal"]) + req["path"], params, tenant_headers(req["form"], "ref"))
    try:
        return [x["value"] for x in json.loads(env["body"])["values"]]
    except (ValueError, KeyError, TypeError):
        raise Incomplete(f"the unlimited reference answer of {case_id(req)} is unusable: {env['status']} {env['body'][:100]}")


def run(state: dict, rows: list[dict], out: str, allowance: int | None = None) -> list:
    reqs = []
    for row in rows:
        reqs += expand(row, state, lambda form, layer, row=row: resolve_picks(row, state, layer, form))
    allowance = allowance if allowance is not None else max(60, len(reqs) // 10)
    spent = 0
    log = gzip.open(os.path.join(out, "bodies.jsonl.gz"), "wt", encoding="utf-8")
    try:
        for req in reqs:
            meta = meta_of(req)
            if meta.get("limit_arbitrary"):
                meta["universe"] = universe_of(req, state)
            ans = sample(req, state)
            res = evaluate_case(meta, ans)
            more = []
            if res.verdict not in STABLE:
                # a request already classed fixed or improved is re-sampled for free: it never uses the allowance
                free = res.verdict in ("fixed", "improved")
                if free or spent < allowance:
                    spent += 0 if free else 1
                    more = [sample(req, state) for _ in range(RESAMPLES)]
                else:
                    meta["resample_skipped"] = True  # reported: a verdict nobody confirmed
            write_case(out, meta, ans, more)
            for rnd, a in enumerate([ans] + more):
                for t, e in a.items():
                    log.write(json.dumps({"request": case_id(req), "round": rnd, "target": t, "method": req["method"],
                                          "path": req["path"], "params": req["params"], **e}) + "\n")
    finally:
        log.close()
    return reqs


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", required=True, help="state.json written by scripts/proof/stack.py")
    ap.add_argument("--out", required=True)
    ap.add_argument("--tier", action="append", required=True, help="row set: a name under runner/rows/ or a path")
    ap.add_argument("--only", help="regexp on the request id")
    ap.add_argument("--no-seed-check", action="store_true")
    ap.add_argument("--pr-label", default="PR")
    a = ap.parse_args(argv)
    state = load_json(a.state)
    os.makedirs(a.out, exist_ok=True)
    rows = load_tier(a.tier)
    if a.only:
        rows = [r for r in rows if re.search(a.only, r["id"])]
    t0 = time.time()
    forms = SEED_FORMS + (("keyorder",) if state.get("keyorder") else ())
    if not a.no_seed_check:
        wait_stable(state, forms=forms)
    seed = {} if a.no_seed_check else seed_equality(state, forms=forms)
    run(state, rows, a.out)
    items = run_dir(os.path.join(a.out, "cases"))
    write_reports(a.out, items, state, seed, label=a.pr_label, seconds=time.time() - t0)
    print(read_text(os.path.join(a.out, "report.txt")))
    return 1 if any(r.verdict in FAILING for _, r in items) else 0


if __name__ == "__main__":
    sys.exit(main())
