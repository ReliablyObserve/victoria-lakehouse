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
import json
import os
import re
import sys
import time

from ..metrics.cases import evaluate_case, run_dir
from ..metrics.verdict import FAILING
from . import client
from .report import write_reports
from .rows import expand, load_tier, tenant_headers

TARGETS = ("ref", "base", "pr")
RESAMPLES = 2
STABLE = ("exact", "same")


class Incomplete(SystemExit):
    def __init__(self, msg: str):
        super().__init__(f"incomplete run: {msg}")
        self.code = 2


def target_url(state: dict, target: str, signal: str) -> str:
    return f"http://127.0.0.1:{state['ports'][f'{target}-{signal}']}"


def count_query(signal: str) -> str:
    return "* | stats count() c" if signal == "logs" else "span_id:* | stats count() c"


def count_rows(state: dict, target: str, signal: str, form: str, layer: str) -> int | None:
    env = client.send("POST", target_url(state, target, signal) + "/select/logsql/query",
                      {"query": count_query(signal), "start": state[layer]["start"], "end": state[layer]["end"]},
                      tenant_headers(form, target))
    if env["status"] != 200:
        return None
    try:
        return int(json.loads(env["body"].strip().splitlines()[-1])["c"])
    except (ValueError, KeyError, IndexError):
        return None


def seed_equality(state: dict, forms=("numeric", "alias"), layers=("cold", "buffer")) -> dict:
    """The same row count on ref, base and PR for every tenant form, layer and signal, before any comparison."""
    seen = {}
    for signal in ("logs", "traces"):
        for form in forms:
            for layer in layers:
                counts = {t: count_rows(state, t, signal, form, layer) for t in TARGETS}
                seen[f"{signal}/{form}/{layer}"] = counts
                if None in counts.values() or len(set(counts.values())) != 1:
                    raise Incomplete(f"seed equality failed for {signal}/{form}/{layer}: {counts}")
    return seen


def buffered_rows(state: dict, target: str, signal: str, form: str, layer: str) -> int | None:
    """Rows of the tenant held unflushed in the insert buffer in the layer's window (reported, never absorbed)."""
    if target == "ref" or layer != "buffer":
        return None
    from ..stack import buffered_rows as br
    acct = "1001" if form in ("alias", "numeric1001") else "0"
    try:
        return br(state["ports"][f"{target}-{signal}"], signal, state[layer]["start"], state[layer]["end"], acct, "0")
    except Exception:  # noqa: BLE001 - informational only
        return None


def resolve_picks(row: dict, state: dict) -> dict:
    """A value taken once from the reference (first row of a sorted query), used on every target."""
    out = {}
    for name, spec in (row.get("pick") or {}).items():
        sig = "traces" if spec.get("signal") == "traces" else "logs"
        env = client.send("POST", target_url(state, "ref", sig) + "/select/logsql/query",
                          {"query": spec["query"], "start": state["cold"]["start"], "end": state["cold"]["end"], "limit": "1"},
                          tenant_headers("numeric", "ref"))
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
    m.update(r.get("meta", {}))
    return m


def write_case(root: str, meta: dict, ans: dict, resamples: list[dict]) -> None:
    d = os.path.join(root, "cases", meta["id"])
    os.makedirs(d, exist_ok=True)
    json.dump(meta, open(os.path.join(d, "meta.json"), "w"), indent=1, sort_keys=True)
    for n, e in ans.items():
        json.dump(e, open(os.path.join(d, n + ".json"), "w"), indent=1)
    for i, rs in enumerate(resamples, 1):
        sub = os.path.join(d, f"resample-{i}")
        os.makedirs(sub, exist_ok=True)
        for n, e in rs.items():
            json.dump(e, open(os.path.join(sub, n + ".json"), "w"), indent=1)


def run(state: dict, rows: list[dict], out: str, allowance: int | None = None) -> list:
    reqs = []
    for row in rows:
        reqs += expand(row, state, resolve_picks(row, state))
    allowance = allowance if allowance is not None else max(60, len(reqs) // 10)
    spent = 0
    log = gzip.open(os.path.join(out, "bodies.jsonl.gz"), "wt", encoding="utf-8")
    try:
        for req in reqs:
            meta = meta_of(req)
            ans = sample(req, state)
            res = evaluate_case(meta, ans)
            more = []
            if res.verdict not in STABLE and spent < allowance:
                if res.verdict not in ("fixed", "improved"):
                    spent += 1
                more = [sample(req, state) for _ in range(RESAMPLES)]
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
    state = json.load(open(a.state))
    os.makedirs(a.out, exist_ok=True)
    rows = load_tier(a.tier)
    if a.only:
        rows = [r for r in rows if re.search(a.only, r["id"])]
    t0 = time.time()
    seed = {} if a.no_seed_check else seed_equality(state)
    run(state, rows, a.out)
    items = run_dir(os.path.join(a.out, "cases"))
    write_reports(a.out, items, state, seed, label=a.pr_label, seconds=time.time() - t0)
    print(open(os.path.join(a.out, "report.txt")).read())
    return 1 if any(r.verdict in FAILING for _, r in items) else 0


if __name__ == "__main__":
    sys.exit(main())
