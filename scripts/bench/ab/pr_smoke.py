#!/usr/bin/env python3
"""The per-PR perf smoke: base build vs PR build vs hot VictoriaLogs/VictoriaTraces on one fresh, isolated stack.

  python3 scripts/bench/ab/pr_smoke.py --base origin/main --out /tmp/ab-smoke               # head = HEAD
  python3 scripts/bench/ab/pr_smoke.py --base <sha> --head origin/some-branch --out /tmp/ab
  python3 scripts/bench/ab/pr_smoke.py --same-build --out /tmp/ab-aa                         # A/A: the noise floor

Steps (each its own module, usable alone):
  1. selection.py     the shape sets the changed files need (nothing for docs/CI-only changes: the comments say "skipped")
  2. stack.py         detached trees of base and head, their images, the stack (scripts/proof's compose + toxiproxy), the seed,
                      the forced compaction (compacted layer), the buffer layer; scripts/proof's seed-equality gate
  3. latency.py       S3 latency 0 ms, then 50 ms on the control smoke shapes; every profile verified by a probe (a failed probe
                      invalidates the run)
  4. measure.py       every selected cell, three targets interleaved, every answer validated; a cell that moved beyond noise is
                      measured again with --confirm-runs runs
  5. api_matrix.py    every read-only registry row against base, PR and hot VL/VT
  6. components.py    ingest throughput and compaction, base vs PR
  7. known.py         the differences found, joined with the registry rows and issues; UNTRACKED and NEW ones fail
  8. comment.py       perf-smoke.md and api-matrix.md plus their verdicts

Outputs in --out: selection.json, state.json, raw-*.json, summary-*.json, api-matrix.json, components.json, known-perf.json, known-api.json,
perf-smoke.md, api-matrix.md, perf-verdict.json, api-verdict.json, meta.json.
Exit status: the larger of the two verdicts (comment.py documents the codes).
"""
from __future__ import annotations

import argparse
import functools
import os
import re
import subprocess
import sys
import time
import traceback
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import abreg  # noqa: E402
import api_matrix  # noqa: E402
import comment  # noqa: E402
import components  # noqa: E402
import known as knownmod  # noqa: E402
import latency  # noqa: E402
import measure  # noqa: E402
import report  # noqa: E402
import selection  # noqa: E402
import stack  # noqa: E402

CONTROL = selection.CONTROL
MOVED = report.MOVED


def log(msg: str) -> None:
    print(f"[pr-smoke] {msg}", file=sys.stderr, flush=True)


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=abreg.ROOT, check=True, capture_output=True, text=True).stdout.strip()


def pins() -> dict:
    """The upstream versions the reference runs, from the Makefile (the one place that pins them)."""
    with open(os.path.join(abreg.ROOT, "Makefile"), encoding="utf-8") as f:
        text = f.read()
    out = {}
    for key, name in (("VL_VERSION_LOGS", "VictoriaLogs"), ("VT_VERSION", "VictoriaTraces")):
        m = re.search(rf"^{key}\s*:=\s*(\S+)", text, re.M)
        if m:
            out[name] = m.group(1)
    out["RustFS"] = re.search(r"rustfs/rustfs:([^@\s]+)", stack.e2e_pin(r"(rustfs/rustfs:\S+)")).group(1)
    out["toxiproxy"] = stack.toxiproxy_image().rsplit(":", 1)[-1]
    return out


def select_all(spec: dict) -> dict:
    return {"run": True, "changed": 0, "relevant": ["(all sets requested)"], "ignored": [], "why": {},
            "sets": {n: [s["name"] for s in v["shapes"]] for n, v in spec["sets"].items()}}


def merge_raw(parts: list[dict]) -> dict:
    out = {"rows": [], "s3": {}, "process": {}, "elapsed_s": 0.0}
    for p in parts:
        out["rows"] += p["rows"]
        out["s3"].update(p["s3"])
        out["elapsed_s"] += p.get("elapsed_s", 0)
        for k, v in p["process"].items():
            cur = out["process"].setdefault(k, {"cpu_s": 0.0, "peak_rss_mb": 0.0})
            cur["cpu_s"] = round((cur["cpu_s"] or 0) + (v.get("cpu_s") or 0), 2)
            cur["peak_rss_mb"] = max(cur["peak_rss_mb"], v.get("peak_rss_mb") or 0)
    out["elapsed_s"] = round(out["elapsed_s"], 1)
    return out


def run_cells(state: dict, spec: dict, cells: list[dict], runs: int, shift_s: int = 0) -> dict:
    return measure.run_matrix(state, spec, cells, runs, log=log, shift_s=shift_s) if cells else {"rows": [], "s3": {}, "process": {}, "elapsed_s": 0}


def reload_state(out: str) -> dict:
    return abreg.load_json(os.path.join(out, "state.json"))


def confirm(summary: dict, state: dict, spec: dict, cells_by_key: dict, args) -> dict:
    """Measure again the cells the first pass called slower, faster or unstable, with more runs and every window moved measure.SHIFT_S
    earlier (the seeded data is static; a shifted window is the nearest thing to a fresh one, and it does not time the caches the first
    pass filled). Only a move or a problem that holds is reported: a transient error is listed as flaky."""
    moved = [cells_by_key[c["cell"]] for c in summary["cells"] if c["verdict"] in MOVED and c["cell"] in cells_by_key]
    if not moved:
        return summary
    log(f"re-measuring {len(moved)} cell(s) the first pass moved")
    again = report.summarize(run_cells(state, spec, moved, args.confirm_runs, measure.SHIFT_S), "confirm", args.noise, args.min_delta_ms, report.ref_skip_from(spec), report.sample_skip_from(spec))
    return report.merge_confirm(summary, again)


def profile_cells(spec: dict, cells: list[dict]) -> list[dict]:
    """The cells of a non-zero S3 latency profile: the control smoke shapes on the flushed layers."""
    return [c for c in cells if c["set"] in CONTROL and c["shape"].get("smoke") and c["layer"] in ("cold", "compacted")]


PROBE_SHAPES = {"logs": {"name": "latency probe", "signal": "logs", "api": "logsql", "kind": "scalar", "query": "level:ERROR | stats count() n"},
                "traces": {"name": "latency probe", "signal": "traces", "api": "logsql", "kind": "scalar",
                           "query": "span_id:* `resource_attr:service.name`:=\"api-gateway\" | stats count() n"}}


def lh_means(state: dict) -> dict:
    """Mean S3 request duration of one cold query on each of the four Lakehouse processes, from their own histograms."""
    end = state["layers"]["cold"]["end"]
    start = (measure.parse_ts(end) - measure.dt.timedelta(hours=1)).strftime(measure.FMT)
    out = {}
    for target in ("base", "pr"):
        for sig, shape in PROBE_SHAPES.items():
            req = measure.build_request(shape, state, target, start, end, "numeric")
            out[f"{target}-{sig}"] = latency.lh_s3_mean_ms(measure.base_url(state, target, sig), functools.partial(measure.send, req), measure.PEER_KEY)
    return out


def set_latency(ms: float, state: dict, means_at_zero: dict | None = None) -> dict:
    """Inject `ms` and prove it twice: the probe through the proxy vs direct must measure it, and the own S3 request duration of
    each of the four Lakehouse processes must have risen by about as much (so all their traffic crosses the proxy). Else the run
    is invalid."""
    api = stack.api_url()
    latency.set_latency(api, ms)
    ports = stack.extra_ports()
    res = latency.probe(f"http://127.0.0.1:{ports['ab-s3-direct']}/health", f"http://127.0.0.1:{ports['ab-s3-proxied']}/health", ms)
    res["lh_mean_ms"] = lh_means(state)
    if ms > 0:
        res["lh_view"] = {k: latency.lh_view_ok((means_at_zero or {}).get(k), v, ms) for k, v in res["lh_mean_ms"].items()}
        res["ok"] = res["ok"] and all(v["ok"] for v in res["lh_view"].values())
    shown = ", ".join(f"{k} {v:.0f}" if v is not None else f"{k} none" for k, v in res["lh_mean_ms"].items())
    log(f"S3 latency {ms:g} ms: probe measured +{res['added_ms']} ms; Lakehouse's own S3 mean (ms): {shown} ({'ok' if res['ok'] else 'FAILED'})")
    return res


def measure_all(args, sel: dict, spec: dict, out: str, meta: dict) -> dict:
    ns = types.SimpleNamespace(out=out)
    cells = measure.plan(spec, list(sel["sets"]), sel["sets"])
    by_key = {c["key"]: c for c in cells}
    log(f"{len(cells)} cells in {len(sel['sets'])} set(s)")
    stack.cmd_up(ns, reuse=args.reuse_stack, preflighted=True)
    stack.cmd_seed(ns)
    stack.cmd_compact(ns)
    stack.cmd_hold(ns)
    state = reload_state(out)
    # the same rows on hot, base and PR for every tenant form and layer, before anything is compared
    from scripts.proof.runner import run as proof_run  # noqa: PLC0415
    forms = proof_run.SEED_FORMS
    log("waiting for the counts to settle, then checking seed equality")
    proof_run.wait_stable(state, forms=forms, layers=("cold", "buffer"))
    seed = proof_run.seed_equality(state, forms=forms, layers=("cold", "buffer"))
    meta["seed_equality"] = {k: v["counts"]["ref"] for k, v in seed.items()}
    c = meta["seed_equality"]
    meta["data"] = (f"{stack.HOURS_BACK} h seeded in {stack.CHUNKS} chunks; tenant 0:0 {c.get('logs/numeric/cold')} log rows and {c.get('traces/numeric/cold')} spans, "
                    f"tenant 1001:0 (alias acme-corp) {c.get('logs/numeric1001/cold')} and {c.get('traces/numeric1001/cold')}; buffer window {c.get('logs/numeric/buffer')} rows and "
                    f"{c.get('traces/numeric/buffer')} spans")
    probes = [set_latency(0.0, state)]
    mean0 = probes[0]["lh_mean_ms"]  # per process
    pre = [c for c in cells if c["layer"] != "after-restart"]
    post = [c for c in cells if c["layer"] == "after-restart"]
    parts = [run_cells(state, spec, pre, args.runs)]
    if post:
        stack.cmd_restart(ns)
        state = reload_state(out)
        parts.append(run_cells(state, spec, post, args.runs))
    raw = merge_raw(parts)
    raw.update(runs=args.runs, sets=list(sel["sets"]), state={k: state[k] for k in ("H", "layers", "project")})
    abreg.dump_json(os.path.join(out, "raw-0ms.json"), raw)
    summary = report.summarize(raw, "pr-smoke 0 ms", args.noise, args.min_delta_ms, report.ref_skip_from(spec), report.sample_skip_from(spec))
    summary = confirm(summary, state, spec, by_key, args)
    abreg.dump_json(os.path.join(out, "summary-0ms.json"), summary)
    extra = {}
    for ms_ in args.latencies:
        if ms_ <= 0:
            continue
        sub = profile_cells(spec, cells)
        probes.append(set_latency(ms_, state, mean0))
        if sub:
            r = run_cells(state, spec, sub, args.runs)
            r.update(runs=args.runs, sets=list(sel["sets"]))
            s = report.summarize(r, f"pr-smoke {ms_:g} ms", args.noise, args.min_delta_ms, report.ref_skip_from(spec), report.sample_skip_from(spec))
            extra[f"{ms_:g}"] = confirm(s, state, spec, {c["key"]: c for c in sub}, args)
            abreg.dump_json(os.path.join(out, f"summary-{ms_:g}ms.json"), extra[f"{ms_:g}"])
    latency.clear(stack.api_url())
    meta["latency"] = probes
    log("API matrix")
    matrix = api_matrix.run_all(state, abreg.load_rows(), args.rounds, log=log)
    abreg.dump_json(os.path.join(out, "api-matrix.json"), matrix)
    log("components")
    comps = components.measure(state)
    abreg.dump_json(os.path.join(out, "components.json"), comps)
    return {"summary": summary, "extra": extra, "matrix": matrix, "components": comps, "state": state}


def extra_profile_lines(extra: dict) -> list[str]:
    lines = []
    for ms_, s in extra.items():
        moved = [c for c in s["cells"] if c["verdict"] not in ("same", "n/a") or c["difference"] == "new" or "more" in c["s3_verdict"].values()]
        lines += ["", f"<details><summary>S3 latency {ms_} ms (verified by probe): {len(s['cells'])} control smoke cells, "
                      f"{', '.join(f'{n} {k}' for k, n in s['verdicts'].items())}</summary>", ""]
        lines += comment.perf_table(moved or s["cells"][:12])
        lines += ["", "</details>"]
    return lines


def finish(out: str, perf_text: str, api_text: str, perf_v: dict, api_v: dict, meta: dict) -> int:
    abreg.write_text(os.path.join(out, "perf-smoke.md"), perf_text)
    abreg.write_text(os.path.join(out, "api-matrix.md"), api_text)
    abreg.dump_json(os.path.join(out, "perf-verdict.json"), perf_v)
    abreg.dump_json(os.path.join(out, "api-verdict.json"), api_v)
    abreg.dump_json(os.path.join(out, "meta.json"), meta)
    print(perf_text)
    print(api_text)
    return max(perf_v["exit"], api_v["exit"])


def render_all(out: str, result: dict, sel: dict, spec: dict, meta: dict, args) -> int:
    """Known differences, the two comments and their verdicts, from the results of a run (saved in --out, so --rerender can redo it)."""
    rows = abreg.load_rows()
    gaps = abreg.gaps_doc_issues()
    # each comment reports the differences its own run found, so a perf check never fails on an API row and the reverse
    known_perf = knownmod.join(knownmod.observed_from_perf(result["summary"], spec, result["extra"]), rows, gaps, args.require_issue)
    known = knownmod.join(knownmod.observed_from_api(result["matrix"]), rows, gaps, args.require_issue)
    known["registry"] = known_perf["registry"] = meta["registry_gap_stats"] = knownmod.registry_gap_stats(rows, gaps)
    abreg.dump_json(os.path.join(out, "known-perf.json"), known_perf)
    abreg.dump_json(os.path.join(out, "known-api.json"), known)
    perf_text, perf_v = comment.render_perf(result["summary"], sel, meta, known_perf, result["components"], result["extra"], args.gate_s3_all)
    if result["extra"]:
        marker = "\n<sub>"
        i = perf_text.rfind(marker)
        perf_text = perf_text[:i] + "\n".join(extra_profile_lines(result["extra"])) + "\n" + perf_text[i:]
    api_text, api_v = comment.render_api(result["matrix"], sel, meta, known)
    return finish(out, perf_text, api_text, perf_v, api_v, meta)


def rerender(out: str, args) -> int:
    """Rebuild summaries, known differences and comments from the saved raw results of an earlier run, without measuring."""
    spec = abreg.load_json(abreg.SHAPES)
    meta = abreg.load_json(os.path.join(out, "meta.json"))
    sel = abreg.load_json(os.path.join(out, "selection.json"))
    # the saved summary already holds the re-measured cells; only the result-vs-hot fields are derived again
    summary = report.apply_ref_skip(abreg.load_json(os.path.join(out, "summary-0ms.json")), report.ref_skip_from(spec), report.sample_skip_from(spec))
    extra = {}
    for name in sorted(os.listdir(out)):
        m = re.fullmatch(r"summary-(\d+(?:\.\d+)?)ms\.json", name)
        if m and float(m.group(1)) > 0:
            extra[m.group(1)] = abreg.load_json(os.path.join(out, name))
    result = {"summary": summary, "extra": extra, "matrix": abreg.load_json(os.path.join(out, "api-matrix.json")),
              "components": abreg.load_json(os.path.join(out, "components.json"))}
    return render_all(out, result, sel, spec, meta, args)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default="", help="base git ref (default: merge-base of origin/main and the head)")
    ap.add_argument("--head", default="HEAD", help="the PR's git ref")
    ap.add_argument("--base-dir", default="", help="an existing tree of the base commit (default: a detached worktree this run creates and removes)")
    ap.add_argument("--head-dir", default="", help="an existing tree of the head commit (default: likewise)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--sets", default="", help="comma-separated set names to run instead of the selection ('all' for every set)")
    ap.add_argument("--runs", type=int, default=6, help="per target and cell; the first is cold")
    ap.add_argument("--confirm-runs", type=int, default=7)
    ap.add_argument("--rounds", type=int, default=3, help="API matrix rounds per row")
    ap.add_argument("--latencies", default="0,50", help="S3 latency profiles in ms; profiles above 0 run the control smoke shapes only")
    ap.add_argument("--noise", type=float, default=report.NOISE_REL)
    ap.add_argument("--min-delta-ms", type=float, default=report.NOISE_ABS_MS)
    ap.add_argument("--same-build", action="store_true", help="A/A: the head build is also the base (the noise floor)")
    ap.add_argument("--skip-build", action="store_true", help="the images already exist (stack.py build)")
    ap.add_argument("--keep-stack", action="store_true", help="leave the stack and the trees (for a re-run)")
    ap.add_argument("--rerender", action="store_true", help="rebuild summaries, known differences and comments from the saved results in --out; measures nothing")
    ap.add_argument("--gate-s3-all", action="store_true", help="more S3 GETs or bytes than the base fail the check on every shape, not only on the control sets (off by default: owner decision)")
    ap.add_argument("--reuse-stack", action="store_true", help="use a stack of this project that is already running instead of refusing (it is also left up)")
    ap.add_argument("--require-issue", action="store_true", help="a known difference whose registry row cites no GitHub issue fails")
    a = ap.parse_args(argv)
    a.latencies = [float(x) for x in a.latencies.split(",") if x]
    out = os.path.abspath(a.out)
    if a.rerender:
        return rerender(out, a)
    os.makedirs(out, exist_ok=True)
    t0 = time.time()
    meta: dict = {"runs": a.runs, "confirm_runs": a.confirm_runs, "same_build": a.same_build, "versions": {}, "host_load": ", ".join(f"{x:.1f}" for x in os.getloadavg())}
    sel: dict = {"run": True, "sets": {}}
    spec = abreg.load_json(abreg.SHAPES)
    trees: list[str] = []
    result = None
    owns_stack = images = False
    try:
        head = a.head
        meta["head"] = git("rev-parse", head)
        base = a.base or git("merge-base", "origin/main", head)
        meta["base"] = git("rev-parse", base)
        meta["versions"] = pins()
        if a.sets:
            sel = select_all(spec)
            if a.sets != "all":
                sel["sets"] = {n: sel["sets"][n] for n in a.sets.split(",")}
        else:
            sel = selection.select(selection.changed_files(meta["base"], meta["head"]), spec, selection.build_index(abreg.load_rows(), abreg.load_features()),
                                   base=meta["base"], head=meta["head"])
        abreg.dump_json(os.path.join(out, "selection.json"), sel)
        if not sel["run"]:
            perf_text, perf_v = comment.render_perf(None, sel, meta)
            api_text, api_v = comment.render_api(None, sel, meta)
            return finish(out, perf_text, api_text, perf_v, api_v, meta)
        parent = os.path.join(out, "trees")
        os.makedirs(parent, exist_ok=True)
        head_dir = a.head_dir or None
        base_dir = a.base_dir or None
        # refuse before anything is built or tagged when the project's stack is somebody else's; only after this passes is it ours to remove
        owns_stack = not stack.preflight(a.reuse_stack)
        if not a.skip_build:
            images = True  # from here on a failure may leave images or worktrees behind: the teardown removes what this run made
            if head_dir is None:
                head_dir = stack.tree_add(meta["head"], parent, "head")
                trees.append(head_dir)
            if base_dir is None:
                base_dir = head_dir if a.same_build else stack.tree_add(meta["base"], parent, "base")
                if base_dir != head_dir:
                    trees.append(base_dir)
            log("building the images")
            stack.cmd_build(types.SimpleNamespace(base=base_dir, pr=head_dir))
        result = measure_all(a, sel, spec, out, meta)
        meta["elapsed_s"] = round(time.time() - t0)
        meta["host_load_end"] = ", ".join(f"{x:.1f}" for x in os.getloadavg())
        return render_all(out, result, sel, spec, meta, a)
    except stack.StackBusy as e:
        reason = f"StackBusy: {e}"
        perf_text, perf_v = comment.errored(sel, meta, reason, comment.PERF_MARKER, "Performance A/B vs main, against VictoriaLogs / VictoriaTraces")
        api_text, api_v = comment.errored(sel, meta, reason, comment.API_MARKER, "API endpoints: Lakehouse base vs PR vs VictoriaLogs / VictoriaTraces")
        perf_v["exit"] = api_v["exit"] = stack.EXIT_BUSY
        return finish(out, perf_text, api_text, perf_v, api_v, meta)
    except (Exception, SystemExit) as e:  # noqa: BLE001 - any failure must still produce the comments and exit 3
        traceback.print_exc()
        meta["elapsed_s"] = round(time.time() - t0)
        reason = f"{type(e).__name__}: {e}"
        perf_text, perf_v = comment.errored(sel, meta, reason, comment.PERF_MARKER, "Performance A/B vs main, against VictoriaLogs / VictoriaTraces")
        api_text, api_v = comment.errored(sel, meta, reason, comment.API_MARKER, "API endpoints: Lakehouse base vs PR vs VictoriaLogs / VictoriaTraces")
        return finish(out, perf_text, api_text, perf_v, api_v, meta)
    finally:
        if not a.keep_stack:
            steps = ([("stack", stack.down_stack)] if owns_stack else []) + ([("images", stack.remove_images)] if images and owns_stack else []) \
                + [(f"tree {t}", functools.partial(stack.tree_remove, t)) for t in trees]
            for step, fn in steps:
                try:
                    fn()
                except Exception as e:  # noqa: BLE001 - teardown must go on
                    log(f"teardown {step} failed: {e}")


if __name__ == "__main__":
    sys.exit(main())
