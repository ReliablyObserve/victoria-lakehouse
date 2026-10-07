"""CLI: python3 -m scripts.proof.metrics <dir of captured triples> [--check] [--json]"""
from __future__ import annotations

import argparse
import json
import sys

from .cases import check_expectations, run_dir
from .report import render_table
from .verdict import FAILING


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="proof-metrics", description=__doc__)
    ap.add_argument("root", help="directory of case directories (meta.json, ref.json, base.json, pr.json)")
    ap.add_argument("--check", action="store_true", help="fail when a case differs from its recorded expectation")
    ap.add_argument("--json", action="store_true", help="print machine-readable results")
    ap.add_argument("--surface", action="append", help="only these surfaces (vl-native, vt-native, jaeger, loki, tempo)")
    ap.add_argument("--core", action="store_true", help="only the native core surfaces (vl-native, vt-native, jaeger)")
    ap.add_argument("--fail-on-regression", action="store_true", help="exit 1 on regressed, nondeterministic or harness-error")
    a = ap.parse_args(argv)

    from .common import CORE_SURFACES
    want = set(a.surface or (CORE_SURFACES if a.core else ()))
    items = [(m, r) for m, r in run_dir(a.root) if not want or r.surface in want]
    if not items:
        print("no cases found", file=sys.stderr)
        return 2
    results = [r for _, r in items]
    if a.json:
        print(json.dumps([{
            "id": r.id, "surface": r.surface, "signal": r.signal, "verdict": r.verdict,
            "base": r.base.facets if r.base else None, "pr": r.pr.facets if r.pr else None,
            "samples": r.samples, "latency": r.latency,
        } for r in results], indent=1, default=str))
    else:
        print(render_table(results))
    rc = 0
    if a.check:
        for m, r in items:
            errs = check_expectations(m, r)
            for e in errs:
                print(f"EXPECTATION {r.id}: {e}", file=sys.stderr)
            rc |= bool(errs)
    if a.fail_on_regression and any(r.verdict in FAILING for r in results):
        rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
