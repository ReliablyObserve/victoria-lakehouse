"""Builders of raw measure.py rows for the report, comment and known tests."""


def row(cell, target, run, ms, result="42", valid=True, reason="", shape="logs count_total", set_="control-logs", layer="cold", rng="1h",
        form="numeric", signal="logs", cold_cleared=None):
    r = {"cell": cell, "set": set_, "shape": shape, "signal": signal, "layer": layer, "range": rng, "form": form, "target": target, "run": run,
         "cold": run == 0, "status": 200 if valid else 500, "ms": ms, "valid": valid, "reason": reason, "result": result if valid else "",
         "bytes_in": 10, "window": ["2026-10-12T12:00:00Z", "2026-10-12T13:00:00Z"]}
    if cold_cleared is not None and run == 0:
        r["cold_cleared"] = cold_cleared
    return r


def cell_rows(key, base_ms, pr_ms, ref_ms=5.0, runs=6, base_result="42", pr_result="42", ref_result="42", **kw):
    rows = []
    for t, ms, res in (("ref", ref_ms, ref_result), ("base", base_ms, base_result), ("pr", pr_ms, pr_result)):
        for run in range(runs):
            rows.append(row(key, t, run, ms * (3 if run == 0 else 1), res, **kw))
    return rows


def raw(rows, s3=None):
    return {"rows": rows, "s3": s3 or {}, "process": {}, "runs": 6, "sets": ["control-logs"], "elapsed_s": 1.0}
