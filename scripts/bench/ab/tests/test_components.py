"""components.py: the ingest batches are deterministic and the same bytes go to every target."""
import datetime as dt
import json

import components

T0 = dt.datetime(2026, 10, 12, 13, 30, tzinfo=dt.timezone.utc)


def test_batches_are_deterministic_and_well_formed():
    assert components.logs_batch(T0, 50, "x") == components.logs_batch(T0, 50, "x")
    assert components.spans_batch(T0, 50, "x") == components.spans_batch(T0, 50, "x")
    assert components.logs_batch(T0, 50, "x") != components.logs_batch(T0, 50, "y")
    rows = [json.loads(line) for line in components.logs_batch(T0, 10, "x").decode().splitlines()]
    assert len(rows) == 10 and {"ts", "msg", "service.name", "level", "req"} <= set(rows[0]) and rows[0]["ts"].endswith("Z")
    body = json.loads(components.spans_batch(T0, 10, "x"))
    spans = body["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert len(spans) == 10 and all(len(s["traceId"]) == 32 and len(s["spanId"]) == 16 for s in spans)
    assert int(spans[0]["endTimeUnixNano"]) > int(spans[0]["startTimeUnixNano"])


def test_rate_and_compaction_rows_without_data():
    assert components.rate(1000, 2.0) == 500 and components.rate(1, 0) is None
    rows = components.compaction_rows({"ab": {"recompact": {"base-logs": {"dt=a/hour=00": {"body": '{"bytes_written":1000000,"fenced_files":0,"input_files":8,"output_files":2,"output_lev', "seconds": 1.5}}}}})
    assert rows[0]["base"] == "8 -> 2 files, 1.00 MB, 1.5 s" and rows[0]["pr"] == "0 -> 0 files, 0.00 MB, 0.0 s"
