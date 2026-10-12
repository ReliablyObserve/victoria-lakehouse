"""Assertions over the result files of a reader-matrix run (READERS_RESULTS=<dir with result-*.json>).

One test per engine and signal. A cell passes when every check equals Lakehouse's own answer, or
when it is a known gap that still fails; a known gap that starts passing fails here so its entry
is removed together with the fix. Run with `pytest -m results`.
"""
import glob
import os

import pytest

import lib

pytestmark = pytest.mark.results


def results():
    d = os.environ.get("READERS_RESULTS")
    assert d, "READERS_RESULTS must point at the directory with the result-*.json files of a matrix run"
    files = sorted(glob.glob(os.path.join(d, "result-*.json")))
    assert files, "no result-*.json in %s: the matrix did not run" % d
    out = []
    for f in files:
        out += lib.read_json(f)["results"]
    return out


def check(engine, signal):
    rs = [r for r in results() if r["engine"] == engine and r["signal"] == signal]
    assert rs, "%s/%s: no cell ran (a missing run is a failure, not a skip)" % (engine, signal)
    layers = {r["layer"] for r in rs}
    if engine != "parquet-tools":
        assert {"raw", "compacted", "pruned"} <= layers, "%s/%s ran only %s" % (engine, signal, sorted(layers))
        tenants = {r["tenant"] for r in rs}
        assert {"numeric", "alias", "big", "golden", "bloom"} <= tenants, "%s/%s ran only %s" % (engine, signal, sorted(tenants))
        for r in rs:
            if r["layer"] in ("raw", "compacted"):
                # every cell says which objects were read (or that the engine cannot say), never silence
                assert "files" in r["queries"], "%s: no files check" % r["cell"]
    bad = []
    for r in rs:
        for q, v in r["queries"].items():
            if not v["ok"]:
                bad.append("%s %s: %s (got %r, want %r)" % (r["cell"], q, v["status"], v["got"], v["want"]))
    assert not bad, "\n".join(bad)


def test_duckdb_logs():
    check("duckdb", "logs")


def test_duckdb_traces():
    check("duckdb", "traces")


def test_pyarrow_logs():
    check("pyarrow", "logs")


def test_pyarrow_traces():
    check("pyarrow", "traces")


def test_pandas_logs():
    check("pandas", "logs")


def test_pandas_traces():
    check("pandas", "traces")


def test_polars_logs():
    check("polars", "logs")


def test_polars_traces():
    check("polars", "traces")


def test_datafusion_logs():
    check("datafusion", "logs")


def test_datafusion_traces():
    check("datafusion", "traces")


def test_clickhouse_logs():
    check("clickhouse", "logs")


def test_clickhouse_traces():
    check("clickhouse", "traces")


def test_trino_logs():
    check("trino", "logs")


def test_trino_traces():
    check("trino", "traces")


def test_spark_logs():
    check("spark", "logs")


def test_spark_traces():
    check("spark", "traces")


def test_parquet_tools_logs():
    check("parquet-tools", "logs")


def test_parquet_tools_traces():
    check("parquet-tools", "traces")


def test_lakehouse_answers_equal_the_writer_side_truth_and_do_not_change_across_compaction():
    d = os.environ.get("READERS_RESULTS")
    assert d, "READERS_RESULTS must be set"
    truth = lib.read_json(os.path.join(d, "truth.json"))["answers"]
    for layer in ("raw", "compacted"):
        got = lib.read_json(os.path.join(d, "oracle-%s.json" % layer))["answers"]
        assert got == truth, layer
    # the truth proves nothing where it is empty: every check but utc_check has a non-zero answer
    for key, a in truth.items():
        for q, v in a.items():
            if q != "utc_check":
                assert v and (not isinstance(v, dict) or any(v.values())) and (not isinstance(v, list) or any(v)), "%s %s is empty in the truth" % (key, q)


def test_second_compaction_scan_does_nothing():
    """#343 (fixed by #347): the compaction planner counted the files of all tenants of a partition together
    and kept rewriting what it had just written. Once every group is compacted nothing may change."""
    d = os.environ.get("READERS_RESULTS")
    assert d, "READERS_RESULTS must be set"
    h = lib.read_json(os.path.join(d, "storage-health.json"))
    assert h["ok"], h["violations"]
    assert h["new_scans_with_compactions"] == {"logs": 0, "traces": 0}
    assert h["objects_before"] == h["objects_after"]


def test_the_bucket_is_restored_as_lakehouse_left_it():
    """The engines read a bucket that still holds Lakehouse's own non-Parquet objects."""
    import glob
    d = os.environ.get("READERS_RESULTS")
    assert d, "READERS_RESULTS must be set"
    for bucket in ("obs-archive", "obs-raw"):
        others = [f for f in glob.glob(os.path.join(d, "fixture", bucket, "**", "*"), recursive=True)
                  if os.path.isfile(f) and not f.endswith(".parquet")]
        assert others, "%s holds only Parquet objects: the _pmeta.bundle / _meta / _tombstones objects were not restored" % bucket


def test_lakehouse_answers_do_not_change_across_compaction():
    d = os.environ.get("READERS_RESULTS")
    assert d, "READERS_RESULTS must be set"
    a = lib.read_json(os.path.join(d, "oracle-raw.json"))["answers"]
    b = lib.read_json(os.path.join(d, "oracle-compacted.json"))["answers"]
    assert a == b


def _fixture_schema(signal):
    import glob

    import pyarrow.parquet as pq
    d = os.environ.get("READERS_RESULTS")
    assert d, "READERS_RESULTS must be set"
    files = sorted(glob.glob(os.path.join(d, "fixture", "obs-archive", lib.TENANTS["numeric"]["prefix"], signal, "dt=*", "hour=*", "*.parquet")))
    assert files, "the fixture holds no %s objects" % signal
    return pq.read_schema(files[0])


def test_known_divergences_that_external_readers_see():
    """Issue-linked expectations about what a reader of the files sees (never skips).

    #333: spans flushed to Parquet have no `_msg`; the traces files have no message column at all,
    so a reader sees the span name instead. When #333 is fixed this assertion flips and is removed.
    #331 / #274 concern how the logs read path renames or adds severity fields; the Parquet columns a
    reader sees are `severity_text` (the jsonline `level`) and `severity_number` (a nullable int32: NULL when the
    source did not send one, an explicit 0 stays 0; #274), which is what the fixture rows carry.
    """
    traces = _fixture_schema("traces")
    assert "_msg" not in traces.names, "#333 is fixed: drop this expectation and the gap"
    logs = _fixture_schema("logs")
    assert str(logs.field("severity_number").type) == "int32"
    assert logs.field("severity_number").nullable, "severity_number must be nullable so an absent value is NULL (#274)"
    assert str(logs.field("severity_text").type) == "string"
    assert str(logs.field("account_id").type) == "uint32" and str(logs.field("project_id").type) == "uint32"
    assert str(logs.field("timestamp_unix_nano").type) == "int64"
