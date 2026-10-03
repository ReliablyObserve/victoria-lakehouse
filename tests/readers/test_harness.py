"""Self-tests of the reader-matrix harness: no stack, no engines needed.

They pin the things the matrix's credibility rests on: every documented example is runnable and
complete, the example values really get replaced, the answer shapes are compared exactly, a known
gap cannot hide a pass, and the committed coverage table and engine versions are the ones CI runs.
"""
import json
import os
import re

import pytest

import engines
import gaps
import lib
import matrix
import report

HERE = os.path.dirname(os.path.abspath(__file__))
MANIFEST = json.load(open(os.path.join(HERE, "engines.json")))
SNIPS = lib.doc_snippets()
RUNNABLE = [e["id"] for e in MANIFEST["engines"]]


def snippet(engine, signal):
    return lib.find_snippet(SNIPS, engine, signal)


@pytest.mark.parametrize("engine", RUNNABLE)
@pytest.mark.parametrize("signal", lib.SIGNALS)
def test_every_engine_has_a_documented_example_per_signal(engine, signal):
    assert snippet(engine, signal)["body"].strip()


def test_every_marker_names_a_manifest_engine_and_a_signal():
    for s in SNIPS:
        assert s["engine"] in RUNNABLE, s["attrs"]
        assert s["signal"] in lib.SIGNALS, s["attrs"]


@pytest.mark.parametrize("engine", [e for e in RUNNABLE if e != "parquet-tools"])
@pytest.mark.parametrize("signal", lib.SIGNALS)
def test_example_answers_every_check_exactly_once(engine, signal):
    body = snippet(engine, signal)["body"]
    if snippet(engine, signal)["lang"] == "sql":
        names = [n for n, _ in lib.split_sql(body) if n]
    else:
        names = re.findall(r"^q_(\w+)\s*=", body, re.M)
    base = [n for n in names if n in lib.QUERIES]
    assert sorted(base) == sorted(lib.QUERIES), "missing or duplicated checks: %s" % sorted(set(lib.QUERIES) ^ set(base))
    for n in names:
        assert lib.base_query(n), "%s is not a known check" % n


@pytest.mark.parametrize("engine", RUNNABLE)
@pytest.mark.parametrize("signal", lib.SIGNALS)
def test_materialise_replaces_every_example_value(engine, signal):
    params = {"from_ns": 1790000000000000001, "to_ns": 1790000003600000002, "trace_id": "f" * 32, "dt": "2026-10-01"}
    body = lib.materialise(snippet(engine, signal)["body"], params, "s3:9000", "obs-raw", "3000000000/0", 3000000000)
    for lit in ("localhost:9000", lib.DOC_DEFAULTS["from_ns"], lib.DOC_DEFAULTS["to_ns"], lib.DOC_DEFAULTS["trace_id"], "2026-01-01"):
        assert lit not in body, lit
    assert "obs-archive" not in body
    assert "4401/1" not in body
    assert "obs-raw/3000000000/0" in body or "s3://obs-raw/3000000000/0" in body or "obs-raw" in body


def test_split_sql_names_and_skips_comments():
    sql = "SET a = 1;\n-- plain comment\n-- q: count\nSELECT count(*)\n  FROM t;\n-- q: tenant\nSELECT 1;\n"
    assert lib.split_sql(sql) == [(None, "SET a = 1"), ("count", "SELECT count(*)\n  FROM t"), ("tenant", "SELECT 1")]


def test_normalise_shapes_are_exact():
    assert lib.normalise("count", [["3000"]]) == 3000
    assert lib.normalise("by_service", [["a", 1], ["b", "2"]]) == {"a": 1, "b": 2}
    assert lib.normalise("ts_bounds", [[1, 2]]) == [1, 2]
    assert lib.normalise("tenant", [[4401, 1]]) == [[4401, 1]]
    assert lib.normalise("tenant_masked", [[3000000000, 0]]) == [[3000000000, 0]]
    with pytest.raises(KeyError):
        lib.normalise("nonsense", [[1]])


def test_rfc3339_round_trip_keeps_nanoseconds():
    ns = 1790759229397030336
    assert lib.parse_rfc3339_ns(lib.rfc3339_ns(ns)) == ns


def test_base_query_maps_suffixed_checks():
    assert lib.base_query("trace_by_id_nobloom") == "trace_by_id"
    assert lib.base_query("tenant_masked") == "tenant"
    assert lib.base_query("count") == "count"
    assert lib.base_query("bogus") is None


def test_python_runner_isolates_a_failing_query():
    body = "q_count = 3\nq_tenant = 1 / 0\nq_ts_bounds = [1, 2]\n"
    got = engines.exec_body(body, "unit")
    assert got["count"] == 3
    assert isinstance(got["tenant"], engines.QueryError) and "ZeroDivisionError" in got["tenant"]
    assert got["ts_bounds"] == [1, 2]


def test_python_runner_keeps_running_after_a_helper_statement_fails():
    got = engines.exec_body("boom = 1 / 0\nq_count = 5\n", "unit")
    assert got == {"count": 5}


def test_python_runner_reports_a_cell_that_produced_nothing():
    with pytest.raises(RuntimeError):
        engines.exec_body("boom = 1 / 0\n", "unit")


def test_to_rows_handles_pyarrow_and_scalars():
    import pyarrow as pa
    assert engines.to_rows(pa.table({"a": [1, 2], "b": ["x", "y"]})) == [[1, "x"], [2, "y"]]
    assert engines.to_rows(7) == [[7]]
    assert engines.to_rows({"min": 1, "max": 9}) == [[1, 9]]


def test_judge_known_gap_statuses():
    gap = {"issue": "https://example/1", "id": "x"}
    assert matrix.judge({"count": 3}, "count", 3, None)["status"] == "pass"
    assert matrix.judge({"count": 4}, "count", 3, None)["status"] == "fail"
    assert matrix.judge({}, "count", 3, None)["status"] == "fail"
    assert matrix.judge({"count": engines.QueryError("boom")}, "count", 3, None)["status"] == "fail"
    assert matrix.judge({"count": 4}, "count", 3, gap)["status"] == "known-gap"
    assert matrix.judge({"count": 3}, "count", 3, gap)["status"] == "gap-closed"
    assert matrix.judge({"count": 3}, "count", 3, dict(gap, optional=True))["status"] == "pass"
    assert matrix.judge({"count": 3}, "count", 3, gap)["ok"] is False


def test_a_wrong_answer_is_never_a_known_gap_pass():
    gap = {"issue": "https://example/1", "id": "x"}
    r = matrix.judge({"by_service": {"a": 1}}, "by_service", {"a": 2}, None)
    assert r["status"] == "fail" and r["ok"] is False
    assert matrix.judge({"by_service": {"a": 1}}, "by_service", {"a": 2}, gap)["status"] == "known-gap"


def test_every_gap_is_issue_linked_or_an_explained_engine_caveat():
    ids = set()
    engines_in_manifest = set(RUNNABLE)
    for g in gaps.GAPS:
        assert g["id"] not in ids
        ids.add(g["id"])
        assert set(g["engines"]) <= engines_in_manifest
        assert g["note"].strip() and g.get("label")
        if g["issue"] is None:
            assert "engine behaviour" in g["note"], "a gap without an issue must say it is the engine's behaviour"
        else:
            assert re.fullmatch(r"https://github\.com/ReliablyObserve/victoria-lakehouse/issues/\d+", g["issue"])


def test_known_gap_lookup_is_specific():
    assert gaps.known_gap("trino", "logs", "raw", "big", "tenant")
    assert gaps.known_gap("trino", "logs", "raw", "numeric", "tenant") is None
    assert gaps.known_gap("trino", "logs", "raw", "big", "count") is None
    assert gaps.known_gap("duckdb", "logs", "raw", "big", "tenant") is None
    assert gaps.known_gap("polars", "logs", "raw", "numeric", "count")
    assert gaps.known_gap("polars", "logs", "compacted", "numeric", "count") is None


def test_committed_coverage_table_is_the_generated_one():
    report.check_doc(lib.DOC)


def test_manifest_versions_are_the_pinned_ones():
    pins = dict(re.findall(r"^([A-Za-z0-9_.-]+)==([\w.]+)$", open(os.path.join(HERE, "requirements.txt")).read(), re.M))
    by_id = {e["id"]: e["version"] for e in MANIFEST["engines"]}
    for eid, dist in (("duckdb", "duckdb"), ("pyarrow", "pyarrow"), ("pandas", "pandas"), ("polars", "polars"),
                      ("datafusion", "datafusion"), ("parquet-tools", "parquet-tools")):
        assert by_id[eid] == pins[dist], eid
    compose = open(os.path.join(HERE, "docker-compose.yml")).read()
    assert "CLICKHOUSE_VERSION:-%s" % by_id["clickhouse"] in compose
    assert "TRINO_VERSION:-%s" % by_id["trino"] in compose
    assert engines.SPARK_IMAGE.split(":")[1].startswith(by_id["spark"])


def test_compose_uses_only_the_reserved_port_block():
    compose = open(os.path.join(HERE, "docker-compose.yml")).read()
    ports = [int(p) for p in re.findall(r"127\.0\.0\.1:(\d+):", compose)]
    assert ports and all(39400 <= p <= 39499 for p in ports), ports
    assert "name: lhreaders" in compose
