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
import truth

HERE = os.path.dirname(os.path.abspath(__file__))
MANIFEST = lib.read_json(os.path.join(HERE, "engines.json"))
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


QE = engines.QueryError
ERR_GAP = {"issue": "https://example/1", "id": "x", "error": r"invalid utf-?8"}
VAL_GAP = {"issue": "https://example/2", "id": "y", "got": {"big": [[-1294967296, 0]]}}


def test_judge_without_a_gap_is_exact():
    assert matrix.judge({"count": 3}, "count", 3, None)["status"] == "pass"
    assert matrix.judge({"count": 4}, "count", 3, None)["status"] == "fail"
    assert matrix.judge({}, "count", 3, None)["status"] == "fail"
    assert matrix.judge({"count": QE("boom")}, "count", 3, None)["status"] == "fail"


def test_a_gap_covers_only_the_error_it_names():
    err = QE("ComputeError: Invalid UTF-8 sequence in footer")
    assert matrix.judge({"count": err}, "count", 3, ERR_GAP)["status"] == "known-gap"
    # another error under the same gap is a failure, not a known gap
    assert matrix.judge({"count": QE("connection refused")}, "count", 3, ERR_GAP)["status"] == "fail"
    # a wrong answer is never excused by an error-text gap
    assert matrix.judge({"count": 4}, "count", 3, ERR_GAP)["status"] == "fail"
    assert matrix.judge({"by_service": {"a": 1}}, "by_service", {"a": 2}, ERR_GAP)["status"] == "fail"


def test_a_gap_covers_only_the_wrong_value_it_names():
    assert matrix.judge({"tenant": [[-1294967296, 0]]}, "tenant", [[3000000000, 0]], VAL_GAP, "big")["status"] == "known-gap"
    # the same query wrong in another way, an error, or another tenant's value: failures
    assert matrix.judge({"tenant": [[-5, 0]]}, "tenant", [[3000000000, 0]], VAL_GAP, "big")["status"] == "fail"
    assert matrix.judge({"tenant": QE("boom")}, "tenant", [[3000000000, 0]], VAL_GAP, "big")["status"] == "fail"
    assert matrix.judge({"tenant": [[-1294967296, 0]]}, "tenant", [[3000000000, 0]], VAL_GAP, "golden")["status"] == "fail"


def test_a_gap_that_starts_passing_turns_red():
    r = matrix.judge({"count": 3}, "count", 3, ERR_GAP)
    assert r["status"] == "gap-closed" and r["ok"] is False


def test_pruning_control_must_be_an_error():
    ok = matrix.judge_pruning(5, QE("not a parquet file"), 5, None)
    assert ok[0] == "pass"
    # the control answered (even with the right total): the engine may have skipped the garbage object
    assert matrix.judge_pruning(5, 99, 5, None)[0] == "fail"
    assert matrix.judge_pruning(5, None, 5, None)[0] == "fail"
    # the filtered query failed or answered wrongly
    assert matrix.judge_pruning(QE("boom"), QE("boom"), 5, None)[0] == "fail"
    assert matrix.judge_pruning(4, QE("boom"), 5, None)[0] == "fail"


def test_pruning_gaps_are_narrow_and_close():
    gap = {"issue": None, "id": "p", "error": r"footer|magic"}
    assert matrix.judge_pruning(QE("registration failed: bad magic"), QE("bad magic"), 5, gap)[0] == "known-gap"
    assert matrix.judge_pruning(QE("timeout"), QE("timeout"), 5, gap)[0] == "fail"
    assert matrix.judge_pruning(7, QE("bad magic"), 5, gap)[0] == "fail"
    assert matrix.judge_pruning(5, QE("bad magic"), 5, gap)[0] == "gap-closed"
    assert matrix.judge_pruning(None, None, 5, gap, setup_error="Error: bad footer")[0] == "known-gap"


FACTS = {"nonpow2_total": 1, "nonutf8_total": 2,
         "cells": {"4402/3/logs/raw": {"trace_id": [96], "service.name": [32]},
                   "4401/1/logs/raw": {"trace_id": [32], "service.name": [32]}},
         "footer": {"4401/1/logs/raw": {"objects": 3, "nonutf8": 2, "nonutf8_dts": ["2026-01-01"], "first_nonutf8": False},
                    "4401/1/logs/compacted": {"objects": 2, "nonutf8": 0, "nonutf8_dts": [], "first_nonutf8": False},
                    "4401/1/traces/raw": {"objects": 3, "nonutf8": 0, "nonutf8_dts": [], "first_nonutf8": False}}}


def gap_for(engine, sig, layer, tenant, q, prefix, facts=FACTS):
    return gaps.known_gap(engine, sig, layer, tenant, q, facts, prefix)


def test_bloom_gap_applies_exactly_where_a_file_has_a_non_power_of_two_filter():
    assert gap_for("clickhouse", "logs", "raw", "bloom", "trace_by_id", "4402/3")
    assert gap_for("clickhouse", "logs", "raw", "numeric", "trace_by_id", "4401/1") is None
    # the filter of the query decides: field_filter touches service.name and severity_text only
    assert gap_for("clickhouse", "logs", "raw", "bloom", "field_filter", "4402/3") is None
    assert gap_for("clickhouse", "logs", "compacted", "bloom", "trace_by_id", "4402/3") is None


def test_bloom_gap_closes_when_the_writer_stops_writing_such_filters():
    fixed = dict(FACTS, nonpow2_total=0)
    g = gap_for("clickhouse", "logs", "raw", "numeric", "trace_by_id", "4401/1", fixed)
    assert g and matrix.judge({"trace_by_id": 5}, "trace_by_id", 5, g)["status"] == "gap-closed"


def test_footer_gap_applies_exactly_where_an_object_has_a_non_utf8_footer_value():
    assert gap_for("polars", "logs", "raw", "numeric", "count", "4401/1")
    assert gap_for("polars", "logs", "compacted", "numeric", "count", "4401/1") is None   # compaction left no such object here
    assert gap_for("polars", "traces", "raw", "numeric", "count", "4401/1") is None       # facts say none here
    assert gap_for("datafusion", "logs", "raw", "numeric", "count", "4401/1")
    assert gap_for("duckdb", "logs", "raw", "numeric", "count", "4401/1") is None


def test_footer_gap_of_a_partition_pruned_query_looks_only_at_its_partition():
    # Polars prunes dt= before it opens a footer: dt_filter meets only the filtered day's objects
    assert gaps.known_gap("polars", "logs", "raw", "numeric", "dt_filter", FACTS, "4401/1", "2026-01-01")
    assert gaps.known_gap("polars", "logs", "raw", "numeric", "dt_filter", FACTS, "4401/1", "2026-01-02") is None
    # a query that is not partition-pruned still meets every object of the cell
    assert gaps.known_gap("polars", "logs", "raw", "numeric", "count", FACTS, "4401/1", "2026-01-02")
    # DataFusion infers the schema from every footer at registration: no partition exemption
    assert gaps.known_gap("datafusion", "logs", "raw", "numeric", "dt_filter", FACTS, "4401/1", "2026-01-02")
    # the CI case: the day's objects are clean, so a Polars answer is a pass, and a UTF-8 error there is a FAIL
    g = gaps.known_gap("polars", "logs", "raw", "numeric", "dt_filter", FACTS, "4401/1", "2026-01-02")
    assert matrix.judge({"dt_filter": 5}, "dt_filter", 5, g)["status"] == "pass"
    assert matrix.judge({"dt_filter": QE("ComputeError: invalid utf-8")}, "dt_filter", 5, g)["status"] == "fail"
    # the day that holds such an object keeps the gap, and a correct answer there turns red
    g = gaps.known_gap("polars", "logs", "raw", "numeric", "dt_filter", FACTS, "4401/1", "2026-01-01")
    assert matrix.judge({"dt_filter": QE("ComputeError: invalid utf-8")}, "dt_filter", 5, g)["status"] == "known-gap"
    assert matrix.judge({"dt_filter": 5}, "dt_filter", 5, g)["status"] == "gap-closed"


def test_footer_gap_of_a_partition_pruned_query_includes_the_first_object_of_the_listing():
    # Polars reads the schema from the first object (sorted by key) before it prunes: a clean day
    # still fails when that object carries a non-UTF-8 value (CI: 4401/1/logs/compacted, 2026-10-04)
    first_bad = dict(FACTS, footer=dict(FACTS["footer"], **{"4401/1/logs/raw": dict(FACTS["footer"]["4401/1/logs/raw"],
                                                                                    first_nonutf8=True)}))
    g = gaps.known_gap("polars", "logs", "raw", "numeric", "dt_filter", first_bad, "4401/1", "2026-01-02")
    assert g and matrix.judge({"dt_filter": QE("ComputeError: invalid utf8")}, "dt_filter", 5, g)["status"] == "known-gap"


def test_a_partition_pruned_footer_gap_needs_the_partition():
    with pytest.raises(ValueError):
        gaps.known_gap("polars", "logs", "raw", "numeric", "dt_filter", FACTS, "4401/1")


def test_facts_record_the_partition_of_an_object():
    assert truth.dt_of("4401/1/logs/dt=2026-01-01/hour=03/x.parquet") == "2026-01-01"
    assert truth.dt_of("dt=2026-01-02/hour=00/x.parquet") == "2026-01-02"
    assert truth.dt_of("4401/1/logs/x.parquet") is None


def test_footer_gap_closes_when_no_object_has_a_non_utf8_footer_value():
    fixed = dict(FACTS, nonutf8_total=0)
    g = gap_for("polars", "logs", "compacted", "numeric", "count", "4401/1", fixed)
    assert g and matrix.judge({"count": 5}, "count", 5, g)["status"] == "gap-closed"


def test_the_doc_view_of_a_data_dependent_gap_is_where_it_can_occur():
    assert gaps.known_gap("polars", "logs", "compacted", "numeric", "count")
    assert gaps.known_gap("clickhouse", "traces", "raw", "numeric", "trace_by_id")


def test_files_check_compares_with_the_inventory():
    inv = ["dt=2026-01-01/hour=00/a.parquet", "dt=2026-01-01/hour=01/compacted-L2-b.parquet"]
    assert matrix.files_check({"files": list(inv)}, inv, None, "numeric")["status"] == "pass"
    wrong = matrix.files_check({"files": inv[:1]}, inv, None, "numeric")
    assert wrong["status"] == "fail" and wrong["got"]["missing"] == [inv[1]]
    # a compacted object where the raw layer was asked for is an unexpected file
    mixed = matrix.files_check({"files": inv[:1] + ["dt=2026-01-01/hour=01/compacted-L2-zz.parquet"]}, inv, None, "numeric")
    assert mixed["status"] == "fail" and mixed["got"]["unexpected"]
    assert matrix.files_check({}, inv, None, "numeric")["status"] == "fail"
    assert matrix.files_check({"files": QE("boom")}, inv, None, "numeric")["status"] == "fail"
    assert matrix.files_check({"files": QE("invalid utf-8")}, inv, ERR_GAP, "numeric")["status"] == "known-gap"


def test_files_normalise_keeps_the_object_name_whatever_the_engine_prints():
    rows = [["s3://obs-raw/4401/1/logs/dt=2026-01-01/hour=00/a.parquet"], ["obs-raw/4401/1/logs/dt=2026-01-01/hour=00/a.parquet"],
            ["http://s3:9000/obs-raw/4401/1/logs/dt=2026-01-01/hour=01/b.parquet"], ["/_pmeta.bundle"]]
    assert lib.normalise("files", rows) == ["dt=2026-01-01/hour=00/a.parquet", "dt=2026-01-01/hour=01/b.parquet"]


def test_every_gap_is_issue_linked_or_an_explained_engine_caveat():
    ids = set()
    engines_in_manifest = set(RUNNABLE)
    for g in gaps.GAPS:
        assert g["id"] not in ids
        ids.add(g["id"])
        assert set(g["engines"]) <= engines_in_manifest
        assert g["note"].strip() and g.get("label")
        assert g.get("error") or g.get("got"), "a gap must name its error text or its exact wrong answer: %s" % g["id"]
        re.compile(g.get("error", ""))
        if g["issue"] is None:
            assert "engine behaviour" in g["note"], "a gap without an issue must say it is the engine's behaviour"
        else:
            assert re.fullmatch(r"https://github\.com/ReliablyObserve/victoria-lakehouse/issues/\d+", g["issue"])


def test_known_gap_lookup_is_specific():
    assert gaps.known_gap("trino", "logs", "raw", "big", "tenant")
    assert gaps.known_gap("trino", "traces", "compacted", "golden", "tenant")
    assert gaps.known_gap("trino", "logs", "raw", "numeric", "tenant") is None
    assert gaps.known_gap("trino", "logs", "raw", "big", "count") is None
    assert gaps.known_gap("duckdb", "logs", "raw", "big", "tenant") is None
    assert gaps.known_gap("polars", "logs", "raw", "numeric", "count")
    # compaction keeps the body token bloom footer KV since #344, so compacted logs files are refused too
    assert gaps.known_gap("polars", "logs", "compacted", "numeric", "count")
    assert gaps.known_gap("datafusion", "traces", "pruned", "prune", "dt_filter")["issue"].endswith("/340")
    pruned_facts = dict(FACTS, footer=dict(FACTS["footer"], **{"prune/1/logs/pruned": {"objects": 24, "nonutf8": 0, "nonutf8_dts": [],
                                                                                        "first_nonutf8": False},
                                                                "prune/1/traces/pruned": {"objects": 24, "nonutf8": 24,
                                                                                          "nonutf8_dts": ["2026-01-01"],
                                                                                          "first_nonutf8": True}}))
    assert gaps.known_gap("datafusion", "logs", "pruned", "prune", "dt_filter", pruned_facts, "prune/1")["issue"] is None
    assert gaps.known_gap("datafusion", "traces", "pruned", "prune", "dt_filter", pruned_facts, "prune/1")["issue"].endswith("/340")
    assert gaps.known_gap("polars", "logs", "pruned", "prune", "dt_filter", pruned_facts, "prune/1", "2026-01-01") is None
    assert gaps.known_gap("polars", "traces", "pruned", "prune", "dt_filter", pruned_facts, "prune/1", "2026-01-01")["issue"].endswith("/340")
    assert gaps.known_gap("clickhouse", "logs", "raw", "numeric", "trace_by_id")["bloom"]
    assert gaps.known_gap("clickhouse", "logs", "raw", "numeric", "count") is None


def test_committed_coverage_table_is_the_generated_one():
    report.check_doc(lib.DOC)


def test_manifest_versions_are_the_pinned_ones():
    pins = dict(re.findall(r"^([A-Za-z0-9_.-]+)==([\w.]+)$", lib.read_text(os.path.join(HERE, "requirements.txt")), re.M))
    by_id = {e["id"]: e["version"] for e in MANIFEST["engines"]}
    for eid, dist in (("duckdb", "duckdb"), ("pyarrow", "pyarrow"), ("pandas", "pandas"), ("polars", "polars"),
                      ("datafusion", "datafusion"), ("parquet-tools", "parquet-tools")):
        assert by_id[eid] == pins[dist], eid
    compose = lib.read_text(os.path.join(HERE, "docker-compose.yml"))
    assert "clickhouse/clickhouse-server:%s@sha256:" % by_id["clickhouse"] in compose
    assert "trinodb/trino:%s@sha256:" % by_id["trino"] in compose
    assert engines.SPARK_IMAGE.split(":")[1].startswith(by_id["spark"])


def test_every_image_is_pinned_by_digest():
    """No floating tag: a moved tag cannot change what the matrix measures. Images built from this
    repository (the Lakehouse binaries and datagen) are referenced through variables."""
    compose = lib.read_text(os.path.join(HERE, "docker-compose.yml"))
    images = re.findall(r"^\s+image: (\S+)", compose, re.M)
    assert images
    for img in images:
        if img.startswith("${"):
            continue
        assert re.search(r"@sha256:[0-9a-f]{64}$", img), "image not pinned by digest: %s" % img
    assert re.search(r"@sha256:[0-9a-f]{64}$", engines.SPARK_IMAGE)


def test_compose_uses_only_the_reserved_port_block():
    compose = lib.read_text(os.path.join(HERE, "docker-compose.yml"))
    ports = [int(p) for p in re.findall(r"127\.0\.0\.1:(\d+):", compose)]
    assert ports and all(39400 <= p <= 39499 for p in ports), ports
    assert "name: lhreaders" in compose
