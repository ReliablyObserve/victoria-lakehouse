"""api_rows.py: registry rows -> read-only requests, or a listed reason why not."""
import abreg
import api_rows

VALUES = api_rows.window_values({"start": "2026-10-12T12:00:00Z", "end": "2026-10-12T13:00:00Z"})


def row(**kw):
    base = {"id": "vl.select.x", "surface": "vl", "kind": "select", "expect": "pass", "targets": ["hot", "cold"], "seed": ["logs.base"],
            "request": {"method": "GET", "path": "/select/logsql/field_values", "params": {"query": "*", "start": "{{seed.start}}", "end": "{{seed.cold_end}}", "field": "level"}},
            "compare": {"type": "values-with-hits"}}
    base.update(kw)
    return base


def one(r, picks=None):
    out, sk = api_rows.translate([r], VALUES, picks)
    return out, sk


def test_a_select_row_becomes_one_request_with_the_window_substituted():
    out, sk = one(row())
    assert sk == [] and len(out) == 1
    r = out[0]
    assert r["params"]["start"] == "2026-10-12T12:00:00Z" and r["kind"] == "values" and r["surface"] == "vl-native" and r["lh_only"] is False


def test_endpoint_decides_the_metrics_kind():
    k = lambda path, ctype="exact-json": api_rows.proof_kind({"compare": {"type": ctype}}, path)  # noqa: E731
    assert k("/select/logsql/stats_query", "ndjson-multiset") == "count"
    assert k("/select/logsql/stats_query_range", "series") == "series_prom"
    assert k("/select/logsql/hits", "series") == "series_hits"
    assert k("/select/logsql/field_names") == "values" and k("/select/logsql/streams") == "values"
    assert k("/select/tempo/api/metrics/query_range", "series") == "series_tempo"
    assert k("/select/jaeger/api/traces", "trace") == "trace_jaeger" and k("/select/tempo/api/search", "trace") == "tempo_search"
    assert k("/select/logsql/query", "ndjson-multiset") == "rows"
    assert k("/x", "status") is None


def test_rows_that_cannot_run_read_only_are_listed_with_a_reason_never_dropped():
    cases = [
        (row(request=None), "no HTTP request"),
        (row(kind="insert", request={"method": "POST", "path": "/insert/jsonline"}), "not read-only"),
        (row(request={"method": "POST", "path": "/delete/run_task", "params": {}}, kind="select"), "mutating"),
        (row(seed=["logs.fieldmeta"]), "needs seed logs.fieldmeta"),
        (row(id="vl.select.tenant_scope.default.prefix"), "isolation fixture"),
        (row(request={"method": "GET", "path": "/select/logsql/tail", "params": {}}), "streaming"),
        (row(request={"method": "GET", "path": "/select/logsql/query", "params": {"query": "*", "x": "{{tombstone_id}}"}}), "placeholder"),
        (row(request={"method": "GET", "path": "/select/tempo/api/v2/traces/{{seed.trace_id}}", "headers": {"Accept": "application/protobuf"}}), "protobuf"),
        (row(compare={"type": "golden"}), "not run by a read-only matrix"),
    ]
    for r, why in cases:
        out, sk = one(r)
        assert out == [] and len(sk) == 1 and why in sk[0]["reason"], (why, sk)
    out, sk = api_rows.translate([row(request=None)] * 2, VALUES)
    assert api_rows.skip_counts(sk) == {"no HTTP request (flag or configuration row)": 2}


def test_tempo_times_are_unix_seconds_and_picks_fill_the_trace_id():
    r = row(id="vt.tempo.search.x", surface="vt", request={"method": "GET", "path": "/select/tempo/api/search",
            "params": {"q": "{ }", "start": "{{seed.start}}", "end": "{{seed.cold_end}}"}}, compare={"type": "trace"})
    out, _ = one(r)
    assert out[0]["params"]["start"] == "1791806400" and out[0]["surface"] == "tempo"
    t = row(id="vt.jaeger.by_id", surface="vt", request={"method": "GET", "path": "/select/jaeger/api/traces/{{seed.trace_id}}"}, compare={"type": "trace"})
    out, sk = one(t, {"seed.trace_id": "abc"})
    assert out[0]["path"].endswith("/abc") and out[0]["surface"] == "jaeger" and out[0]["signal"] == "traces"


def test_limit_without_sort_is_arbitrary_and_stats_first_only_when_it_cuts():
    f = api_rows.limit_of
    assert f({"query": "* | copy a as b | limit 100"}, "/select/logsql/query") == (100, False)
    assert f({"query": "* | stats by (x) count() | limit 5"}, "/select/logsql/query") == (5, True)
    assert f({"query": "* | sort by (_time) | limit 100"}, "/select/logsql/query") == (100, True)   # ties on the sort key past the limit
    assert f({"query": "* | sort by (service.name) | limit 100"}, "/select/logsql/query") is None
    assert f({"query": "*"}, "/select/logsql/query") is None
    assert f({"query": "* | limit 3"}, "/select/logsql/field_values") is None


def test_lakehouse_only_rows_follow_the_endpoint_and_both_binaries_when_agnostic():
    r = row(id="lh.stats.overview.schema", surface="lh", targets=["cold"], request={"method": "GET", "path": "/lakehouse/api/v1/stats/overview"},
            compare={"type": "schema"})
    out, _ = one(r)
    assert sorted(o["signal"] for o in out) == ["logs", "traces"] and all(o["lh_only"] for o in out) and {o["surface"] for o in out} == {"lh-logs", "lh-traces"}
    assert out[0]["id"].endswith("@logs") and out[0]["row"] == "lh.stats.overview.schema"


def test_the_real_registry_translates_without_an_error_and_accounts_for_every_row():
    rows = abreg.load_rows()
    out, sk = api_rows.translate(rows, VALUES, {"seed.trace_id": "t", "seed.stream_id": "s"})
    assert len({o["row"] for o in out}) + len(sk) == len(rows)  # every row either runs (possibly on two binaries) or is listed
    assert len(out) > 100


def test_sample_is_random_and_therefore_not_scored():
    assert api_rows.random_pipe({"query": "* | sample 10 | limit 100"}) and not api_rows.random_pipe({"query": "* | limit 100"})
