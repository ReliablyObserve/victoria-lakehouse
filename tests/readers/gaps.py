"""Known gaps of the reader matrix: Lakehouse (or an engine) diverges from the truth and a
tracking issue exists. A gap is not a skip, and it is narrow:

  * it names the cells it covers (engine, signal, layer, tenant, query);
  * it names the failure it covers: `error` is a regex the engine's error text must match, or `got`
    is the exact wrong answer (a per-tenant dict is allowed). A cell that fails differently, or
    returns another wrong value, is a FAIL, not a known gap;
  * the cell still runs, and when it starts passing the matrix turns red (`gap-closed`) so the
    entry is removed in the change that fixes it.

`bloom` and `footer` mark the two gaps that depend on the bytes Lakehouse wrote. ClickHouse fails an
equality filter on a bloom column exactly when a file it reads has a split-block bloom filter whose size
is not a power of two; Polars and DataFusion refuse exactly the files whose footer key-values are not
UTF-8. For them the fixture reads the files (truth.py facts) and the matrix expects the error in
exactly the cells where the facts say so, and nowhere else. When no file has the property any more the
gap applies to every cell, so they all pass and turn red: the entry is removed with the fix."""

ISSUES = "https://github.com/ReliablyObserve/victoria-lakehouse/issues/"
ALL_QUERIES = ("count", "by_service", "field_filter", "time_range", "map_filter",
               "trace_by_id", "dt_filter", "ts_bounds", "utc_check", "tenant")
ALL_TENANTS = ("numeric", "alias", "big", "golden", "bloom", "prune")

UTF8_ERR = r"invalid utf-?8|utf-?8 error|not valid utf-?8"

GAPS = [
    {"id": "footer-kv-not-utf8/polars", "issue": ISSUES + "340", "engines": ("polars",), "footer": True,
     "layers": ("raw", "compacted", "pruned"), "label": "files with a non-UTF-8 footer value", "error": UTF8_ERR,
     # scan_parquet prunes the dt= partitions before it opens a footer: these queries meet only their day's objects
     "partition_pruned": ("dt_filter",),
     # where every object is affected (the doc table says `no` there, `partial` elsewhere)
     "full": (("logs", "raw"), ("traces", "raw"), ("traces", "compacted"), ("traces", "pruned")),
     "note": "the traces footer KV _trace_idx and the logs footer KV _bloom_body_rg_N hold raw bytes, not UTF-8; Polars refuses a file "
             "that carries either. Every traces object and every raw logs object does; a compacted logs object only sometimes "
             "(compaction keeps the body token blooms of some files, #344)"},
    {"id": "footer-kv-not-utf8/datafusion", "issue": ISSUES + "340", "engines": ("datafusion",), "footer": True,
     "layers": ("raw", "compacted", "pruned"), "label": "files with a non-UTF-8 footer value", "error": UTF8_ERR,
     "full": (("logs", "raw"), ("traces", "raw"), ("traces", "compacted"), ("traces", "pruned")),
     "note": "the traces footer KV _trace_idx and the logs footer KV _bloom_body_rg_N hold raw bytes, not UTF-8; DataFusion (arrow-rs) "
             "refuses a file that carries either. Every traces object and every raw logs object does; a compacted logs object only "
             "sometimes (compaction keeps the body token blooms of some files, #344)"},
    {"id": "schema-inference-opens-every-file/datafusion", "issue": None, "engines": ("datafusion",),
     "signals": ("logs",), "layers": ("pruned",), "queries": ("dt_filter",), "label": "needs an explicit schema",
     "error": r"range start must not be greater than end|footer|magic number|not a valid parquet",
     "note": "engine behaviour, not a Lakehouse defect: register_parquet infers the schema from every file footer "
             "before any partition filter applies, so one unreadable object in an unrelated partition fails the "
             "registration. Pass an explicit schema= to avoid the listing-time footer reads"},
    {"id": "sbbf-size-not-power-of-two/clickhouse", "issue": ISSUES + "341", "engines": ("clickhouse",),
     "queries": ("trace_by_id", "field_filter"), "bloom": True, "label": "equality filters on a bloom column need bloom push down off",
     "error": r"Given length of bitset is illegal",
     "note": "split-block bloom filters whose size is not a power of two (96 bytes for 60 values, 160 after compaction) "
             "are written for every bloom column with enough distinct values; ClickHouse fails an equality filter on such "
             "a column. The workaround is input_format_parquet_bloom_filter_push_down = 0"},
    {"id": "uint32-tenant-id/trino", "issue": ISSUES + "342", "engines": ("trino",),
     "tenants": ("big", "golden"), "queries": ("tenant",), "label": "tenant IDs >= 2^31 need a mask",
     "got": {"big": [[-1294967296, 0]], "golden": [[-2, 0]]},
     "note": "Trino reads UINT_32 account_id 3000000000 as -1294967296 and 4294967294 as -2"},
]

# The columns an engine's equality filter touches, per signal and check (the bloom gap).
EQUALITY_COLUMNS = {
    ("logs", "trace_by_id"): ("trace_id",),
    ("traces", "trace_by_id"): ("trace_id",),
    ("logs", "field_filter"): ("service.name", "severity_text"),
    ("traces", "field_filter"): ("service.name", "status.code"),
}


def known_gap(engine, signal, layer, tenant, query, facts=None, prefix=None, dt=None):
    """The gap covering this cell and check, or None. Without `facts` a data-dependent gap (`bloom`,
    `footer`) is returned wherever it can occur (the view of the coverage table); with `facts`
    (truth.py facts) only where the files make it happen, and everywhere when no file does any more
    (the gap is then closed and its cells must start passing). `dt` is the partition a
    partition-pruned query reads (the cell's `dt_filter` day)."""
    for g in GAPS:
        if engine in g["engines"] and signal in g.get("signals", ("logs", "traces")) \
                and layer in g.get("layers", ("raw", "compacted")) \
                and tenant in g.get("tenants", ALL_TENANTS) \
                and query in g.get("queries", ALL_QUERIES):
            if facts is None or applies(g, facts, prefix, signal, layer, query, dt):
                return g
    return None


def applies(gap, facts, prefix, signal, layer, query, dt=None):
    cell = "%s/%s/%s" % (prefix, signal, layer)
    if gap.get("bloom"):
        if facts["nonpow2_total"] == 0:
            return True
        sizes = facts["cells"].get(cell, {})
        return any(s & (s - 1) for c in EQUALITY_COLUMNS.get((signal, query), ()) for s in sizes.get(c, []))
    if gap.get("footer"):
        if facts["nonutf8_total"] == 0:
            return True
        f = facts["footer"].get(cell, {})
        if query in gap.get("partition_pruned", ()):
            if dt is None:
                raise ValueError("gap %s: query %s reads one partition, the caller must name it" % (gap["id"], query))
            return dt in f.get("nonutf8_dts", ())
        return f.get("nonutf8", 0) > 0
    return True


def gap_matches(gap, tenant, got):
    """Does the observed failure `got` (an error text or a wrong value) belong to this gap?"""
    import re
    is_err = isinstance(got, str) and type(got).__name__ == "QueryError"
    if "error" in gap and is_err:
        return re.search(gap["error"], got, re.I) is not None
    if "got" in gap and not is_err:
        want = gap["got"]
        if isinstance(want, dict):
            want = want.get(tenant)
        return got == want
    return False
