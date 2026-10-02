"""Known gaps of the reader matrix: Lakehouse (or an engine) diverges from the oracle and a
tracking issue exists. A gap is not a skip: the cell still runs, must still fail, and the matrix
turns red when the cell starts passing so the entry is removed in the same change that fixes it.

`optional` marks a gap whose trigger depends on the generated data (a bloom filter of an unlucky
size); there a pass is a pass and only a fail is attributed to the issue."""

ISSUES = "https://github.com/ReliablyObserve/victoria-lakehouse/issues/"
ALL_QUERIES = ("count", "by_service", "field_filter", "time_range", "map_filter",
               "trace_by_id", "dt_filter", "ts_bounds", "utc_check", "tenant")

GAPS = [
    {"id": "footer-kv-not-utf8/polars-logs", "issue": ISSUES + "340", "engines": ("polars",),
     "signals": ("logs",), "layers": ("raw",), "label": "raw files",
     "note": "token bloom footer KV (_bloom_body_rg_N) is not UTF-8; Polars refuses the file"},
    {"id": "footer-kv-not-utf8/polars-traces", "issue": ISSUES + "340", "engines": ("polars",),
     "signals": ("traces",), "layers": ("raw", "compacted", "pruned"), "label": "traces",
     "note": "_trace_idx footer KV is not UTF-8; Polars refuses the file"},
    {"id": "footer-kv-not-utf8/datafusion-logs", "issue": ISSUES + "340", "engines": ("datafusion",),
     "signals": ("logs",), "layers": ("raw",), "label": "raw files",
     "note": "token bloom footer KV (_bloom_body_rg_N) is not UTF-8; DataFusion (arrow-rs) refuses the file"},
    {"id": "footer-kv-not-utf8/datafusion-traces", "issue": ISSUES + "340", "engines": ("datafusion",),
     "signals": ("traces",), "label": "traces",
     "note": "_trace_idx footer KV is not UTF-8; DataFusion (arrow-rs) refuses the file"},
    {"id": "schema-inference-opens-every-file/datafusion", "issue": None, "engines": ("datafusion",),
     "layers": ("pruned",), "queries": ("dt_filter",), "label": "needs an explicit schema",
     "note": "engine behaviour, not a Lakehouse defect: register_parquet infers the schema from every file footer "
             "before any partition filter applies, so one unreadable object in an unrelated partition fails the "
             "registration. Pass an explicit schema= to avoid the listing-time footer reads"},
    {"id": "sbbf-size-not-power-of-two/clickhouse", "issue": ISSUES + "341", "engines": ("clickhouse",),
     "queries": ("trace_by_id",), "optional": True, "label": "trace-ID lookups need bloom push down off",
     "note": "bloom filter bitset size is not a power of two; ClickHouse fails an equality filter on a bloom column"},
    {"id": "uint32-tenant-id/trino-big", "issue": ISSUES + "342", "engines": ("trino",),
     "tenants": ("big",), "queries": ("tenant",), "label": "tenant IDs >= 2^31 need a mask",
     "note": "Trino reads UINT_32 account_id 3000000000 as -1294967296"},
]


def known_gap(engine, signal, layer, tenant, query):
    for g in GAPS:
        if engine in g["engines"] and signal in g.get("signals", ("logs", "traces")) \
                and layer in g.get("layers", ("raw", "compacted")) \
                and tenant in g.get("tenants", ("numeric", "alias", "big", "prune")) \
                and query in g.get("queries", ALL_QUERIES):
            return g
    return None
