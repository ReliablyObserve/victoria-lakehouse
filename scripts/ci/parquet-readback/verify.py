#!/usr/bin/env python3
"""Multi-engine parquet readback gate (pyarrow + duckdb).

Reads the files produced by gen/main.go (the REAL production schemas +
writer options) with BOTH pyarrow and duckdb and asserts:

  1. aggregates match the writer-truth manifest in EXACT arithmetic
     (row count, integer column sums computed as Python bigints —
     nanosecond timestamp sums overflow int64 — and distinct counts of
     low-cardinality string columns), independently for each engine;
  2. pyarrow <-> duckdb row-level equality: EXCEPT ALL in both
     directions over ALL columns (maps included) returns zero rows;
  3. the schema-tag encodings actually landed: every delta-tagged
     column chunk uses DELTA_BINARY_PACKED, every dict-tagged column
     chunk uses RLE_DICTIONARY;
  4. PageIndex (ColumnIndex + OffsetIndex) is present on 100% of
     column chunks — the read-side page-skipping work depends on it;
  5. the span events / links JSON blob columns (traces file): optional
     BYTE_ARRAY, ZSTD, no dictionary, and the event / link counts, the
     exception events and the stack-trace bytes decoded from the JSON
     match the writer truth under pyarrow and under duckdb's JSON functions.

Any failure exits non-zero with a per-check report. Every parquet encoding
change ships behind this gate.

Usage: python3 scripts/ci/parquet-readback/verify.py /tmp/parquet-readback
"""

import base64
import json
import os
import sys

import duckdb
import pyarrow.compute as pc
import pyarrow.parquet as pq

FAILURES = []


def check(ok: bool, label: str, detail: str = "") -> None:
    status = "PASS" if ok else "FAIL"
    line = f"  [{status}] {label}"
    if detail and not ok:
        line += f" — {detail}"
    print(line)
    if not ok:
        FAILURES.append(label)


def exact_int_sum(tbl, col: str) -> int:
    """Exact (arbitrary-precision) sum — pc.sum wraps on int64 overflow."""
    return sum(v for v in tbl.column(col).to_pylist() if v is not None)


def verify_pyarrow(path: str, truth: dict) -> None:
    tbl = pq.read_table(path)
    check(tbl.num_rows == truth["rows"], f"pyarrow rows == {truth['rows']}",
          f"got {tbl.num_rows}")
    for col, want in sorted(truth["int64_sums"].items()):
        got = exact_int_sum(tbl, col)
        check(got == want, f"pyarrow sum({col}) == {want}", f"got {got}")
    for col, want in sorted(truth["distinct_counts"].items()):
        got = pc.count_distinct(tbl.column(col)).as_py()
        check(got == want, f"pyarrow distinct({col}) == {want}", f"got {got}")


def verify_duckdb(con, path: str, truth: dict) -> None:
    (rows,) = con.execute(
        "SELECT count(*) FROM read_parquet(?)", [path]).fetchone()
    check(rows == truth["rows"], f"duckdb rows == {truth['rows']}",
          f"got {rows}")
    for col, want in sorted(truth["int64_sums"].items()):
        # duckdb sums integers into HUGEINT (int128) — exact for our sizes.
        (got,) = con.execute(
            f'SELECT sum("{col}") FROM read_parquet(?)', [path]).fetchone()
        check(int(got) == want, f"duckdb sum({col}) == {want}", f"got {got}")
    for col, want in sorted(truth["distinct_counts"].items()):
        (got,) = con.execute(
            f'SELECT count(DISTINCT "{col}") FROM read_parquet(?)',
            [path]).fetchone()
        check(got == want, f"duckdb distinct({col}) == {want}", f"got {got}")


def verify_cross_engine(con, path: str) -> None:
    """pyarrow rows == duckdb rows, proven with EXCEPT ALL both ways."""
    tbl = pq.read_table(path)
    con.register("pa_tbl", tbl)
    (a,) = con.execute(
        "SELECT count(*) FROM (SELECT * FROM pa_tbl "
        "EXCEPT ALL SELECT * FROM read_parquet(?))", [path]).fetchone()
    (b,) = con.execute(
        "SELECT count(*) FROM (SELECT * FROM read_parquet(?) "
        "EXCEPT ALL SELECT * FROM pa_tbl)", [path]).fetchone()
    check(a == 0, "EXCEPT ALL pyarrow→duckdb == 0 rows", f"got {a}")
    check(b == 0, "EXCEPT ALL duckdb→pyarrow == 0 rows", f"got {b}")
    con.unregister("pa_tbl")


def verify_encodings_and_pageindex(path: str, truth: dict) -> None:
    md = pq.ParquetFile(path).metadata
    encodings: dict[str, set] = {}
    missing_ci, missing_oi, total = 0, 0, 0
    for rg in range(md.num_row_groups):
        for c in range(md.num_columns):
            col = md.row_group(rg).column(c)
            encodings.setdefault(col.path_in_schema, set()).update(
                col.encodings)
            total += 1
            missing_ci += 0 if col.has_column_index else 1
            missing_oi += 0 if col.has_offset_index else 1

    for col in truth["delta_columns"]:
        got = encodings.get(col, set())
        check("DELTA_BINARY_PACKED" in got,
              f"encoding {col} has DELTA_BINARY_PACKED", f"got {sorted(got)}")
    for col in truth["dict_columns"]:
        got = encodings.get(col, set())
        check("RLE_DICTIONARY" in got,
              f"encoding {col} has RLE_DICTIONARY", f"got {sorted(got)}")

    check(missing_ci == 0,
          f"PageIndex ColumnIndex on {total}/{total} column chunks",
          f"{missing_ci} chunks missing")
    check(missing_oi == 0,
          f"PageIndex OffsetIndex on {total}/{total} column chunks",
          f"{missing_oi} chunks missing")
    check(md.num_row_groups > 1,
          f"multiple row groups present ({md.num_row_groups})",
          "need >1 to exercise MaxRowsPerRowGroup")


def verify_span_extras(con, path: str, truth: dict) -> None:
    """The span events / links blob columns (traces file only).

    Both are plain optional BYTE_ARRAY columns holding a UTF-8 JSON array per
    span, so every engine reads them as strings and any JSON function decodes
    them. The expected counts come from the writer; pyarrow (json.loads) and
    duckdb (json_array_length / json_extract_string) must each reproduce them.
    """
    want = truth.get("span_extras")
    if want is None:
        return

    # 1. physical layout: optional BYTE_ARRAY, ZSTD, no dictionary.
    pf = pq.ParquetFile(path)
    md = pf.metadata
    for name in ("span.events_json", "span.links_json"):
        idx = next((i for i in range(md.num_columns)
                    if md.row_group(0).column(i).path_in_schema == name), None)
        check(idx is not None, f"column {name} exists")
        if idx is None:
            continue
        sc = pf.schema.column(idx)
        check(sc.physical_type == "BYTE_ARRAY" and sc.max_definition_level == 1,
              f"{name} is an optional BYTE_ARRAY",
              f"got {sc.physical_type} def={sc.max_definition_level}")
        encs, codecs = set(), set()
        for rg in range(md.num_row_groups):
            col = md.row_group(rg).column(idx)
            encs.update(col.encodings)
            codecs.add(col.compression)
        check(not (encs & {"RLE_DICTIONARY", "PLAIN_DICTIONARY"}),
              f"{name} has no dictionary encoding", f"got {sorted(encs)}")
        check(codecs == {"ZSTD"}, f"{name} is ZSTD", f"got {sorted(codecs)}")

    # 2. pyarrow + json.loads
    tbl = pq.read_table(path, columns=["span.events_json", "span.links_json"])
    ev_rows = tbl.column("span.events_json").to_pylist()
    ln_rows = tbl.column("span.links_json").to_pylist()
    spans_ev = events = exc = stack = 0
    b_vals = b_len = b_names = b_name_len = b64_keys = malformed = 0
    first = -1
    for i, v in enumerate(ev_rows):
        if v is None:
            continue
        arr = json.loads(v)
        spans_ev += 1
        events += len(arr)
        exc += sum(1 for e in arr if e.get("event_name") == "exception")
        stack += sum(len(e.get("event_attr:exception.stacktrace", "")) for e in arr
                     if isinstance(e.get("event_attr:exception.stacktrace", ""), str))
        for e in arr:
            for k, val in e.items():
                if k.startswith("$b64:"):
                    b64_keys += 1
                if isinstance(val, dict):
                    if set(val) != {"$bytes"}:
                        malformed += 1
                    raw = base64.b64decode(val["$bytes"])
                    b_vals += 1
                    b_len += len(raw)
                    if k == "event_name":
                        b_names += 1
                        b_name_len += len(raw)
        if first < 0:
            first = i
    spans_ln = links = flags = 0
    for v in ln_rows:
        if v is None:
            continue
        arr = json.loads(v)
        spans_ln += 1
        links += len(arr)
        flags += sum(int(l["link_flags"]) for l in arr)
    for label, got, w in (
        ("spans with events", spans_ev, want["spans_with_events"]),
        ("events", events, want["events"]),
        ("exception events", exc, want["exception_events"]),
        ("stack trace bytes", stack, want["stacktrace_bytes"]),
        ("first span with events", first, want["first_span_events_at"]),
        ("spans with links", spans_ln, want["spans_with_links"]),
        ("links", links, want["links"]),
        ("link_flags sum", flags, want["link_flags_sum"]),
        ("$bytes values", b_vals, want["bytes_values"]),
        ("$bytes decoded length", b_len, want["bytes_value_len"]),
        ("$bytes event names", b_names, want["bytes_event_names"]),
        ("$bytes event name length", b_name_len, want["bytes_event_name_len"]),
        ("$b64: keys", b64_keys, want["b64_keys"]),
        ("malformed value objects", malformed, 0),
    ):
        check(got == w, f"pyarrow {label} == {w}", f"got {got}")

    # 3. duckdb + JSON functions
    (dn, dl) = con.execute(
        """SELECT count(*), coalesce(sum(octet_length(from_base64(json_extract_string(e, '$.event_name."$bytes"')))), 0)
           FROM (SELECT unnest(from_json("span.events_json", '["JSON"]')) AS e
                 FROM read_parquet(?) WHERE "span.events_json" IS NOT NULL)
           WHERE json_extract_string(e, '$.event_name."$bytes"') IS NOT NULL""",
        [path]).fetchone()
    check(int(dn) == want["bytes_event_names"] and int(dl) == want["bytes_event_name_len"],
          f"duckdb decodes {want['bytes_event_names']} $bytes event names, {want['bytes_event_name_len']} bytes",
          f"got {dn}, {dl}")
    (dk,) = con.execute(
        """SELECT coalesce(sum(len(list_filter(json_keys(e), k -> starts_with(k, '$b64:')))), 0)
           FROM (SELECT unnest(from_json("span.events_json", '["JSON"]')) AS e
                 FROM read_parquet(?) WHERE "span.events_json" IS NOT NULL)""",
        [path]).fetchone()
    check(int(dk) == want["b64_keys"], f"duckdb sees {want['b64_keys']} $b64: keys", f"got {dk}")
    ev, = con.execute(
        'SELECT count(*) FROM read_parquet(?) WHERE "span.events_json" IS NOT NULL',
        [path]).fetchone()
    check(ev == want["spans_with_events"],
          f"duckdb spans with events == {want['spans_with_events']}", f"got {ev}")
    (n_ev,) = con.execute(
        'SELECT sum(json_array_length("span.events_json")) FROM read_parquet(?)',
        [path]).fetchone()
    check(int(n_ev) == want["events"], f"duckdb events == {want['events']}", f"got {n_ev}")
    (n_exc,) = con.execute(
        """SELECT count(*) FROM (
             SELECT unnest(from_json("span.events_json", '["JSON"]')) AS e
             FROM read_parquet(?) WHERE "span.events_json" IS NOT NULL)
           WHERE json_extract_string(e, '$.event_name') = 'exception'""",
        [path]).fetchone()
    check(n_exc == want["exception_events"],
          f"duckdb exception events == {want['exception_events']}", f"got {n_exc}")
    (n_ln,) = con.execute(
        'SELECT sum(json_array_length("span.links_json")) FROM read_parquet(?)',
        [path]).fetchone()
    check(int(n_ln) == want["links"], f"duckdb links == {want['links']}", f"got {n_ln}")
    (fl,) = con.execute(
        """SELECT sum(CAST(json_extract_string(l, '$.link_flags') AS BIGINT)) FROM (
             SELECT unnest(from_json("span.links_json", '["JSON"]')) AS l
             FROM read_parquet(?) WHERE "span.links_json" IS NOT NULL)""",
        [path]).fetchone()
    check(int(fl) == want["link_flags_sum"],
          f"duckdb link_flags sum == {want['link_flags_sum']}", f"got {fl}")


def main() -> int:
    outdir = sys.argv[1] if len(sys.argv) > 1 else "/tmp/parquet-readback"
    with open(os.path.join(outdir, "manifest.json")) as fh:
        manifest = json.load(fh)

    con = duckdb.connect()
    for truth in manifest["files"]:
        path = os.path.join(outdir, truth["file"])
        print(f"\n=== {truth['signal']}: {path} ===")
        verify_pyarrow(path, truth)
        verify_duckdb(con, path, truth)
        verify_cross_engine(con, path)
        verify_encodings_and_pageindex(path, truth)
        verify_span_extras(con, path, truth)

    print()
    if FAILURES:
        print(f"parquet-readback: {len(FAILURES)} check(s) FAILED:")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("parquet-readback: PASS — both engines read every file, "
          "aggregates match writer truth, row-level equality holds, "
          "encodings + PageIndex verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
