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
     column chunks — the read-side page-skipping work depends on it.

Any failure exits non-zero with a per-check report. This is the gate
from docs/architecture/parquet-compression-research.md: every parquet
encoding change ships behind it.

Usage: python3 scripts/ci/parquet-readback/verify.py /tmp/parquet-readback
"""

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
