#!/usr/bin/env python3
"""Unit tests for scripts/bench/report.py's response-validation rules (v3
validation): a cell is only meaningful when every timed iteration returned a correct,
valid response. Run with:

    python3 -m unittest discover -s scripts/bench/tests
"""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import report  # noqa: E402


def make_row(result, iters_valid=20, iters_invalid=0, invalid_reasons=None, avg_bytes=100):
    return {
        "result": result,
        "iters_valid": iters_valid,
        "iters_invalid": iters_invalid,
        "invalid_reasons": invalid_reasons,
        "avg_bytes": avg_bytes,
        "p95_ms": 5.0,
    }


class TestParseResult(unittest.TestCase):
    def test_plain_scalar(self):
        self.assertEqual(report.parse_result("17132"), {"count": 17132, "hash": None})

    def test_scan_with_hash(self):
        r = report.parse_result("rows=1000;hash=abc123")
        self.assertEqual(r, {"count": 1000, "hash": "abc123"})

    def test_scan_ch_rows_only(self):
        self.assertEqual(report.parse_result("rows=1000"), {"count": 1000, "hash": None})

    def test_spans(self):
        self.assertEqual(report.parse_result("spans=8"), {"count": 8, "hash": None})

    def test_unparseable(self):
        self.assertIsNone(report.parse_result("invalid:parse-error"))
        self.assertIsNone(report.parse_result(None))


class TestCellStatus(unittest.TestCase):
    def test_all_valid_cell_passes(self):
        brow = make_row("17132")
        row = make_row("17132")  # exactly equal — shared window bounds, static seed
        ok, note = report.cell_status(row, brow)
        self.assertTrue(ok, note)
        self.assertEqual(note, "")

    def test_near_miss_is_invalid_no_tolerance(self):
        # run.sh shares one set of window bounds across every system in a
        # cell, and the seed is a static backfill with no live ingest — a
        # result that's merely CLOSE (not exactly equal) is a real
        # divergence now, not measurement noise to tolerate.
        brow = make_row("17132")
        row = make_row("17150")
        ok, note = report.cell_status(row, brow)
        self.assertFalse(ok)
        self.assertIn("17150", note)
        self.assertIn("17132", note)

    def test_groupby_same_total_different_groups_is_invalid(self):
        # count_by_service/high_card: rows=<groups>;hash=<sorted group,count
        # pairs>. Equal totals split across different groups must not match.
        brow = make_row("rows=2;hash=" + "a" * 64)
        row = make_row("rows=2;hash=" + "b" * 64)
        ok, note = report.cell_status(row, brow)
        self.assertFalse(ok)
        self.assertIn("different rows", note)

    def test_invalid_iteration_marks_cell(self):
        brow = make_row("17132")
        row = make_row("17132", iters_valid=18, iters_invalid=2, invalid_reasons="empty result")
        ok, note = report.cell_status(row, brow)
        self.assertFalse(ok)
        self.assertIn("2/20 invalid", note)
        self.assertIn("empty result", note)

    def test_result_mismatch_vs_baseline(self):
        brow = make_row("17132")
        row = make_row("9000")  # way off
        ok, note = report.cell_status(row, brow)
        self.assertFalse(ok)
        self.assertIn("9000", note)
        self.assertIn("17132", note)

    def test_equal_rows_different_hash(self):
        brow = make_row("rows=1000;hash=aaaa")
        row = make_row("rows=1000;hash=bbbb")
        ok, note = report.cell_status(row, brow)
        self.assertFalse(ok)
        self.assertIn("different rows", note)

    def test_equal_rows_same_hash_passes(self):
        brow = make_row("rows=1000;hash=aaaa")
        row = make_row("rows=1000;hash=aaaa")
        ok, note = report.cell_status(row, brow)
        self.assertTrue(ok, note)

    def test_ch_scan_rows_only_no_hash_required(self):
        # ClickHouse scan never carries a hash (different projection) — must
        # not be penalized for lacking one when row counts agree.
        brow = make_row("rows=1000;hash=aaaa")
        ch_row = make_row("rows=1000")
        ok, note = report.cell_status(ch_row, brow)
        self.assertTrue(ok, note)

    def test_ch_scan_row_count_mismatch_still_caught(self):
        brow = make_row("rows=1000;hash=aaaa")
        ch_row = make_row("rows=500")
        ok, note = report.cell_status(ch_row, brow)
        self.assertFalse(ok)

    def test_missing_row(self):
        brow = make_row("17132")
        ok, note = report.cell_status(None, brow)
        self.assertFalse(ok)
        self.assertEqual(note, "missing")

    def test_miss_scenario_zero_equals_zero(self):
        # trace_lookup: baseline and engine both legitimately 0 — not a mismatch.
        brow = make_row("spans=0")
        row = make_row("spans=0")
        ok, note = report.cell_status(row, brow)
        self.assertTrue(ok, note)

    def test_pre_validation_json_fallback_errored(self):
        # Pre-validation JSON (no iters_valid/iters_invalid at all) falls
        # back to the old iters==0 / avg_bytes==0 checks.
        brow = make_row("17132")
        row = {"result": "17132", "iters": 0, "errors": 20, "avg_bytes": 0}
        ok, note = report.cell_status(row, brow)
        self.assertFalse(ok)
        self.assertEqual(note, "errored")

    def test_pre_validation_json_fallback_empty_bytes(self):
        brow = make_row("17132")
        row = {"result": "17132", "iters": 20, "errors": 0, "avg_bytes": 0}
        ok, note = report.cell_status(row, brow)
        self.assertFalse(ok)
        self.assertEqual(note, "empty (0 bytes)")


class TestScanWindowValidation(unittest.TestCase):
    """A truncated scan's per-iteration `result` legitimately varies run to
    run (VictoriaLogs returns an arbitrary subset once more than `limit` rows
    match) — cross-system validity for a `scan` row is checked against the
    reference `window_rows`/`window_hash` instead, by design: identity can
    never hold for a truncated scan (see run.sh's SCAN_LIMIT comment)."""

    def test_window_hash_match_passes_despite_differing_iteration_hash(self):
        brow = make_row("rows=1000;hash=zzzz")
        brow["window_rows"], brow["window_hash"] = 1500, "aaaa"
        row = make_row("rows=1000;hash=yyyy")  # different truncated subset, same window
        row["window_rows"], row["window_hash"] = 1500, "aaaa"
        ok, note = report.cell_status(row, brow)
        self.assertTrue(ok, note)

    def test_window_hash_mismatch_fails(self):
        brow = make_row("rows=1000;hash=zzzz")
        brow["window_rows"], brow["window_hash"] = 1500, "aaaa"
        row = make_row("rows=1000;hash=yyyy")
        row["window_rows"], row["window_hash"] = 1500, "bbbb"
        ok, note = report.cell_status(row, brow)
        self.assertFalse(ok)
        self.assertIn("hash mismatch", note)

    def test_ch_scan_window_rows_only(self):
        # ClickHouse's scan carries window_rows but no window_hash.
        brow = make_row("rows=1000;hash=zzzz")
        brow["window_rows"], brow["window_hash"] = 1500, "aaaa"
        ch_row = make_row("rows=1000")
        ch_row["window_rows"], ch_row["window_hash"] = 1500, None
        ok, note = report.cell_status(ch_row, brow)
        self.assertTrue(ok, note)

    def test_ch_scan_window_rows_mismatch_caught(self):
        brow = make_row("rows=1000;hash=zzzz")
        brow["window_rows"], brow["window_hash"] = 1500, "aaaa"
        ch_row = make_row("rows=1000")
        ch_row["window_rows"], ch_row["window_hash"] = 500, None
        ok, note = report.cell_status(ch_row, brow)
        self.assertFalse(ok)
        self.assertIn("window_rows", note)

    def test_window_rows_missing_on_one_side_invalid(self):
        brow = make_row("rows=1000;hash=zzzz")
        brow["window_rows"], brow["window_hash"] = 1500, "aaaa"
        row = make_row("rows=1000;hash=yyyy")  # no window_rows at all
        ok, note = report.cell_status(row, brow)
        self.assertFalse(ok)
        self.assertIn("window_rows", note)

    def test_window_rows_unequal_is_invalid_exact_equality_required(self):
        # run.sh now shares one set of window bounds across every system in
        # a cell (computed once per signal/query/range/latency, not once per
        # system), and the seed is a static backfill with no live ingest —
        # so with byte-identical bounds every system MUST return exactly the
        # same window_rows. A close-but-unequal count (541 vs 539) is no
        # longer treated as harmless drift: it's a real divergence.
        brow = make_row("rows=541;hash=zzzz")
        brow["window_rows"], brow["window_hash"] = 541, "aaaa"
        row = make_row("rows=539;hash=yyyy")
        row["window_rows"], row["window_hash"] = 539, "bbbb"
        ok, note = report.cell_status(row, brow)
        self.assertFalse(ok)
        self.assertIn("window_rows", note)
        self.assertIn("541", note)
        self.assertIn("539", note)

    def test_base_status_window_rows_zero_is_baseline_empty(self):
        brow = make_row("rows=0;hash=" + "e" * 64)
        brow["window_rows"], brow["window_hash"] = 0, "e" * 64
        ok, note = report.base_status(brow, "scan")
        self.assertFalse(ok)
        self.assertIn("baseline-empty", note)


class TestBaseStatus(unittest.TestCase):
    def test_baseline_empty_result(self):
        brow = make_row("0")
        ok, note = report.base_status(brow, "count_total")
        self.assertFalse(ok)
        self.assertIn("baseline-empty", note)

    def test_baseline_empty_but_documented_miss_is_ok(self):
        brow = make_row("spans=0")
        ok, note = report.base_status(brow, "trace_lookup")
        self.assertTrue(ok, note)

    def test_baseline_missing(self):
        ok, note = report.base_status(None, "count_total")
        self.assertFalse(ok)
        self.assertIn("missing", note)

    def test_baseline_own_iterations_invalid(self):
        brow = make_row("17132", iters_valid=15, iters_invalid=5, invalid_reasons="flapping")
        ok, note = report.base_status(brow, "count_total")
        self.assertFalse(ok)
        self.assertIn("invalid", note)

    def test_baseline_valid(self):
        brow = make_row("17132")
        ok, note = report.base_status(brow, "count_total")
        self.assertTrue(ok, note)

    def test_baseline_avg_bytes_zero_is_empty(self):
        brow = make_row("17132", avg_bytes=0)
        ok, note = report.base_status(brow, "count_total")
        self.assertFalse(ok)
        self.assertIn("avg_bytes=0", note)

    def test_baseline_avg_bytes_zero_ok_for_miss_scenario(self):
        brow = make_row("spans=0", avg_bytes=0)
        ok, note = report.base_status(brow, "trace_lookup")
        self.assertTrue(ok, note)


class TestRendering(unittest.TestCase):
    """valid_str / invalid_note / render_result / shape_flag, plus an
    end-to-end main() run on a small 2-cell fixture asserting the rendered
    valid columns, the overall summary line, and the Invalid-cells section."""

    def test_valid_str_full(self):
        self.assertEqual(report.valid_str(make_row("17132")), "20/20")

    def test_valid_str_partial(self):
        row = make_row("17132", iters_valid=18, iters_invalid=2)
        self.assertEqual(report.valid_str(row), "18/20")

    def test_valid_str_missing_row(self):
        self.assertEqual(report.valid_str(None), "0/0")
        self.assertEqual(report.valid_str({}), "0/0")

    def test_valid_str_legacy_fallback(self):
        row = {"iters": 15, "errors": 5}
        self.assertEqual(report.valid_str(row), "15/20")

    def test_invalid_note_format(self):
        row = make_row("17132", iters_valid=18, iters_invalid=2, invalid_reasons="empty result")
        self.assertEqual(report.invalid_note(row), "2/20 invalid: empty result")

    def test_invalid_note_unknown_reason(self):
        row = make_row("17132", iters_valid=18, iters_invalid=2, invalid_reasons=None)
        self.assertEqual(report.invalid_note(row), "2/20 invalid: unknown")

    def test_render_result_plain_scalar(self):
        self.assertEqual(report.render_result(make_row("17132")), "17132")

    def test_render_result_scan_with_window_and_hash(self):
        row = make_row("rows=1000;hash=abcd")
        row["window_rows"], row["window_hash"] = 14301, "1234567890abcdef"
        self.assertEqual(report.render_result(row), "rows=1000/14301;window=12345678")

    def test_render_result_scan_ch_no_hash(self):
        row = make_row("rows=1000")
        row["window_rows"], row["window_hash"] = 14301, None
        self.assertEqual(report.render_result(row), "rows=1000/14301")

    def test_render_result_groupby_truncates_hash_for_display(self):
        row = make_row("rows=5;hash=" + "ab" * 32)
        self.assertEqual(report.render_result(row), "rows=5;hash=" + "ab" * 4)

    def test_render_result_missing_row(self):
        self.assertIsNone(report.render_result(None))
        self.assertIsNone(report.render_result({}))

    def test_shape_flag_triggers_on_10x_difference(self):
        row = make_row("17132", avg_bytes=10000)
        brow = make_row("17132", avg_bytes=100)
        self.assertEqual(report.shape_flag(row, brow), " ⚠ shape")

    def test_shape_flag_silent_when_close(self):
        row = make_row("17132", avg_bytes=100)
        brow = make_row("17132", avg_bytes=110)
        self.assertEqual(report.shape_flag(row, brow), "")

    def test_main_end_to_end_two_cells(self):
        rows = [
            # cell 1: fully valid, LH 2x baseline
            {"signal": "logs", "query": "count_total", "range": "1h", "latency_ms": 0,
             "system": "victorialogs", "p95_ms": 5.0, "result": "100",
             "iters_valid": 20, "iters_invalid": 0, "invalid_reasons": None,
             "avg_bytes": 50, "disk_profile": "local-ssd"},
            {"signal": "logs", "query": "count_total", "range": "1h", "latency_ms": 0,
             "system": "lakehouse", "p95_ms": 10.0, "result": "100",
             "iters_valid": 20, "iters_invalid": 0, "invalid_reasons": None,
             "avg_bytes": 50, "disk_profile": "local-ssd"},
            {"signal": "logs", "query": "count_total", "range": "1h", "latency_ms": 0,
             "system": "clickhouse", "p95_ms": 90.0, "result": "100",
             "iters_valid": 20, "iters_invalid": 0, "invalid_reasons": None,
             "avg_bytes": 50, "disk_profile": "local-ssd"},
            # cell 2: LH invalid (flapped)
            {"signal": "logs", "query": "scan", "range": "24h", "latency_ms": 0,
             "system": "victorialogs", "p95_ms": 8.0, "result": "rows=1000;hash=aa",
             "window_rows": 14000, "window_hash": "aaaa",
             "iters_valid": 20, "iters_invalid": 0, "invalid_reasons": None,
             "avg_bytes": 500, "disk_profile": "local-ssd"},
            {"signal": "logs", "query": "scan", "range": "24h", "latency_ms": 0,
             "system": "lakehouse", "p95_ms": 20.0, "result": "rows=1000;hash=bb",
             "window_rows": 14000, "window_hash": "bbbb",
             "iters_valid": 1, "iters_invalid": 19, "invalid_reasons": "flapping",
             "avg_bytes": 500, "disk_profile": "local-ssd"},
            {"signal": "logs", "query": "scan", "range": "24h", "latency_ms": 0,
             "system": "clickhouse", "p95_ms": 80.0, "result": "rows=1000",
             "window_rows": 14000, "window_hash": None,
             "iters_valid": 20, "iters_invalid": 0, "invalid_reasons": None,
             "avg_bytes": 500, "disk_profile": "local-ssd"},
        ]
        raw = os.path.join(os.path.dirname(__file__), "_tmp_main_e2e.json")
        out = os.path.join(os.path.dirname(__file__), "_tmp_main_e2e.md")
        try:
            with open(raw, "w") as f:
                json.dump(rows, f)
            sys.argv = ["report.py", raw, out]
            report.main()
            with open(out) as f:
                md = f.read()
        finally:
            for p in (raw, out):
                if os.path.exists(p):
                    os.remove(p)
        self.assertIn("1 valid LH cells, 1 invalid", md)
        self.assertIn("20/20", md)  # the fully-valid cell's validity columns
        self.assertIn("19/20 invalid: flapping", md)  # the flapped LH cell (1 valid, 19 invalid)
        self.assertIn("## ⚠️ Invalid cells", md)
        self.assertIn("lakehouse — logs/scan/24h/lat0ms:", md)


if __name__ == "__main__":
    unittest.main()
