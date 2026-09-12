#!/usr/bin/env python3
"""Unit tests for scripts/bench/report.py's response-validation rules (Task 6):
a cell is only meaningful when every timed iteration returned a correct,
valid response. Run with:

    python3 -m unittest discover -s scripts/bench/tests
"""
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
        row = make_row("17150")  # within 5% tolerance
        ok, note = report.cell_status(row, brow)
        self.assertTrue(ok, note)
        self.assertEqual(note, "")

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


if __name__ == "__main__":
    unittest.main()
