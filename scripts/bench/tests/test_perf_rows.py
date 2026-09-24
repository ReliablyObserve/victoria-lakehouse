"""Tests for scripts/bench/field_metadata/perf_rows.py: the rows it generates
round-trip through its own CI gate, and every regression the gate exists for
fails it."""
import copy
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "field_metadata"))
import perf_rows  # noqa: E402


def rec(cell, exact=True, set_ok=True, gets=4, nbytes=1000, rgs=2, pages=10, ram=0, ns=2_000_000):
    return {"build": "after", "cell": cell, "set_ok": set_ok, "hits_ok": exact, "ns": ns,
            "gets": gets, "bytes": nbytes, "row_groups": rgs, "pages": pages,
            "catalog_answers": ram, "truth": "t", "values": 3}


FLUSHED = "fv_level/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms"
COMPACTED = "fv_level/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms"
TRACES = "traces.streams/pmeta=true/layout=compacted/window=edge/filter=svc/s3=100ms"


def groups(records):
    g = {}
    for r in records:
        g.setdefault(r["cell"], []).append(r)
    return g


def baseline():
    recs = []
    for cell in (FLUSHED, COMPACTED, TRACES):
        recs += [rec(cell), rec(cell)]
    return recs


def rows_from(records, at="test"):
    g = groups(records)
    text = perf_rows.HEADER + "\n".join(perf_rows.row_for(c, rs, at) for c, rs in g.items()) + "\n"
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        f.write(text)
    return perf_rows.load_rows(f.name), text


class PerfRowsTest(unittest.TestCase):
    def test_generated_rows_pass_their_own_gate(self):
        recs = baseline()
        rows, text = rows_from(recs)
        self.assertEqual(perf_rows.check(groups(recs), rows), [])
        self.assertIn('"vl.perf.fv_level.pmeta_off.layout_flushed.window_cut.filter_none.s3_0ms"', text)
        self.assertIn('"vt.perf.streams.pmeta_on.layout_compacted.window_edge.filter_svc.s3_100ms"', text)
        self.assertIn('budget: { p50_ms: 2.000, p90_ms: 2.000, valid: "2/2" }', text)
        self.assertIn('path: "scan"', text)

    def test_a_differ_row_carries_a_note_and_no_budget(self):
        recs = [rec(FLUSHED, exact=False), rec(FLUSHED, exact=False)]
        _, text = rows_from(recs, at="#239")
        self.assertIn('expect: "differ"', text)
        self.assertIn('differ_note: "0/2 exact at #239: hits are not exact"', text)
        self.assertNotIn("budget:", text.splitlines()[-1])

    def test_counters_do_not_depend_on_latency(self):
        recs = baseline()
        rows, _ = rows_from(recs)
        fresh = [dict(r, cell=r["cell"].replace("s3=100ms", "s3=0ms")) for r in recs]
        self.assertEqual(perf_rows.check(groups(fresh), rows), [])

    def fails(self, mutate):
        recs = baseline()
        rows, _ = rows_from(recs)
        fresh = copy.deepcopy(recs)
        mutate(fresh)
        return perf_rows.check(groups(fresh), rows)

    def test_more_gets_fail(self):
        f = self.fails(lambda rs: [r.update(gets=5) for r in rs if r["cell"] == FLUSHED])
        self.assertTrue(any("s3_gets 5 > registry 4" in x for x in f), f)

    def test_more_bytes_fail(self):
        f = self.fails(lambda rs: [r.update(bytes=1001) for r in rs if r["cell"] == TRACES])
        self.assertTrue(any("s3_bytes" in x for x in f), f)

    def test_fewer_gets_pass(self):
        self.assertEqual(self.fails(lambda rs: [r.update(gets=1) for r in rs if r["cell"] == FLUSHED]), [])

    def test_path_change_fails(self):
        f = self.fails(lambda rs: [r.update(catalog_answers=1, gets=0) for r in rs if r["cell"] == FLUSHED])
        self.assertTrue(any("path ram != registry scan" in x for x in f), f)

    def test_a_wrong_answer_fails_a_pass_row(self):
        f = self.fails(lambda rs: rs[0].update(hits_ok=False))
        self.assertTrue(any("expect pass but not exact" in x for x in f), f)

    def test_compaction_must_not_cost_exactness(self):
        f = self.fails(lambda rs: [r.update(hits_ok=False) for r in rs if r["cell"] == COMPACTED])
        self.assertTrue(any(x.startswith(COMPACTED.rsplit("/", 1)[0]) and "not exact after compaction" in x for x in f), f)

    def test_unregistered_and_stale_cells_fail(self):
        recs = baseline()
        rows, _ = rows_from(recs)
        extra = recs + [rec("streams/pmeta=true/layout=flushed/window=narrow/filter=none/s3=0ms")]
        self.assertTrue(any("has no registry row" in x for x in perf_rows.check(groups(extra), rows)))
        fewer = [r for r in recs if r["cell"] != TRACES]
        self.assertTrue(any("does not measure" in x for x in perf_rows.check(groups(fewer), rows)))

    def test_a_differ_row_that_became_exact_must_be_promoted(self):
        wrong = [rec(FLUSHED, exact=False), rec(FLUSHED, exact=False)]
        rows, _ = rows_from(wrong)
        f = perf_rows.check(groups([rec(FLUSHED), rec(FLUSHED)]), rows)
        self.assertTrue(any("now exact" in x for x in f), f)

    def test_the_worst_iteration_is_recorded_and_held(self):
        recs = [rec(FLUSHED, gets=35), rec(FLUSHED, gets=36)]
        rows, text = rows_from(recs)
        self.assertIn("s3_gets: 36", text)
        self.assertEqual(perf_rows.check(groups([rec(FLUSHED, gets=36)]), rows), [])
        f = perf_rows.check(groups([rec(FLUSHED, gets=35), rec(FLUSHED, gets=37)]), rows)
        self.assertTrue(any("s3_gets 37 > registry 36" in x for x in f), f)

    def test_bad_cell_names_are_rejected(self):
        with self.assertRaises(ValueError):
            perf_rows.parse_cell("fv_level/window=cut")


if __name__ == "__main__":
    unittest.main()
