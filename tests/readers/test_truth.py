"""Self-tests of the writer-side truth: no stack, no engines needed.

The truth is what makes the matrix more than Lakehouse comparing itself with itself, so the pieces it
is built from are pinned here: the golden batch really holds the edge cases it claims, the expected
answers are derived from the rows the writer sent and refuse to be empty, the bloom filter header
parser reads the real thing, and every mutation of mutation-proof.sh still finds its anchor in the code.
"""
import json
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone

import pytest

import fixture
import golden
import lib
import mutants
import truth

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = lib.REPO


def test_golden_batch_uses_every_nanosecond_digit_position_and_value():
    logs, spans = golden.golden_rows()
    for rows in (logs, spans):
        fracs = {r["ts"] % 10**9 for r in rows}
        text = "".join("%09d" % f for f in fracs)
        assert set(text) >= set("0123456789"), "every digit value appears in the fractions"
        for pos in range(9):  # each of the nine positions carries a non-zero digit in some row
            assert any(("%09d" % f)[pos] != "0" for f in fracs), "position %d" % pos
        assert len({r["ts"] for r in rows}) == len(rows), "timestamps are unique"


def test_golden_batch_straddles_the_day_boundary_to_the_nanosecond():
    logs, _ = golden.golden_rows()
    ts = sorted(r["ts"] for r in logs)
    midnight = [t for t in ts if t % (24 * 3600 * 10**9) == 0]
    assert len(midnight) == 1
    assert midnight[0] - 1 in ts and midnight[0] + 1 in ts


def test_golden_batch_has_odd_map_keys_and_a_trace_with_several_rows():
    logs, spans = golden.golden_rows()
    keys = {k for r in logs for k in r["attrs"]}
    assert {"key with spaces", "a=b", "quote'key", "unicode.ключ"} <= keys
    assert any(len(k) == 100 for k in keys)
    ids = [r["trace_id"] for r in logs]
    assert max(ids.count(i) for i in set(ids)) >= 3


def test_bloom_tenant_is_sixty_rows_in_one_hour_per_round():
    for rnd in (1, 2):
        logs, spans = golden.bloom_rows(rnd)
        for rows in (logs, spans):
            assert len(rows) == 60
            hours = {r["ts"] // (3600 * 10**9) for r in rows}
            assert len(hours) <= 2  # one hour (spans end 1000 ns later, which may cross the edge)
        assert len({r["trace_id"] for r in spans}) == 60, "60 distinct trace IDs give the 96-byte filter"
    assert golden.bloom_rows(1)[0][0]["trace_id"] != golden.bloom_rows(2)[0][0]["trace_id"]


def test_golden_manifest_matches_the_rows():
    logs, spans = golden.golden_rows()
    m = golden.manifest_of("golden", logs, spans)
    assert m["logs"]["count"] == len(logs) and m["traces"]["count"] == len(spans)
    assert sum(m["logs"]["by_service"].values()) == len(logs)
    assert m["logs"]["ts_min"] == min(r["ts"] for r in logs)
    assert m["logs"]["map_filter"] == sum(1 for r in logs if r["attrs"].get("format") == "nginx")
    assert m["traces"]["map_filter"] == sum(1 for r in spans if r["attrs"].get("rpc.system") == "grpc")
    assert m["logs"]["map_keys"]["format"] == len(logs)


def sig(timestamps, **kw):
    s = {"count": len(timestamps), "by_service": {"api-gateway": len(timestamps)}, "errors": 1, "field_filter": 1,
         "map_filter": 1, "map_keys": {"format": 1}, "timestamps": sorted(timestamps), "trace_counts": {"t1": 2, "t2": 1},
         "map_column": "log.attributes"}
    s.update(kw)
    s["ts_min"], s["ts_max"] = s["timestamps"][0], s["timestamps"][-1]
    return s


def test_window_edges_are_exact_row_timestamps_not_whole_seconds():
    day = int(datetime(2026, 10, 1, tzinfo=timezone.utc).timestamp()) * 10**9
    ts = [day + i * 7_000_000_123 + 11 for i in range(40)]
    p = truth.derive_params(sig(ts))
    assert p["from_ns"] in ts and p["to_ns"] in ts
    assert p["from_ns"] % 10**9 != 0
    assert p["trace_id"] == "t1"
    a = truth.expected_answers("numeric", lib.TENANTS["numeric"], sig(ts), p)
    # inclusive lower edge, exclusive upper edge
    assert a["time_range"] == sum(1 for t in ts if p["from_ns"] <= t < p["to_ns"])
    assert a["time_range"] == ts.index(p["to_ns"]) - ts.index(p["from_ns"])
    # rounding the stored timestamps to microseconds moves the answer or the bounds
    cut = [t // 1000 * 1000 for t in ts]
    assert [min(cut), max(cut)] != [min(ts), max(ts)]


def test_truth_refuses_an_answer_that_proves_nothing():
    ts = [10**18 + i for i in range(10)]
    p = truth.derive_params(sig(ts))
    good = truth.expected_answers("numeric", lib.TENANTS["numeric"], sig(ts), p)
    truth.guard_nonzero("x", good)
    for q, empty in (("count", 0), ("field_filter", 0), ("map_filter", 0), ("trace_by_id", 0), ("by_service", {}), ("tenant", [])):
        with pytest.raises(AssertionError):
            truth.guard_nonzero("x", dict(good, **{q: empty}))
    truth.guard_nonzero("x", dict(good, utc_check=0))  # the only answer that is correctly zero


def test_merge_sums_the_parts_of_a_tenant():
    with tempfile.TemporaryDirectory() as d:
        for i, ts in enumerate(([100, 200], [300])):
            part = {"name": "numeric", "logs": sig(ts), "traces": sig(ts)}
            lib.write_json(os.path.join(d, "numeric-r%d.json" % i), part)
        m = truth.merge(d)["numeric"]
        assert m["logs"]["count"] == 3 and m["logs"]["timestamps"] == [100, 200, 300]
        assert m["logs"]["trace_counts"]["t1"] == 4 and m["logs"]["ts_max"] == 300
        assert m["account"] == 4401


def test_sbbf_header_parser_reads_the_numbytes_field():
    def header(n):
        zz = (n << 1) ^ (n >> 31)
        out = bytearray([0x15])
        while True:
            b = zz & 0x7F
            zz >>= 7
            out.append(b | (0x80 if zz else 0))
            if not zz:
                break
        return bytes(out) + b"\x00" * 8

    class Fs:
        def __init__(self, n):
            self.n = n

        def cat_file(self, key, start, end):
            return header(self.n)[: end - start]

    for n in (32, 64, 96, 160, 12512, 1048576):
        assert truth.sbbf_num_bytes(Fs(n), "k", 0) == n
    assert truth.nonpow2([32, 64]) is False and truth.nonpow2([32, 96]) is True


def test_cell_of_names_the_cell_and_the_pruned_layer():
    assert truth.cell_of("4401/1/logs/dt=2026-01-01/hour=00/a.parquet", "raw") == "4401/1/logs/raw"
    assert truth.cell_of("prune/1/traces/dt=2026-01-01/hour=00/a.parquet", "compacted") == "prune/1/traces/pruned"


def test_assert_bloom_accepts_96_and_reports_a_closed_gap(tmp_path, capsys):
    cells = {"4402/3/logs/raw": {"trace_id": [96]}, "4402/3/traces/raw": {"trace_id": [96]},
             "4402/3/logs/compacted": {"trace_id": [160]}, "4402/3/traces/compacted": {"trace_id": [160]}}
    p = tmp_path / "s.json"
    p.write_text(json.dumps({"nonpow2_total": 4, "cells": cells}))
    truth.assert_bloom(str(p))
    p.write_text(json.dumps({"nonpow2_total": 4, "cells": dict(cells, **{"4402/3/logs/raw": {"trace_id": [128]}})}))
    with pytest.raises(SystemExit):
        truth.assert_bloom(str(p))
    p.write_text(json.dumps({"nonpow2_total": 0, "cells": {}}))
    truth.assert_bloom(str(p))
    assert "gap entry must be removed" in capsys.readouterr().out


MUTANT_FILES = ["internal/schema/row.go", "internal/storage/parquets3/writer.go", "lakehouse-traces/internal/storage/parquets3/writer.go",
                "internal/storage/parquets3/buffer_flusher.go", "tests/readers/run.sh", "tests/readers/lib.py"]


@pytest.mark.parametrize("mutant", sorted(mutants.MUTANTS))
def test_every_mutant_still_finds_its_anchor_and_changes_the_code(mutant, tmp_path):
    for f in MUTANT_FILES:
        dst = tmp_path / f
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(os.path.join(REPO, f), dst)
    before = {f: (tmp_path / f).read_text() for f in MUTANT_FILES}
    mutants.MUTANTS[mutant](str(tmp_path))
    changed = [f for f in MUTANT_FILES if (tmp_path / f).read_text() != before[f]]
    assert changed, "mutant %s changed nothing" % mutant


def test_mutation_proof_lists_the_same_mutants():
    text = lib.read_text(os.path.join(HERE, "mutation-proof.sh"))
    for m in mutants.MUTANTS:
        assert "  %s " % m in text or " %s)" % m in text or "%s " % m in text, m
    assert "FIXTURE_CONTINUE" in text and "git -C \"$ROOT\" archive HEAD" in text


def test_datagen_flags_exist():
    src = lib.read_text(os.path.join(REPO, "cmd", "datagen", "main.go"))
    for flag in ('"seed"', '"manifest"', '"manifest-name"', '"now"'):
        assert flag in src


C0 = {"logs": {"runs_total": 5.0, "files_input_total": 40.0, "files_output_total": 8.0},
      "traces": {"runs_total": 4.0, "files_input_total": 30.0, "files_output_total": 6.0}}


def test_storage_health_accepts_a_second_scan_that_does_nothing():
    before = {"objects": ["a", "b"], "counters": C0}
    assert fixture.health_violations(before, {"objects": ["a", "b"], "counters": C0}, 0, 0) == []


def test_storage_health_catches_a_planner_that_keeps_rewriting():
    """The #343 shape: a later scan compacts again (it logs, its counters move, the objects change)."""
    before = {"objects": ["a", "b"], "counters": C0}
    moved = {"logs": dict(C0["logs"], runs_total=7.0, files_input_total=48.0), "traces": C0["traces"]}
    now = {"objects": ["a", "c"], "counters": moved}
    v = fixture.health_violations(before, now, 1, 0)
    assert any("logs: 1 scans after the compaction converged reported compactions" in x for x in v)
    assert any("runs_total moved" in x for x in v) and any("files_input_total moved" in x for x in v)
    assert any("1 added, 1 removed" in x for x in v)
    assert fixture.health_violations(before, {"objects": ["a", "b"], "counters": C0}, 0, 1)


def test_buffer_state_reads_the_insert_buffer_metrics(monkeypatch):
    text = ("lakehouse_buffer_pending_rows 12\n"
            'lakehouse_buffer_segments{state="active"} 1\n'
            'lakehouse_buffer_segments{state="pending"} 0\n'
            'lakehouse_buffer_segments{state="committed"} 3\n')

    class R:
        def read(self):
            return text.encode()

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", lambda url, timeout=0: R())
    rows, segs = fixture.buffer_state("http://x")
    assert rows == 12 and segs == {"active": 1.0, "pending": 0.0, "committed": 3.0}


def test_buffer_drained_needs_no_pending_rows_and_no_open_segment(monkeypatch):
    states = {"logs": (0.0, {"active": 0.0, "pending": 0.0, "committed": 2.0}), "traces": (0.0, {"active": 0.0, "pending": 0.0})}
    monkeypatch.setattr(fixture, "buffer_state", lambda base: states["logs"] if base == lib.LOGS_URL else states["traces"])
    assert fixture.buffer_drained() == []
    states["traces"] = (5.0, {"active": 0.0, "pending": 0.0})
    assert fixture.buffer_drained()
    states["traces"] = (0.0, {"active": 1.0})
    assert fixture.buffer_drained() == []   # the open segment is always there; its rows show in pending_rows
    states["traces"] = (0.0, {"active": 0.0, "pending": 1.0})
    assert fixture.buffer_drained()


def test_every_fixture_step_is_defined():
    """The dispatcher of fixture.py names its steps; a step whose helper went missing must fail here, not
    twenty minutes into a fixture run."""
    import ast
    src = lib.read_text(os.path.join(HERE, "fixture.py"))
    tree = ast.parse(src)
    defined = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
    imported = {a.asname or a.name for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom)) for a in n.names}
    local = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)}
    called = {n.func.id for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    builtins_ = set(dir(__builtins__)) if not isinstance(__builtins__, dict) else set(__builtins__)
    missing = called - defined - imported - local - builtins_
    assert not missing, sorted(missing)
