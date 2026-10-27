"""runner_fit.py: the parsers of what a runner prints, the phase timeline, the fit verdict and the seed lever.

Plain unittest (runs under pytest too), so the pr-proof workflow can run it first with the standard library only.
"""
import json
import os
import sys
import tempfile
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import runner_fit as rf  # noqa: E402

FREE = """               total        used        free      shared  buff/cache   available
Mem:           15990        6200        5100          40        4690        9500
Swap:           4095         120        3975
"""
DF = '{"Type":"Images","Size":"4.5GB","Reclaimable":"1GB (22%)"}\n{"Type":"Containers","Size":"12.3MB","Reclaimable":"0B"}\n' \
     '{"Type":"Local Volumes","Size":"800MB","Reclaimable":"0B"}\n{"Type":"Build Cache","Size":"2.1GB","Reclaimable":"2.1GB"}\n'
STATS = '{"Name":"lh-ab-lh-logs-pr-1","MemUsage":"512MiB / 15.62GiB","CPUPerc":"12.5%"}\n{"Name":"lh-ab-vl-ref-1","MemUsage":"1.5GiB / 15.62GiB","CPUPerc":"0.00%"}\n'


def sample(t, used, avail, free_disk, cpu, containers=None):
    return {"t": t, "mem": {"total": 16000, "used": used, "free": 1, "buff_cache": 1, "available": avail, "swap_total": 0, "swap_used": 0},
            "cpu_stat": cpu, "disk": {"/var/lib/docker": {"total_mb": 70000, "used_mb": 70000 - free_disk, "free_mb": free_disk}},
            "docker_df": {"images": 4 * 1024 ** 3}, "containers": containers or {}, "load1": 3.0}


class Parsers(unittest.TestCase):
    def test_size_units_decimal_and_binary(self):
        self.assertEqual(rf.parse_size("512B"), 512)
        self.assertEqual(rf.parse_size("1.5kB"), 1500)
        self.assertEqual(rf.parse_size("2.1GB"), 2.1e9)
        self.assertEqual(rf.parse_size("512MiB"), 512 * 1024 ** 2)
        self.assertEqual(rf.parse_size("2KiB"), 2048)
        self.assertEqual(rf.parse_size("1.5GiB"), 1.5 * 1024 ** 3)
        self.assertIsNone(rf.parse_size("--"))
        self.assertIsNone(rf.parse_size(""))

    def test_free(self):
        m = rf.parse_free(FREE)
        self.assertEqual((m["total"], m["used"], m["available"], m["buff_cache"], m["swap_used"]), (15990, 6200, 9500, 4690, 120))
        self.assertIsNone(rf.parse_free("garbage"))

    def test_docker_df(self):
        d = rf.parse_docker_df(DF)
        self.assertEqual(set(d), {"images", "containers", "volumes", "build_cache"})
        self.assertEqual(d["images"], 4_500_000_000)

    def test_stats(self):
        s = rf.parse_stats(STATS)
        self.assertEqual(s["lh-ab-lh-logs-pr-1"], {"mem_mb": 512.0, "cpu": 12.5})
        self.assertEqual(s["lh-ab-vl-ref-1"]["mem_mb"], 1536.0)

    def test_cpu_stat(self):
        self.assertEqual(rf.parse_cpu_stat("cpu  10 0 5 80 5 0 0 0 0 0\ncpu0 1 1 1 1"), (85, 100))
        self.assertIsNone(rf.parse_cpu_stat("nothing"))


class Samples(unittest.TestCase):
    def test_cpu_series_is_busy_percent_between_reads(self):
        s = [sample(0, 1, 1, 1, (100, 200)), sample(10, 1, 1, 1, (120, 300)), sample(20, 1, 1, 1, (120, 400))]
        self.assertEqual(rf.cpu_series(s), [80.0, 100.0])

    def test_summary_peaks_and_minima(self):
        s = [sample(0, 4000, 11000, 30000, (0, 100), {"a": {"mem_mb": 100.0, "cpu": 1.0}}),
             sample(10, 9000, 6000, 20000, (50, 200), {"a": {"mem_mb": 300.0, "cpu": 50.0}, "b": {"mem_mb": 200.0, "cpu": None}}),
             sample(20, 5000, 9000, 25000, (60, 300))]
        r = rf.summarize_samples(s)
        self.assertEqual((r["mem_used_max_mb"], r["mem_available_min_mb"], r["interval_s"]), (9000, 6000, 10.0))
        self.assertEqual(r["disk"]["/var/lib/docker"]["free_min_mb"], 20000)
        self.assertEqual(r["containers"]["a"]["mem_max_mb"], 300.0)
        self.assertEqual(r["containers_mem_sum_max_mb"], 500.0)
        self.assertEqual(r["docker_df_max_mb"]["images"], 4096)

    def test_empty_samples_do_not_raise(self):
        self.assertEqual(rf.summarize_samples([])["n"], 0)


class Verdict(unittest.TestCase):
    OK = [{"name": "x", "ok": True}]

    def summary(self, avail=9000, free=20000):
        return rf.summarize_samples([sample(0, 4000, avail, free, (0, 100)), sample(10, 5000, avail, free, (10, 200))])

    def test_fits_with_headroom(self):
        v = rf.fit_verdict(self.summary(), {"containers": {"c": {"oom_killed": False}}, "kernel_oom_lines": []}, self.OK, "")
        self.assertTrue(v["fits"], v)

    def test_no_samples_proves_nothing(self):
        v = rf.fit_verdict(rf.summarize_samples([]), None, self.OK, "")
        self.assertFalse(v["fits"])
        self.assertIn("no memory samples", v["reasons"][0])

    def test_low_available_memory_fails(self):
        v = rf.fit_verdict(self.summary(avail=500), None, self.OK, "")
        self.assertFalse(v["fits"])
        self.assertIn("MemAvailable", v["reasons"][0])

    def test_oom_killed_container_fails(self):
        v = rf.fit_verdict(self.summary(), {"containers": {"lh-ab-lh-logs-pr-1": {"oom_killed": True}}, "kernel_oom_lines": []}, self.OK, "")
        self.assertFalse(v["fits"])
        self.assertIn("lh-ab-lh-logs-pr-1", v["reasons"][0])

    def test_kernel_oom_line_fails(self):
        self.assertFalse(rf.fit_verdict(self.summary(), {"containers": {}, "kernel_oom_lines": ["Out of memory: Killed process 1"]}, self.OK, "")["fits"])

    def test_low_disk_and_enospc_fail(self):
        self.assertFalse(rf.fit_verdict(self.summary(free=300), None, self.OK, "")["fits"])
        v = rf.fit_verdict(self.summary(), None, self.OK, "write /x: No space left on device")
        self.assertFalse(v["fits"])

    def test_a_failed_phase_is_listed_but_is_not_a_resource_verdict(self):
        v = rf.fit_verdict(self.summary(), None, [{"name": "pr-smoke", "ok": False}], "")
        self.assertTrue(v["fits"])
        self.assertEqual(v["failed_phases"], ["pr-smoke"])


class Timeline(unittest.TestCase):
    def test_pairing_and_open_phase(self):
        ev = [{"name": "a", "event": "start", "t": 10.0}, {"name": "a", "event": "end", "t": 25.5, "rc": 0, "ok": True},
              {"name": "b", "event": "start", "t": 30.0}]
        p = rf.pair_phases(ev)
        self.assertEqual((p[0]["name"], p[0]["seconds"], p[0]["ok"]), ("a", 15.5, True))
        self.assertEqual((p[1]["name"], p[1]["seconds"], p[1]["ok"]), ("b", None, False))

    LOG = [(100.0, "[ab-stack] up: project lh-ab"), (160.0, "[ab-stack] seeded 6h in 50s"), (170.0, "[ab-stack] recompact base-logs x: 200"),
           (180.0, "[ab-stack] recompact pr-traces y: 200"), (200.0, "[ab-stack] buffer layer: {}"),
           (230.0, "[pr-smoke] S3 latency 0 ms: probe measured +0 ms"), (240.0, "[pr-smoke] cell 1/3 a"), (250.0, "[pr-smoke] cell 2/3 b"),
           (260.0, "[ab-stack] after restart, unflushed rows per tenant: {}"), (270.0, "[pr-smoke] cell 3/3 c"),
           (275.0, "[pr-smoke] re-measuring 1 cell(s) the first pass moved"), (280.0, "[pr-smoke] cell 1/1 a"), (290.0, "[pr-smoke] S3 latency 50 ms: probe"),
           (330.0, "[pr-smoke] API matrix"), (400.0, "[pr-smoke] components")]

    def test_sub_phases_follow_the_log_lines(self):
        s = rf.sub_phases(self.LOG, 90.0, 410.0)
        self.assertEqual(s["up"], 10.0)
        self.assertEqual(s["seed"], 60.0)
        self.assertEqual(s["compact"], 20.0)  # up to the LAST recompact line
        self.assertEqual(s["hold"], 20.0)
        self.assertEqual(s["seed_equality"], 30.0)
        self.assertEqual(s["perf_0ms"], 60.0)  # to the first non-zero latency profile line
        self.assertEqual(s["perf_profiles"], 40.0)
        self.assertEqual(s["api_matrix"], 70.0)
        self.assertEqual(s["components_and_render"], 10.0)

    def test_derived_restart_and_confirm(self):
        s = rf.sub_phases(self.LOG, 90.0, 410.0)
        self.assertEqual(s["restart_approx"], 10.0)  # last cell line before the restart finished line
        self.assertEqual(s["confirm_remeasure_approx"], 15.0)  # re-measuring line to the first line that is not a cell line

    def test_missing_boundaries_are_skipped(self):
        s = rf.sub_phases([(1.0, "[ab-stack] up: x"), (5.0, "[pr-smoke] API matrix")], 0.0, 9.0)
        self.assertEqual(s["up"], 1.0)
        self.assertNotIn("seed", s)
        self.assertEqual(s["perf_0ms"], 4.0)
        self.assertNotIn("perf_profiles", s)  # no extra latency profile ran: the API matrix line must not bound two sub-phases
        self.assertEqual(s["components_and_render"], 4.0)


class SeedLever(unittest.TestCase):
    def fake(self):
        return types.SimpleNamespace(CHUNKS=6, TENANTS=[("t0", "0", "0", 31, 12000, 3000), ("t1", "1", "0", 32, 3, 1)], BUFFER_TENANTS=[("t0", "0", "0", 41, 1600, 400)])

    def test_defaults_leave_the_harness_untouched(self):
        st = self.fake()
        rf.apply_seed_lever(st, 0, 1.0)
        self.assertEqual((st.CHUNKS, st.TENANTS[0][4], st.BUFFER_TENANTS[0][4]), (6, 12000, 1600))

    def test_chunks_and_scale(self):
        st = self.fake()
        rf.apply_seed_lever(st, 3, 0.5)
        self.assertEqual(st.CHUNKS, 3)
        self.assertEqual(st.TENANTS[0][4:], (6000, 1500))
        self.assertEqual(st.TENANTS[1][4:], (2, 1))  # never below one row (3*0.5=1.5 rounds to 2, 1*0.5 stays 1)
        self.assertEqual(st.BUFFER_TENANTS[0][4:], (800, 200))
        self.assertEqual(st.TENANTS[0][:4], ("t0", "0", "0", 31))  # identity and seed unchanged


class RunWrapper(unittest.TestCase):
    def run_cmd(self, out, name, ok, cmd):
        return rf.main(["run", "--out", out, "--name", name, "--ok", ok, "--", *cmd])

    def test_records_phase_log_and_exit_status(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(self.run_cmd(d, "good", "", [sys.executable, "-c", "print('hello'); print('second')"]), 0)
            self.assertEqual(self.run_cmd(d, "accepted", "1", [sys.executable, "-c", "import sys; sys.exit(1)"]), 0)
            self.assertEqual(self.run_cmd(d, "bad", "1", [sys.executable, "-c", "import sys; sys.exit(3)"]), 3)
            self.assertEqual(self.run_cmd(d, "missing", "", ["/nonexistent/binary"]), 127)
            phases = {p["name"]: p for p in rf.pair_phases(rf.read_jsonl(rf.phases_path(d)))}
            self.assertTrue(phases["good"]["ok"] and phases["accepted"]["ok"])
            self.assertFalse(phases["bad"]["ok"])
            self.assertEqual((phases["bad"]["rc"], phases["missing"]["rc"]), (3, 127))
            lines = rf.read_timed_log(os.path.join(d, "logs", "good.log"))
            self.assertEqual([t for _, t in lines], ["hello", "second"])

    def test_mark_events_pair_like_run_events(self):
        with tempfile.TemporaryDirectory() as d:
            rf.main(["mark", "--out", d, "--name", "build", "start"])
            rf.main(["mark", "--out", d, "--name", "build", "end"])
            self.assertEqual(len(rf.pair_phases(rf.read_jsonl(rf.phases_path(d)))), 1)


class OptionalInputs(unittest.TestCase):
    def test_a_half_written_line_is_skipped_and_the_rest_read(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "s.jsonl")
            with open(path, "w", encoding="utf-8") as f:
                f.write('{"a": 1}\n{"b": \n{"c": 3}\n')
            self.assertEqual(rf.read_jsonl(path), [{"a": 1}, {"c": 3}])
        self.assertEqual(rf.read_jsonl("/nonexistent/file.jsonl"), [])

    def test_missing_or_broken_json_is_none(self):
        with tempfile.TemporaryDirectory() as d:
            bad = os.path.join(d, "bad.json")
            with open(bad, "w", encoding="utf-8") as f:
                f.write("{not json")
            good = os.path.join(d, "good.json")
            with open(good, "w", encoding="utf-8") as f:
                f.write('{"exit": 1}')
            self.assertIsNone(rf.load_json_or_none(bad))
            self.assertIsNone(rf.load_json_or_none(os.path.join(d, "absent.json")))
            self.assertEqual(rf.load_json_or_none(good), {"exit": 1})
            self.assertEqual(rf.pr_smoke_facts(d), {})  # no meta, verdicts or summary: nothing to report, no error
            os.makedirs(os.path.join(d, "x"))
            with open(os.path.join(d, "x", "perf-verdict.json"), "w", encoding="utf-8") as f:
                f.write('{"exit": 2}')
            self.assertEqual(rf.pr_smoke_facts(os.path.join(d, "x")), {"perf_exit": 2})

    def test_a_report_with_no_inputs_at_all_still_renders(self):
        with tempfile.TemporaryDirectory() as d:
            r = rf.build_report(d, {"variant": "v"}, "")
            self.assertFalse(r["fit"]["fits"])  # nothing sampled proves nothing
            self.assertIn("DOES NOT FIT", rf.render_markdown(r))


class PerCell(unittest.TestCase):
    def test_seconds_per_cell_from_the_perf_sub_phase(self):
        with tempfile.TemporaryDirectory() as d:
            rf.append_jsonl(rf.phases_path(d), {"name": "pr-smoke", "event": "start", "t": 90.0})
            rf.append_jsonl(rf.phases_path(d), {"name": "pr-smoke", "event": "end", "t": 410.0, "rc": 0, "ok": True})
            os.makedirs(os.path.join(d, "logs"))
            with open(os.path.join(d, "logs", "pr-smoke.log"), "w", encoding="utf-8") as f:
                f.write("".join(f"{t}\t{text}\n" for t, text in Timeline.LOG))
            smoke = os.path.join(d, "pr-smoke")
            os.makedirs(smoke)
            with open(os.path.join(smoke, "summary-0ms.json"), "w", encoding="utf-8") as f:
                json.dump({"cells": [{"verdict": "same"}] * 4}, f)
            r = rf.build_report(d, {}, smoke)
            self.assertEqual(r["pr_smoke_sub_phases"]["perf_0ms_s_per_cell"], 15.0)  # 60 s over 4 cells


class Report(unittest.TestCase):
    def test_report_from_a_directory_of_measurements(self):
        with tempfile.TemporaryDirectory() as d:
            for i, (used, avail) in enumerate([(3000, 12000), (9500, 5000)]):
                rf.append_jsonl(os.path.join(d, "samples.jsonl"), sample(i * 10, used, avail, 20000, (i * 50, 100 * (i + 1))))
            rf.append_jsonl(rf.phases_path(d), {"name": "build", "event": "start", "t": 0.0})
            rf.append_jsonl(rf.phases_path(d), {"name": "build", "event": "end", "t": 120.0, "rc": 0, "ok": True})
            rf.append_jsonl(rf.phases_path(d), {"name": "pr-smoke", "event": "start", "t": 120.0})
            rf.append_jsonl(rf.phases_path(d), {"name": "pr-smoke", "event": "end", "t": 500.0, "rc": 0, "ok": True})
            smoke = os.path.join(d, "pr-smoke")
            os.makedirs(smoke)
            with open(os.path.join(smoke, "summary-0ms.json"), "w", encoding="utf-8") as f:
                json.dump({"cells": [{"confirmed": True, "verdict": "same"}, {"verdict": "same"}, {"verdict": "slower"}], "verdicts": {"same": 2}}, f)
            r = rf.build_report(d, {"variant": "v"}, smoke)
            self.assertEqual(r["wall_s"], 500.0)
            self.assertEqual(r["core_proof_s"], 500.0)
            self.assertEqual((r["pr_smoke"]["moved_first_pass"], r["pr_smoke"]["moved_after_confirm"]), (1, 1))
            self.assertTrue(r["fit"]["fits"])
            self.assertNotIn("perf_0ms_s_per_cell", r["pr_smoke_sub_phases"])  # no pr-smoke log, so no sub-phase to divide
            md = rf.render_markdown(r)
            self.assertIn("FITS", md)
            self.assertIn("| build | 120 s (2.0 min) | 0 |", md)


if __name__ == "__main__":
    unittest.main()
