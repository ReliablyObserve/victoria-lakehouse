"""stack.py: ports, windows, the seed plan, compose wiring. (The docker-driving commands are exercised by the sample run.)"""
import datetime as dt
import os
import re

import abreg
import stack
from scripts.proof import stack as proof

def read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


NOW = dt.datetime(2026, 10, 12, 15, 37, tzinfo=dt.timezone.utc)


def test_project_ports_are_distinct_free_of_other_stacks_and_below_the_ephemeral_range():
    ports = stack.published_ports()
    assert len(set(ports.values())) == len(ports)
    assert stack.PROJECT == "lh-ab" or os.environ.get("AB_PROJECT")
    for p in ports.values():
        assert not any(lo <= p <= hi for lo, hi in proof.RESERVED_PORTS), p  # 13xxx/18xxx/3002 (loki-vl-proxy) and 29xxx/39xxx/47xxx (owner stacks)
        assert p < proof.EPHEMERAL_FLOOR
        assert p not in (3002, 19428)


def test_extra_ports_follow_the_block_and_do_not_collide_with_the_proof_stack():
    extra = stack.extra_ports(45600)
    assert extra == {"ab-s3-direct": 46060, "ab-s3-proxied": 46061, "ab-toxi-api": 46062}
    own = {k: v for k, v in proof.make_ports("x", {"PROOF_PORT_BASE": "45600"}).items() if not k.startswith("ab-")}
    assert not set(extra.values()) & set(own.values())


def test_the_port_block_leaves_room_for_the_overlay_ports():
    """scripts.proof offers bases whose block holds its own offsets (452); the overlay's (460..462) are part of the span here."""
    top = max(stack.EXTRA_OFFSETS.values())
    assert top > max(proof.PORT_OFFSETS.values())
    bases = stack.allowed_bases()
    assert bases and proof.port_base() in bases
    for b in bases:
        assert b + top < proof.EPHEMERAL_FLOOR
        assert not any(b <= hi and b + top >= lo for lo, hi in proof.RESERVED_PORTS)
    stack.check_block()


def test_state_has_the_four_layers_hour_aligned_and_a_widened_cold_window():
    st = stack.make_state(NOW)
    assert st["H"] == "2026-10-12T14:00:00Z" and st["project"] == stack.PROJECT
    assert st["cold"] == {"start": "2026-10-12T08:00:00Z", "end": "2026-10-12T14:00:00Z"}
    lay = st["layers"]
    assert lay["cold"] == {"start": "2026-10-12T12:00:00Z", "end": "2026-10-12T14:00:00Z"}
    assert lay["compacted"] == {"start": "2026-10-12T08:00:00Z", "end": "2026-10-12T12:00:00Z"}
    assert lay["buffer"] == {"start": "2026-10-12T14:00:00Z", "end": "2026-10-12T15:00:00Z"} and lay["after-restart"] == lay["buffer"]
    assert lay["compacted"]["end"] == lay["cold"]["start"]  # the two flushed layers tile the cold window
    for w in lay.values():
        assert all(re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:00:00Z", w[k]) for k in w)
    assert {"ab-toxi-api", "base-logs", "pr-traces"} <= set(st["ports"])


def test_seed_plan_sends_the_same_seed_and_now_to_hot_and_base_and_to_the_pr():
    st = stack.make_state(NOW)
    plan = stack.seed_plan(st)
    assert len(plan) == stack.CHUNKS * len(stack.TENANTS)
    seeds = [p["tenant"][3] for p in plan]
    assert len(set(seeds)) == len(seeds)  # a distinct seed per chunk and tenant
    p = plan[0]
    a = proof.datagen_cmd(p["now"], p["hours_back"], p["tenant"], hot=True, lh="base")
    b = proof.datagen_cmd(p["now"], p["hours_back"], p["tenant"], hot=False, lh="pr")
    seed = f"--seed={p['tenant'][3]}"
    assert seed in a and seed in b and f"--now={st['H']}" in a and f"--now={st['H']}" in b and f"--hours-back={stack.HOURS_BACK}" in a
    assert any(x.startswith("--vl-endpoint") for x in a) and not any(x.startswith("--vl-endpoint") for x in b)
    assert stack.seed_plan(stack.make_state(NOW)) == plan  # deterministic


def test_phases_park_the_background_s3_work_while_a_layer_is_measured():
    assert stack.PHASES["seed"]["LH_MANIFEST_REFRESH"] == "5s" and stack.PHASES["measure"]["LH_MANIFEST_REFRESH"] == "1h"
    assert stack.PHASES["measure"]["LH_COMPACTION_INTERVAL"] == stack.PHASES["seed"]["LH_COMPACTION_INTERVAL"] == "24h"
    assert stack.PHASES["measure"]["LH_POLICY"].endswith("policy-hold.yml") and stack.PHASES["seed"]["LH_POLICY"].endswith("policy-flush.yml")
    stack.set_phase("measure")
    try:
        env = stack.compose_env()
        assert env["LH_MANIFEST_REFRESH"] == "1h" and env["LH_S3_ENDPOINT"] == "http://s3-latency:9000"
        assert env["AB_TOXIPROXY_IMAGE"].startswith("ghcr.io/shopify/toxiproxy:") and env["PORT_AB_TOXI_API"].isdigit()
    finally:
        stack.set_phase("seed")


def test_partitions_and_hours():
    st = stack.make_state(NOW)
    hs = stack.hours_in(st["layers"]["compacted"])
    assert [stack.partition_of(h) for h in hs] == ["dt=2026-10-12/hour=08", "dt=2026-10-12/hour=09", "dt=2026-10-12/hour=10", "dt=2026-10-12/hour=11"]


def test_every_lakehouse_service_of_the_proof_compose_file_reads_the_overlay_variables_with_the_old_values_as_defaults():
    text = read(proof.COMPOSE)
    assert text.count("${LH_S3_ENDPOINT:-http://s3:9000}") == 4
    assert text.count("${LH_MANIFEST_REFRESH:-5s}") == 4 and text.count("${LH_COMPACTION_INTERVAL:-2m}") == 4


def test_overlay_adds_the_proxy_and_makes_every_lakehouse_wait_for_it():
    yaml = abreg.need_yaml()
    doc = yaml.safe_load(read(stack.OVERLAY))
    svc = doc["services"]
    assert "s3-latency" in svc and svc["s3-latency"]["healthcheck"]["test"][0] == "CMD"
    for name in ("lh-logs-base", "lh-traces-base", "lh-logs-pr", "lh-traces-pr"):
        assert svc[name]["depends_on"]["s3-latency"]["condition"] == "service_healthy"
    proof_services = yaml.safe_load(read(proof.COMPOSE))["services"]
    assert set(stack.SERVICES) <= set(proof_services) | {"s3-latency"}
    assert set(svc) <= set(proof_services) | {"s3-latency"}


def test_images_removed_on_down_are_only_this_projects():
    tags = proof.image_tags()
    assert all(t.startswith(f"{stack.PROJECT}-") for t in tags) and f"{stack.PROJECT}-logs:base" in tags and f"{stack.PROJECT}-datagen:1" in tags


def test_pinned_images_come_from_the_e2e_compose_file():
    assert stack.toxiproxy_image().startswith("ghcr.io/shopify/toxiproxy:")
    assert "@sha256:" in stack.e2e_pin(r"image:\s*(rustfs/rustfs:\S+)")


def test_a_forced_compaction_that_merged_nothing_is_a_problem():
    ok = {"base-logs": {"dt=a/hour=08": {"status": 200, "body": '{"bytes_written":5,"fenced_files":0,"input_files":8,"output_files":2,"output_lev'}}}
    assert stack.recompact_problems(ok) == []
    bad = {"base-logs": {"h1": {"status": 400, "body": "fewer than two compactable files"},
                         "h2": {"status": 200, "body": '{"input_files":8,"output_files":8}'},
                         "h3": {"status": 200, "body": '{"input_files":8,"output_files":0}'},
                         "h4": {"status": 200, "body": "{}"}}}
    problems = stack.recompact_problems(bad)
    assert len(problems) == 4 and "HTTP 400" in problems[0]


def test_a_running_stack_of_the_project_is_refused_unless_reuse_is_asked_for(monkeypatch):
    import pytest
    monkeypatch.setattr(stack, "running", lambda: True)
    with pytest.raises(stack.StackBusy, match="already running"):
        stack.cmd_up(None)


def test_r1_two_runs_cannot_both_hold_the_project_lock(tmp_path):
    import pytest
    first = stack.ProjectLock("lh-ab-test", str(tmp_path)).acquire()
    second = stack.ProjectLock("lh-ab-test", str(tmp_path))
    with pytest.raises(stack.StackBusy, match="holds the lock"):
        second.acquire()
    first.release()
    second.acquire().release()      # free again once the first run is over
    stack.ProjectLock("another-project", str(tmp_path)).acquire().release()   # projects do not block each other


def test_r1_a_lock_file_left_by_a_dead_process_does_not_wedge_the_next_run(tmp_path):
    import subprocess
    import sys
    code = ("import sys, os; sys.path.insert(0, %r); import stack; stack.ProjectLock('lh-ab-test', %r).acquire(); os._exit(0)" % (stack.HERE, str(tmp_path)))
    assert subprocess.run([sys.executable, "-c", code], timeout=60).returncode == 0       # dies holding the lock, the file stays behind
    lock = stack.ProjectLock("lh-ab-test", str(tmp_path))
    assert os.path.exists(lock.path)
    lock.acquire().release()


def test_r1_the_run_holds_the_lock_from_the_preflight_to_the_teardown_and_a_second_run_gets_exit_4(tmp_path, monkeypatch):
    import pr_smoke
    calls = []
    real = stack.ProjectLock
    monkeypatch.setattr(pr_smoke.stack, "ProjectLock", lambda *a, **k: real("lh-ab-test", str(tmp_path)))
    monkeypatch.setattr(stack, "running", lambda: False)
    monkeypatch.setattr(stack, "check_block", lambda: None)
    monkeypatch.setattr(stack.P, "busy_ports", lambda ports: [])
    monkeypatch.setattr(pr_smoke.selection, "changed_files", lambda b, h: ["internal/x/y.go"])
    monkeypatch.setattr(pr_smoke, "git", lambda *a: "a" * 40)
    monkeypatch.setattr(stack, "cmd_build", lambda a: None)
    monkeypatch.setattr(stack, "down_stack", lambda: calls.append("down"))
    monkeypatch.setattr(stack, "remove_images", lambda: calls.append("rmi"))

    def measure_all(*a, **k):
        # while the first run works, a second one is refused (exit 4) and tears nothing down
        assert pr_smoke.main(["--out", str(tmp_path / "second"), "--base", "x", "--head", "y", "--skip-build"]) == stack.EXIT_BUSY
        assert calls == []
        raise RuntimeError("stop")
    monkeypatch.setattr(pr_smoke, "measure_all", measure_all)
    assert pr_smoke.main(["--out", str(tmp_path / "first"), "--base", "x", "--head", "y"]) == 3
    assert calls == ["down", "rmi"]
    real("lh-ab-test", str(tmp_path)).acquire().release()      # the first run released the lock at the end
