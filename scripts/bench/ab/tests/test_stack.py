"""stack.py: ports, windows, the seed plan, compose wiring. (The docker-driving commands are exercised by the sample run.)"""
import datetime as dt
import os
import re

import abreg
import stack
from scripts.proof import stack as proof

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
    assert not set(extra.values()) & set(proof.make_ports("x", {"PROOF_PORT_BASE": "45600"}).values())


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
    text = open(proof.COMPOSE, encoding="utf-8").read()
    assert text.count("${LH_S3_ENDPOINT:-http://s3:9000}") == 4
    assert text.count("${LH_MANIFEST_REFRESH:-5s}") == 4 and text.count("${LH_COMPACTION_INTERVAL:-2m}") == 4


def test_overlay_adds_the_proxy_and_makes_every_lakehouse_wait_for_it():
    yaml = abreg.need_yaml()
    doc = yaml.safe_load(open(stack.OVERLAY, encoding="utf-8"))
    svc = doc["services"]
    assert "s3-latency" in svc and svc["s3-latency"]["healthcheck"]["test"][0] == "CMD"
    for name in ("lh-logs-base", "lh-traces-base", "lh-logs-pr", "lh-traces-pr"):
        assert svc[name]["depends_on"]["s3-latency"]["condition"] == "service_healthy"
    proof_services = yaml.safe_load(open(proof.COMPOSE, encoding="utf-8"))["services"]
    assert set(stack.SERVICES) <= set(proof_services) | {"s3-latency"}
    assert set(svc) <= set(proof_services) | {"s3-latency"}


def test_images_removed_on_down_are_only_this_projects():
    tags = proof.image_tags()
    assert all(t.startswith(f"{stack.PROJECT}-") for t in tags) and f"{stack.PROJECT}-logs:base" in tags and f"{stack.PROJECT}-datagen:1" in tags


def test_pinned_images_come_from_the_e2e_compose_file():
    assert stack.toxiproxy_image().startswith("ghcr.io/shopify/toxiproxy:")
    assert "@sha256:" in stack.e2e_pin(r"image:\s*(rustfs/rustfs:\S+)")
