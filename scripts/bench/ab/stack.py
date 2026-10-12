#!/usr/bin/env python3
"""The A/B stack of the PR perf smoke: scripts/proof's stack plus a verified S3 latency proxy.

Nothing here is a second stack. The compose file, the images, the tenants, the datagen seeding, the buffer
probes and the runner are scripts/proof's (deployment/docker/docker-compose-proof.yml, scripts/proof/stack.py,
scripts/proof/runner); this module adds what a performance measurement needs on top:

  * its own compose project (`lh-ab`) and port block, derived by scripts/proof from the project name and
    checked free before anything starts;
  * toxiproxy between every Lakehouse and RustFS (docker-compose.ab.yml) and latency injection that is
    verified (latency.py): a probe fails the run when the measured delay is off;
  * a seed sized for timing (several chunks per hour, so partitions hold more than one file), a forced
    compaction of the older hours (the `compacted` layer), the `buffer` and `after-restart` layers;
  * builds of the base and PR Lakehouse images from detached trees, removed again without --force.

  stack.py ports                          the ports of this project and whether they are free
  stack.py build  --base DIR --pr DIR     images lh-ab-<signal>:base / :pr, datagen, hot VL and VT
  stack.py up                             start the services (flush 5 s: the seeding mode)
  stack.py seed   --out DIR               seed hot + base, then the PR; writes DIR/state.json
  stack.py compact --out DIR              force-compact the older hours on base and PR
  stack.py hold   --out DIR               measurement mode: 1 h flush, no background S3 traffic; ingest the buffer window
  stack.py restart --out DIR              docker restart of the four Lakehouse containers (after-restart layer)
  stack.py latency set MS | clear | probe MS      verified S3 latency
  stack.py down                           compose down -v and this project's images, by exact tag
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import abreg  # noqa: E402

PROJECT = os.environ.get("AB_PROJECT", "lh-ab")
os.environ["PROOF_PROJECT"] = PROJECT  # scripts.proof.stack reads it at import: images, network and ports follow
from scripts.proof import stack as P  # noqa: E402
from scripts.proof.jsonio import dump_json, load_json  # noqa: E402

OVERLAY = os.path.join(HERE, "docker-compose.ab.yml")
E2E_COMPOSE = os.path.join(abreg.ROOT, "deployment", "docker", "docker-compose-e2e.yml")
SERVICES = ["s3", "s3-init", "s3-latency", "vl-ref", "vt-ref", "lh-logs-base", "lh-traces-base", "lh-logs-pr", "lh-traces-pr"]
LH_CONTAINER = {"base-logs": "lh-logs-base", "base-traces": "lh-traces-base", "pr-logs": "lh-logs-pr", "pr-traces": "lh-traces-pr"}
# Ports of this overlay, past the last port scripts.proof allots (452).
EXTRA_OFFSETS = {"ab-s3-direct": 460, "ab-s3-proxied": 461, "ab-toxi-api": 462}

# Seed sized for timing. (name, account, project, seed, logs, traces) per tenant; the numbers are per chunk.
HOURS_BACK = 6
CHUNKS = 6
CHUNK_PAUSE_S = 7  # longer than the 5 s flush of the seeding policy, so a chunk closes its own buffer segment
TENANTS = [("t0", "0", "0", 31, 12000, 3000), ("t1001", "1001", "0", 32, 3000, 750)]
BUFFER_TENANTS = [("t0", "0", "0", 41, 1600, 400), ("t1001", "1001", "0", 42, 400, 100)]
COMPACTED_HOURS = 4  # the oldest hours of the cold window that are force-compacted; the newest ones stay as flushed
COLD_HOURS = HOURS_BACK - COMPACTED_HOURS
PEER_KEY = P.PEER_KEY

# While a layer is measured nothing may touch S3 in the background: the manifest is read once at startup and
# the compaction scheduler is parked, so a per-query S3 delta is the query's. While seeding, the manifest
# refreshes quickly so new files are seen.
PHASES = {
    "seed": {"LH_POLICY": "./proof/policy-flush.yml", "LH_MANIFEST_REFRESH": "5s", "LH_COMPACTION_INTERVAL": "24h"},
    "measure": {"LH_POLICY": "./proof/policy-hold.yml", "LH_MANIFEST_REFRESH": "1h", "LH_COMPACTION_INTERVAL": "24h"},
}
_phase = {"name": "seed"}


def log(msg: str) -> None:
    print(f"[ab-stack] {msg}", file=sys.stderr, flush=True)


# ------------------------------------------------------------------------------------------------ pins and ports
def e2e_pin(pattern: str) -> str:
    """An image reference pinned in the e2e compose file (the one place that pins it)."""
    with open(E2E_COMPOSE, encoding="utf-8") as f:
        m = re.search(pattern, f.read())
    if not m:
        raise RuntimeError(f"{E2E_COMPOSE} no longer pins {pattern!r}")
    return m.group(1)


def toxiproxy_image() -> str:
    return e2e_pin(r"image:\s*(ghcr\.io/shopify/toxiproxy:\S+)")


def extra_ports(base: int | None = None) -> dict[str, int]:
    base = P.port_base() if base is None else base
    return {k: base + off for k, off in EXTRA_OFFSETS.items()}


def published_ports() -> dict[str, int]:
    """Every host port this stack publishes (the proof stack's used services plus the overlay's)."""
    used = {k: v for k, v in P.PORTS.items() if k in ("ref-logs", "ref-traces", "base-logs", "base-traces", "pr-logs", "pr-traces")}
    return {**used, **extra_ports()}


def api_url() -> str:
    return f"http://127.0.0.1:{extra_ports()['ab-toxi-api']}"


def phase_env(name: str) -> dict[str, str]:
    return dict(PHASES[name])


def compose_env() -> dict:
    env = P.compose_env()
    for k, v in extra_ports().items():
        env["PORT_" + k.upper().replace("-", "_")] = str(v)
    env.update(AB_TOXIPROXY_IMAGE=toxiproxy_image(), LH_S3_ENDPOINT="http://s3-latency:9000", **phase_env(_phase["name"]))
    return env


def compose_files() -> list[str]:
    out = []
    for f in (P.COMPOSE, OVERLAY):
        out += ["-f", f]
    return out


def sh(cmd, **kw):  # pragma: no cover - runs docker
    return subprocess.run(cmd, check=True, text=True, **kw)


def compose(*args, env=None, **kw):  # pragma: no cover - runs docker
    e = dict(os.environ, **compose_env(), **(env or {}))
    return sh(["docker", "compose", "-p", PROJECT, *compose_files(), *args], env=e, **kw)


def set_phase(name: str) -> None:
    if name not in PHASES:
        raise ValueError(f"unknown phase {name!r}")
    _phase["name"] = name


# ------------------------------------------------------------------------------------------------ windows
def make_state(now: dt.datetime | None = None) -> dict:
    """scripts.proof's state with the cold window widened to HOURS_BACK and the layers of the perf smoke.

    Layers (absolute, hour aligned; H is the end of the cold window):
      cold          [H-2h, H)        flushed, not compacted
      compacted     [H-6h, H-2h)     the older hours, force-compacted
      buffer        [H, H+1h)        rows ingested after the flush, held in the insert buffer
      after-restart [H, H+1h)        the same rows after the Lakehouse containers were restarted
    """
    st = P.make_state(now)
    h = dt.datetime.strptime(st["H"], "%Y-%m-%dT%H:%M:%SZ")
    f = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    st["cold"] = {"start": f(h - dt.timedelta(hours=HOURS_BACK)), "end": f(h)}
    st["layers"] = {
        "cold": {"start": f(h - dt.timedelta(hours=COLD_HOURS)), "end": f(h)},
        "compacted": {"start": f(h - dt.timedelta(hours=HOURS_BACK)), "end": f(h - dt.timedelta(hours=COLD_HOURS))},
        "buffer": dict(st["buffer"]),
        "after-restart": dict(st["buffer"]),
    }
    st["project"] = PROJECT
    st["ports"] = {**st["ports"], **extra_ports()}
    st["ab"] = {"hours_back": HOURS_BACK, "chunks": CHUNKS, "tenants": [list(t) for t in TENANTS],
                "buffer_tenants": [list(t) for t in BUFFER_TENANTS]}
    return st



def seed_plan(state: dict) -> list[dict]:
    """The datagen invocations of the cold window: per tenant CHUNKS chunks, each a distinct seed, each sent once to
    hot + base and once to the PR with the same seed and --now, so all three hold the same rows."""
    plan = []
    for i in range(CHUNKS):
        for name, acct, proj, seed, logs, traces in TENANTS:
            plan.append({"chunk": i, "tenant": (name, acct, proj, seed * 100 + i, logs, traces), "hours_back": HOURS_BACK, "now": state["H"]})
    return plan


# ------------------------------------------------------------------------------------------------ build
def tree_add(ref: str, parent: str, name: str) -> str:  # pragma: no cover - runs git
    """A detached worktree of `ref` under `parent` (the caller removes it with tree_remove)."""
    path = os.path.join(parent, name)
    sh(["git", "worktree", "add", "--detach", path, ref], cwd=abreg.ROOT, stdout=subprocess.DEVNULL)
    return path


def tree_remove(path: str) -> bool:  # pragma: no cover - runs git
    """Remove a worktree this run created. Never --force: a tree that is not clean stays and is reported."""
    r = subprocess.run(["git", "worktree", "remove", path], cwd=abreg.ROOT, capture_output=True, text=True)
    if r.returncode != 0:
        log(f"worktree {path} was not removed ({r.stderr.strip()[:160]}); remove it by hand")
    return r.returncode == 0


def cmd_build(a):  # pragma: no cover - drives docker
    for d, tag in ((a.base, "base"), (a.pr, "pr")):
        if not d:
            continue
        for sig in ("logs", "traces"):
            sh(["docker", "build", "-q", "-f", os.path.join(d, f"Dockerfile.{sig}"), "-t", f"{PROJECT}-{sig}:{tag}", d],
               env=dict(os.environ, GOWORK="off"))
    sh(["docker", "build", "-q", "-f", os.path.join(a.base, "Dockerfile.datagen"), "-t", P.DATAGEN, a.base])
    compose("build", "-q", "vl-ref", "vt-ref")


# ------------------------------------------------------------------------------------------------ lifecycle
def cmd_ports(_a):
    ports = published_ports()
    busy = P.busy_ports(ports.values())
    print(json.dumps({"project": PROJECT, "base": P.port_base(), "ports": ports, "busy": busy}, indent=1))
    return 1 if busy else 0


def running() -> bool:  # pragma: no cover - runs docker
    out = subprocess.run(["docker", "compose", "-p", PROJECT, *compose_files(), "ps", "-q"], capture_output=True, text=True,
                         env=dict(os.environ, **compose_env())).stdout.strip()
    return bool(out)


def cmd_up(_a):  # pragma: no cover - drives docker
    if not running():
        busy = P.busy_ports(published_ports().values())
        if busy:
            raise SystemExit(f"ports already in use on 127.0.0.1: {busy}; set PROOF_PORT_BASE or another AB_PROJECT")
    set_phase("seed")
    compose("up", "-d", "--wait", "--wait-timeout", "240", *SERVICES)
    # the proxy must be a pass-through before anything is measured or seeded
    sys.path.insert(0, HERE)
    import latency  # noqa: PLC0415
    latency.clear(api_url())
    log(f"up: project {PROJECT}, ports {published_ports()}")


def hot_and_base(state: dict, item: dict, with_pr: bool) -> None:  # pragma: no cover - drives docker
    cmd = P.datagen_cmd(item["now"], item["hours_back"], item["tenant"], hot=not with_pr, lh="pr" if with_pr else "base")
    sh(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def cmd_seed(a):  # pragma: no cover - drives docker
    state = make_state()
    os.makedirs(a.out, exist_ok=True)
    t0 = time.time()
    last_chunk = None
    for item in seed_plan(state):
        if last_chunk is not None and item["chunk"] != last_chunk:
            time.sleep(CHUNK_PAUSE_S)
        last_chunk = item["chunk"]
        hot_and_base(state, item, with_pr=False)
        hot_and_base(state, item, with_pr=True)
    P.wait_drained(state)
    state["ab"]["seed_seconds"] = round(time.time() - t0, 1)
    dump_json(os.path.join(a.out, "state.json"), state, indent=1)
    log(f"seeded {HOURS_BACK}h in {state['ab']['seed_seconds']}s ({CHUNKS} chunks per tenant)")


def hours_in(window: dict) -> list[dt.datetime]:
    t = dt.datetime.strptime(window["start"], "%Y-%m-%dT%H:%M:%SZ")
    end = dt.datetime.strptime(window["end"], "%Y-%m-%dT%H:%M:%SZ")
    out = []
    while t < end:
        out.append(t)
        t += dt.timedelta(hours=1)
    return out


def partition_of(hour: dt.datetime) -> str:
    return f"dt={hour:%Y-%m-%d}/hour={hour:%H}"


def post_json(url: str, body: dict, headers: dict | None = None, timeout: float = 330.0) -> tuple[int, str]:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST", headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 - loopback test stack
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


def cmd_compact(a):  # pragma: no cover - drives docker
    state = load_json(os.path.join(a.out, "state.json"))
    results = {}
    for key in ("base-logs", "base-traces", "pr-logs", "pr-traces"):
        port = state["ports"][key]
        for hour in hours_in(state["layers"]["compacted"]):
            part = partition_of(hour)
            t0 = time.time()
            status, body = post_json(f"http://127.0.0.1:{port}/lakehouse/compaction/recompact", {"partition": part})
            results.setdefault(key, {})[part] = {"status": status, "body": body[:400], "seconds": round(time.time() - t0, 3)}
            log(f"recompact {key} {part}: {status} {body[:100]}")
    state["ab"]["recompact"] = results
    dump_json(os.path.join(a.out, "state.json"), state, indent=1)


def buffer_counts(state: dict) -> dict:  # pragma: no cover - needs the stack
    """Unflushed rows per Lakehouse and tenant (the key-order tenant of the proof stack is not seeded here)."""
    return {k: v for k, v in P.read_buffered(state).items() if not k.endswith(f"-{P.KEYORDER_TENANT[1]}")}


def settled_buffer(state: dict, timeout: float = 90, step: float = 3) -> dict:  # pragma: no cover - needs the stack
    """The buffer counts once ingest is visible: two equal consecutive reads, all of them non-zero."""
    deadline, last = time.time() + timeout, None
    while True:
        cur = buffer_counts(state)
        if cur == last and all(cur.values()):
            return cur
        if time.time() > deadline:
            raise SystemExit(f"buffer counts did not settle: {cur}")
        last = cur
        time.sleep(step)


def cmd_hold(a):  # pragma: no cover - drives docker
    """Measurement mode: recreate the Lakehouse containers with a 1 h flush and no background S3 work, then ingest the buffer window."""
    path = os.path.join(a.out, "state.json")
    state = load_json(path)
    set_phase("measure")
    compose("up", "-d", "--wait", "--wait-timeout", "240", *SERVICES)
    h = dt.datetime.strptime(state["H"], "%Y-%m-%dT%H:%M:%SZ")
    now = (h + dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for t in BUFFER_TENANTS:
        sh(P.datagen_cmd(now, 1, t, hot=True, lh="base"), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        sh(P.datagen_cmd(now, 1, t, hot=False, lh="pr"), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    state["buffered"] = settled_buffer(state)
    dump_json(path, state, indent=1)
    log(f"buffer layer: {state['buffered']}")


def cmd_restart(a):  # pragma: no cover - drives docker
    """docker restart keeps the containers' filesystems, so the durable insert buffer is what comes back."""
    path = os.path.join(a.out, "state.json")
    state = load_json(path)
    names = [subprocess.run(["docker", "compose", "-p", PROJECT, *compose_files(), "ps", "-q", svc], capture_output=True, text=True,
                            env=dict(os.environ, **compose_env())).stdout.strip() for svc in LH_CONTAINER.values()]
    sh(["docker", "restart", *[n for n in names if n]], stdout=subprocess.DEVNULL)
    deadline = time.time() + 180
    for key in LH_CONTAINER:
        while True:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{state['ports'][key]}/health", timeout=3) as r:  # noqa: S310
                    if r.status == 200:
                        break
            except (urllib.error.URLError, OSError):
                pass  # still restarting: retry until the deadline below
            if time.time() > deadline:
                raise SystemExit(f"{key} did not become healthy after the restart")
            time.sleep(2)
    state["buffered_after_restart"] = buffer_counts(state)
    dump_json(path, state, indent=1)
    log(f"after restart, unflushed rows per tenant: {state['buffered_after_restart']}")


def cmd_latency(a):  # pragma: no cover - drives docker
    import latency  # noqa: PLC0415
    ports = extra_ports()
    if a.action == "clear":
        latency.clear(api_url())
        print(json.dumps({"injected_ms": 0}))
        return 0
    ms = float(a.ms)
    if a.action == "set":
        print(json.dumps({"injected_ms": latency.set_latency(api_url(), ms)}))
        return 0
    latency.set_latency(api_url(), ms)
    res = latency.probe(f"http://127.0.0.1:{ports['ab-s3-direct']}/health", f"http://127.0.0.1:{ports['ab-s3-proxied']}/health", ms)
    print(json.dumps(res))
    return 0 if res["ok"] else 1


def cmd_down(_a):  # pragma: no cover - drives docker
    compose("down", "-v", "--remove-orphans")
    for img in P.image_tags():  # exact tags this project builds, nothing else (the pulled RustFS/toxiproxy/mc images are shared)
        subprocess.run(["docker", "rmi", img], check=False)


def main(argv=None):  # pragma: no cover - drives docker
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--base", required=True)
    b.add_argument("--pr", required=True)
    for name in ("seed", "compact", "hold", "restart"):
        sub.add_parser(name).add_argument("--out", required=True)
    for name in ("ports", "up", "down"):
        sub.add_parser(name)
    lat = sub.add_parser("latency")
    lat.add_argument("action", choices=("set", "clear", "probe"))
    lat.add_argument("ms", nargs="?", default=0)
    a = ap.parse_args(argv)
    rc = globals()["cmd_" + a.cmd](a)
    return rc or 0


if __name__ == "__main__":
    sys.exit(main())
