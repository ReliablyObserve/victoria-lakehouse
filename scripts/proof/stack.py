#!/usr/bin/env python3
"""Bring up, seed and tear down the proof stack (deployment/docker/docker-compose-proof.yml).

  stack.py build  --main DIR [--pr DIR]    build the Lakehouse images (lhproof-<signal>:base / :pr) and datagen
  stack.py up                              compose up in the cold-layer (5 s flush) mode
  stack.py seed   --out DIR                phase 1: cold batch to hot + base + pr, wait until the buffers drained
  stack.py hold   --out DIR                phase 2: restart Lakehouse with a 1 h flush, ingest the buffer batch
  stack.py status
  stack.py down                            compose down -v and remove the images this stack built, by exact tag

The same datagen seed and `--now` go to hot + base and, in a second run, to the PR, so the three
sides hold identical rows. Windows are absolute and hour aligned (state.json):
cold = [H-4h, H), buffer = [H, H+1h). Tenants: 0:0 and 1001:0 (the alias `acme-corp`).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import socket
import subprocess
import time
import urllib.request
import zlib
try:
    from .jsonio import dump_json, load_json, read_text
except ImportError:  # run as a script: python3 scripts/proof/stack.py
    from jsonio import dump_json, load_json, read_text  # type: ignore[no-redef]

HERE = os.path.dirname(os.path.abspath(__file__))
COMPOSE = os.path.join(HERE, "..", "..", "deployment", "docker", "docker-compose-proof.yml")
PROJECT = os.environ.get("PROOF_PROJECT", "lhproof")
NETWORK = f"{PROJECT}_proof-net"
PEER_KEY = "proof-peer-key"
# Every image the stack builds carries the project name, so two stacks never share or remove each other's images.
DATAGEN = f"{PROJECT}-datagen:1"
BUILT_BY_COMPOSE = ("vl-ref", "vt-ref", "lvp-ref", "grafana")


def lvp_version() -> str:
    """The loki-vl-proxy release pinned in Dockerfile.loki-vl-proxy (the one pin; the daily bump workflow edits it)."""
    text = read_text(os.path.join(os.path.dirname(COMPOSE), "Dockerfile.loki-vl-proxy"))
    return re.search(r"^ARG VERSION=(\S+)", text, re.M).group(1)


def image_tags(project: str = PROJECT) -> list[str]:
    """The images this stack builds, by exact tag: the only ones `down` removes. Read from the compose file (every image
    named `${PROOF_PREFIX:-lhproof}-<name>:<tag>`), plus the datagen image stack.py builds itself; the proxy's tag is the
    release the Dockerfile pins, so a run records which proxy produced it."""
    text = read_text(COMPOSE).replace("${LVP_VERSION:-pinned}", lvp_version())
    tags = {f"{project}-{n}:{t}" for n, t in re.findall(r"\$\{PROOF_PREFIX:-lhproof\}-([a-z-]+):([^\s}]+)", text)}
    tags.add(f"{project}-datagen:1")
    return sorted(tags)


IMAGES = image_tags()

# offsets from the port base; the default stack (project lhproof) sits at 48000
PORT_OFFSETS = {"ref-logs": 428, "ref-traces": 429, "base-logs": 430, "base-traces": 431, "pr-logs": 432, "pr-traces": 433,
                "grafana": 300, "jaeger-ref": 440, "jaeger-base": 441, "jaeger-pr": 442,
                "loki-ref": 450, "loki-base": 451, "loki-pr": 452}


# Ports other stacks of this host use (the owner's e2e and review stacks 29xxx, e2e Lakehouse/proxy 20xxx-23xxx, loki-vl-proxy's
# 33xxx/34xxx, the shared benchmark and `lhmain` stacks 39xxx/47xxx, the default proof stack 48xxx). Derived bases stay out of
# them and below 49152, where the operating system hands out ephemeral ports.
RESERVED_PORTS = ((20000, 23999), (29000, 29999), (33000, 34999), (39000, 39999), (47000, 48999))
EPHEMERAL_FLOOR = 49152
DEFAULT_PROJECT_BASE = 48000


def allowed_bases(reserved=RESERVED_PORTS, floor=EPHEMERAL_FLOOR) -> list[int]:
    """Port bases (multiples of 100) whose whole block [base, base + largest offset] is free of reserved ranges and ephemeral ports."""
    span = max(PORT_OFFSETS.values())
    return [b for b in range(24000, floor, 100)
            if b + span < floor and not any(b <= hi and b + span >= lo for lo, hi in reserved)]


def port_base(project: str = PROJECT, env=None) -> int:
    """PROOF_PORT_BASE, else 48000 for the default project and a base derived from the project name for any other."""
    env = os.environ if env is None else env
    if env.get("PROOF_PORT_BASE"):
        return int(env["PROOF_PORT_BASE"])
    if project == "lhproof":
        return DEFAULT_PROJECT_BASE
    bases = allowed_bases()
    return bases[zlib.crc32(project.encode()) % len(bases)]


def busy_ports(ports, host: str = "127.0.0.1") -> list[int]:
    """The ports of `ports` something already listens on (or that cannot be bound)."""
    busy = []
    for p in sorted(ports):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, p))
            except OSError:
                busy.append(p)
    return busy


def make_ports(project: str = PROJECT, env=None) -> dict:
    base = port_base(project, env)
    return {name: base + off for name, off in PORT_OFFSETS.items()}


PORTS = make_ports()


def compose_env() -> dict:
    """Variables the compose file reads: the image prefix and one host port per published service."""
    env = {"PROOF_PREFIX": PROJECT, "LVP_VERSION": lvp_version()}
    for k, v in PORTS.items():
        env["PORT_" + k.upper().replace("-", "_")] = str(v)
    return env


# (name, account, project, seed, logs, traces)
TENANTS = [("t0", "0", "0", 11, 3000, 600), ("t1001", "1001", "0", 12, 1500, 300)]
BUFFER_TENANTS = [("t0", "0", "0", 21, 800, 160), ("t1001", "1001", "0", 22, 400, 80)]
KEYORDER_TENANT = ("t7", "7", "0")


def sh(cmd, **kw):  # pragma: no cover - runs docker
    return subprocess.run(cmd, check=True, text=True, **kw)


def compose(*args, env=None, **kw):  # pragma: no cover - runs docker
    e = dict(os.environ, **compose_env(), **(env or {}))
    return sh(["docker", "compose", "-p", PROJECT, "-f", COMPOSE, *args], env=e, **kw)


def hour_floor(now: dt.datetime) -> dt.datetime:
    return now.replace(minute=0, second=0, microsecond=0)


def make_state(now: dt.datetime | None = None) -> dict:
    h = hour_floor(now or dt.datetime.now(dt.timezone.utc)) - dt.timedelta(hours=1)
    f = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    return {"H": f(h), "cold": {"start": f(h - dt.timedelta(hours=4)), "end": f(h)},
            "buffer": {"start": f(h), "end": f(h + dt.timedelta(hours=1))}, "ports": dict(PORTS), "project": PROJECT,
            "tenants": {"numeric": [{"account": a, "project": p} for _, a, p, *_ in TENANTS], "alias": {"acme-corp": "1001:0"}}}


def datagen_cmd(now: str, hours_back: int, tenant, hot: bool, lh: str | None) -> list[str]:
    _, acct, proj, seed, logs, traces = tenant
    cmd = ["docker", "run", "--rm", "--network", NETWORK, DATAGEN, f"--logs={logs}", f"--traces={traces}",
           f"--hours-back={hours_back}", f"--seed={seed}", f"--now={now}", f"--account-id={acct}", f"--project-id={proj}"]
    if hot:
        cmd += ["--vl-endpoint=http://vl-ref:9428", "--vt-endpoint=http://vt-ref:10428"]
    if lh:
        cmd += [f"--lh-logs-endpoint=http://lh-logs-{lh}:9428", f"--lh-traces-endpoint=http://lh-traces-{lh}:10428"]
    return cmd


def http_json(url: str, headers: dict | None = None, timeout: float = 30):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 - loopback proof stack
        return r.read().decode()


def buffered_rows(port: int, mode: str, start: str, end: str, account: str, project: str) -> int:
    """Rows of one tenant held in the insert buffer, from /internal/buffer/query (parity suite helper)."""
    def ns(s):
        return str(int(dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp() * 1e9))
    q = f"start={ns(start)}&end={ns(end)}&mode={mode}&tenant_scope=v1&account_id={account}&project_id={project}"
    body = http_json(f"http://127.0.0.1:{port}/internal/buffer/query?{q}", {"Authorization": f"Bearer {PEER_KEY}"})
    n = 0
    for line in body.splitlines():
        if not line.strip():
            continue
        if mode == "traces":
            try:
                if not json.loads(line).get("span_id"):
                    continue
            except ValueError:
                continue
        n += 1
    return n


def wait_drained(state: dict, timeout: float = 240) -> None:
    deadline = time.time() + timeout
    while True:
        left = 0
        for v in ("base", "pr"):
            for sig, mode in (("logs", "logs"), ("traces", "traces")):
                for _, a, p, *_ in TENANTS:
                    left += buffered_rows(PORTS[f"{v}-{sig}"], mode, state["cold"]["start"], state["cold"]["end"], a, p)
            left += buffered_rows(PORTS[f"{v}-logs"], "logs", state["cold"]["start"], state["cold"]["end"], KEYORDER_TENANT[1], KEYORDER_TENANT[2])
        if left == 0:
            return
        if time.time() > deadline:
            raise SystemExit(f"buffers still hold {left} cold-window rows after {timeout}s")
        time.sleep(5)


def keyorder_rows(start: str, n_streams: int = 3, per_stream: int = 40) -> str:
    """NDJSON of a fixture that exercises the order of the JSON members of a row (#429, #432, #452): several streams in
    one window, rows of different streams sharing a timestamp (ties for a limit to cut), one field that is the same in
    the whole stream (`zeta`), one that varies (`alpha`), one that is sparse (`mid`), and field names that are not in
    alphabetical order in the input. Deterministic: no clock, no randomness."""
    t0 = dt.datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ") + dt.timedelta(minutes=5)
    lines = []
    for i in range(per_stream):
        ts = (t0 + dt.timedelta(seconds=(i // 2) * 7)).strftime("%Y-%m-%dT%H:%M:%SZ")  # two rows per second and stream
        for s_ in range(n_streams):
            row = {"zeta": f"z{s_}", "ts": ts, "msg": f"line {i} of stream a{s_}", "app": f"a{s_}", "env": "proof",
                   "alpha": f"v{(i * 7 + s_) % 11}", "req": f"r{s_}-{i:03d}"}
            if i % 3 == 0:
                row["mid"] = f"m{i}"
            lines.append(json.dumps(row))
    return "\n".join(lines) + "\n"


def keyorder_request(port: int, start: str) -> urllib.request.Request:
    q = "_stream_fields=app,env&_msg_field=msg&_time_field=ts"
    return urllib.request.Request(f"http://127.0.0.1:{port}/insert/jsonline?{q}", data=keyorder_rows(start).encode(), method="POST",
                                  headers={"AccountID": KEYORDER_TENANT[1], "ProjectID": KEYORDER_TENANT[2],
                                           # without it VictoriaLogs answers 200 and ingests nothing
                                           "Content-Type": "application/stream+json"})


def post_keyorder(port: int, start: str) -> None:  # pragma: no cover - needs the stack
    with urllib.request.urlopen(keyorder_request(port, start), timeout=30) as r:  # noqa: S310 - loopback proof stack
        r.read()


def seed_keyorder(state: dict, window: str) -> None:  # pragma: no cover - needs the stack
    for k in ("ref-logs", "base-logs", "pr-logs"):
        post_keyorder(PORTS[k], state[window]["start"])


def read_buffered(state: dict) -> dict:
    out = {f"{v}-{sig}-{a_}": buffered_rows(PORTS[f"{v}-{sig}"], sig, state["buffer"]["start"], state["buffer"]["end"], a_, p_)
           for v in ("base", "pr") for sig in ("logs", "traces") for _, a_, p_, *_ in BUFFER_TENANTS}
    for v in ("base", "pr"):  # the key-order fixture is logs only
        out[f"{v}-logs-{KEYORDER_TENANT[1]}"] = buffered_rows(PORTS[f"{v}-logs"], "logs", state["buffer"]["start"], state["buffer"]["end"],
                                                              KEYORDER_TENANT[1], KEYORDER_TENANT[2])
    return out


def settled_buffered(state: dict, timeout: float = 60, step: float = 3) -> dict:
    """Unflushed rows per Lakehouse and tenant once ingest has become visible: two equal consecutive reads, all nonzero."""
    deadline = time.time() + timeout
    last = None
    while True:
        cur = read_buffered(state)
        if cur == last and all(cur.values()):
            return cur
        if time.time() > deadline:
            raise SystemExit(f"buffer counts did not settle: {cur}")
        last = cur
        time.sleep(step)


def cmd_build(a):  # pragma: no cover - drives docker compose
    for d, tag in ((a.main, "base"), (a.pr, "pr")):
        if not d:
            continue
        for sig in ("logs", "traces"):
            sh(["docker", "build", "-q", "-f", os.path.join(d, f"Dockerfile.{sig}"), "-t", f"{PROJECT}-{sig}:{tag}", d],
               env=dict(os.environ, GOWORK="off"))
    sh(["docker", "build", "-q", "-f", os.path.join(a.main, "Dockerfile.datagen"), "-t", DATAGEN, a.main])
    # hot VictoriaLogs/VictoriaTraces (with a probe binary), loki-vl-proxy (Dockerfile.loki-vl-proxy pins the release) and Grafana
    compose("build", "-q", *BUILT_BY_COMPOSE)


def cmd_up(_a):  # pragma: no cover - drives docker compose
    running = subprocess.run(["docker", "compose", "-p", PROJECT, "-f", COMPOSE, "ps", "-q"], capture_output=True, text=True,
                             env=dict(os.environ, **compose_env())).stdout.strip()
    busy = [] if running else busy_ports(PORTS.values())  # a stack of this project that is already up owns its ports
    if busy:
        raise SystemExit(f"ports already in use on 127.0.0.1: {busy}; set PROOF_PORT_BASE or another PROOF_PROJECT")
    compose("up", "-d", "--wait", "--wait-timeout", "240")


def cmd_seed(a):  # pragma: no cover - drives docker compose
    state = make_state()
    os.makedirs(a.out, exist_ok=True)
    for t in TENANTS:
        sh(datagen_cmd(state["H"], 4, t, hot=True, lh="base"))
        sh(datagen_cmd(state["H"], 4, t, hot=False, lh="pr"))
    seed_keyorder(state, "cold")
    wait_drained(state)
    state["keyorder"] = True
    dump_json(os.path.join(a.out, "state.json"), state, indent=1)
    print(json.dumps(state))


def cmd_hold(a):  # pragma: no cover - drives docker compose
    path = os.path.join(a.out, "state.json")
    state = load_json(path)
    compose("up", "-d", "--wait", "--wait-timeout", "240", env={"LH_POLICY": "./proof/policy-hold.yml"})
    h = dt.datetime.strptime(state["H"], "%Y-%m-%dT%H:%M:%SZ")
    now = (h + dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for t in BUFFER_TENANTS:
        sh(datagen_cmd(now, 1, t, hot=True, lh="base"))
        sh(datagen_cmd(now, 1, t, hot=False, lh="pr"))
    seed_keyorder(state, "buffer")
    state["buffered"] = settled_buffered(state)
    state["trace_id"] = pick_trace(state)
    dump_json(path, state, indent=1)
    print(json.dumps(state["buffered"]))


def pick_trace(state: dict) -> str:
    """A trace of tenant 0:0 whose spans carry events and links (smallest trace id: deterministic)."""
    import urllib.parse
    q = {"query": '"event:event_name:0":* "link:link_span_id:0":* | sort by (trace_id) | fields trace_id | limit 1',
         "start": state["cold"]["start"], "end": state["cold"]["end"]}
    body = http_json(f"http://127.0.0.1:{PORTS['ref-traces']}/select/logsql/query?" + urllib.parse.urlencode(q),
                     {"AccountID": "0", "ProjectID": "0"})
    return json.loads(body.splitlines()[0])["trace_id"]


def cmd_status(_a):  # pragma: no cover - drives docker compose
    compose("ps", "--format", "{{.Name}} {{.Status}}")


def cmd_down(_a):  # pragma: no cover - drives docker compose
    compose("down", "-v", "--remove-orphans")
    for img in image_tags():  # exact tags of this project, nothing else
        subprocess.run(["docker", "rmi", img], check=False)


def main(argv=None):  # pragma: no cover - drives docker compose
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--main", required=True)
    b.add_argument("--pr")
    for name in ("seed", "hold"):
        s = sub.add_parser(name)
        s.add_argument("--out", required=True)
    for name in ("up", "status", "down"):
        sub.add_parser(name)
    a = ap.parse_args(argv)
    globals()["cmd_" + a.cmd](a)


if __name__ == "__main__":
    main()
