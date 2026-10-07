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
import subprocess
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
COMPOSE = os.path.join(HERE, "..", "..", "deployment", "docker", "docker-compose-proof.yml")
PROJECT = os.environ.get("PROOF_PROJECT", "lhproof")
NETWORK = f"{PROJECT}_proof-net"
PEER_KEY = "proof-peer-key"
DATAGEN = "lhproof-datagen:1"
IMAGES = ["lhproof-vlproxy:2.5.1", "lhproof-logs:base", "lhproof-traces:base", "lhproof-logs:pr", "lhproof-traces:pr", DATAGEN, "lhproof-grafana:1"]

PORTS = {"ref-logs": 48428, "ref-traces": 48429, "base-logs": 48430, "base-traces": 48431,
         "pr-logs": 48432, "pr-traces": 48433, "grafana": 48300,
         "jaeger-ref": 48440, "jaeger-base": 48441, "jaeger-pr": 48442,
         "loki-ref": 48450, "loki-base": 48451, "loki-pr": 48452}

# (name, account, project, seed, logs, traces)
TENANTS = [("t0", "0", "0", 11, 3000, 600), ("t1001", "1001", "0", 12, 1500, 300)]
BUFFER_TENANTS = [("t0", "0", "0", 21, 800, 160), ("t1001", "1001", "0", 22, 400, 80)]


def sh(cmd, **kw):  # pragma: no cover - runs docker
    return subprocess.run(cmd, check=True, text=True, **kw)


def compose(*args, env=None, **kw):  # pragma: no cover - runs docker
    e = dict(os.environ, **(env or {}))
    return sh(["docker", "compose", "-p", PROJECT, "-f", COMPOSE, *args], env=e, **kw)


def hour_floor(now: dt.datetime) -> dt.datetime:
    return now.replace(minute=0, second=0, microsecond=0)


def make_state(now: dt.datetime | None = None) -> dict:
    h = hour_floor(now or dt.datetime.now(dt.timezone.utc)) - dt.timedelta(hours=1)
    f = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    return {"H": f(h), "cold": {"start": f(h - dt.timedelta(hours=4)), "end": f(h)},
            "buffer": {"start": f(h), "end": f(h + dt.timedelta(hours=1))}, "ports": PORTS,
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
        if left == 0:
            return
        if time.time() > deadline:
            raise SystemExit(f"buffers still hold {left} cold-window rows after {timeout}s")
        time.sleep(5)


def cmd_build(a):  # pragma: no cover - drives docker compose
    for d, tag in ((a.main, "base"), (a.pr, "pr")):
        if not d:
            continue
        for sig in ("logs", "traces"):
            sh(["docker", "build", "-q", "-f", os.path.join(d, f"Dockerfile.{sig}"), "-t", f"lhproof-{sig}:{tag}", d],
               env=dict(os.environ, GOWORK="off"))
    sh(["docker", "build", "-q", "-f", os.path.join(a.main, "Dockerfile.datagen"), "-t", DATAGEN, a.main])


def cmd_up(_a):  # pragma: no cover - drives docker compose
    compose("up", "-d", "--wait", "--wait-timeout", "240")


def cmd_seed(a):  # pragma: no cover - drives docker compose
    state = make_state()
    os.makedirs(a.out, exist_ok=True)
    for t in TENANTS:
        sh(datagen_cmd(state["H"], 4, t, hot=True, lh="base"))
        sh(datagen_cmd(state["H"], 4, t, hot=False, lh="pr"))
    wait_drained(state)
    with open(os.path.join(a.out, "state.json"), "w") as f:
        json.dump(state, f, indent=1)
    print(json.dumps(state))


def cmd_hold(a):  # pragma: no cover - drives docker compose
    path = os.path.join(a.out, "state.json")
    state = json.load(open(path))
    compose("up", "-d", "--wait", "--wait-timeout", "240", env={"LH_POLICY": "./proof/policy-hold.yml"})
    h = dt.datetime.strptime(state["H"], "%Y-%m-%dT%H:%M:%SZ")
    now = (h + dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for t in BUFFER_TENANTS:
        sh(datagen_cmd(now, 1, t, hot=True, lh="base"))
        sh(datagen_cmd(now, 1, t, hot=False, lh="pr"))
    state["buffered"] = {f"{v}-{sig}-{a_}": buffered_rows(PORTS[f"{v}-{sig}"], sig, state["buffer"]["start"], state["buffer"]["end"], a_, p_)
                         for v in ("base", "pr") for sig in ("logs", "traces") for _, a_, p_, *_ in BUFFER_TENANTS}
    state["trace_id"] = pick_trace(state)
    json.dump(state, open(path, "w"), indent=1)
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
    for img in IMAGES:
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
