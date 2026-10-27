#!/usr/bin/env python3
"""Runner-fit measurement for the PR proof stack on a GitHub-hosted runner (.github/workflows/pr-proof.yaml).

It answers one question with numbers: does the proof stack (scripts/bench/ab) fit a standard `ubuntu-latest` runner
(4 vCPU, 16 GB RAM, a few GB to tens of GB of disk), how long does each phase take, and which lever shortens it. It does not
change the harness: the seed-size lever patches two module constants of scripts/bench/ab/stack.py in memory before pr_smoke runs.

  runner_fit.py env     --out DIR                        write DIR/env.json (cores, memory, disks, docker, runner image)
  runner_fit.py sample  --out DIR [--interval 10]        sample until SIGTERM into DIR/samples.jsonl: free -m, df, docker system df, docker stats, load, cpu
  runner_fit.py run     --out DIR --name N [--ok 0,1] -- CMD...
                                                         time CMD, tee its output with a timestamp per line to DIR/logs/N.log,
                                                         record the phase in DIR/phases.jsonl; exits with CMD's status (codes in --ok count as success)
  runner_fit.py mark    --out DIR --name N start|end     record a phase boundary (for steps that are not shell commands)
  runner_fit.py pr-smoke --out DIR [--seed-chunks N] [--seed-scale F] -- PR_SMOKE_ARGS...
                                                         scripts/bench/ab/pr_smoke.py with the seed lever applied
  runner_fit.py stack   ACTION                           pull | build-probe | ui-pull | ui-build | ui-up (the compose helpers of scripts/bench/ab/stack.py)
  runner_fit.py peaks   --out DIR                        per-container cgroup memory.peak, OOMKilled and restart counts, kernel OOM lines -> DIR/peaks.json
  runner_fit.py leftovers --out DIR                      containers, volumes and images of the project that remain -> DIR/leftovers.json
  runner_fit.py report  --out DIR [--variant JSON] [--pr-smoke DIR]
                                                         DIR/runner-fit.json and a markdown table on stdout and in $GITHUB_STEP_SUMMARY

Standard library only (the pr-smoke and stack actions import scripts/bench/ab, which needs PyYAML). Every number it writes is measured on the
machine it ran on; the verdict thresholds are constants below.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
AB = os.path.join(ROOT, "scripts", "bench", "ab")

# Verdict thresholds: headroom the stack must leave. Reported with the numbers, so a reader can apply another.
MIN_AVAILABLE_MB = 1024      # MemAvailable never below this while the stack runs
MIN_FREE_DISK_MB = 1024      # free space on the Docker root never below this
NO_SPACE = re.compile(r"no space left on device", re.I)

UNITS = {"b": 1, "kb": 1000, "mb": 1000 ** 2, "gb": 1000 ** 3, "tb": 1000 ** 4,
         "kib": 1024, "mib": 1024 ** 2, "gib": 1024 ** 3, "tib": 1024 ** 4}


def log(msg: str) -> None:
    print(f"[runner-fit] {msg}", file=sys.stderr, flush=True)


# ------------------------------------------------------------------------------------------------ parsers
def parse_size(text: str) -> float | None:
    """Bytes of a docker size string: '512B', '1.5kB', '3.2GB' (decimal), '123.4MiB', '15.62GiB' (binary), '0B'."""
    m = re.fullmatch(r"\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]+)\s*", text or "")
    if not m or m.group(2).lower() not in UNITS:
        return None
    return float(m.group(1)) * UNITS[m.group(2).lower()]


def parse_free(text: str) -> dict | None:
    """`free -m` -> megabytes: total, used, free, buff_cache, available, swap_total, swap_used."""
    out: dict = {}
    for line in (text or "").splitlines():
        f = line.split()
        if line.startswith("Mem:") and len(f) >= 7:
            out.update(total=int(f[1]), used=int(f[2]), free=int(f[3]), buff_cache=int(f[5]), available=int(f[6]))
        elif line.startswith("Swap:") and len(f) >= 3:
            out.update(swap_total=int(f[1]), swap_used=int(f[2]))
    return out or None


def parse_docker_df(text: str) -> dict | None:
    """`docker system df --format '{{json .}}'` -> bytes per type: images, containers, volumes, build_cache (size and reclaimable)."""
    out = {}
    for line in (text or "").splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        key = str(row.get("Type", "")).lower().replace("local ", "").replace(" ", "_")
        key = {"build_cache": "build_cache"}.get(key, key)
        size = parse_size(str(row.get("Size", "")))
        if key and size is not None:
            out[key] = round(size)
    return out or None


def parse_stats(text: str) -> dict | None:
    """`docker stats --no-stream --format '{{json .}}'` -> {container: {"mem_mb": .., "cpu": ..}} (MemUsage '123MiB / 15GiB')."""
    out = {}
    for line in (text or "").splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        used = parse_size(str(row.get("MemUsage", "")).split("/")[0].strip())
        cpu = str(row.get("CPUPerc", "")).rstrip("%")
        try:
            cpu_v = float(cpu)
        except ValueError:
            cpu_v = None
        if row.get("Name") and used is not None:
            out[row["Name"]] = {"mem_mb": round(used / 1024 ** 2, 1), "cpu": cpu_v}
    return out or None


def parse_cpu_stat(text: str) -> tuple[int, int] | None:
    """First line of /proc/stat -> (idle ticks, total ticks)."""
    for line in (text or "").splitlines():
        if line.startswith("cpu "):
            v = [int(x) for x in line.split()[1:]]
            if len(v) >= 4:
                idle = v[3] + (v[4] if len(v) > 4 else 0)
                return idle, sum(v[:8])
    return None


# ------------------------------------------------------------------------------------------------ small helpers
def out_cmd(cmd: list[str], timeout: float = 30) -> str | None:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return r.stdout if r.returncode == 0 else None


def read_file(path: str) -> str | None:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except OSError:
        return None


def load_json(path: str):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def append_jsonl(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, sort_keys=True) + "\n")


def json_or_none(text: str):
    """The parsed JSON of `text`, or None when it is not JSON (a half-written line of a file another process is still appending to)."""
    try:
        return json.loads(text)
    except ValueError:
        return None


def read_jsonl(path: str) -> list[dict]:
    """The rows of a JSONL file; a line that does not parse is skipped, the measurement goes on."""
    return [row for row in (json_or_none(line) for line in (read_file(path) or "").splitlines()) if row is not None]


def load_json_or_none(path: str):
    """The parsed JSON file at `path`, or None when it is missing or unreadable: every input of the report is optional, a phase that
    failed early leaves some of them out, and the report must still say what it has."""
    try:
        return load_json(path)
    except (OSError, ValueError):
        return None


def disk(path: str) -> dict | None:
    try:
        u = shutil.disk_usage(path)
    except OSError:
        return None
    mb = 1024 ** 2
    return {"total_mb": u.total // mb, "used_mb": (u.total - u.free) // mb, "free_mb": u.free // mb}


def docker_root() -> str:
    return (out_cmd(["docker", "info", "--format", "{{.DockerRootDir}}"]) or "").strip() or "/var/lib/docker"


# ------------------------------------------------------------------------------------------------ env, sample
def cmd_env(a) -> int:
    env = {"nproc": os.cpu_count(), "uname": (out_cmd(["uname", "-sr"]) or "").strip(),
           "free_mb": parse_free(out_cmd(["free", "-m"]) or ""),
           "disks": {p: disk(p) for p in ("/", "/mnt", docker_root())},
           "docker_version": (out_cmd(["docker", "version", "--format", "{{.Server.Version}}"]) or "").strip(),
           "compose_version": (out_cmd(["docker", "compose", "version", "--short"]) or "").strip(),
           "docker_storage_driver": (out_cmd(["docker", "info", "--format", "{{.Driver}}"]) or "").strip(),
           "docker_root": docker_root(),
           "runner": {k: os.environ.get(k, "") for k in ("ImageOS", "ImageVersion", "RUNNER_OS", "RUNNER_ARCH", "RUNNER_ENVIRONMENT")},
           "cpu_model": next((l.split(":", 1)[1].strip() for l in (read_file("/proc/cpuinfo") or "").splitlines() if l.startswith("model name")), "")}
    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, "env.json"), "w", encoding="utf-8") as f:
        json.dump(env, f, indent=1, sort_keys=True)
    print(json.dumps(env, indent=1, sort_keys=True))
    return 0


def collect_sample(root: str) -> dict:
    s: dict = {"t": round(time.time(), 1)}
    if hasattr(os, "getloadavg"):
        s["load1"] = os.getloadavg()[0]
    s["cpu_stat"] = parse_cpu_stat(read_file("/proc/stat") or "")
    s["mem"] = parse_free(out_cmd(["free", "-m"]) or "")
    s["disk"] = {p: disk(p) for p in ("/", "/mnt", root)}
    s["docker_df"] = parse_docker_df(out_cmd(["docker", "system", "df", "--format", "{{json .}}"], 60) or "")
    s["containers"] = parse_stats(out_cmd(["docker", "stats", "--no-stream", "--format", "{{json .}}"], 60) or "")
    return s


def cmd_sample(a) -> int:
    os.makedirs(a.out, exist_ok=True)
    path = os.path.join(a.out, "samples.jsonl")
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    root = docker_root()
    while True:
        t0 = time.time()
        append_jsonl(path, collect_sample(root))
        if stop.wait(max(0.0, a.interval - (time.time() - t0))):
            if time.time() - t0 > 0.5:  # a last sample after the signal, unless one was just taken
                append_jsonl(path, collect_sample(root))
            return 0


# ------------------------------------------------------------------------------------------------ run, mark
def phases_path(out: str) -> str:
    return os.path.join(out, "phases.jsonl")


def cmd_mark(a) -> int:
    append_jsonl(phases_path(a.out), {"name": a.name, "event": a.event, "t": round(time.time(), 2)})
    return 0


def cmd_run(a) -> int:
    cmd = a.cmd[1:] if a.cmd and a.cmd[0] == "--" else a.cmd
    if not cmd:
        raise SystemExit("run: no command")
    ok = {int(x) for x in a.ok.split(",") if x}
    logdir = os.path.join(a.out, "logs")
    os.makedirs(logdir, exist_ok=True)
    t0 = time.time()
    append_jsonl(phases_path(a.out), {"name": a.name, "event": "start", "t": round(t0, 2)})
    rc = 127
    try:
        with open(os.path.join(logdir, a.name + ".log"), "w", encoding="utf-8") as lf:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace", bufsize=1)
            for line in p.stdout:
                lf.write(f"{time.time():.2f}\t{line}" if line.endswith("\n") else f"{time.time():.2f}\t{line}\n")
                lf.flush()
                sys.stdout.write(line)
                sys.stdout.flush()
            rc = p.wait()
    except OSError as e:
        log(f"{a.name}: cannot run {cmd[0]}: {e}")
    t1 = time.time()
    append_jsonl(phases_path(a.out), {"name": a.name, "event": "end", "t": round(t1, 2), "rc": rc, "ok": rc in ok or rc == 0})
    log(f"phase {a.name}: {t1 - t0:.1f} s, exit {rc}{' (accepted)' if rc in ok and rc != 0 else ''}")
    return 0 if (rc == 0 or rc in ok) else (rc or 1)


# ------------------------------------------------------------------------------------------------ harness launchers
def load_stack(project: str):
    """scripts/bench/ab/stack.py with PROJECT taken from AB_PROJECT (read at import)."""
    os.environ.setdefault("AB_PROJECT", project)
    for p in (ROOT, AB):
        if p not in sys.path:
            sys.path.insert(0, p)
    import stack  # noqa: PLC0415
    return stack


def scale_tenants(tenants: list[tuple], scale: float) -> list[tuple]:
    """(name, account, project, seed, logs, traces) with the row counts scaled (at least 1)."""
    return [(n, a, p, s, max(1, round(lg * scale)), max(1, round(tr * scale))) for n, a, p, s, lg, tr in tenants]


def apply_seed_lever(stack, chunks: int | None, scale: float | None) -> None:
    """The seed-size lever: fewer chunks (each costs a 7 s pause) and fewer rows per chunk. Defaults leave stack.py untouched."""
    if chunks:
        stack.CHUNKS = chunks
    if scale and scale != 1.0:
        stack.TENANTS = scale_tenants(stack.TENANTS, scale)
        stack.BUFFER_TENANTS = scale_tenants(stack.BUFFER_TENANTS, scale)


def cmd_pr_smoke(a) -> int:
    rest = a.args[1:] if a.args and a.args[0] == "--" else a.args
    stack = load_stack(os.environ.get("AB_PROJECT", "lh-ab"))
    apply_seed_lever(stack, a.seed_chunks, a.seed_scale)
    log(f"seed: {stack.CHUNKS} chunks, tenants {stack.TENANTS}, buffer {stack.BUFFER_TENANTS}")
    import pr_smoke  # noqa: PLC0415
    return pr_smoke.main(["--out", a.out, *rest])


UI_SERVICES = ["lvp-ref", "lvp-base", "lvp-pr", "jaeger-query", "jaeger-ui-ref", "jaeger-ui-base", "jaeger-ui-pr", "grafana"]


def cmd_stack(a) -> int:
    stack = load_stack(os.environ.get("AB_PROJECT", "lh-ab"))
    stack.set_phase("measure")
    if a.action == "pull":
        stack.compose("pull", "-q", "s3", "s3-init", "s3-latency")
    elif a.action == "build-probe":
        stack.compose("build", "-q", "vl-ref", "vt-ref")
    elif a.action == "ui-pull":
        stack.compose("pull", "-q", "jaeger-query", "jaeger-ui-ref")
    elif a.action == "ui-build":
        stack.compose("build", "-q", "lvp-ref", "grafana")
    elif a.action == "ui-up":
        stack.compose("up", "-d", "--wait", "--wait-timeout", "240", *UI_SERVICES)
    else:
        raise SystemExit(f"unknown stack action {a.action!r}")
    return 0


# ------------------------------------------------------------------------------------------------ peaks, leftovers
def cgroup_peak_mb(container_id: str) -> float | None:
    """memory.peak of the container's cgroup (v2, systemd driver or cgroupfs), in MiB; None when the kernel does not expose it."""
    for path in (f"/sys/fs/cgroup/system.slice/docker-{container_id}.scope/memory.peak", f"/sys/fs/cgroup/docker/{container_id}/memory.peak"):
        text = read_file(path)
        if text and text.strip().isdigit():
            return round(int(text) / 1024 ** 2, 1)
    return None


def project_containers(project: str) -> list[str]:
    out = out_cmd(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={project}", "--no-trunc"]) or ""
    return [x for x in out.split() if x]


def cmd_peaks(a) -> int:
    project = os.environ.get("AB_PROJECT", "lh-ab")
    rows = {}
    for cid in project_containers(project):
        info = out_cmd(["docker", "inspect", "-f", "{{.Name}} {{.State.OOMKilled}} {{.RestartCount}} {{.HostConfig.Memory}}", cid]) or ""
        f = info.split()
        if len(f) < 4:
            continue
        rows[f[0].lstrip("/")] = {"peak_mb": cgroup_peak_mb(cid), "oom_killed": f[1] == "true", "restarts": int(f[2]), "mem_limit_bytes": int(f[3])}
    dmesg = out_cmd(["sudo", "-n", "dmesg", "--ctime"]) or out_cmd(["dmesg"]) or ""
    oom = [l.strip()[:200] for l in dmesg.splitlines() if re.search(r"out of memory|oom-kill|killed process", l, re.I)]
    res = {"containers": rows, "kernel_oom_lines": oom[:20], "kernel_log_readable": bool(dmesg)}
    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, "peaks.json"), "w", encoding="utf-8") as f:
        json.dump(res, f, indent=1, sort_keys=True)
    log(f"peaks: {len(rows)} containers, oom-killed {[k for k, v in rows.items() if v['oom_killed']]}, kernel oom lines {len(oom)}")
    return 0


def cmd_leftovers(a) -> int:
    project = os.environ.get("AB_PROJECT", "lh-ab")
    label = f"label=com.docker.compose.project={project}"
    left = {"containers": (out_cmd(["docker", "ps", "-aq", "--filter", label]) or "").split(),
            "volumes": (out_cmd(["docker", "volume", "ls", "-q", "--filter", label]) or "").split(),
            "networks": (out_cmd(["docker", "network", "ls", "-q", "--filter", label]) or "").split(),
            "images": [i for i in (out_cmd(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"]) or "").split() if i.startswith(project + "-")]}
    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, "leftovers.json"), "w", encoding="utf-8") as f:
        json.dump(left, f, indent=1, sort_keys=True)
    log(f"leftovers of project {project}: {left}")
    return 0


# ------------------------------------------------------------------------------------------------ report
def pair_phases(events: list[dict]) -> list[dict]:
    """phases.jsonl -> [{name, start, end, seconds, rc, ok}] in start order; a phase with no end event is open (seconds None)."""
    open_: dict = {}
    done = []
    for e in sorted(events, key=lambda x: x["t"]):
        if e["event"] == "start":
            open_[e["name"]] = e
        elif e["event"] == "end" and e["name"] in open_:
            s = open_.pop(e["name"])
            done.append({"name": e["name"], "start": s["t"], "end": e["t"], "seconds": round(e["t"] - s["t"], 1), "rc": e.get("rc"), "ok": e.get("ok", True)})
    for name, s in open_.items():
        done.append({"name": name, "start": s["t"], "end": None, "seconds": None, "rc": None, "ok": False})
    return sorted(done, key=lambda p: p["start"])


# pr_smoke / stack.py log lines that bound the sub-phases inside the `pr-smoke` phase. (label, regex, "first" | "last"); each boundary
# is the timestamp of that line and the sub-phase is the time since the previous boundary. Order is the order the harness logs them.
BOUNDARIES = [
    ("up", r"\[ab-stack\] up:", "first"),
    ("seed", r"\[ab-stack\] seeded", "first"),
    ("compact", r"\[ab-stack\] recompact ", "last"),
    ("hold", r"\[ab-stack\] buffer layer:", "first"),
    ("seed_equality", r"S3 latency 0 ms", "first"),
    ("perf_0ms", r"S3 latency (?!0 ms)|\[pr-smoke\] API matrix", "first"),
    ("perf_profiles", r"\[pr-smoke\] API matrix", "first"),
    ("api_matrix", r"\[pr-smoke\] components", "first"),
]


def read_timed_log(path: str) -> list[tuple[float, str]]:
    rows = []
    for line in (read_file(path) or "").splitlines():
        t, _, text = line.partition("\t")
        try:
            rows.append((float(t), text))
        except ValueError:
            continue
    return rows


def sub_phases(lines: list[tuple[float, str]], start: float, end: float | None) -> dict:
    """Seconds per sub-phase of the pr-smoke run, from the timestamps of the harness's own log lines. Missing boundaries are skipped
    (the time folds into the next one). Also derived (from line gaps, so approximate): the forced restart and the confirm re-measures."""
    out: dict = {}
    prev = start
    for label, rx, which in BOUNDARIES:
        hits = [t for t, text in lines if re.search(rx, text) and t > prev]
        if not hits:
            continue
        t = hits[0] if which == "first" else hits[-1]
        out[label] = round(t - prev, 1)
        prev = t
    if end is not None and end >= prev:
        out["components_and_render"] = round(end - prev, 1)
    cell = re.compile(r"cell \d+/\d+")
    restart = [t for t, text in lines if "after restart, unflushed rows" in text]
    if restart:
        before = [t for t, text in lines if cell.search(text) and t < restart[0]]
        if before:
            out["restart_approx"] = round(restart[0] - before[-1], 1)
    confirm = 0.0
    for i, (t, text) in enumerate(lines):
        if "re-measuring" in text and "cell(s)" in text:
            stop = next((t2 for t2, tx in lines[i + 1:] if tx.strip() and not cell.search(tx)), None)
            if stop is not None:
                confirm += stop - t
    if confirm:
        out["confirm_remeasure_approx"] = round(confirm, 1)
    return out


def cpu_series(samples: list[dict]) -> list[float]:
    """Utilisation percent between successive /proc/stat reads."""
    out = []
    prev = None
    for s in samples:
        cur = s.get("cpu_stat")
        if cur and prev and cur[1] > prev[1]:
            out.append(round(100.0 * (1 - (cur[0] - prev[0]) / (cur[1] - prev[1])), 1))
        if cur:
            prev = cur
    return out


def percentile(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    return s[min(len(s) - 1, round(q * (len(s) - 1)))]


def summarize_samples(samples: list[dict]) -> dict:
    mem = [s["mem"] for s in samples if s.get("mem")]
    res: dict = {"n": len(samples), "interval_s": round((samples[-1]["t"] - samples[0]["t"]) / max(1, len(samples) - 1), 1) if len(samples) > 1 else None}
    if mem:
        res["mem_total_mb"] = mem[0]["total"]
        res["mem_used_max_mb"] = max(m["used"] for m in mem)
        res["mem_available_min_mb"] = min(m["available"] for m in mem)
        res["mem_buff_cache_max_mb"] = max(m["buff_cache"] for m in mem)
        res["swap_used_max_mb"] = max(m.get("swap_used", 0) for m in mem)
    cpu = cpu_series(samples)
    if cpu:
        res["cpu_pct"] = {"mean": round(sum(cpu) / len(cpu), 1), "p95": percentile(cpu, 0.95), "max": max(cpu), "samples_at_or_above_95": sum(1 for c in cpu if c >= 95)}
    loads = [s["load1"] for s in samples if "load1" in s]
    if loads:
        res["load1_max"] = round(max(loads), 2)
    disks: dict = {}
    for s in samples:
        for path, d in (s.get("disk") or {}).items():
            if d:
                cur = disks.setdefault(path, {"total_mb": d["total_mb"], "free_min_mb": d["free_mb"], "used_max_mb": d["used_mb"], "first_used_mb": d["used_mb"]})
                cur["free_min_mb"] = min(cur["free_min_mb"], d["free_mb"])
                cur["used_max_mb"] = max(cur["used_max_mb"], d["used_mb"])
    res["disk"] = disks
    df: dict = {}
    for s in samples:
        for k, v in (s.get("docker_df") or {}).items():
            df[k] = max(df.get(k, 0), v)
    res["docker_df_max_mb"] = {k: round(v / 1024 ** 2) for k, v in df.items()}
    cont: dict = {}
    sums = []
    for s in samples:
        c = s.get("containers") or {}
        if c:
            sums.append(sum(v["mem_mb"] for v in c.values()))
        for name, v in c.items():
            cur = cont.setdefault(name, {"mem_max_mb": 0.0, "cpu_max": 0.0})
            cur["mem_max_mb"] = max(cur["mem_max_mb"], v["mem_mb"])
            cur["cpu_max"] = max(cur["cpu_max"], v.get("cpu") or 0.0)
    res["containers"] = cont
    res["containers_mem_sum_max_mb"] = round(max(sums), 1) if sums else None
    return res


def fit_verdict(sm: dict, peaks: dict | None, phases: list[dict], logs_text: str) -> dict:
    """Pass/fail on memory and disk only. CPU saturation is reported, never a verdict."""
    reasons = []
    avail = sm.get("mem_available_min_mb")
    if avail is None:
        reasons.append("no memory samples: the sampler did not run, so nothing is proven")
    elif avail < MIN_AVAILABLE_MB:
        reasons.append(f"MemAvailable fell to {avail} MB (< {MIN_AVAILABLE_MB})")
    oom = [k for k, v in ((peaks or {}).get("containers") or {}).items() if v.get("oom_killed")]
    if oom:
        reasons.append(f"OOMKilled: {', '.join(sorted(oom))}")
    if (peaks or {}).get("kernel_oom_lines"):
        reasons.append("kernel OOM lines in dmesg")
    for path, d in (sm.get("disk") or {}).items():
        if d["free_min_mb"] < MIN_FREE_DISK_MB and path != "/mnt":
            reasons.append(f"free space on {path} fell to {d['free_min_mb']} MB (< {MIN_FREE_DISK_MB})")
    if NO_SPACE.search(logs_text):
        reasons.append("'no space left on device' in a phase log")
    failed = [p["name"] for p in phases if not p["ok"]]
    return {"fits": not reasons, "reasons": reasons, "failed_phases": failed,
            "thresholds": {"min_available_mb": MIN_AVAILABLE_MB, "min_free_disk_mb": MIN_FREE_DISK_MB}}


def pr_smoke_facts(d: str) -> dict:
    """What pr_smoke.py wrote that bears on cost and noise: its own elapsed time, cell counts and the A/A moved counts before and
    after the confirming re-measure (this PR changes no Go code, so every cell that moved is a false positive)."""
    facts: dict = {}
    meta = load_json_or_none(os.path.join(d, "meta.json")) or {}
    if meta:
        facts.update(elapsed_s=meta.get("elapsed_s"), data=meta.get("data"), host_load=meta.get("host_load"), same_build=meta.get("same_build"))
    for key, name in (("perf", "perf-verdict.json"), ("api", "api-verdict.json")):
        verdict = load_json_or_none(os.path.join(d, name))
        if verdict is not None:
            facts[key + "_exit"] = verdict.get("exit")
    summary = load_json_or_none(os.path.join(d, "summary-0ms.json"))
    if summary is not None:
        cells = summary.get("cells") or []
        facts["cells"] = len(cells)
        facts["verdicts"] = summary.get("verdicts")
        facts["moved_first_pass"] = sum(1 for c in cells if c.get("confirmed"))
        facts["moved_after_confirm"] = sum(1 for c in cells if c.get("verdict") in ("slower", "faster", "unstable"))
        facts["differences"] = summary.get("differences")
    return facts


def build_report(out: str, variant: dict, smoke_dir: str) -> dict:
    events = read_jsonl(phases_path(out))
    phases = pair_phases(events)
    samples = read_jsonl(os.path.join(out, "samples.jsonl"))
    peaks = load_json_or_none(os.path.join(out, "peaks.json"))
    env = load_json_or_none(os.path.join(out, "env.json")) or {}
    leftovers = load_json_or_none(os.path.join(out, "leftovers.json"))
    sm = summarize_samples(samples)
    logs_text = ""
    logdir = os.path.join(out, "logs")
    if os.path.isdir(logdir):
        for n in sorted(os.listdir(logdir)):
            logs_text += read_file(os.path.join(logdir, n)) or ""
    smoke_phase = next((p for p in phases if p["name"] == "pr-smoke"), None)
    sub = {}
    if smoke_phase:
        sub = sub_phases(read_timed_log(os.path.join(logdir, "pr-smoke.log")), smoke_phase["start"], smoke_phase["end"])
    closed = [p for p in phases if p["end"]]
    wall = round(max(p["end"] for p in closed) - min(p["start"] for p in closed), 1) if closed else None
    # the measured working time of the proof itself: everything but setup and teardown
    core_names = {"pull", "build", "build-probe", "pr-smoke"}
    facts = pr_smoke_facts(smoke_dir) if smoke_dir else {}
    if sub.get("perf_0ms") and facts.get("cells"):
        sub["perf_0ms_s_per_cell"] = round(sub["perf_0ms"] / facts["cells"], 2)  # includes the restart and the confirm re-measures
    core = round(sum(p["seconds"] for p in phases if p["seconds"] and (p["name"] in core_names or p["name"].startswith("build-"))), 1)
    return {"variant": variant, "env": env, "phases": phases, "pr_smoke_sub_phases": sub, "wall_s": wall, "core_proof_s": core,
            "samples": sm, "peaks": peaks, "leftovers": leftovers, "pr_smoke": facts,
            "fit": fit_verdict(sm, peaks, phases, logs_text)}


def mins(s) -> str:
    return "n/a" if s is None else f"{s:.0f} s ({s / 60:.1f} min)"


def render_markdown(r: dict) -> str:
    v, sm, fit = r["variant"], r["samples"], r["fit"]
    L = [f"### Runner fit: `{v.get('variant', '?')}`", ""]
    L.append(f"**{'FITS' if fit['fits'] else 'DOES NOT FIT'}** the runner on memory and disk (measured on this run). "
             + ("" if fit["fits"] else "Reasons: " + "; ".join(fit["reasons"]) + ". ") +
             (f"Failed phases: {', '.join(fit['failed_phases'])}." if fit["failed_phases"] else ""))
    L += ["", f"Levers: `{json.dumps({k: x for k, x in v.items() if k != 'variant'}, sort_keys=True)}`", ""]
    e = r.get("env") or {}
    L += ["| Machine (measured) | |", "|---|---|", f"| vCPU | {e.get('nproc')} ({e.get('cpu_model', '')}) |",
          f"| RAM total | {sm.get('mem_total_mb')} MB |", f"| Image | {e.get('runner', {}).get('ImageOS', '')} {e.get('runner', {}).get('ImageVersion', '')} |",
          f"| Docker | {e.get('docker_version')} / compose {e.get('compose_version')} / {e.get('docker_storage_driver')} at {e.get('docker_root')} |", ""]
    L += ["| Phase | Wall time | Exit |", "|---|--:|--:|"]
    for p in r["phases"]:
        L.append(f"| {p['name']} | {mins(p['seconds'])} | {p['rc'] if p['rc'] is not None else 'open'}{'' if p['ok'] else ' FAILED'} |")
    L += ["", f"Wall clock first phase to last: **{mins(r['wall_s'])}**; build + pull + pr-smoke (the proof itself): **{mins(r['core_proof_s'])}**.", ""]
    if r["pr_smoke_sub_phases"]:
        L += ["<details><summary>Inside pr-smoke (seconds, from the harness's log timestamps; *_approx are derived from line gaps)</summary>", "",
              "| Step | s |", "|---|--:|"] + [f"| {k} | {x} |" for k, x in r["pr_smoke_sub_phases"].items()] + ["", "</details>", ""]
    L += ["| Resource (measured, sampled every ~" + str(sm.get("interval_s")) + " s) | Peak / minimum |", "|---|--:|",
          f"| RAM used, max | {sm.get('mem_used_max_mb')} MB of {sm.get('mem_total_mb')} |",
          f"| MemAvailable, min | {sm.get('mem_available_min_mb')} MB |",
          f"| page cache, max | {sm.get('mem_buff_cache_max_mb')} MB |",
          f"| swap used, max | {sm.get('swap_used_max_mb')} MB |",
          f"| containers' RAM, sum max (docker stats) | {sm.get('containers_mem_sum_max_mb')} MB |",
          f"| CPU utilisation mean / p95 / max | {(sm.get('cpu_pct') or {}).get('mean')} / {(sm.get('cpu_pct') or {}).get('p95')} / {(sm.get('cpu_pct') or {}).get('max')} % "
          f"({(sm.get('cpu_pct') or {}).get('samples_at_or_above_95')} samples at or above 95 %) |",
          f"| load average (1 min), max | {sm.get('load1_max')} |"]
    for path, d in (sm.get("disk") or {}).items():
        L.append(f"| disk `{path}` free min / total | {d['free_min_mb']} / {d['total_mb']} MB (used {d['first_used_mb']} to {d['used_max_mb']} MB) |")
    L.append(f"| docker system df, max (MB) | {json.dumps(sm.get('docker_df_max_mb'))} |")
    L.append("")
    top = sorted((sm.get("containers") or {}).items(), key=lambda kv: -kv[1]["mem_max_mb"])[:8]
    if top:
        L += ["<details><summary>Largest containers (docker stats peak, sampled)</summary>", "", "| Container | RAM max MB | CPU max % |", "|---|--:|--:|"]
        L += [f"| {n} | {c['mem_max_mb']} | {c['cpu_max']} |" for n, c in top] + ["", "</details>", ""]
    pk = (r.get("peaks") or {}).get("containers") or {}
    if pk:
        L += ["<details><summary>cgroup memory.peak, OOMKilled, restarts (read before teardown)</summary>", "", "| Container | memory.peak MB | OOMKilled | restarts |", "|---|--:|--:|--:|"]
        L += [f"| {n} | {c['peak_mb']} | {c['oom_killed']} | {c['restarts']} |" for n, c in sorted(pk.items())] + ["", "</details>", ""]
    ps = r.get("pr_smoke") or {}
    if ps:
        L += [f"pr-smoke: {ps.get('cells')} cells, verdicts {ps.get('verdicts')}, perf exit {ps.get('perf_exit')}, api exit {ps.get('api_exit')}; "
              f"cells that moved in the first pass {ps.get('moved_first_pass')}, after the confirming re-measure {ps.get('moved_after_confirm')} "
              f"(a base and a PR build of the same code: every move is a false positive). Data: {ps.get('data')}", ""]
    lo = r.get("leftovers")
    if lo is not None:
        gone = not any(lo.values())
        L.append(f"Teardown leftovers of the compose project: {'none' if gone else json.dumps(lo)}.")
    return "\n".join(L) + "\n"


def cmd_report(a) -> int:
    variant = json.loads(a.variant) if a.variant else {}
    r = build_report(a.out, variant, a.pr_smoke)
    with open(os.path.join(a.out, "runner-fit.json"), "w", encoding="utf-8") as f:
        json.dump(r, f, indent=1, sort_keys=True)
    md = render_markdown(r)
    print(md)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(md)
    return 0


# ------------------------------------------------------------------------------------------------ cli
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="subcmd", required=True)
    for name in ("env", "peaks", "leftovers"):
        sub.add_parser(name).add_argument("--out", required=True)
    s = sub.add_parser("sample")
    s.add_argument("--out", required=True)
    s.add_argument("--interval", type=float, default=10.0)
    r = sub.add_parser("run")
    r.add_argument("--out", required=True)
    r.add_argument("--name", required=True)
    r.add_argument("--ok", default="")
    r.add_argument("cmd", nargs=argparse.REMAINDER)
    m = sub.add_parser("mark")
    m.add_argument("--out", required=True)
    m.add_argument("--name", required=True)
    m.add_argument("event", choices=("start", "end"))
    p = sub.add_parser("pr-smoke")
    p.add_argument("--out", required=True)
    p.add_argument("--seed-chunks", type=int, default=0)
    p.add_argument("--seed-scale", type=float, default=1.0)
    p.add_argument("args", nargs=argparse.REMAINDER)
    st = sub.add_parser("stack")
    st.add_argument("action", choices=("pull", "build-probe", "ui-pull", "ui-build", "ui-up"))
    rep = sub.add_parser("report")
    rep.add_argument("--out", required=True)
    rep.add_argument("--variant", default="")
    rep.add_argument("--pr-smoke", default="")
    a = ap.parse_args(argv)
    return globals()["cmd_" + a.subcmd.replace("-", "_")](a)


if __name__ == "__main__":
    sys.exit(main())
