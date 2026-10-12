"""The older shell injector must fail loudly: it used to report success for a stack it could not reach (2026-09 baseline)."""
import os
import subprocess

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
WITH = os.path.join(ROOT, "scripts", "bench", "with-s3-latency.sh")
INJECT = os.path.join(ROOT, "scripts", "inject-s3-latency.sh")


def run(cmd, tmp_path, docker_ok):
    """Run with a fake `docker` first on PATH: `inspect` succeeds only when docker_ok; `exec` always succeeds."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    fake = bindir / "docker"
    fake.write_text('#!/bin/sh\nif [ "$1" = inspect ]; then exit %d; fi\nexit 0\n' % (0 if docker_ok else 1))
    fake.chmod(0o755)
    marker = tmp_path / "ran"
    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}")
    r = subprocess.run(cmd + ["--", "touch", str(marker)] if cmd[1] == WITH else cmd, capture_output=True, text=True, env=env, timeout=60)
    return r, marker.exists()


@pytest.mark.skipif(not os.path.exists(WITH), reason="scripts not in the tree")
def test_inject_script_fails_when_the_proxy_container_does_not_exist(tmp_path):
    r, _ = run(["bash", INJECT, "50", "0"], tmp_path, docker_ok=False)
    assert r.returncode == 2 and "no container named" in r.stderr


@pytest.mark.skipif(not os.path.exists(WITH), reason="scripts not in the tree")
def test_with_s3_latency_does_not_run_the_command_when_the_injection_failed(tmp_path):
    r, ran = run(["bash", WITH, "50", "0"], tmp_path, docker_ok=False)
    assert r.returncode == 2 and not ran and "could not be injected" in r.stderr


@pytest.mark.skipif(not os.path.exists(WITH), reason="scripts not in the tree")
def test_with_s3_latency_still_runs_the_command_when_the_proxy_exists_and_for_pass_through(tmp_path):
    r, ran = run(["bash", WITH, "50", "0"], tmp_path, docker_ok=True)
    assert r.returncode == 0 and ran
    sub = tmp_path / "passthrough"
    sub.mkdir()
    r, ran = run(["bash", WITH, "0", "0"], sub, docker_ok=False)
    assert ran  # mean 0 is a pass-through: no injection to fail
