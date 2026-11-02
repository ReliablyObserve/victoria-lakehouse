"""The port block of the API matrix stack is chosen by probing, with one re-attempt on another block when the ports are busy.

On a GitHub-hosted runner the block derived from the project name (38300..38762) lies inside Linux's ephemeral range (32768..60999), and
port 38762 was busy (measured: job 125487988500 of PR 853): the run stopped with `StackBusy` (exit 4) and the check went red on a pull request that
was fine. The holder was not identified; most likely a loopback socket left by the preceding harness-test step (inferred: that pytest step ended
19:06:57 and the StackBusy came at 19:07:01).
"""
import json
import os
import sys
import zlib
from types import SimpleNamespace

import pytest

import api_proof
import comment
import stack

SPAN = max(max(stack.P.PORT_OFFSETS.values()), max(stack.EXTRA_OFFSETS.values()))


def free(_ports):
    return []


def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def read_text(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


# ------------------------------------------------------------------------------------------------ choosing a block
def test_the_ports_of_a_block_are_the_six_lakehouse_and_hot_ports_and_the_overlay_ones():
    ports = stack.ports_at(30000)
    assert len(ports) == 9 and 30000 + 428 in ports and 30000 + 462 in ports and 30000 + 433 in ports and 30000 + 300 not in ports  # no grafana


def test_the_first_free_block_is_taken_and_a_busy_port_moves_on_to_the_next_block():
    first = stack.pick_port_base(busy=free, low=49152)
    assert first in stack.allowed_bases()
    busy_first = lambda ports: [p for p in ports if first <= p <= first + SPAN][:1]  # noqa: E731 - one port of the first block is taken
    second = stack.pick_port_base(busy=busy_first, low=49152)
    assert second != first and second in stack.allowed_bases()


def test_the_block_found_busy_is_excluded_from_the_re_attempt():
    first = stack.pick_port_base(busy=free, low=49152)
    assert stack.pick_port_base(exclude=(first,), busy=free, low=49152) != first


def test_blocks_below_the_ephemeral_range_of_the_host_come_first():
    """The operating system hands out ports from `low` up to 60999 to outgoing connections; a service port in there can be taken at any moment."""
    low = 32768
    picked = stack.pick_port_base(busy=free, low=low)
    assert picked + SPAN < low
    assert all(b + SPAN < low for b in stack.candidate_bases(low=low)[:5])
    below = [b for b in stack.candidate_bases(low=low) if b + SPAN < low]
    above = [b for b in stack.candidate_bases(low=low) if b + SPAN >= low]
    assert below and above and stack.candidate_bases(low=low) == below + above


def test_every_candidate_block_busy_is_an_error_naming_how_many_were_probed():
    always = lambda ports: [ports[0]] if ports else []  # noqa: E731
    with pytest.raises(stack.NoFreeBlock) as e:
        stack.pick_port_base(busy=always, low=49152)
    assert str(len(stack.candidate_bases(low=49152))) in str(e.value)


def test_the_ephemeral_range_is_read_from_the_host_and_has_a_default(tmp_path):
    f = tmp_path / "range"
    f.write_text("32768\t60999\n")
    assert stack.ephemeral_low(str(f)) == 32768
    assert stack.ephemeral_low(str(tmp_path / "missing")) == stack.P.EPHEMERAL_FLOOR
    f.write_text("garbage")
    assert stack.ephemeral_low(str(f)) == stack.P.EPHEMERAL_FLOOR


def test_two_projects_start_on_different_blocks_below_the_ephemeral_range(monkeypatch):
    """On Linux the block derived from the name is never below the range, so without a rotation every project would start at the same block."""
    firsts = {}
    for name in ("lh-ab", "lh-apimx", "lh-other", "lh-x1"):
        monkeypatch.setattr(stack, "PROJECT", name)
        c = stack.candidate_bases(low=32768)
        below = [b for b in stack.allowed_bases() if b + SPAN < 32768]
        firsts[name] = c[0]
        derived = stack.P.port_base(name)
        assert c[0] == (derived if derived in below else below[zlib.crc32(name.encode()) % len(below)]), name
        assert sorted(c) == sorted(stack.allowed_bases()), "a rotation is a permutation: no block lost, none doubled"
    assert firsts["lh-ab"] != firsts["lh-apimx"] and len(set(firsts.values())) >= 3


def test_the_blocks_above_the_range_start_at_the_block_derived_from_the_name(monkeypatch):
    monkeypatch.setattr(stack, "PROJECT", "lh-apimx")  # 38300, inside the ephemeral range of Linux
    above = [b for b in stack.candidate_bases(low=32768) if b + SPAN >= 32768]
    assert stack.P.port_base("lh-apimx") == 38300 and above[0] == 38300


def test_candidates_keep_clear_of_the_reserved_ranges_of_the_other_stacks():
    for b in stack.candidate_bases(low=32768):
        assert not any(b <= hi and b + SPAN >= lo for lo, hi in stack.P.RESERVED_PORTS), b


# ------------------------------------------------------------------------------------------------ the re-attempt
BUSY_MD = ("<!-- lh-api-matrix -->\n### x: did not complete\n\nThe run stopped before a result: `StackBusy: " + stack.PORTS_BUSY + " on 127.0.0.1: [38762]; set ...`.\n")


class Fake:
    """Stands in for pr_smoke.py: returns the scripted exit codes and leaves the markdown a busy run leaves."""

    def __init__(self, tmp_path, codes, busy_text=BUSY_MD):
        self.out = str(tmp_path / "out")
        os.makedirs(self.out)
        self.codes, self.busy_text, self.bases = list(codes), busy_text, []

    def __call__(self, base):
        self.bases.append(base)
        rc = self.codes.pop(0)
        with open(os.path.join(self.out, "api-matrix.md"), "w", encoding="utf-8") as f:
            f.write(self.busy_text if rc == 4 else "<!-- lh-api-matrix -->\n### NEUTRAL\n")
        return rc


def pick_from(*bases):
    queue = list(bases)

    def pick(exclude=()):
        b = next(x for x in queue if x not in exclude)
        queue.remove(b)
        return b
    return pick


def test_a_run_that_does_not_find_busy_ports_is_not_re_attempted(tmp_path):
    fake = Fake(tmp_path, [0])
    rc = api_proof.run_with_retry(fake.out, pick_from(24000, 24100), fake)
    assert rc == 0
    assert fake.bases == [24000] and not os.path.exists(os.path.join(fake.out, "reattempt.json"))


def test_a_regression_is_never_re_attempted(tmp_path):
    fake = Fake(tmp_path, [1])
    rc = api_proof.run_with_retry(fake.out, pick_from(24000, 24100), fake)
    assert rc == 1 and fake.bases == [24000]


def test_busy_ports_are_re_attempted_once_on_another_block_and_recorded(tmp_path):
    fake = Fake(tmp_path, [4, 0])
    rc = api_proof.run_with_retry(fake.out, pick_from(24000, 24100), fake)
    assert rc == 0
    assert fake.bases == [24000, 24100]
    rec = read_json(os.path.join(fake.out, "reattempt.json"))
    assert rec == {"first_base": 24000, "busy_ports": [38762], "second_base": 24100, "outcome": "second attempt exit 0"}


def test_busy_again_after_the_re_attempt_stays_exit_four_and_a_third_attempt_is_never_made(tmp_path):
    fake = Fake(tmp_path, [4, 4, 0])
    rc = api_proof.run_with_retry(fake.out, pick_from(24000, 24100, 24200), fake)
    assert rc == 4
    assert fake.bases == [24000, 24100]
    assert read_json(os.path.join(fake.out, "reattempt.json"))["outcome"] == "busy again"


def test_no_other_free_block_for_the_re_attempt_keeps_exit_four(tmp_path):
    fake = Fake(tmp_path, [4])

    def pick(exclude=()):
        if exclude:
            raise stack.NoFreeBlock("none left")
        return 24000
    rc = api_proof.run_with_retry(fake.out, pick, fake)
    assert rc == 4 and fake.bases == [24000]
    assert read_json(os.path.join(fake.out, "reattempt.json"))["outcome"] == "no other free block"


def test_a_stack_busy_for_another_reason_is_not_a_port_collision_and_is_not_re_attempted(tmp_path):
    other = "<!-- lh-api-matrix -->\n`StackBusy: a stack of project lh-apimx is already running`\n"
    fake = Fake(tmp_path, [4, 0], busy_text=other)
    rc = api_proof.run_with_retry(fake.out, pick_from(24000, 24100), fake)
    assert rc == 4 and fake.bases == [24000]


def test_no_free_block_at_all_writes_a_did_not_complete_comment_with_exit_four(tmp_path):
    out = str(tmp_path / "out")

    def pick(exclude=()):
        raise stack.NoFreeBlock("every one of 46 candidate port blocks has a port in use")

    def attempt(base):
        raise AssertionError("nothing may be started")
    rc = api_proof.run_with_retry(out, pick, attempt)
    assert rc == 4
    text = read_text(os.path.join(out, "api-matrix.md"))
    assert "did not complete" in text and "46 candidate" in text and "Infrastructure state, not a regression" in text
    assert read_json(os.path.join(out, "api-verdict.json"))["exit"] == 4


# ------------------------------------------------------------------------------------------------ what the comment says
def test_a_busy_stack_is_called_an_infrastructure_state_and_a_plain_failure_is_not():
    text, v = comment.errored({}, {}, "StackBusy: ports", comment.API_MARKER, comment.API_TITLE, infra=True)
    assert "Infrastructure state, not a regression" in text and v["exit"] == 3
    text, _ = comment.errored({}, {}, "KeyError: x", comment.API_MARKER, comment.API_TITLE)
    assert "Infrastructure state" not in text


def test_the_bundle_says_when_the_run_was_re_attempted(tmp_path):
    api = tmp_path / "api"
    api.mkdir()
    (api / "api-matrix.md").write_text("<!-- lh-api-matrix -->\n### NEUTRAL API matrix: no change in any endpoint\n")
    (api / "api-verdict.json").write_text('{"exit": 0}')
    (api / "reattempt.json").write_text(json.dumps({"first_base": 38300, "busy_ports": [38762], "second_base": 24000, "outcome": "recovered"}))
    out = tmp_path / "o"
    api_proof.main(["bundle", "--out-dir", str(out), "--api-dir", str(api), "--pr-number", "5", "--head-sha", "a" * 40])
    body = (out / "api-matrix.md").read_text()
    assert "Re-attempted once" in body and "38762" in body and "24000" in body and body.startswith("<!-- lh-api-matrix -->\n")


def test_the_constants_api_proof_keeps_are_the_ones_of_stack():
    assert api_proof.EXIT_BUSY == stack.EXIT_BUSY and api_proof.PORTS_BUSY == stack.PORTS_BUSY


def test_the_text_of_the_ports_preflight_is_what_the_wrapper_looks_for(tmp_path, monkeypatch):
    monkeypatch.setattr(stack, "running", lambda: False)
    monkeypatch.setattr(stack, "check_block", lambda: None)
    monkeypatch.setattr(stack.P, "busy_ports", lambda ports: [38762])
    with pytest.raises(stack.StackBusy) as e:
        stack.preflight()
    out = tmp_path
    (out / "api-matrix.md").write_text(f"`StackBusy: {e.value}`")
    assert api_proof.ports_busy(str(out)) == [38762]


def test_the_options_after_the_double_dash_reach_pr_smoke_unchanged(monkeypatch):
    seen = {}
    monkeypatch.setattr(api_proof, "cmd_run", lambda a: seen.update(out=a.out, smoke=a.smoke) or 0)
    rc = api_proof.main(["run", "--out", "o", "--", "--base", "abc", "--head", "HEAD", "--rounds", "3"])
    assert rc == 0
    assert seen == {"out": "o", "smoke": ["--base", "abc", "--head", "HEAD", "--rounds", "3"]}


def test_pr_smoke_calls_a_busy_stack_an_infrastructure_state_and_a_crash_not(tmp_path, monkeypatch):
    import test_api_only as t
    t.Fake(monkeypatch, tmp_path, running=True, base_rows=[])
    rc = t.run(tmp_path)
    busy_text = read_text(str(tmp_path / "out" / "api-matrix.md"))
    assert rc == 4 and "Infrastructure state, not a regression" in busy_text
    t.Fake(monkeypatch, tmp_path / "x", boom=RuntimeError("boom"), base_rows=[])
    (tmp_path / "x").mkdir()
    rc = t.run(tmp_path / "x")
    crash_text = read_text(str(tmp_path / "x" / "out" / "api-matrix.md"))
    assert rc == 3 and "Infrastructure state" not in crash_text


# ------------------------------------------------------------------------------------------------ review follow-up of PR 854
def run_rc(monkeypatch, tmp_path, *argv):
    out = tmp_path / "out"
    rc = api_proof.main(["run", "--out", str(out), "--", *argv])
    return rc, out


def test_a_crash_of_the_run_is_a_did_not_complete_with_exit_three_and_its_files(tmp_path, monkeypatch):
    """A crash (here: the probe cannot bind) exited 1, which the gate reports as a regression."""
    def boom(exclude=()):
        raise OSError("probe failed")
    monkeypatch.setattr(stack, "pick_port_base", boom)
    rc, out = run_rc(monkeypatch, tmp_path, "--base", "x")
    assert rc == 3
    text = read_text(str(out / "api-matrix.md"))
    assert "did not complete" in text and "OSError" in text and "probe failed" in text and "Infrastructure state" not in text
    assert read_json(str(out / "api-verdict.json"))["exit"] == 3


def test_an_interrupt_leaves_the_files_and_still_propagates(tmp_path, monkeypatch):
    def interrupted(exclude=()):
        raise KeyboardInterrupt
    monkeypatch.setattr(stack, "pick_port_base", interrupted)
    with pytest.raises(KeyboardInterrupt):
        api_proof.main(["run", "--out", str(tmp_path / "out"), "--", "--base", "x"])
    assert read_json(str(tmp_path / "out" / "api-verdict.json"))["exit"] == 3


def test_a_failed_import_of_the_harness_is_a_crash_like_any_other(tmp_path, monkeypatch):
    """`import stack` reads the compose files and needs PyYAML; a failure there exited 1 as well."""
    monkeypatch.setitem(sys.modules, "stack", None)  # makes `import stack` raise ImportError
    rc, out = run_rc(monkeypatch, tmp_path, "--base", "x")
    assert rc == 3 and "import of stack halted" in read_text(str(out / "api-matrix.md"))
    assert read_json(str(out / "api-verdict.json"))["exit"] == 3


def test_a_stray_system_exit_of_the_wrapper_is_exit_three_never_a_pass(tmp_path, monkeypatch):
    monkeypatch.setattr(stack, "pick_port_base", lambda exclude=(): sys.exit(0))
    rc, out = run_rc(monkeypatch, tmp_path, "--base", "x")
    assert rc == 3 and read_json(str(out / "api-verdict.json"))["exit"] == 3


def test_an_argument_error_of_the_wrapper_is_exit_three_not_the_invalid_run_two(tmp_path):
    assert api_proof.main(["run", "--", "--base", "x"]) == 3  # no --out: argparse exits 2


def test_the_bundle_of_a_crashed_run_is_incomplete_not_a_regression(tmp_path, monkeypatch):
    monkeypatch.setattr(stack, "pick_port_base", lambda exclude=(): (_ for _ in ()).throw(OSError("probe failed")))
    rc, out = run_rc(monkeypatch, tmp_path, "--base", "x")
    o = tmp_path / "o"
    api_proof.main(["bundle", "--out-dir", str(o), "--api-dir", str(out), "--pr-number", "5", "--head-sha", "a" * 40])
    assert rc == 3 and json.loads((o / "manifest.json").read_text())["comments"][0]["verdict"] == "incomplete"


REAL_REPORT_HOLDERS = api_proof.report_holders


@pytest.fixture(autouse=True)
def no_host_ss(monkeypatch):
    """The unit tests do not run `ss` on the host; the test of the holder report puts the real function back."""
    monkeypatch.setattr(api_proof, "report_holders", lambda ports: None)


class Procs:
    """Stands in for subprocess.run: pr_smoke.py runs return the scripted codes and leave the markdown a run leaves; `ss` runs return the scripted text."""

    def __init__(self, out, codes, ss_text="", ss_error=None):
        self.out, self.codes, self.calls, self.ss_calls, self.ss_text, self.ss_error = out, list(codes), [], [], ss_text, ss_error

    def __call__(self, argv, **kw):
        if argv[0] == "ss":
            self.ss_calls.append(argv)
            if self.ss_error:
                raise self.ss_error
            return SimpleNamespace(returncode=0, stdout=self.ss_text, stderr="")
        self.calls.append((argv, kw.get("env")))
        rc = self.codes.pop(0)
        os.makedirs(self.out, exist_ok=True)
        with open(os.path.join(self.out, "api-matrix.md"), "w", encoding="utf-8") as f:
            f.write(BUSY_MD if rc == 4 else "<!-- lh-api-matrix -->\n### NEUTRAL\n\n<sub>stamp</sub>\n")
        return SimpleNamespace(returncode=rc)


def test_pr_smoke_gets_api_only_out_its_options_after_the_double_dash_and_the_block_of_each_attempt(tmp_path, monkeypatch):
    """Mutants: PROOF_PORT_BASE dropped from the child environment, `--api-only` dropped, the options reordered."""
    out = str(tmp_path / "out")
    procs = Procs(out, [4, 0])
    monkeypatch.setattr(api_proof.subprocess, "run", procs)
    picks = iter([24000, 24100])
    monkeypatch.setattr(stack, "pick_port_base", lambda exclude=(): next(picks))
    monkeypatch.setenv("PROOF_PORT_BASE", "1")  # a stale value of the parent must not reach the child
    rc = api_proof.main(["run", "--out", out, "--", "--base", "abc", "--head", "HEAD", "--rounds", "3"])
    assert rc == 0 and len(procs.calls) == 2
    want = [sys.executable, api_proof.PR_SMOKE, "--api-only", "--out", out, "--base", "abc", "--head", "HEAD", "--rounds", "3"]
    for (argv, env), base in zip(procs.calls, ("24000", "24100")):
        assert argv == want
        assert env["PROOF_PORT_BASE"] == base and env["PATH"] == os.environ["PATH"]


def test_a_second_attempt_that_dies_without_writing_does_not_reuse_the_busy_files_of_the_first(tmp_path, monkeypatch):
    out = str(tmp_path / "out")
    procs = Procs(out, [4, 3])

    def second_writes_nothing(argv, **kw):
        if argv[0] != "ss" and len(procs.calls) == 1:
            procs.calls.append((argv, kw.get("env")))
            return SimpleNamespace(returncode=3)
        return procs(argv, **kw)
    for name in api_proof.DATA_FILES:  # what a busy first attempt leaves
        os.makedirs(out, exist_ok=True)
        with open(os.path.join(out, name), "w", encoding="utf-8") as f:
            f.write("{}")
    monkeypatch.setattr(api_proof.subprocess, "run", second_writes_nothing)
    picks = iter([24000, 24100])
    monkeypatch.setattr(stack, "pick_port_base", lambda exclude=(): next(picks))
    rc = api_proof.main(["run", "--out", out, "--", "--base", "abc"])
    assert rc == 3
    for name in ("api-matrix.md", "api-verdict.json", *api_proof.DATA_FILES):
        assert not os.path.exists(os.path.join(out, name)), name
    o = tmp_path / "o"
    api_proof.main(["bundle", "--out-dir", str(o), "--api-dir", out, "--pr-number", "5", "--head-sha", "a" * 40])
    body = (o / "api-matrix.md").read_text()
    assert "did not complete" in body and "StackBusy" not in body and "(recovered)" not in body and "second attempt exit 3" in body


@pytest.mark.parametrize("second,outcome", [(0, "second attempt exit 0"), (1, "second attempt exit 1"), (3, "second attempt exit 3"), (4, "busy again")])
def test_the_outcome_of_the_second_attempt_is_its_exit_code_not_a_recovery(tmp_path, second, outcome):
    fake = Fake(tmp_path, [4, second])
    api_proof.run_with_retry(fake.out, pick_from(24000, 24100), fake)
    assert read_json(os.path.join(fake.out, "reattempt.json"))["outcome"] == outcome


def test_the_re_attempt_note_comes_before_the_footer_so_the_section_order_holds(tmp_path):
    api = tmp_path / "api"
    api.mkdir()
    text, v = comment.errored({}, {}, "x", comment.API_MARKER, comment.API_TITLE)
    (api / "api-matrix.md").write_text(text)
    (api / "api-verdict.json").write_text('{"exit": 3}')
    (api / "reattempt.json").write_text(json.dumps({"first_base": 38300, "busy_ports": [38762], "second_base": 24000, "outcome": "second attempt exit 3"}))
    o = tmp_path / "o"
    api_proof.main(["bundle", "--out-dir", str(o), "--api-dir", str(api), "--pr-number", "5", "--head-sha", "a" * 40])
    body = (o / "api-matrix.md").read_text()
    assert body.index("Re-attempted once") < body.index("<sub>") and body.rstrip().endswith("</sub>")
    assert body.startswith("<!-- lh-api-matrix -->\n")


def test_the_holder_of_a_busy_port_is_named_in_the_log_and_a_missing_ss_never_fails_the_step(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(api_proof, "report_holders", REAL_REPORT_HOLDERS)
    fake = Fake(tmp_path, [4, 0])
    procs = Procs(fake.out, [], ss_text='LISTEN 0 4096 127.0.0.1:38762 0.0.0.0:* users:(("docker-proxy",pid=1,fd=4))\n')
    monkeypatch.setattr(api_proof.subprocess, "run", procs)
    assert api_proof.run_with_retry(fake.out, pick_from(24000, 24100), fake) == 0
    assert procs.ss_calls == [["ss", "-Htanp", "( sport = :38762 )"]]
    assert "docker-proxy" in capsys.readouterr().err
    for i, err in enumerate((FileNotFoundError("ss"), OSError("x"))):
        fake = Fake(tmp_path / str(i), [4, 0])
        monkeypatch.setattr(api_proof.subprocess, "run", Procs(fake.out, [], ss_error=err))
        assert api_proof.run_with_retry(fake.out, pick_from(24000, 24100), fake) == 0
