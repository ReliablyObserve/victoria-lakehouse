"""The port block of the API matrix stack is chosen by probing, with one re-attempt on another block when the ports are busy.

On a GitHub-hosted runner the block derived from the project name (38300..38762) lies inside Linux's ephemeral range (32768..60999), and
an outgoing connection of the runner held 38762: the run stopped with `StackBusy` (exit 4) and the check went red on a pull request that was fine.
"""
import json
import os

import pytest

import api_proof
import comment
import stack

SPAN = max(max(stack.P.PORT_OFFSETS.values()), max(stack.EXTRA_OFFSETS.values()))


def free(_ports):
    return []


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
    assert api_proof.run_with_retry(fake.out, pick_from(24000, 24100), fake) == 0
    assert fake.bases == [24000] and not os.path.exists(os.path.join(fake.out, "reattempt.json"))


def test_a_regression_is_never_re_attempted(tmp_path):
    fake = Fake(tmp_path, [1])
    assert api_proof.run_with_retry(fake.out, pick_from(24000, 24100), fake) == 1 and fake.bases == [24000]


def test_busy_ports_are_re_attempted_once_on_another_block_and_recorded(tmp_path):
    fake = Fake(tmp_path, [4, 0])
    assert api_proof.run_with_retry(fake.out, pick_from(24000, 24100), fake) == 0
    assert fake.bases == [24000, 24100]
    rec = json.load(open(os.path.join(fake.out, "reattempt.json")))
    assert rec == {"first_base": 24000, "busy_ports": [38762], "second_base": 24100, "outcome": "recovered"}


def test_busy_again_after_the_re_attempt_stays_exit_four_and_a_third_attempt_is_never_made(tmp_path):
    fake = Fake(tmp_path, [4, 4, 0])
    assert api_proof.run_with_retry(fake.out, pick_from(24000, 24100, 24200), fake) == 4
    assert fake.bases == [24000, 24100]
    assert json.load(open(os.path.join(fake.out, "reattempt.json")))["outcome"] == "busy again"


def test_no_other_free_block_for_the_re_attempt_keeps_exit_four(tmp_path):
    fake = Fake(tmp_path, [4])

    def pick(exclude=()):
        if exclude:
            raise stack.NoFreeBlock("none left")
        return 24000
    assert api_proof.run_with_retry(fake.out, pick, fake) == 4 and fake.bases == [24000]
    assert json.load(open(os.path.join(fake.out, "reattempt.json")))["outcome"] == "no other free block"


def test_a_stack_busy_for_another_reason_is_not_a_port_collision_and_is_not_re_attempted(tmp_path):
    other = "<!-- lh-api-matrix -->\n`StackBusy: a stack of project lh-apimx is already running`\n"
    fake = Fake(tmp_path, [4, 0], busy_text=other)
    assert api_proof.run_with_retry(fake.out, pick_from(24000, 24100), fake) == 4 and fake.bases == [24000]


def test_no_free_block_at_all_writes_a_did_not_complete_comment_with_exit_four(tmp_path):
    out = str(tmp_path / "out")

    def pick(exclude=()):
        raise stack.NoFreeBlock("every one of 46 candidate port blocks has a port in use")

    def attempt(base):
        raise AssertionError("nothing may be started")
    assert api_proof.run_with_retry(out, pick, attempt) == 4
    text = open(os.path.join(out, "api-matrix.md"), encoding="utf-8").read()
    assert "did not complete" in text and "46 candidate" in text and "Infrastructure state, not a regression" in text
    assert json.load(open(os.path.join(out, "api-verdict.json")))["exit"] == 4


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
    assert api_proof.main(["run", "--out", "o", "--", "--base", "abc", "--head", "HEAD", "--rounds", "3"]) == 0
    assert seen == {"out": "o", "smoke": ["--base", "abc", "--head", "HEAD", "--rounds", "3"]}
