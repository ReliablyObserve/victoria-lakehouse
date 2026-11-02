"""prev.py: which release is `prev`, what the registry says, the cache pins, and the state vocabulary.

Tag resolution runs against real throw-away git repositories (ancestry is what is being tested, so it is not faked);
the registry is a fake opener; docker steps are injected."""
import io
import json
import os
import re
import subprocess
import urllib.error

import pytest

import prev

D1 = "sha256:" + "1" * 64
D2 = "sha256:" + "2" * 64
D3 = "sha256:" + "3" * 64


# ------------------------------------------------------------------------------------------------ git fixtures
class Repo:
    def __init__(self, path):
        os.makedirs(path)
        self.path = str(path)
        self.n = 0
        self.git("init", "-q", "-b", "main")

    def git(self, *args, check=True):
        return subprocess.run(["git", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", "-c", "init.defaultBranch=main", *args],
                              cwd=self.path, check=check, capture_output=True, text=True).stdout.strip()

    def commit(self, files=None):
        self.n += 1
        for name, text in (files or {f"f{self.n}.txt": str(self.n)}).items():
            full = os.path.join(self.path, name)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w", encoding="utf-8") as f:
                f.write(text)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", f"c{self.n}")
        return self.git("rev-parse", "HEAD")

    def tag(self, name, annotated=False):
        if annotated:
            self.git("tag", "-a", "-m", name, name)
        else:
            self.git("tag", name)


@pytest.fixture
def repo(tmp_path, monkeypatch):
    for k, v in {"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
                 "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"}.items():
        monkeypatch.setenv(k, v)
    return Repo(tmp_path / "r")


def pr_merge(repo, pr_files=None):
    """main -> branch pr (one commit) -> merge commit on a detached HEAD, the way refs/pull/N/merge looks. HEAD^1 is main."""
    main = repo.git("rev-parse", "HEAD")
    repo.git("checkout", "-q", "-b", "pr")
    repo.commit(pr_files or {"pr.txt": "pr"})
    repo.git("checkout", "-q", "--detach", main)
    repo.git("merge", "-q", "--no-ff", "-m", "merge", "pr")
    return main


# ------------------------------------------------------------------------------------------------ which release
def test_the_newest_release_that_is_an_ancestor_of_the_base_wins_with_numeric_order(repo):
    for t in ("v0.9.0", "v0.90.0", "v0.100.0"):
        repo.commit()
        repo.tag(t)
    repo.commit()
    r = prev.resolve_tag("HEAD", cwd=repo.path)
    assert r["tag"] == "v0.100.0"  # not v0.90.0: the order is numeric
    assert r["base_sha"] == repo.git("rev-parse", "HEAD")


def test_default_base_is_the_first_parent_of_the_merge_commit(repo):
    repo.commit()
    repo.tag("v1.0.0")
    repo.commit()
    main = pr_merge(repo)
    assert prev.resolve_tag(cwd=repo.path)["base_sha"] == main
    assert prev.resolve_tag(cwd=repo.path)["tag"] == "v1.0.0"


def test_a_tag_on_the_pr_branch_is_not_an_ancestor_of_main_and_is_not_chosen(repo):
    repo.commit()
    repo.tag("v1.0.0")
    repo.commit()
    main = repo.git("rev-parse", "HEAD")
    repo.git("checkout", "-q", "-b", "pr")
    repo.commit()
    repo.tag("v9.9.9")  # a release cut on the PR branch (or after the merge-base): not what main contains
    repo.git("checkout", "-q", "--detach", main)
    repo.git("merge", "-q", "--no-ff", "-m", "merge", "pr")
    assert prev.resolve_tag(cwd=repo.path)["tag"] == "v1.0.0"
    assert prev.resolve_tag("HEAD", cwd=repo.path)["tag"] == "v9.9.9"  # on the merge commit itself it is reachable: the base matters


def test_a_backport_tag_on_a_side_branch_is_not_an_ancestor(repo):
    repo.commit()
    repo.tag("v1.0.0")
    fork = repo.commit()
    repo.commit()
    repo.tag("v1.1.0")
    repo.git("checkout", "-q", "-b", "backport", fork)
    repo.commit()
    repo.tag("v1.0.5")  # a hotfix of the old line: lower than v1.1.0 and not an ancestor of main either
    repo.git("checkout", "-q", "main")
    repo.commit()
    assert prev.resolve_tag("HEAD", cwd=repo.path)["tag"] == "v1.1.0"
    # from the backport branch the hotfix is the newest ancestor, and v1.1.0 is not an ancestor at all
    repo.git("checkout", "-q", "backport")
    assert prev.resolve_tag("HEAD", cwd=repo.path)["tag"] == "v1.0.5"


def test_a_hotfix_merged_back_into_main_counts_when_it_is_the_highest_version(repo):
    repo.commit()
    repo.tag("v1.0.0")
    fork = repo.commit()
    repo.git("checkout", "-q", "-b", "hotfix", fork)
    repo.commit()
    repo.tag("v1.0.1")
    repo.git("checkout", "-q", "main")
    repo.commit()
    repo.git("merge", "-q", "--no-ff", "-m", "merge hotfix", "hotfix")
    assert prev.resolve_tag("HEAD", cwd=repo.path)["tag"] == "v1.0.1"


@pytest.mark.parametrize("stray", ["v1.0.0-fips", "v2.0.0-rc1", "v2.0.0-rc.1", "v2", "v2.0", "foo-stray", "v1abc", "V3.0.0", "3.0.0", "v3.0.0.1"])
def test_only_plain_release_tags_are_releases(repo, stray):
    repo.commit()
    repo.tag("v1.0.0")
    repo.commit()
    repo.tag(stray)
    repo.commit()
    assert prev.resolve_tag("HEAD", cwd=repo.path)["tag"] == "v1.0.0"


def test_annotated_and_lightweight_tags_both_resolve(repo):
    repo.commit()
    repo.tag("v1.0.0")
    repo.commit()
    repo.tag("v1.1.0", annotated=True)
    repo.commit()
    assert prev.resolve_tag("HEAD", cwd=repo.path)["tag"] == "v1.1.0"
    assert prev.resolve_tag("HEAD~1", cwd=repo.path)["tag"] == "v1.1.0"
    assert prev.resolve_tag("HEAD~2", cwd=repo.path)["tag"] == "v1.0.0"


def test_no_tag_is_a_reason_not_an_exception_of_another_kind(repo):
    repo.commit()
    repo.commit()
    with pytest.raises(prev.PrevError) as e:
        prev.resolve_tag("HEAD", cwd=repo.path)
    assert e.value.reason == "no-release-tag"
    repo.tag("v2.0.0-rc1")  # only a pre-release: still no release
    with pytest.raises(prev.PrevError) as e:
        prev.resolve_tag("HEAD", cwd=repo.path)
    assert e.value.reason == "no-release-tag"


def test_an_unresolvable_base_is_its_own_reason(repo):
    repo.commit()
    repo.tag("v1.0.0")
    with pytest.raises(prev.PrevError) as e:
        prev.resolve_tag("HEAD^1", cwd=repo.path)  # the only commit has no parent
    assert e.value.reason == "base-unresolved"
    with pytest.raises(prev.PrevError) as e:
        prev.resolve_tag("no-such-ref", cwd=repo.path)
    assert e.value.reason == "base-unresolved"


def test_a_shallow_clone_is_told_apart_from_no_release(repo, tmp_path):
    repo.commit()
    repo.tag("v1.0.0")
    repo.commit()
    shallow = tmp_path / "shallow"
    subprocess.run(["git", "clone", "-q", "--depth", "1", "--no-tags", f"file://{repo.path}", str(shallow)], check=True, capture_output=True)
    with pytest.raises(prev.PrevError) as e:
        prev.resolve_tag("HEAD", cwd=str(shallow))
    assert e.value.reason == "shallow-clone"


def test_the_flags_contract_is_read_from_the_tags_own_tree(repo):
    repo.commit({"a.txt": "a"})
    repo.tag("v1.0.0")
    repo.commit({prev.FLAGS_FILE: "{}"})
    repo.tag("v1.1.0")
    repo.commit({prev.FLAGS_FILE: "changed after the tag"})
    assert prev.flags_contract("v1.0.0", cwd=repo.path) == "missing"
    assert prev.flags_contract("v1.1.0", cwd=repo.path) == "present"
    os.remove(os.path.join(repo.path, prev.FLAGS_FILE))  # the working tree is not consulted
    assert prev.flags_contract("v1.1.0", cwd=repo.path) == "present"


# ------------------------------------------------------------------------------------------------ the registry
class Resp:
    def __init__(self, status, headers=None, body=b""):
        self.status, self.headers, self._body = status, headers or {}, body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeRegistry:
    """An opener: answers the token and manifest requests from a table; records the requests."""

    def __init__(self, digests=None, token_status=200, manifest_status=None, manifest_headers=None, token_body=None, fail_first=0, drop=False):
        self.digests = digests or {}
        self.token_status, self.manifest_status = token_status, manifest_status or {}
        self.manifest_headers, self.token_body = manifest_headers, token_body
        self.fail_first, self.drop = fail_first, drop
        self.calls = []

    def __call__(self, req):
        self.calls.append((req.get_method(), req.full_url, dict(req.header_items())))
        if self.drop:
            raise urllib.error.URLError("connection refused")
        if self.fail_first > 0 and "/manifests/" in req.full_url:
            self.fail_first -= 1
            raise urllib.error.URLError("reset")
        if "/token" in req.full_url:
            if self.token_status != 200:
                raise urllib.error.HTTPError(req.full_url, self.token_status, "x", {}, io.BytesIO(b"{}"))
            return Resp(200, {}, json.dumps({"token": "T"} if self.token_body is None else self.token_body).encode())
        m = re.search(r"/lakehouse-(logs|traces)/manifests/(.+)$", req.full_url)
        sig, tag = m.group(1), m.group(2)
        status = self.manifest_status.get(sig)
        if status:
            raise urllib.error.HTTPError(req.full_url, status, "x", {}, io.BytesIO(b""))
        if (sig, tag) not in self.digests:
            raise urllib.error.HTTPError(req.full_url, 404, "not found", {}, io.BytesIO(b""))
        headers = self.manifest_headers if self.manifest_headers is not None else {"Docker-Content-Digest": self.digests[(sig, tag)]}
        return Resp(200, headers)


NO_SLEEP = dict(pause_s=0, sleep=lambda s: None)


def test_the_digest_comes_from_the_manifest_head_with_an_anonymous_bearer_token_and_the_oci_index_accept():
    reg = FakeRegistry({("logs", "v1.2.3"): D1})
    assert prev.registry_digest("logs", "v1.2.3", opener=reg, **NO_SLEEP) == D1
    (tm, tu, _), (mm, mu, mh) = reg.calls
    assert tm == "GET" and "scope=repository%3Areliablyobserve%2Fvictoria-lakehouse%2Flakehouse-logs%3Apull" in tu
    assert mm == "HEAD" and mu == "https://ghcr.io/v2/reliablyobserve/victoria-lakehouse/lakehouse-logs/manifests/v1.2.3"
    assert mh["Authorization"] == "Bearer T"
    assert "application/vnd.oci.image.index.v1+json" in mh["Accept"]


@pytest.mark.parametrize("reg,reason", [
    (FakeRegistry({}), "image-not-found"),
    (FakeRegistry({("logs", "v1.2.3"): D1}, token_status=401), "registry-unauthorized"),
    (FakeRegistry({("logs", "v1.2.3"): D1}, token_status=429), "registry-unauthorized"),
    (FakeRegistry({("logs", "v1.2.3"): D1}, token_status=503), "registry-unavailable"),
    (FakeRegistry({("logs", "v1.2.3"): D1}, manifest_status={"logs": 500}), "registry-unavailable"),
    (FakeRegistry({("logs", "v1.2.3"): D1}, manifest_status={"logs": 403}), "registry-unauthorized"),
    (FakeRegistry({("logs", "v1.2.3"): D1}, manifest_status={"logs": 418}), "registry-bad-response"),
    (FakeRegistry({("logs", "v1.2.3"): D1}, drop=True), "registry-unavailable"),
    (FakeRegistry({("logs", "v1.2.3"): D1}, manifest_headers={}), "registry-bad-response"),
    (FakeRegistry({("logs", "v1.2.3"): D1}, manifest_headers={"Docker-Content-Digest": "sha256:abc"}), "registry-bad-response"),
    (FakeRegistry({("logs", "v1.2.3"): D1}, token_body={"nope": 1}), "registry-bad-response"),
    (FakeRegistry({("logs", "v1.2.3"): D1}, token_body={"token": ""}), "registry-bad-response"),
])
def test_every_registry_failure_has_its_own_reason(reg, reason):
    with pytest.raises(prev.RegistryError) as e:
        prev.registry_digest("logs", "v1.2.3", opener=reg, **NO_SLEEP)
    assert e.value.reason == reason


def test_a_5xx_or_a_dropped_connection_is_retried_a_bounded_number_of_times():
    reg = FakeRegistry({("logs", "v1.2.3"): D1}, fail_first=2)
    assert prev.registry_digest("logs", "v1.2.3", opener=reg, retries=2, **NO_SLEEP) == D1
    reg = FakeRegistry({("logs", "v1.2.3"): D1}, fail_first=3)
    with pytest.raises(prev.RegistryError) as e:
        prev.registry_digest("logs", "v1.2.3", opener=reg, retries=2, **NO_SLEEP)
    assert e.value.reason == "registry-unavailable"
    assert sum(1 for c in reg.calls if "/manifests/" in c[1]) == 3  # one try and two retries, not an unbounded loop
    reg = FakeRegistry({}, manifest_status={})  # a 404 is an answer, not a failure: no retry
    with pytest.raises(prev.RegistryError):
        prev.registry_digest("logs", "v1.2.3", opener=reg, retries=2, **NO_SLEEP)
    assert sum(1 for c in reg.calls if "/manifests/" in c[1]) == 1


# ------------------------------------------------------------------------------------------------ prepare
class Docker:
    """Replaces the docker steps; records what ran."""

    def __init__(self, fail_build=False, pull_error=None):
        self.pulled, self.built, self.loaded = [], [], []
        self.fail_build, self.pull_error = fail_build, pull_error

    def puller(self, sig, digest, prefix):
        if self.pull_error:
            raise prev.PrevError(self.pull_error, sig)
        self.pulled.append((sig, digest))
        return f"{prefix}-{sig}:prev"

    def builder(self, tag, sig, prefix, tar, cwd):
        if self.fail_build:
            raise prev.PrevError("build-failed", sig)
        self.built.append((tag, sig))
        os.makedirs(os.path.dirname(tar), exist_ok=True)
        with open(tar, "wb") as f:
            f.write(f"image {tag} {sig}".encode())
        return prev.sha256_file(tar)

    def loader(self, tar, sig, prefix):
        self.loaded.append((tar, sig))
        return f"{prefix}-{sig}:prev"


@pytest.fixture
def released(repo):
    """A repository whose newest release v1.2.3 has the flags file; returns it with a cache dir."""
    repo.commit({"a.txt": "a"})
    repo.tag("v1.0.0")
    repo.commit({prev.FLAGS_FILE: "{}"})
    repo.tag("v1.2.3")
    repo.commit()
    return repo


def run(repo, tmp_path, reg, docker=None, **kw):
    docker = docker or Docker()
    return prev.prepare("HEAD", str(tmp_path / "cache"), cwd=repo.path, opener=reg, retries=0, puller=docker.puller, builder=docker.builder,
                        loader=docker.loader, **{"pause_s": 0, "sleep": lambda s: None, **kw}), docker


def recorded(tmp_path, tag="v1.2.3"):
    with open(prev.record_path(str(tmp_path / "cache"), tag), encoding="utf-8") as f:
        return json.load(f)


def full_registry(tag="v1.2.3"):
    return FakeRegistry({("logs", tag): D1, ("traces", tag): D2})


def test_prev_is_pulled_by_digest_and_the_digests_are_pinned_in_the_cache(released, tmp_path):
    rec, docker = run(released, tmp_path, full_registry())
    assert rec["state"] == prev.EVALUATED and rec["reason"] == ""
    assert rec["tag"] == "v1.2.3" and rec["flags_contract"] == "present"
    assert rec["digests"] == {"logs": D1, "traces": D2}
    assert docker.pulled == [("logs", D1), ("traces", D2)] and not docker.built
    assert rec["images"]["logs"] == {"source": "registry", "digest": D1, "ref": "lh-prev-logs:prev"}
    pinned = recorded(tmp_path)
    assert pinned["pins"] == {"logs": {"digest": D1}, "traces": {"digest": D2}}


def test_a_second_resolution_with_the_same_digests_is_fine_and_a_changed_digest_is_a_harness_error(released, tmp_path):
    run(released, tmp_path, full_registry())
    rec, docker = run(released, tmp_path, full_registry())
    assert rec["state"] == prev.EVALUATED
    moved = FakeRegistry({("logs", "v1.2.3"): D1, ("traces", "v1.2.3"): D3})
    rec, docker = run(released, tmp_path, moved)
    assert rec["state"] == prev.INVALID and rec["reason"] == "digest-changed"
    assert not docker.pulled  # nothing is pulled once the pin is broken
    pinned = recorded(tmp_path)
    assert pinned["pins"]["traces"]["digest"] == D2  # the first resolution stays the pin


def test_a_pull_that_yields_another_digest_is_invalid_not_a_pass(released, tmp_path):
    rec, _ = run(released, tmp_path, full_registry(), Docker(pull_error="digest-mismatch"))
    assert rec["state"] == prev.INVALID and rec["reason"] == "digest-mismatch"
    assert not rec["images"].get("traces")


def test_a_failed_pull_is_not_evaluated(released, tmp_path):
    rec, _ = run(released, tmp_path, full_registry(), Docker(pull_error="pull-failed"))
    assert rec["state"] == prev.NOT_EVALUATED and rec["reason"] == "pull-failed"


def test_a_tag_that_predates_the_flags_contract_keeps_its_tag_and_digests_but_is_not_evaluated(repo, tmp_path):
    repo.commit({"a.txt": "a"})
    repo.tag("v1.2.3")
    repo.commit()
    rec, docker = run(repo, tmp_path, full_registry())
    assert rec["state"] == prev.NOT_EVALUATED and rec["reason"] == "tag-older-than-flags-contract"
    assert rec["tag"] == "v1.2.3" and rec["flags_contract"] == "missing"
    assert rec["digests"] == {"logs": D1, "traces": D2}  # the resolver itself worked
    assert not docker.pulled and not docker.built and not rec["images"]


def test_no_tag_asks_the_registry_nothing_and_is_not_evaluated(repo, tmp_path):
    repo.commit()
    reg = full_registry()
    rec, docker = run(repo, tmp_path, reg)
    assert rec["state"] == prev.NOT_EVALUATED and rec["reason"] == "no-release-tag" and rec["tag"] == ""
    assert reg.calls == [] and not docker.pulled


def test_an_image_the_registry_lacks_is_built_from_the_tag_and_pinned_by_sha256(released, tmp_path):
    reg = FakeRegistry({("logs", "v1.2.3"): D1})  # no traces image for the tag
    rec, docker = run(released, tmp_path, reg)
    assert rec["state"] == prev.EVALUATED
    assert docker.pulled == [("logs", D1)] and docker.built == [("v1.2.3", "traces")]
    assert rec["images"]["traces"]["source"] == "build"
    sha = rec["images"]["traces"]["sha256"]
    pinned = recorded(tmp_path)
    assert pinned["pins"]["traces"] == {"sha256": sha} and pinned["pins"]["logs"] == {"digest": D1}
    # the next run loads the cached tarball instead of building again
    rec2, docker2 = run(released, tmp_path, FakeRegistry({("logs", "v1.2.3"): D1}))
    assert rec2["state"] == prev.EVALUATED and rec2["images"]["traces"]["source"] == "cache"
    assert docker2.loaded and not docker2.built


def test_a_cached_image_that_no_longer_matches_its_pin_is_invalid(released, tmp_path):
    run(released, tmp_path, FakeRegistry({("logs", "v1.2.3"): D1}))
    tar = tmp_path / "cache" / "v1.2.3" / "lakehouse-traces.tar"
    tar.write_bytes(b"tampered")
    rec, docker = run(released, tmp_path, FakeRegistry({("logs", "v1.2.3"): D1}))
    assert rec["state"] == prev.INVALID and rec["reason"] == "cache-corrupt"
    assert not docker.loaded


def test_a_failed_fallback_build_is_not_evaluated(released, tmp_path):
    rec, _ = run(released, tmp_path, FakeRegistry({}), Docker(fail_build=True))
    assert rec["state"] == prev.NOT_EVALUATED and rec["reason"] == "build-failed"
    assert "evaluated" not in prev.verdict_phrase(rec).replace("not evaluated", "")


def test_with_the_fallback_off_a_missing_image_is_not_evaluated(released, tmp_path):
    rec, docker = run(released, tmp_path, FakeRegistry({}), build=False)
    assert rec["state"] == prev.NOT_EVALUATED and rec["reason"] == "image-not-found"
    assert not docker.built


@pytest.mark.parametrize("reg,reason", [
    (FakeRegistry({}, token_status=401), "registry-unauthorized"),
    (FakeRegistry({}, drop=True), "registry-unavailable"),
    (FakeRegistry({("logs", "v1.2.3"): D1, ("traces", "v1.2.3"): D2}, manifest_headers={}), "registry-bad-response"),
])
def test_a_registry_outage_is_not_evaluated_and_does_not_fall_back_to_a_build(released, tmp_path, reg, reason):
    """A 404 means there is no image; an outage says nothing about it, and a build would swap the release recipe for another."""
    rec, docker = run(released, tmp_path, reg)
    assert rec["state"] == prev.NOT_EVALUATED and rec["reason"] == reason
    assert not docker.built and not docker.pulled


def test_resolve_only_mode_starts_no_docker(released, tmp_path):
    rec, docker = run(released, tmp_path, full_registry(), pull=False)
    assert rec["state"] == prev.EVALUATED and rec["images"] == {} and rec["digests"] == {"logs": D1, "traces": D2}
    assert not docker.pulled


def test_the_first_digest_is_pinned_even_when_the_run_stops_before_pulling(released, tmp_path):
    run(released, tmp_path, full_registry(), pull=False)
    moved = FakeRegistry({("logs", "v1.2.3"): D3, ("traces", "v1.2.3"): D2})
    rec, _ = run(released, tmp_path, moved)
    assert rec["state"] == prev.INVALID and rec["reason"] == "digest-changed"


# ------------------------------------------------------------------------------------------------ the vocabulary
def test_every_reason_the_module_raises_has_a_phrase():
    with open(prev.__file__, encoding="utf-8") as f:
        src = f.read()
    used = set(re.findall(r'(?:PrevError|RegistryError)\(\s*"([a-z-]+)"', src)) | set(re.findall(r'finish\(\s*"([a-z-]+)"', src))
    assert used, "no reason found: the scan is broken"
    assert used <= set(prev.REASONS), sorted(used - set(prev.REASONS))
    assert set(prev.INVALID_REASONS) <= set(prev.REASONS)
    for sig in ("image-not-found", "registry-unauthorized", "registry-unavailable", "registry-bad-response"):
        assert sig in prev.REASONS  # the registry reasons surface as the record's reason


def test_the_states_are_distinct_and_only_evaluated_exits_zero():
    assert len({prev.EVALUATED, prev.NOT_EVALUATED, prev.INVALID}) == 3
    assert prev.EXIT[prev.EVALUATED] == 0
    assert prev.EXIT[prev.NOT_EVALUATED] not in (0, 1, 2, 3) and prev.EXIT[prev.INVALID] == 2


@pytest.mark.parametrize("reason", sorted(prev.REASONS))
def test_a_record_without_a_usable_prev_is_never_worded_like_a_pass(reason):
    state = prev.INVALID if reason in prev.INVALID_REASONS else prev.NOT_EVALUATED
    text = prev.verdict_phrase({"state": state, "reason": reason, "tag": "v1.2.3"})
    assert text.startswith("prev not evaluated" if state == prev.NOT_EVALUATED else "prev invalid")
    assert prev.REASONS[reason] in text and "v1.2.3" in text
    assert "pass" not in text.lower()


def test_an_evaluated_record_is_worded_with_its_tag():
    assert prev.verdict_phrase({"state": prev.EVALUATED, "tag": "v1.2.3"}) == "prev v1.2.3"


def test_a_record_with_an_unknown_state_exits_non_zero(tmp_path, capsys):
    p = tmp_path / "rec.json"
    p.write_text(json.dumps({"state": "pass"}), encoding="utf-8")
    assert prev.main(["line", str(p)]) == 2
    p.write_text(json.dumps({"state": prev.NOT_EVALUATED, "reason": "no-release-tag"}), encoding="utf-8")
    assert prev.main(["line", str(p)]) == prev.EXIT[prev.NOT_EVALUATED]
    assert "prev not evaluated" in capsys.readouterr().out


def test_cache_key_depends_on_the_tag_the_harness_and_the_seed():
    assert prev.cache_key("v1.2.3") == "proof-prev-v1.2.3"
    a = prev.cache_key("v1.2.3", "h1", "s1")
    assert a != prev.cache_key("v1.2.3", "h2", "s1") != prev.cache_key("v1.2.3", "h1", "s2")
    assert a == prev.cache_key("v1.2.3", "h1", "s1") and a.startswith("proof-prev-v1.2.3-")


def test_prepare_writes_the_record_file_and_exits_by_state(released, tmp_path, monkeypatch):
    out = tmp_path / "out" / "prev.json"
    monkeypatch.setattr(prev, "prepare", lambda *a, **k: {"state": prev.NOT_EVALUATED, "reason": "no-release-tag", "tag": ""})
    assert prev.main(["prepare", "--out", str(out), "--cache-dir", str(tmp_path / "c")]) == prev.EXIT[prev.NOT_EVALUATED]
    assert json.loads(out.read_text(encoding="utf-8"))["reason"] == "no-release-tag"
