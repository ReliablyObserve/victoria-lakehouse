#!/usr/bin/env python3
"""The `prev` target of the PR proof: the last release users run, resolved, pinned by digest and cached by tag.

A pull request is measured against `main` and hot VictoriaLogs/VictoriaTraces. `prev` adds the release that is
the newest ancestor of the commit the PR is tested on top of, so "better than main" can also be told from
"better than what users run". This module only finds and fetches it; starting it with the flags of its own
tree and comparing it belongs to later changes.

  prev.py tag      [--rev HEAD^1]              the release tag, and why (no network, no docker)
  prev.py digests  [--rev HEAD^1]              the tag and the digest GHCR reports for each binary (anonymous)
  prev.py prepare  [--rev HEAD^1] --out FILE   tag, digests, pull (else build from the tag), cache, record
  prev.py line     FILE                        the one verdict phrase of a record

Which release. The newest `vMAJOR.MINOR.PATCH` tag that is an ancestor of --rev (default `HEAD^1`: in a pull
request run HEAD is the merge commit and its first parent is `main`; pass --rev HEAD on a `main` run). Not
"latest release" from the API: a backport branch or a release cut after the merge-base is not an ancestor and
is not chosen. Pre-release and `-fips` tags are not releases. The ancestor rule is
scripts/ci/check_changelog_pr.py `release_tags`, the one the release job uses for "the previous release".

Which image. `ghcr.io/reliablyobserve/victoria-lakehouse/lakehouse-{logs,traces}:<tag>`, resolved once to an
OCI index digest over the registry API (anonymous token), then pulled BY DIGEST. A git tag is immutable, a
registry tag is not, so the digest of the first resolution is recorded in the cache (`<cache>/<tag>/record.json`)
and a later resolution that differs is a harness error (state `invalid`), never silently re-pinned. When the
registry has no image for the tag the image is built from a detached worktree of the tag, with the Dockerfiles
and pins of that tree, and kept as a tarball cached by tag, pinned by its sha256.

State vocabulary (record["state"]; one of these, never a bare pass):
  evaluated       prev is a usable target: a tag and both images are present and the tag's tree has the flags file
  not-evaluated   no usable prev, `reason` says why (REASONS). It annotates a verdict, it does not fail one
  invalid         the harness itself is wrong (a digest that changed under a tag, a pulled image of another digest)
The record also carries `flags_contract` (present | missing) separately, so a tag that predates the flags file
still shows its tag and digests.

Exit status: 0 evaluated, 10 not evaluated, 2 invalid, 64 usage.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import abreg  # noqa: E402,F401  (puts the repository root on sys.path)
from scripts.ci import check_changelog_pr as cc  # noqa: E402

REGISTRY = "https://ghcr.io"
IMAGE_REPO = "reliablyobserve/victoria-lakehouse"  # <registry>/<IMAGE_REPO>/lakehouse-<signal>
SIGNALS = ("logs", "traces")
FLAGS_FILE = "scripts/proof/target-flags.json"  # introduced by the per-target flags contract; a tag without it predates the contract
DEFAULT_REV = "HEAD^1"
SCHEMA = 1
INDEX_ACCEPT = ", ".join((
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
))
DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
RETRIES = 2  # extra attempts for a registry answer that is a 5xx or no answer
RETRY_PAUSE_S = 2.0
TIMEOUT_S = 20.0

EVALUATED, NOT_EVALUATED, INVALID = "evaluated", "not-evaluated", "invalid"
EXIT = {EVALUATED: 0, NOT_EVALUATED: 10, INVALID: 2}
EXIT_USAGE = 64

# reason -> the phrase printed after "prev not evaluated:". Every state other than `evaluated` carries one.
REASONS = {
    "base-unresolved": "the base revision does not resolve",
    "shallow-clone": "the clone is shallow, release tags cannot be told from history (fetch with full history and tags)",
    "no-release-tag": "no release tag is an ancestor of the base",
    "tag-older-than-flags-contract": "the tag predates the proof flags contract",
    "image-not-found": "the registry has no image for the tag and the fallback build is off",
    "registry-unauthorized": "the registry refused an anonymous pull token",
    "registry-unavailable": "the registry did not answer",
    "registry-bad-response": "the registry answered without a digest",
    "pull-failed": "docker could not pull the image by digest",
    "build-failed": "the fallback build from the tag failed",
    "digest-changed": "the registry digest of the tag differs from the one recorded when it was first resolved",
    "digest-mismatch": "the pulled image has another digest than the one requested",
    "cache-corrupt": "the cached image of the tag does not match its recorded sha256",
}
INVALID_REASONS = ("digest-changed", "digest-mismatch", "cache-corrupt")


class PrevError(Exception):
    """A step that decides the state: `reason` is a key of REASONS."""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


def log(msg: str) -> None:
    print(f"[prev] {msg}", file=sys.stderr, flush=True)


# ------------------------------------------------------------------------------------------------ which release
def _git(*args: str, cwd=None) -> str:
    return cc.run_git(*args, cwd=cwd)


def resolve_base(rev: str, cwd=None) -> str:
    try:
        return _git("rev-parse", "--verify", "--quiet", f"{rev}^{{commit}}", cwd=cwd)
    except subprocess.CalledProcessError:
        raise PrevError("base-unresolved", rev) from None


def is_shallow(cwd=None) -> bool:
    try:
        return _git("rev-parse", "--is-shallow-repository", cwd=cwd) == "true"
    except subprocess.CalledProcessError:
        return False


def resolve_tag(rev: str = DEFAULT_REV, cwd=None) -> dict:
    """{"tag", "base_rev", "base_sha"} of the newest release tag that is an ancestor of rev; PrevError when there is none.

    A shallow clone is told apart from "no release yet": its history stops early, so it would report no tag or an old one."""
    sha = resolve_base(rev, cwd)
    if is_shallow(cwd):
        raise PrevError("shallow-clone", rev)
    tags = cc.release_tags(sha, cwd=cwd)
    if not tags:
        raise PrevError("no-release-tag", rev)
    return {"tag": tags[-1], "base_rev": rev, "base_sha": sha}


def flags_contract(tag: str, cwd=None) -> str:
    """present | missing: whether the tag's own tree has the file that lists the flags its binaries start with."""
    try:
        _git("cat-file", "-e", f"refs/tags/{tag}:{FLAGS_FILE}", cwd=cwd)
        return "present"
    except subprocess.CalledProcessError:
        return "missing"


# ------------------------------------------------------------------------------------------------ the registry
class RegistryError(Exception):
    def __init__(self, reason: str, detail: str = ""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


def image_name(signal: str) -> str:
    return f"{IMAGE_REPO}/lakehouse-{signal}"


def _open(req: urllib.request.Request):  # pragma: no cover - the network; tests replace it
    return urllib.request.urlopen(req, timeout=TIMEOUT_S)  # noqa: S310 - fixed https registry host


def _request(url: str, method: str, headers: dict, opener) -> tuple[int, dict, bytes]:
    """(status, lower-cased headers, body); a 4xx/5xx is returned, a connection failure raises RegistryError."""
    req = urllib.request.Request(url, method=method, headers=headers)
    try:
        with opener(req) as r:
            return r.status, {k.lower(): v for k, v in r.headers.items()}, r.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}, e.read()
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        raise RegistryError("registry-unavailable", f"{url}: {e}") from None


def _with_retries(fn, retries: int, pause_s: float, sleep):
    last: RegistryError | None = None
    for attempt in range(retries + 1):
        try:
            return fn()
        except RegistryError as e:
            if e.reason != "registry-unavailable":
                raise
            last = e
            if attempt < retries:
                sleep(pause_s)
    assert last is not None
    raise last


def registry_digest(signal: str, tag: str, registry: str = REGISTRY, opener=_open, retries: int = RETRIES,
                    pause_s: float = RETRY_PAUSE_S, sleep=time.sleep) -> str:
    """The digest the registry reports for <image>:<tag>, over an anonymous pull token.

    RegistryError reasons: image-not-found (404 on the manifest), registry-unauthorized (no token),
    registry-unavailable (5xx or no answer after the retries), registry-bad-response (no usable digest)."""
    repo = image_name(signal)

    def token() -> str:
        q = urllib.parse.urlencode({"service": urllib.parse.urlparse(registry).netloc, "scope": f"repository:{repo}:pull"})
        status, _h, body = _request(f"{registry}/token?{q}", "GET", {}, opener)
        if status in (401, 403):
            raise RegistryError("registry-unauthorized", f"token HTTP {status}")
        if status >= 500:
            raise RegistryError("registry-unavailable", f"token HTTP {status}")
        if status != 200:
            raise RegistryError("registry-unauthorized", f"token HTTP {status}")
        try:
            tok = json.loads(body)["token"]
        except (ValueError, KeyError, TypeError):
            raise RegistryError("registry-bad-response", "the token answer has no token") from None
        if not isinstance(tok, str) or not tok:
            raise RegistryError("registry-bad-response", "the token answer has an empty token")
        return tok

    def manifest(tok: str) -> str:
        status, headers, _b = _request(f"{registry}/v2/{repo}/manifests/{tag}", "HEAD",
                                       {"Authorization": f"Bearer {tok}", "Accept": INDEX_ACCEPT}, opener)
        if status == 404:
            raise RegistryError("image-not-found", f"{repo}:{tag}")
        if status in (401, 403):
            raise RegistryError("registry-unauthorized", f"manifest HTTP {status}")
        if status >= 500:
            raise RegistryError("registry-unavailable", f"manifest HTTP {status}")
        if status != 200:
            raise RegistryError("registry-bad-response", f"manifest HTTP {status}")
        digest = headers.get("docker-content-digest", "")
        if not DIGEST_RE.fullmatch(digest):
            raise RegistryError("registry-bad-response", f"no usable docker-content-digest ({digest!r})")
        return digest

    return _with_retries(lambda: manifest(_with_retries(token, retries, pause_s, sleep)), retries, pause_s, sleep)


# ------------------------------------------------------------------------------------------------ cache
def cache_key(tag: str, harness_hash: str = "", seed_hash: str = "") -> str:
    """Key of the deterministic outputs measured on a tag: they depend on the tag, the harness and the seed, nothing else."""
    return "proof-prev-" + tag + ("-" + hashlib.sha256(f"{harness_hash}\0{seed_hash}".encode()).hexdigest()[:12] if (harness_hash or seed_hash) else "")


def default_cache_dir() -> str:
    return os.environ.get("PROOF_PREV_CACHE") or os.path.join(os.environ.get("RUNNER_TEMP") or tempfile.gettempdir(), "lh-proof-prev")


def record_path(cache_dir: str, tag: str) -> str:
    return os.path.join(cache_dir, tag, "record.json")


def load_pins(cache_dir: str, tag: str) -> dict:
    """The pins recorded for a tag: {signal: {"source", "digest" | "sha256"}}. A missing or unreadable file is no pins."""
    try:
        with open(record_path(cache_dir, tag), encoding="utf-8") as f:
            pins = json.load(f).get("pins")
    except (OSError, ValueError):
        return {}
    return pins if isinstance(pins, dict) else {}


def save_pins(cache_dir: str, tag: str, pins: dict) -> None:
    write_atomic(record_path(cache_dir, tag), {"schema": SCHEMA, "tag": tag, "pins": pins})


def check_pin(pins: dict, signal: str, digest: str) -> None:
    """A registry digest that differs from the one recorded for the tag is a harness error."""
    known = (pins.get(signal) or {}).get("digest")
    if known and known != digest:
        raise PrevError("digest-changed", f"{signal}: recorded {known}, registry now reports {digest}")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_atomic(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


# ------------------------------------------------------------------------------------------------ docker (not exercised by unit tests)
def local_ref(prefix: str, signal: str) -> str:
    return f"{prefix}-{signal}:prev"


def sh(cmd: list[str], **kw) -> subprocess.CompletedProcess:  # pragma: no cover - runs docker/git
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def docker_pull(signal: str, digest: str, prefix: str) -> str:  # pragma: no cover - drives docker
    """Pull <image>@<digest>, check the pulled image reports that digest, tag it local_ref. Returns the local ref."""
    ref = f"{REGISTRY.split('//', 1)[1]}/{image_name(signal)}@{digest}"
    r = sh(["docker", "pull", "-q", ref])
    if r.returncode != 0:
        raise PrevError("pull-failed", f"{ref}: {r.stderr.strip()[:200]}")
    r = sh(["docker", "image", "inspect", "--format", "{{json .RepoDigests}}", ref])
    if r.returncode != 0 or digest not in r.stdout:
        raise PrevError("digest-mismatch", f"{ref}: pulled image reports {r.stdout.strip()[:200]}")
    local = local_ref(prefix, signal)
    sh(["docker", "tag", ref, local])
    return local


def docker_build(tag: str, signal: str, prefix: str, tar_path: str, cwd=None) -> str:  # pragma: no cover - drives git and docker
    """Build the image of the tag from a detached worktree of the tag, with that tree's own Dockerfile and pins, save it
    as a tarball. The worktree is removed again without --force. Returns the sha256 of the tarball."""
    root = cwd or abreg.ROOT
    local = local_ref(prefix, signal)
    with tempfile.TemporaryDirectory(prefix="lh-prev-tree-") as parent:
        tree = os.path.join(parent, tag)
        r = sh(["git", "worktree", "add", "--detach", tree, f"refs/tags/{tag}"], cwd=root)
        if r.returncode != 0:
            raise PrevError("build-failed", f"worktree of {tag}: {r.stderr.strip()[:200]}")
        try:
            r = sh(["docker", "build", "-q", "-f", os.path.join(tree, f"Dockerfile.{signal}"), "--build-arg", f"VERSION={tag}",
                    "-t", local, tree], env=dict(os.environ, GOWORK="off"))
            if r.returncode != 0:
                raise PrevError("build-failed", f"Dockerfile.{signal} of {tag}: {r.stderr.strip()[-300:]}")
            os.makedirs(os.path.dirname(tar_path), exist_ok=True)
            r = sh(["docker", "save", "-o", tar_path, local])
            if r.returncode != 0:
                raise PrevError("build-failed", f"docker save: {r.stderr.strip()[:200]}")
        finally:
            r = sh(["git", "worktree", "remove", tree], cwd=root)
            if r.returncode != 0:
                log(f"worktree {tree} was not removed ({r.stderr.strip()[:160]}); remove it by hand")
    return sha256_file(tar_path)


def docker_load(tar_path: str, signal: str, prefix: str) -> str:  # pragma: no cover - drives docker
    r = sh(["docker", "load", "-q", "-i", tar_path])
    if r.returncode != 0:
        raise PrevError("cache-corrupt", f"docker load {tar_path}: {r.stderr.strip()[:200]}")
    return local_ref(prefix, signal)


# ------------------------------------------------------------------------------------------------ prepare
def prepare(rev: str = DEFAULT_REV, cache_dir: str | None = None, cwd=None, pull: bool = True, build: bool = True,
            prefix: str = "lh-prev", registry: str = REGISTRY, opener=_open, retries: int = RETRIES, pause_s: float = RETRY_PAUSE_S,
            sleep=time.sleep, puller=None, builder=None, loader=None) -> dict:
    """Resolve the tag, the digests and (when pull is set) the images; return the record. Never raises for a missing prev:
    the state and reason say what happened. `puller`, `builder` and `loader` replace the docker steps in tests."""
    puller, builder, loader = puller or docker_pull, builder or docker_build, loader or docker_load
    cache_dir = cache_dir or default_cache_dir()
    rec: dict = {"schema": SCHEMA, "state": NOT_EVALUATED, "reason": "", "detail": "", "tag": "", "base_rev": rev, "base_sha": "",
                 "flags_contract": "", "digests": {}, "images": {}, "pins": {}, "cache_key": ""}

    def finish(reason: str, detail: str = "") -> dict:
        rec["reason"], rec["detail"] = reason, detail
        rec["state"] = INVALID if reason in INVALID_REASONS else NOT_EVALUATED
        return rec

    try:
        rec.update(resolve_tag(rev, cwd))
    except PrevError as e:
        return finish(e.reason, e.detail)
    tag = rec["tag"]
    rec["cache_key"] = cache_key(tag)
    rec["flags_contract"] = flags_contract(tag, cwd)
    pins = load_pins(cache_dir, tag)
    rec["pins"] = {k: dict(v) for k, v in pins.items() if k in SIGNALS and isinstance(v, dict)}
    not_found: list[str] = []
    for sig in SIGNALS:
        try:
            digest = registry_digest(sig, tag, registry, opener, retries, pause_s, sleep)
            check_pin(pins, sig, digest)
            rec["digests"][sig] = digest
            rec["pins"].setdefault(sig, {})["digest"] = digest
        except RegistryError as e:
            if e.reason != "image-not-found":
                return finish(e.reason, f"{sig}: {e.detail}")
            not_found.append(sig)
        except PrevError as e:
            return finish(e.reason, e.detail)
    if rec["digests"]:
        save_pins(cache_dir, tag, rec["pins"])  # the first resolution is the pin, whatever happens next
    if not_found and not build:
        return finish("image-not-found", f"{', '.join(not_found)} of {tag}")
    if rec["flags_contract"] == "missing":
        return finish("tag-older-than-flags-contract", tag)  # tag and digests stay in the record: the resolver itself worked
    if not pull:
        rec["state"] = EVALUATED
        return rec
    for sig in SIGNALS:
        try:
            if sig in rec["digests"]:
                rec["images"][sig] = {"source": "registry", "digest": rec["digests"][sig], "ref": puller(sig, rec["digests"][sig], prefix)}
                continue
            tar = os.path.join(cache_dir, tag, f"lakehouse-{sig}.tar")
            want = (pins.get(sig) or {}).get("sha256", "")
            if want and os.path.isfile(tar):
                if sha256_file(tar) != want:
                    raise PrevError("cache-corrupt", f"{tar}")
                rec["images"][sig] = {"source": "cache", "sha256": want, "ref": loader(tar, sig, prefix)}
            else:
                sha = builder(tag, sig, prefix, tar, cwd)
                rec["pins"].setdefault(sig, {})["sha256"] = sha
                rec["images"][sig] = {"source": "build", "sha256": sha, "ref": local_ref(prefix, sig)}
        except PrevError as e:
            return finish(e.reason, f"{sig}: {e.detail}")
    rec["state"] = EVALUATED
    save_pins(cache_dir, tag, rec["pins"])
    return rec


def verdict_phrase(rec: dict) -> str:
    """The words a verdict line shows for prev. An unevaluated prev is never worded like a pass."""
    state = rec.get("state")
    if state == EVALUATED:
        return f"prev {rec['tag']}"
    reason = rec.get("reason") or "unknown"
    text = REASONS.get(reason, reason)
    tag = f" ({rec['tag']})" if rec.get("tag") else ""
    if state == INVALID:
        return f"prev invalid{tag}: {text}"
    return f"prev not evaluated{tag}: {text}"


# ------------------------------------------------------------------------------------------------ cli
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("tag", "digests", "prepare"):
        p = sub.add_parser(name)
        p.add_argument("--rev", default=DEFAULT_REV, help="the commit prev must be an ancestor of (default HEAD^1; HEAD on a main run)")
        if name != "tag":
            p.add_argument("--registry", default=REGISTRY)
        if name == "prepare":
            p.add_argument("--out", required=True)
            p.add_argument("--cache-dir", default=None)
            p.add_argument("--no-pull", action="store_true", help="resolve the tag and digests only; start no docker")
            p.add_argument("--no-build", action="store_true", help="no fallback build: an image the registry lacks is not-evaluated")
            p.add_argument("--image-prefix", default="lh-prev", help="local image names <prefix>-<signal>:prev")
    p = sub.add_parser("line")
    p.add_argument("record")
    a = ap.parse_args(argv)
    if a.cmd == "line":
        with open(a.record, encoding="utf-8") as f:
            rec = json.load(f)
        print(verdict_phrase(rec))
        return EXIT.get(rec.get("state"), 2)
    if a.cmd == "tag":
        try:
            print(json.dumps(resolve_tag(a.rev), indent=1))
            return 0
        except PrevError as e:
            print(json.dumps({"state": NOT_EVALUATED, "reason": e.reason, "detail": e.detail}, indent=1))
            return EXIT[NOT_EVALUATED]
    rec = prepare(a.rev, getattr(a, "cache_dir", None), pull=not getattr(a, "no_pull", a.cmd == "digests"),
                  build=not getattr(a, "no_build", False), prefix=getattr(a, "image_prefix", "lh-prev"), registry=a.registry)
    if a.cmd == "prepare":
        write_atomic(a.out, rec)
    else:
        print(json.dumps(rec, indent=1, sort_keys=True))
    log(verdict_phrase(rec))
    return EXIT[rec["state"]]


if __name__ == "__main__":
    sys.exit(main())
