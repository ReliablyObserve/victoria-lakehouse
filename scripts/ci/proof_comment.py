#!/usr/bin/env python3
"""The PR proof commenter: validates what a pull request's CI produced and posts it as sticky comments.

Runs from the BASE branch only, in `.github/workflows/pr-proof-comment.yaml` (a `workflow_run` workflow). The
producers (`pr-quality`, `pr-proof`) run pull request code and upload one artifact, `lh-proof-comment`. Everything in
that artifact is untrusted data: this script is the only thing that reads it. The contract is documented in
docs/ci-proof-comment.md and scripts/ci/proof-comment-manifest.schema.json.

  proof_comment.py validate      --out DIR      artifact -> checked bundle (job `validate`, read-only token)
  proof_comment.py check-bundle  --bundle DIR   re-check a bundle and that the PR head is still current (write jobs)
  proof_comment.py comment       --bundle DIR --images-published true|false     upsert the sticky comments
  proof_comment.py dry-run       --source PATH --pr N                           render a fixture, no API calls

Inputs from the event reach this script through environment variables only (WR_*), are format-checked here, and are
cross-checked against the GitHub API. The PR number and head SHA in the artifact are claims, never facts.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import stat
import struct
import subprocess
import sys
import zipfile
import zlib

# ---------------------------------------------------------------------------------------------------------------
# The contract (mirrored in scripts/ci/proof-comment-manifest.schema.json; a test keeps the two in step)
# ---------------------------------------------------------------------------------------------------------------
SCHEMA_VERSION = 1
ARTIFACT_NAME = "lh-proof-comment"
BOT_LOGIN = "github-actions[bot]"
VISUALS_BRANCH = "pr-visuals"

# kind -> marker. The perf/api/quality markers are the constants of scripts/bench/ab (a test compares them).
MARKERS = {
    "pr-quality": "<!-- lh-pr-quality -->",
    "perf-smoke": "<!-- lh-perf-smoke -->",
    "api-matrix": "<!-- lh-api-matrix -->",
    "visual-proof": "<!-- lh-visual-proof -->",
}
# producing workflow file -> the kinds it may post. A pull request may edit its own copy of a workflow, so a
# workflow can only ever post the kinds of its own path, never another one's.
PRODUCERS = {
    ".github/workflows/pr-quality.yaml": frozenset({"pr-quality"}),
    ".github/workflows/pr-proof.yaml": frozenset({"perf-smoke", "api-matrix", "visual-proof"}),
}
VERDICTS = ("pass", "fail", "incomplete", "skipped")

MAX_MANIFEST_BYTES = 64 * 1024
MAX_BODY_BYTES = 256 * 1024
MAX_IMAGE_BYTES = 300_000  # the limit of scripts/proof/visual/publish.py
MAX_IMAGES = 80  # the limit of publish.py
MAX_IMAGE_PIXELS_SIDE = 8192
MAX_TOTAL_BYTES = 8 * 1024 * 1024
MAX_ZIP_BYTES = 20 * 1024 * 1024
MAX_ENTRIES = 120
MAX_ISSUE_REFS = 20
COMMENT_LIMIT = 65_000  # GitHub's limit is 65536 characters; this leaves headroom
MAX_PAGES = 50

SHA_RE = re.compile(r"[0-9a-f]{40}")
REPO_RE = re.compile(r"[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}")
IMAGE_NAME_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,60}\.png")
BODY_NAMES = {f"{k}.md": k for k in MARKERS}
WF_PATH_RE = re.compile(r"(\.github/workflows/[A-Za-z0-9_.-]+\.ya?ml)(?:@.*)?")
FIXTURE_ROOT = "scripts/ci/tests/testdata/proof_comment"

PNG_SIG = b"\x89PNG\r\n\x1a\n"
PNG_IEND = b"\x00\x00\x00\x00IEND\xaeB`\x82"

MANIFEST_KEYS = {"schema_version", "pr_number", "head_sha", "comments", "images", "allowed_issue_refs"}


class Reject(Exception):
    """The input breaks the contract or a cross-check. Nothing is posted; the job fails."""


class Skip(Exception):
    """A normal reason to post nothing (closed PR, superseded head). The job succeeds."""


class NoArtifact(Skip):
    """The producing run uploaded no `lh-proof-comment` artifact (or it expired)."""


def log(level: str, msg: str) -> None:
    """Workflow command; `msg` is escaped so data can never start another command or add a line."""
    msg = re.sub(r"[\x00-\x1f\x7f]", " ", str(msg))[:400].replace("%", "%25")
    print(f"::{level}::proof-comment: {msg}", flush=True)


# ---------------------------------------------------------------------------------------------------------------
# Reading the artifact: a zip or a directory, into {name: bytes}. Never extracted to disk, never trusted.
# ---------------------------------------------------------------------------------------------------------------
def allowed_name(name: str) -> bool:
    if name == "manifest.json" or name in BODY_NAMES:
        return True
    m = re.fullmatch(r"images/(.+)", name)
    return bool(m and IMAGE_NAME_RE.fullmatch(m.group(1)))


def size_cap(name: str) -> int:
    if name == "manifest.json":
        return MAX_MANIFEST_BYTES
    if name.startswith("images/"):
        return MAX_IMAGE_BYTES
    return MAX_BODY_BYTES


def _unsafe_path(name: str) -> bool:
    return (not name or name.startswith("/") or "\\" in name or "\x00" in name or ":" in name
            or any(p in ("", ".", "..") for p in name.split("/")))


def read_zip(data: bytes) -> dict:
    if len(data) > MAX_ZIP_BYTES:
        raise Reject(f"artifact zip is {len(data)} bytes (limit {MAX_ZIP_BYTES})")
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as e:
        raise Reject(f"not a zip file: {e}") from None
    infos = zf.infolist()
    if len(infos) > MAX_ENTRIES:
        raise Reject(f"{len(infos)} zip entries (limit {MAX_ENTRIES})")
    try:
        return _read_zip_entries(zf, infos)
    except (zipfile.BadZipFile, zlib.error, NotImplementedError, EOFError, RuntimeError, OSError, ValueError, OverflowError) as e:
        raise Reject(f"unreadable zip: {type(e).__name__}") from None


def _read_zip_entries(zf, infos) -> dict:
    files, total = {}, 0
    for info in infos:
        name = info.filename
        if name == "images/" and info.is_dir():
            continue
        if _unsafe_path(name):
            raise Reject(f"unsafe path in the zip: {name[:80]!r}")
        mode = (info.external_attr >> 16) & 0o170000
        if info.is_dir() or stat.S_ISLNK(mode) or mode not in (0, stat.S_IFREG):
            raise Reject(f"zip entry {name[:80]!r} is not a regular file (directory, symlink or special file)")
        if info.flag_bits & 0x1:
            raise Reject(f"zip entry {name[:80]!r} is encrypted")
        if not allowed_name(name):
            raise Reject(f"zip entry {name[:80]!r} is not in the allowlist")
        if name in files:
            raise Reject(f"duplicate zip entry {name!r}")
        cap = size_cap(name)
        if info.file_size > cap:
            raise Reject(f"zip entry {name!r} declares {info.file_size} bytes (limit {cap})")
        with zf.open(info) as f:
            body = f.read(cap + 1)  # the declared size is attacker data: bound the real read too
        if len(body) > cap:
            raise Reject(f"zip entry {name!r} is larger than it declares (limit {cap})")
        total += len(body)
        if total > MAX_TOTAL_BYTES:
            raise Reject(f"zip content exceeds {MAX_TOTAL_BYTES} bytes")
        files[name] = body
    return files


def read_dir(root: str) -> dict:
    """The same contract for an unpacked directory (the dry-run fixture)."""
    files, total = {}, 0
    stack = [("", root)]
    while stack:
        rel, path = stack.pop()
        with os.scandir(path) as it:
            for e in sorted(it, key=lambda x: x.name):
                name = f"{rel}{e.name}"
                st = os.lstat(e.path)
                if stat.S_ISDIR(st.st_mode) and name == "images":
                    stack.append(("images/", e.path))
                    continue
                if not stat.S_ISREG(st.st_mode):
                    raise Reject(f"{name!r} is not a regular file (directory, symlink or special file)")
                if _unsafe_path(name) or not allowed_name(name):
                    raise Reject(f"{name[:80]!r} is not in the allowlist")
                if st.st_size > size_cap(name):
                    raise Reject(f"{name!r} is {st.st_size} bytes (limit {size_cap(name)})")
                with open(e.path, "rb") as f:
                    files[name] = f.read(size_cap(name) + 1)
                total += len(files[name])
                if total > MAX_TOTAL_BYTES or len(files) > MAX_ENTRIES:
                    raise Reject("artifact directory is too large")
    return files


_PNG_DEPTHS = {0: (1, 2, 4, 8, 16), 2: (8, 16), 3: (1, 2, 4, 8), 4: (8, 16), 6: (8, 16)}
_PNG_CHANNELS = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}
# Critical chunks plus colour-space chunks that carry a few fixed bytes. No text, profile, time or private chunk:
# those can hold arbitrary data, and the file is hosted on raw.githubusercontent.com.
_PNG_ANCILLARY = {b"pHYs": 9, b"sRGB": 1, b"gAMA": 4, b"cHRM": 32, b"tRNS": None}
MAX_PIXELS = 16_000_000


def valid_png(data: bytes) -> bool:
    """A plain, non-interlaced PNG and nothing else.

    Every chunk is walked and its CRC checked; only IHDR, PLTE, IDAT, IEND and pHYs, sRGB, gAMA, cHRM, tRNS are allowed, in
    a legal order, with their exact lengths; nothing may follow IEND; the IDAT stream must inflate to exactly the size the
    IHDR implies (so it carries no hidden payload) and every row's filter byte must be 0..4. Width and height are at most
    8192 and the picture at most 16 million pixels.
    """
    try:
        return _check_png(data)
    except (zlib.error, struct.error, ValueError, OverflowError):
        return False


def _check_png(data: bytes) -> bool:
    if len(data) < 8 + 25 + 12 or len(data) > MAX_IMAGE_BYTES or not data.startswith(PNG_SIG):
        return False
    pos, first, seen, idat, ended, plte = 8, True, set(), [], False, False
    ihdr = None
    last_was_idat = False
    while pos < len(data):
        if ended or pos + 12 > len(data):
            return False  # trailing bytes after IEND, or a truncated chunk header
        length, ctype = struct.unpack(">I4s", data[pos:pos + 8])
        end = pos + 8 + length + 4
        if length > 0x7FFFFFFF or end > len(data) or not re.fullmatch(rb"[A-Za-z]{4}", ctype):
            return False
        body = data[pos + 8:pos + 8 + length]
        if struct.unpack(">I", data[end - 4:end])[0] != zlib.crc32(ctype + body) & 0xFFFFFFFF:
            return False
        if first:
            if ctype != b"IHDR" or length != 13:
                return False
            w, h, depth, ctp, comp, filt, interlace = struct.unpack(">IIBBBBB", body)
            if not (0 < w <= MAX_IMAGE_PIXELS_SIDE and 0 < h <= MAX_IMAGE_PIXELS_SIDE and w * h <= MAX_PIXELS):
                return False
            if depth not in _PNG_DEPTHS.get(ctp, ()) or (comp, filt, interlace) != (0, 0, 0):
                return False
            ihdr = (w, h, depth, ctp)
            first = False
        else:
            if ctype == b"IDAT":
                if seen & {b"IEND"} or (idat and not last_was_idat):
                    return False  # IDAT chunks must be consecutive
                idat.append(body)
            elif ctype == b"IEND":
                if length != 0 or not idat:
                    return False
                ended = True
            elif ctype == b"PLTE":
                if ctype in seen or idat or length == 0 or length % 3 or length > 768 or ihdr[3] in (0, 4):
                    return False
                plte = True
            elif ctype in _PNG_ANCILLARY:
                want = _PNG_ANCILLARY[ctype]
                if ctype in seen or idat or (want is not None and length != want) or (want is None and length > 256):
                    return False
            else:
                return False
            if ctype in seen and ctype != b"IDAT":
                return False
        seen.add(ctype)
        last_was_idat = ctype == b"IDAT"
        pos = end
    if not ended or (ihdr[3] == 3 and not plte):
        return False
    w, h, depth, ctp = ihdr
    row = 1 + (w * depth * _PNG_CHANNELS[ctp] + 7) // 8
    return _inflates_to_exactly(b"".join(idat), row, h)


def _inflates_to_exactly(stream: bytes, row: int, rows: int) -> bool:
    """The zlib stream holds exactly rows*row bytes and every row starts with a filter type 0..4 (decoded in small pieces)."""
    d, total, want = zlib.decompressobj(), 0, row * rows
    chunk = stream
    for _ in range(want // (1 << 16) + len(stream) + 8):  # bounded: every round consumes input or yields output
        out = d.decompress(chunk, 1 << 16)
        chunk = d.unconsumed_tail
        for i in range((-total) % row, len(out), row):
            if out[i] > 4:
                return False
        total += len(out)
        if total > want:
            return False
        if not chunk and not out:
            break
    return d.eof and not d.unused_data and total == want


# ---------------------------------------------------------------------------------------------------------------
# The manifest
# ---------------------------------------------------------------------------------------------------------------
def _no_dup_keys(pairs):
    d = {}
    for k, v in pairs:
        if k in d:
            raise ValueError(f"duplicate key {k!r}")
        d[k] = v
    return d


def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def parse_manifest(raw: bytes) -> dict:
    def bad_const(c):
        raise ValueError(f"constant {c} not allowed")
    try:
        m = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_dup_keys, parse_constant=bad_const)
    except (ValueError, UnicodeDecodeError, RecursionError) as e:
        raise Reject(f"manifest.json is not valid JSON: {e}") from None
    if not isinstance(m, dict) or set(m) != MANIFEST_KEYS:
        raise Reject(f"manifest.json keys must be exactly {sorted(MANIFEST_KEYS)}")
    if m["schema_version"] != SCHEMA_VERSION or not _is_int(m["schema_version"]):
        raise Reject(f"schema_version must be {SCHEMA_VERSION}")
    if not _is_int(m["pr_number"]) or not 1 <= m["pr_number"] <= 10**9:
        raise Reject("pr_number must be a positive integer")
    if not isinstance(m["head_sha"], str) or not SHA_RE.fullmatch(m["head_sha"]):
        raise Reject("head_sha must be 40 lower-case hex characters")
    comments = m["comments"]
    if not isinstance(comments, list) or not 1 <= len(comments) <= len(MARKERS):
        raise Reject("comments must list 1 to 4 entries")
    seen = set()
    for c in comments:
        if not isinstance(c, dict) or set(c) != {"kind", "verdict"}:
            raise Reject("each comment must be {kind, verdict}")
        if c["kind"] not in MARKERS or c["verdict"] not in VERDICTS:
            raise Reject("unknown comment kind or verdict")
        if c["kind"] in seen:
            raise Reject(f"duplicate comment kind {c['kind']}")
        seen.add(c["kind"])
    images = m["images"]
    if not isinstance(images, list) or len(images) > MAX_IMAGES:
        raise Reject(f"images must be a list of at most {MAX_IMAGES}")
    names = set()
    for i in images:
        if not isinstance(i, dict) or set(i) != {"name", "sha256", "bytes"}:
            raise Reject("each image must be {name, sha256, bytes}")
        if not isinstance(i["name"], str) or not IMAGE_NAME_RE.fullmatch(i["name"]) or i["name"] in names:
            raise Reject("image name must match [a-z0-9][a-z0-9_.-]{0,60}.png and be unique")
        names.add(i["name"])
        if not isinstance(i["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", i["sha256"]):
            raise Reject("image sha256 must be 64 lower-case hex characters")
        if not _is_int(i["bytes"]) or not 1 <= i["bytes"] <= MAX_IMAGE_BYTES:
            raise Reject("image bytes out of range")
    refs = m["allowed_issue_refs"]
    if (not isinstance(refs, list) or len(refs) > MAX_ISSUE_REFS or len(set(refs)) != len(refs)
            or not all(_is_int(r) and 1 <= r <= 9_999_999 for r in refs)):
        raise Reject(f"allowed_issue_refs must be at most {MAX_ISSUE_REFS} distinct integers in 1..9999999")
    return m


def check_files(files: dict, m: dict, producer_path: str) -> None:
    """The files of the artifact are exactly what the manifest declares, and the producer may post those kinds."""
    kinds = {c["kind"] for c in m["comments"]}
    extra = kinds - PRODUCERS[producer_path]
    if extra:
        raise Reject(f"{producer_path} may not post {sorted(extra)}")
    if m["images"] and "visual-proof" not in kinds:
        raise Reject("only a manifest that posts visual-proof may carry images (publishing replaces the PR's whole image folder)")
    bodies = {n for n in files if n in BODY_NAMES}
    if bodies != {f"{k}.md" for k in kinds}:
        raise Reject("the .md files must match the manifest's comments exactly")
    listed = {f"images/{i['name']}": i for i in m["images"]}
    present = {n for n in files if n.startswith("images/")}
    if present != set(listed):
        raise Reject("the images must match the manifest's images exactly")
    for n, i in listed.items():
        data = files[n]
        if not valid_png(data):
            raise Reject(f"{n} is not a valid PNG within the limits")
        if len(data) != i["bytes"] or hashlib.sha256(data).hexdigest() != i["sha256"]:
            raise Reject(f"{n} does not match its size or sha256 in the manifest")


# ---------------------------------------------------------------------------------------------------------------
# The sanitiser: producer Markdown -> Markdown that cannot notify, link out, embed, or run anything
# ---------------------------------------------------------------------------------------------------------------
ZWSP = chr(0x200B)  # zero-width space: breaks @mentions, #N references and URL autolinks without showing
# C0/C1 controls except \t \n, bidi controls, and the private-use range this module uses as sentinels
_STRIP = re.compile("[\\x00-\\x08\\x0b\\x0c\\x0e-\\x1f\\x7f" + "".join(chr(c) for c in (0x200E, 0x200F, 0x61C)) + chr(0x202A) + "-" + chr(0x202E) + chr(0x2066) + "-" + chr(0x2069) + chr(0xE000) + "-" + chr(0xF8FF) + "]")
S0, S1 = chr(0xE000), chr(0xE001)  # sentinel brackets around the index of a kept span
_ENTITY_OK = re.compile(r"&(?!(?:amp|lt|gt|#91|#93|#33|#36);)")


def _link_urls(repo: str) -> str:
    r = re.escape(repo)
    return (rf"https://github\.com/{r}/(?:actions/runs/\d{{1,20}}(?:/job/\d{{1,20}})?|issues/\d{{1,9}}|pull/\d{{1,9}}"
            rf"|commit/[0-9a-f]{{7,40}}|blob/[0-9a-f]{{40}}/(?:(?!\.{{1,2}}(?:/|$))[A-Za-z0-9_.-]{{1,100}}/){{0,8}}(?!\.{{1,2}}(?![A-Za-z0-9_.-]))[A-Za-z0-9_.-]{{1,100}})")


def _escape_plain(s: str) -> str:
    s = _ENTITY_OK.sub("&amp;", s)
    s = s.replace("$", "&#36;")  # no $...$ math (unverified rendering of links inside it)
    s = re.sub(r"@(?=[A-Za-z0-9_])", "@" + ZWSP, s)  # user, org/team and e-mail mentions
    s = re.sub(r"(?<!&)#(?=\d)", "#" + ZWSP, s)  # issue/PR cross references, incl. owner/repo#N (not our own &#91; entities)
    s = s.replace("<", "&lt;").replace("[", "&#91;").replace("]", "&#93;")
    s = re.sub(r"(?<![A-Za-z0-9])(GH-)(?=\d)", r"\1" + ZWSP, s, flags=re.I)
    s = re.sub(r"(?<![A-Za-z0-9.])(www)\.(?=[A-Za-z0-9])", r"\1" + ZWSP + ".", s, flags=re.I)
    s = s.replace("://", ":" + ZWSP + "//")
    return s


def sanitize_body(text: str, *, repo: str, image_names, allowed_refs) -> tuple:
    """Return (markdown, referenced image names).

    Kept verbatim: `![alt](image:NAME.png)` for an image the manifest lists, `[text](url)` and bare URLs of this
    repository's actions runs / issues / pulls / commits / blobs, the exact tags <details> <summary> <sub> and
    their closing tags, and `#N` for N in allowed_refs. Everything else is neutralised: `<` and `[` `]` become entities (no
    HTML, no links, no images), mentions and cross references get a zero-width space, other URLs lose their scheme
    match. The result is idempotent under a second pass.
    """
    for ch in (chr(0x2028), chr(0x2029), chr(0x85)):
        text = text.replace(ch, "\n")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _STRIP.sub("", text)
    allowed_refs = {int(r) for r in allowed_refs}
    image_names = set(image_names)
    urls = _link_urls(repo)
    store, used = [], set()

    def keep(s: str) -> str:
        store.append(s)
        return f"{S0}{len(store) - 1}{S1}"

    def image(m):
        alt, name = _escape_plain(m.group(1)), m.group(2)
        if name in image_names:
            used.add(name)
            return keep(f"![{alt.replace('[', '').replace(']', '')}](image:{name})")
        return alt

    def url_ok(url: str) -> bool:
        """Issue and pull URLs create cross references like `#N`: only the numbers the manifest allows."""
        n = re.search(r"/(?:issues|pull)/(\d+)$", url)
        return not n or int(n.group(1)) in allowed_refs

    def link(m):
        if not url_ok(m.group(2)):
            return m.group(1)  # the text only; it is escaped below
        return keep(f"[{_escape_plain(m.group(1))}]({m.group(2)})")

    def bare(m):
        return keep(m.group(1)) if url_ok(m.group(1)) else m.group(1)

    def ref(m):
        return keep(m.group(0)) if int(m.group(1)) in allowed_refs else m.group(0)

    text = re.sub(r"!\[([^\]\n\[]{0,100})\]\(image:/?([a-z0-9][a-z0-9_.-]{0,60}\.png)\)", image, text)  # `image:` + `/` is --image-base image:
    text = re.sub(rf"!?\[([^\]\n\[]{{1,200}})\]\(({urls})\)", link, text, flags=re.I)
    text = re.sub(rf"(?<![A-Za-z0-9/(\[])({urls})(?![A-Za-z0-9/\-])", bare, text, flags=re.I)
    text = re.sub(r"</?(?:details|summary|sub)>", lambda m: keep(m.group(0)), text)
    text = re.sub(r"(?<![A-Za-z0-9&#/.\-])#(\d{1,7})\b", ref, text)
    text = re.sub("!(?=" + S0 + ")", "&#33;", text)  # a "!" directly before a kept link would turn it into an image
    text = _escape_plain(text)
    for _ in range(4):  # a kept link may hold a kept image; indexes only point backwards, so this ends
        text = re.sub(S0 + "(" + r"\d+)" + S1, lambda m: store[int(m.group(1))], text)
    return text, used


def body_text(raw: bytes, kind: str) -> str:
    """The producer's body without its own first-line marker (the renderers start with it; the commenter adds the marker)."""
    text = raw.decode("utf-8", "replace").replace("\r\n", "\n").lstrip("\n")
    first, _, rest = text.partition("\n")
    return rest if first.strip() == MARKERS[kind] else text


def closers_for(text: str) -> str:
    """What closes an open code fence, <summary>, <sub> and <details>, so the footer can never be hidden by them."""
    out = ""
    if sum(1 for ln in text.splitlines() if ln.lstrip().startswith("```")) % 2:
        out += "\n```"
    for tag in ("summary", "sub"):
        out += f"</{tag}>" * max(0, text.count(f"<{tag}>") - text.count(f"</{tag}>"))
    out += "\n</details>" * max(0, text.count("<details>") - text.count("</details>"))
    return out


def truncate(text: str, budget: int) -> str:
    """The text closed (see closers_for) and, when longer than `budget`, cut at a line boundary with a note."""
    if len(text) + len(closers_for(text)) <= budget:
        return text + closers_for(text)
    note = "\n\n_(truncated: the report is longer than a GitHub comment can hold; the full run log has the rest)_"
    limit = budget - len(note)
    while True:
        cut = text[:max(limit, 0)]
        nl = cut.rfind("\n")
        if nl > 0:
            cut = cut[:nl]
        cut = re.sub(r"[&<]\w{0,4}$", "", cut)
        out = cut + closers_for(cut) + note
        if len(out) <= budget or limit <= 0:
            return out[:budget]
        limit -= max(1, len(out) - budget)


# ---------------------------------------------------------------------------------------------------------------
# Building the comment
# ---------------------------------------------------------------------------------------------------------------
META_RE = re.compile(r"<!-- lh-proof-run: (\d{1,20}) ([0-9a-f]{40}) -->")


def published_name(head_sha: str, name: str) -> str:
    return f"{head_sha[:12]}-{name}"


def image_url(repo: str, pr: int, head_sha: str, name: str) -> str:
    return f"https://raw.githubusercontent.com/{repo}/{VISUALS_BRANCH}/pr-{pr}/{published_name(head_sha, name)}"


CONCLUSION_RE = re.compile(r"[a-z_]{1,30}")
STALE_REASONS = {
    "no-artifact": "the producing run uploaded no `lh-proof-comment` artifact",
    "rejected": "the `lh-proof-comment` artifact of the producing run failed the commenter's validation",
}


def run_url(repo: str, run_id: int) -> str:
    return f"https://github.com/{repo}/actions/runs/{run_id}"


def render_comment(kind: str, body: str, *, repo: str, pr: int, head_sha: str, run_id: int, images_published: bool,
                   verdict: str = "unknown", conclusion: str = "unknown") -> str:
    """marker + body (images resolved to this PR's pr-visuals URLs) + provenance footer + hidden run metadata, within the limit."""
    def resolve(m):
        alt, name = m.group(1), m.group(2)
        if images_published:
            return f"![{alt}]({image_url(repo, pr, head_sha, name)})"
        return f"_{alt or name} (image not published)_"
    body = re.sub(r"!\[([^\]\n]{0,700})\]\(image:([a-z0-9][a-z0-9_.-]{0,60}\.png)\)", resolve, body)  # alt grows when escaped
    head = MARKERS[kind] + "\n"
    warn = ""
    if verdict == "pass" and conclusion not in ("success", "neutral"):
        warn = f"\n\n> The producer reported `pass`, but its run concluded `{conclusion}`: read this report with care."
    foot = (f"{warn}\n\n<sub>Head `{head_sha[:12]}`. Producer verdict `{verdict}`; [run {run_id}]({run_url(repo, run_id)}) "
            f"concluded `{conclusion}`. Posted by the PR proof commenter.</sub>\n<!-- lh-proof-run: {run_id} {head_sha} -->\n")
    return head + truncate(body.strip("\n"), COMMENT_LIMIT - len(head) - len(foot)) + foot


def render_stale(kind: str, *, repo: str, head_sha: str, run_id: int, conclusion: str, reason: str, commenter_run: int) -> str:
    """The replacement of an older head's report when the current head has no valid proof. Built from API data only."""
    body = (f"{MARKERS[kind]}\n### No valid proof for head `{head_sha[:12]}`\n\n"
            f"{STALE_REASONS[reason]}. [Run {run_id}]({run_url(repo, run_id)}) concluded `{conclusion}`"
            + (f"; the commenter's log is [run {commenter_run}]({run_url(repo, commenter_run)})" if commenter_run else "") + ".\n\n"
            "The report of an earlier head was removed so that it cannot be mistaken for this one.\n\n"
            f"<!-- lh-proof-run: {run_id} {head_sha} -->\n")
    return body


# ---------------------------------------------------------------------------------------------------------------
# GitHub API (through `gh api`, which drops the token on a cross-host redirect)
# ---------------------------------------------------------------------------------------------------------------
class ApiError(Exception):
    def __init__(self, msg, status=None):
        super().__init__(msg)
        self.status = status


class GhApi:
    def _run(self, args, payload=None):
        p = subprocess.run(["gh", "api", *args], input=None if payload is None else json.dumps(payload).encode(),
                           capture_output=True, timeout=120)
        if p.returncode != 0:
            err = p.stderr.decode("utf-8", "replace")
            m = re.search(r"HTTP (\d{3})", err)
            raise ApiError(err.strip()[:300], int(m.group(1)) if m else None)
        return p.stdout

    def get(self, path):
        return json.loads(self._run(["-H", "Accept: application/vnd.github+json", path]) or b"null")

    def get_bytes(self, path):
        return self._run([path])

    def send(self, method, path, payload=None):
        out = self._run(["-X", method, "-H", "Accept: application/vnd.github+json", path] + (["--input", "-"] if payload is not None else []), payload)
        return json.loads(out) if out.strip() else None

    def get_paged(self, path):
        out = []
        for page in range(1, MAX_PAGES + 1):
            chunk = self.get(f"{path}{'&' if '?' in path else '?'}per_page=100&page={page}")
            out.extend(chunk)
            if len(chunk) < 100:
                return out
        raise Reject(f"more than {MAX_PAGES} pages at {path}")


# ---------------------------------------------------------------------------------------------------------------
# The event, and the cross-checks that make the PR number and SHA facts instead of claims
# ---------------------------------------------------------------------------------------------------------------
def event_from_env(env) -> dict:
    def get(name, rx):
        v = env.get(name, "")
        if not re.fullmatch(rx, v):
            raise Reject(f"{name} is missing or malformed")
        return v
    prs = env.get("WR_PRS", "[]") or "[]"
    try:
        nums = json.loads(prs)
    except ValueError:
        raise Reject("WR_PRS is not JSON") from None
    if not isinstance(nums, list) or not all(_is_int(n) for n in nums):
        raise Reject("WR_PRS must be a list of numbers")
    return {
        "repo": get("GITHUB_REPOSITORY", REPO_RE.pattern),
        "run_id": int(get("WR_RUN_ID", r"\d{1,20}")),
        "head_sha": get("WR_HEAD_SHA", SHA_RE.pattern),
        "head_repo": get("WR_HEAD_REPO", REPO_RE.pattern),
        "event": get("WR_EVENT", r"[a-z_]{1,40}"),
        "path": get("WR_PATH", r"[A-Za-z0-9_./@:\-]{1,200}"),
        "conclusion": env.get("WR_CONCLUSION", ""),
        "payload_prs": nums,
    }


def producer_path(path: str) -> str:
    m = WF_PATH_RE.fullmatch(path)
    p = m.group(1) if m else ""
    if p not in PRODUCERS:
        raise Skip(f"workflow path {path!r} is not a proof producer")
    return p


def check_run(api, ev: dict) -> tuple:
    """The producing run is a finished pull_request run of this repository for the head the payload names."""
    if ev["event"] != "pull_request":
        raise Skip(f"run event is {ev['event']}, not pull_request")
    if ev["conclusion"] in ("cancelled", "skipped", "stale"):
        raise Skip(f"run conclusion is {ev['conclusion']}")
    path = producer_path(ev["path"])
    run = api.get(f"repos/{ev['repo']}/actions/runs/{ev['run_id']}")
    if (run.get("event") != "pull_request" or run.get("head_sha") != ev["head_sha"] or run.get("status") != "completed"
            or (run.get("repository") or {}).get("full_name") != ev["repo"]
            or (run.get("head_repository") or {}).get("full_name") != ev["head_repo"]
            or producer_path(str(run.get("path", ""))) != path):
        raise Reject("the run in the API does not match the workflow_run event")
    conclusion = str(run.get("conclusion") or "")
    return path, (conclusion if CONCLUSION_RE.fullmatch(conclusion) else "unknown")


def open_prs_for_head(api, ev: dict) -> dict:
    """{number: pr} of the open PRs of this repo whose head is exactly the producing run's commit and repository."""
    def matching(prs):
        out = {}
        for pr in prs:
            head = pr.get("head") or {}
            if (pr.get("state") == "open" and head.get("sha") == ev["head_sha"]
                    and ((head.get("repo") or {}).get("full_name") == ev["head_repo"])
                    and ((pr.get("base") or {}).get("repo") or {}).get("full_name") == ev["repo"]):
                out[pr["number"]] = pr
        return out
    found = matching(api.get_paged(f"repos/{ev['repo']}/commits/{ev['head_sha']}/pulls"))
    if not found:  # fallback: the most recently updated open PRs
        found = matching(api.get(f"repos/{ev['repo']}/pulls?state=open&sort=updated&direction=desc&per_page=100"))
    return found


def fetch_artifact(api, ev: dict) -> bytes:
    arts = api.get(f"repos/{ev['repo']}/actions/runs/{ev['run_id']}/artifacts?per_page=100").get("artifacts", [])
    named = [a for a in arts if a.get("name") == ARTIFACT_NAME and not a.get("expired")]
    if not named:
        raise NoArtifact(f"run {ev['run_id']} has no {ARTIFACT_NAME} artifact")
    if len(named) > 1:
        raise Reject(f"run {ev['run_id']} has {len(named)} artifacts named {ARTIFACT_NAME}")
    a = named[0]
    wr = a.get("workflow_run") or {}
    if wr.get("id") != ev["run_id"] or wr.get("head_sha") != ev["head_sha"]:
        raise Reject("the artifact does not belong to the producing run")
    if not _is_int(a.get("size_in_bytes")) or a["size_in_bytes"] > MAX_ZIP_BYTES:
        raise Reject("artifact is larger than the limit")
    return api.get_bytes(f"repos/{ev['repo']}/actions/artifacts/{a['id']}/zip")


def load_and_check(files: dict, ev: dict, path: str) -> dict:
    """Manifest + files, against the event. Returns the manifest."""
    if "manifest.json" not in files:
        raise Reject("manifest.json is missing")
    m = parse_manifest(files["manifest.json"])
    if m["head_sha"] != ev["head_sha"]:
        raise Reject("the manifest's head_sha is not the producing run's head")
    check_files(files, m, path)
    return m


# ---------------------------------------------------------------------------------------------------------------
# The bundle: the checked, sanitised artifact handed from the read-only job to the jobs that can write
# ---------------------------------------------------------------------------------------------------------------
def build_bundle(files: dict, m: dict, ev: dict, out: str, conclusion: str = "unknown") -> dict:
    os.makedirs(os.path.join(out, "images"), exist_ok=True)
    declared = {i["name"] for i in m["images"]}
    used_all, bodies = set(), {}
    for c in m["comments"]:
        text, used = sanitize_body(body_text(files[f"{c['kind']}.md"], c["kind"]), repo=ev["repo"],
                                   image_names=declared if c["kind"] == "visual-proof" else (),  # only that body shows images
                                   allowed_refs=m["allowed_issue_refs"])
        bodies[c["kind"]] = text
        used_all |= used
    for kind, text in bodies.items():
        with open(os.path.join(out, f"{kind}.md"), "w", encoding="utf-8") as f:
            f.write(text)
    for name in sorted(used_all):  # only images a body shows are ever published
        with open(os.path.join(out, "images", published_name(m["head_sha"], name)), "wb") as f:
            f.write(files[f"images/{name}"])
    meta = {"mode": "proof", "repo": ev["repo"], "run_id": ev["run_id"], "pr_number": m["pr_number"], "head_sha": m["head_sha"],
            "conclusion": conclusion, "comments": m["comments"], "images": sorted(used_all)}
    with open(os.path.join(out, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, sort_keys=True)
    return meta


def build_stale(out: str, ev: dict, path: str, conclusion: str, prs, reason: str, commenter_run: int) -> dict:
    """The bundle of a head with no valid proof. It holds API facts only, nothing from the artifact."""
    os.makedirs(out, exist_ok=True)
    meta = {"mode": "stale", "repo": ev["repo"], "run_id": ev["run_id"], "head_sha": ev["head_sha"], "prs": sorted(prs),
            "kinds": sorted(PRODUCERS[path]), "reason": reason, "conclusion": conclusion, "commenter_run": commenter_run}
    with open(os.path.join(out, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, sort_keys=True)
    return meta


def read_bundle(bundle: str, repo: str) -> dict:
    """Re-check a bundle before a job with a write token uses it (the bundle crossed an artifact upload)."""
    try:
        with open(os.path.join(bundle, "meta.json"), encoding="utf-8") as f:
            meta = json.load(f)
    except (OSError, ValueError):
        raise Reject("bundle meta.json is missing or malformed") from None
    if (not isinstance(meta, dict) or meta.get("repo") != repo or not _is_int(meta.get("run_id"))
            or not isinstance(meta.get("head_sha"), str) or not SHA_RE.fullmatch(meta["head_sha"])
            or not isinstance(meta.get("conclusion"), str) or not CONCLUSION_RE.fullmatch(meta["conclusion"])):
        raise Reject("bundle meta.json does not match the expected shape")
    names = set(os.listdir(bundle))
    if meta.get("mode") == "stale":
        if (names != {"meta.json"} or meta.get("reason") not in STALE_REASONS or not _is_int(meta.get("commenter_run"))
                or not isinstance(meta.get("prs"), list) or not 1 <= len(meta["prs"]) <= 20 or not all(_is_int(n) and n > 0 for n in meta["prs"])
                or not isinstance(meta.get("kinds"), list) or not meta["kinds"] or not all(k in MARKERS for k in meta["kinds"])):
            raise Reject("stale bundle does not match the expected shape")
        return meta
    if (meta.get("mode") != "proof" or not _is_int(meta.get("pr_number")) or not isinstance(meta.get("images"), list)
            or not isinstance(meta.get("comments"), list)):
        raise Reject("bundle meta.json does not match the expected shape")
    allowed = {f"{k}.md" for k in MARKERS} | {"meta.json", "images"}
    if not names <= allowed:
        raise Reject("unexpected files in the bundle")
    kinds = []
    for c in meta["comments"]:
        if (not isinstance(c, dict) or c.get("kind") not in MARKERS or f"{c['kind']}.md" not in names
                or c.get("verdict") not in VERDICTS):
            raise Reject("bundle comments do not match its files")
        kinds.append(c["kind"])
    bodies = {}
    for k in kinds:
        p = os.path.join(bundle, f"{k}.md")
        if not os.path.isfile(p) or os.path.islink(p) or os.path.getsize(p) > 3 * MAX_BODY_BYTES:
            raise Reject(f"bundle body {k} is not a regular file within the limit")
        with open(p, encoding="utf-8") as f:
            bodies[k] = f.read()
    imgdir = os.path.join(bundle, "images")
    on_disk = set(os.listdir(imgdir)) if os.path.isdir(imgdir) else set()
    want = {published_name(meta["head_sha"], n) for n in meta["images"] if isinstance(n, str) and IMAGE_NAME_RE.fullmatch(n)}
    if len(want) != len(meta["images"]) or on_disk != want:
        raise Reject("bundle images do not match meta.json")
    for n in on_disk:
        p = os.path.join(imgdir, n)
        if os.path.islink(p) or not os.path.isfile(p):
            raise Reject(f"bundle image {n} is not a regular file")
        with open(p, "rb") as f:
            if not valid_png(f.read(MAX_IMAGE_BYTES + 1)):
                raise Reject(f"bundle image {n} is not a valid PNG")
    meta["bodies"] = bodies
    return meta


def current_pr(api, repo: str, pr: int, head_sha: str) -> dict:
    """The PR is open and its head is still head_sha; otherwise Skip."""
    cur = api.get(f"repos/{repo}/pulls/{pr}")
    if cur.get("state") != "open":
        raise Skip(f"PR {pr} is not open")
    if (cur.get("head") or {}).get("sha") != head_sha:
        raise Skip(f"PR {pr} head moved on; this run is superseded")
    return cur


# ---------------------------------------------------------------------------------------------------------------
# The upsert
# ---------------------------------------------------------------------------------------------------------------
def upsert(api, repo: str, pr: int, kind: str, body: str, run_id: int, head_sha: str, create: bool = True) -> str:
    marker = MARKERS[kind]
    mine = sorted((c for c in api.get_paged(f"repos/{repo}/issues/{pr}/comments")
                   if (c.get("user") or {}).get("login") == BOT_LOGIN and (c.get("user") or {}).get("type") == "Bot"
                   and isinstance(c.get("body"), str) and c["body"].lstrip().startswith(marker)), key=lambda c: c["id"])
    newest = max((int(m.group(1)) for m in (META_RE.search(c["body"]) for c in mine) if m), default=0)
    if newest > run_id:  # the newest run id over ALL duplicates: an older run never edits or deletes anything
        return f"kept (a newer run {newest} already posted)"
    if not mine and not create:
        return "nothing to replace"
    current_pr(api, repo, pr, head_sha)  # re-checked right before the write
    if mine:
        keep, stale = mine[0], mine[1:]
        try:
            api.send("PATCH", f"repos/{repo}/issues/comments/{keep['id']}", {"body": body})
            result = "updated"
        except ApiError as e:
            if e.status != 404:
                raise
            api.send("POST", f"repos/{repo}/issues/{pr}/comments", {"body": body})
            result = "created"
    else:
        stale = []
        api.send("POST", f"repos/{repo}/issues/{pr}/comments", {"body": body})
        result = "created"
    for c in stale:
        try:
            api.send("DELETE", f"repos/{repo}/issues/comments/{c['id']}")
        except ApiError as e:
            if e.status != 404:
                raise
    return result + (f", removed {len(stale)} duplicate(s)" if stale else "")


# ---------------------------------------------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------------------------------------------
def set_output(env, **kv) -> None:
    path = env.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        for k, v in kv.items():
            v = str(v)
            if not re.fullmatch(r"[A-Za-z0-9_.-]{0,64}", v):  # outputs are only ever numbers, hex and booleans
                raise Reject(f"refusing to write output {k}")
            f.write(f"{k}={v}\n")


def cmd_validate(args, env, api) -> int:
    ev = event_from_env(env)
    set_output(env, proceed="false", mode="", pr="", head_sha="", has_images="false")
    path, conclusion = check_run(api, ev)
    prs = open_prs_for_head(api, ev)
    if not prs:
        raise Skip("no open PR has this head (closed, or the head moved on)")
    commenter_run = int(env.get("GITHUB_RUN_ID", "0")) if re.fullmatch(r"\d{1,20}", env.get("GITHUB_RUN_ID", "")) else 0

    def stale(reason: str) -> int:
        """The current head has no valid proof: older reports of this producer must not stay on screen as if they were."""
        build_stale(args.out, ev, path, conclusion, prs, reason, commenter_run)
        log("notice", f"no valid proof for head {ev['head_sha'][:12]} ({reason}); the producer's older comments will be replaced")
        set_output(env, proceed="true", mode="stale", head_sha=ev["head_sha"])
        return 0

    try:
        files = read_zip(fetch_artifact(api, ev))
        m = load_and_check(files, ev, path)
        if m["pr_number"] not in prs:
            raise Reject(f"the manifest's PR {m['pr_number']} is not an open PR with this head (have {sorted(prs)})")
        if ev["payload_prs"] and m["pr_number"] not in ev["payload_prs"]:
            raise Reject("the manifest's PR is not among the PRs the event lists")
    except NoArtifact as e:
        log("notice", str(e))
        return stale("no-artifact")
    except Reject as e:
        log("error", f"artifact rejected: {e}")
        return stale("rejected")
    meta = build_bundle(files, m, ev, args.out, conclusion)
    log("notice", f"validated {len(meta['comments'])} comment(s) and {len(meta['images'])} image(s) for PR {m['pr_number']}")
    set_output(env, proceed="true", mode="proof", pr=m["pr_number"], head_sha=m["head_sha"], has_images=str(bool(meta["images"])).lower())
    return 0


def cmd_check_bundle(args, env, api) -> int:
    repo = env.get("GITHUB_REPOSITORY", "")
    if not REPO_RE.fullmatch(repo):
        raise Reject("GITHUB_REPOSITORY is malformed")
    meta = read_bundle(args.bundle, repo)
    set_output(env, current="false", pr="")
    if meta["mode"] != "proof":
        return 0
    current_pr(api, repo, meta["pr_number"], meta["head_sha"])
    set_output(env, current="true", pr=meta["pr_number"])  # the publish step takes the PR number from here
    return 0


def cmd_comment(args, env, api) -> int:
    repo = env.get("GITHUB_REPOSITORY", "")
    if not REPO_RE.fullmatch(repo):
        raise Reject("GITHUB_REPOSITORY is malformed")
    meta = read_bundle(args.bundle, repo)
    if meta["mode"] == "stale":
        for pr in meta["prs"]:
            for kind in meta["kinds"]:
                body = render_stale(kind, repo=repo, head_sha=meta["head_sha"], run_id=meta["run_id"], conclusion=meta["conclusion"],
                                    reason=meta["reason"], commenter_run=meta["commenter_run"])
                try:
                    log("notice", f"PR {pr} {kind}: {upsert(api, repo, pr, kind, body, meta['run_id'], meta['head_sha'], create=False)}")
                except Skip as sk:
                    log("notice", f"PR {pr} {kind}: skipped, {sk}")
        return 0
    published = args.images_published == "true"
    for c in meta["comments"]:
        kind = c["kind"]
        body = render_comment(kind, meta["bodies"][kind], repo=repo, pr=meta["pr_number"], head_sha=meta["head_sha"],
                              run_id=meta["run_id"], images_published=published, verdict=c["verdict"], conclusion=meta["conclusion"])
        try:
            log("notice", f"{kind}: {upsert(api, repo, meta['pr_number'], kind, body, meta['run_id'], meta['head_sha'])}")
        except Skip as sk:
            log("notice", f"{kind}: skipped, {sk}")
    return 0


def cmd_dry_run(args, env, api) -> int:
    src = os.path.realpath(args.source)
    root = os.path.realpath(FIXTURE_ROOT)
    if not (src == root or src.startswith(root + os.sep)):
        raise Reject(f"--source must be inside {FIXTURE_ROOT}")
    if not 1 <= args.pr <= 10**9:
        raise Reject("--pr out of range")
    if os.path.isdir(src):
        files = read_dir(src)
    else:
        with open(src, "rb") as f:
            files = read_zip(f.read(MAX_ZIP_BYTES + 1))
    m = parse_manifest(files.get("manifest.json", b""))
    if m["pr_number"] != args.pr:
        raise Reject("the fixture's pr_number is not --pr")
    check_files(files, m, args.producer)
    repo = env.get("GITHUB_REPOSITORY", "example/example")
    if not REPO_RE.fullmatch(repo):
        raise Reject("GITHUB_REPOSITORY is malformed")
    declared = {i["name"] for i in m["images"]}
    for c in m["comments"]:
        text, _ = sanitize_body(body_text(files[f"{c['kind']}.md"], c["kind"]), repo=repo,
                                image_names=declared if c["kind"] == "visual-proof" else (), allowed_refs=m["allowed_issue_refs"])
        print(f"----- would post ({c['kind']}, verdict {c['verdict']}) -----")
        print(render_comment(c["kind"], text, repo=repo, pr=m["pr_number"], head_sha=m["head_sha"], run_id=0, images_published=True,
                             verdict=c["verdict"], conclusion="success"))
    log("notice", "dry run: validated and rendered; nothing was posted or published")
    return 0


def main(argv=None, env=None, api=None) -> int:
    env = os.environ if env is None else env
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("validate")
    v.add_argument("--out", required=True)
    c = sub.add_parser("check-bundle")
    c.add_argument("--bundle", required=True)
    p = sub.add_parser("comment")
    p.add_argument("--bundle", required=True)
    p.add_argument("--images-published", choices=["true", "false"], required=True)
    d = sub.add_parser("dry-run")
    d.add_argument("--source", required=True)
    d.add_argument("--pr", type=int, required=True)
    d.add_argument("--producer", default=".github/workflows/pr-proof.yaml", choices=sorted(PRODUCERS))
    args = ap.parse_args(argv)
    api = api or GhApi()
    try:
        return {"validate": cmd_validate, "check-bundle": cmd_check_bundle, "comment": cmd_comment,
                "dry-run": cmd_dry_run}[args.cmd](args, env, api)
    except Skip as s:
        log("notice", f"nothing to do: {s}")
        return 0
    except ApiError as e:
        if e.status == 403:  # e.g. a read-only token (Dependabot): post nothing, do not fail the run
            log("warning", f"the GitHub API refused the request (403); nothing is posted. {e}")
            return 0
        log("error", str(e))
        return 1
    except Reject as e:
        log("error", str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())

