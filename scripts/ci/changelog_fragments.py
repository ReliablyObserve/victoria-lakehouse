#!/usr/bin/env python3
"""Turn the changelog fragments of a release commit into the release's CHANGELOG.md section.

    changelog_fragments.py check
    changelog_fragments.py previous-tag --rev HEAD^
    changelog_fragments.py assemble --version 0.168.0 [--date 2026-10-23] [--after-tag v0.167.9]
                                    [--consumed-out PATH]

A change is documented by one file, changelog.d/<PR-or-slug>.<kind>.md, that holds the bullet
(see changelog.d/README.md and check_changelog_pr.py, which owns the format and the PR gate).

`assemble` runs in the release job on the release commit:

* The fragments it releases are the ones present in changelog.d/, minus the files (same path, same
  bytes) already present at --after-tag (the previous release): those were released by it even when its
  release-metadata pull request has not merged yet, so a fragment is released exactly once. A fragment merged after the
  release commit is not in this checkout and lands in the next release.
* `## [Unreleased]` is kept, empty, and the new `## [VERSION] - DATE` section follows it, built from
  the bullets still hand-written under `[Unreleased]` (the transition, see
  check_changelog_pr.legacy_unreleased_window_open) and then the fragments, grouped under the
  Keep a Changelog headings (Added, Changed, Deprecated, Removed, Fixed, Security, then any other
  heading the legacy text used), each group in file-name order.
* The released fragments are removed from the working tree, so the tree is the one the release
  metadata pull request leaves behind, and their paths are written to --consumed-out, one per line,
  for the metadata job, which removes them from git.

Anything it cannot place (an invalid fragment, text under [Unreleased] that is not a bullet below a
heading, a version that is already a section) is an error; nothing is dropped.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import os
import pathlib
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # also under `python3 -I`
import check_changelog_pr as cc  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[2]
SEMVER_RE = re.compile(r"^(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})$")
KNOWN_HEADINGS = tuple(k.capitalize() for k in cc.FRAGMENT_KINDS)


class AssembleError(Exception):
    pass


def load_fragments(directory: pathlib.Path, rel: str = cc.FRAGMENT_DIR) -> tuple[list[cc.Fragment], list[str]]:
    """Every fragment in `directory` (published as `rel/<name>`), and every reason one cannot be used."""
    fragments: list[cc.Fragment] = []
    problems: list[str] = []
    if not directory.is_dir():
        return fragments, problems
    for entry in sorted(directory.iterdir(), key=lambda e: e.name):
        path = f"{rel}/{entry.name}"
        if entry.name == cc.FRAGMENT_README and entry.is_file():
            continue
        if entry.is_symlink() or not entry.is_file():
            problems.append(f"{path}: not a regular file")
            continue
        fr, why = cc.parse_fragment(path, entry.read_text(encoding="utf-8"))
        problems.extend(why)
        if fr is not None:
            fragments.append(fr)
    problems.extend(cc.duplicate_fragment_problems(fragments))
    return fragments, problems


def blobs_at_tag(tag: str, rel: str = cc.FRAGMENT_DIR, cwd: pathlib.Path | None = None) -> dict[str, str]:
    """path -> git blob id of every file under `rel` at git tag `tag`; empty when the tag cannot be read
    (then every fragment is released, which can repeat the previous release's entries but never loses one)."""
    if not tag:
        return {}
    try:
        out = subprocess.run(
            ["git", "ls-tree", "-r", "-z", tag, "--", rel],
            cwd=cwd, capture_output=True, check=True,
        ).stdout.decode("utf-8", "surrogateescape")
    except (OSError, subprocess.CalledProcessError):
        return {}
    blobs = {}
    for entry in out.split("\0"):
        meta, _, path = entry.partition("\t")
        if path:
            blobs[path] = meta.split()[2]
    return blobs


def blob_id(path: pathlib.Path) -> str:
    """The git blob id of the file's bytes, as `git hash-object` computes it."""
    data = path.read_bytes()
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def split_unreleased(text: str) -> tuple[str, str, str]:
    """(everything up to and including the [Unreleased] heading line, its body, the rest)."""
    lines = text.split("\n")
    start = next((i for i, l in enumerate(lines) if l.startswith("## [Unreleased]")), None)
    if start is None:
        raise AssembleError("no '## [Unreleased]' section in the changelog")
    end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith("## [")), len(lines))
    return "\n".join(lines[: start + 1]), "\n".join(lines[start + 1 : end]), "\n".join(lines[end:])


def legacy_groups(body: str) -> dict[str, list[str]]:
    """heading -> bullet blocks (their own text) of the hand-written [Unreleased] body."""
    groups: dict[str, list[str]] = {}
    heading = None
    block: list[str] = []

    def flush() -> None:
        nonlocal block
        while block and not block[-1].strip():
            block.pop()
        if block:
            groups[heading].append("\n".join(block))
        block = []

    for line in body.split("\n"):
        m = re.match(r"^### +(.+?)\s*$", line)
        if m:
            flush()
            heading = m.group(1)
            groups.setdefault(heading, [])
        elif line.startswith("- "):
            if heading is None:
                raise AssembleError("a bullet under [Unreleased] is outside any '### ' heading")
            flush()
            block = [line]
        elif not line.strip():
            if block:
                block.append("")
        elif block and line.startswith(" "):
            block.append(line)
        else:
            raise AssembleError(f"text under [Unreleased] that is not a bullet below a heading: {line[:60]!r}")
    flush()
    return {h: b for h, b in groups.items() if b}


def section_body(legacy_body: str, fragments: list[cc.Fragment]) -> str:
    groups = legacy_groups(legacy_body)
    for fr in sorted(fragments, key=lambda f: f.path):
        groups.setdefault(fr.kind.capitalize(), []).append("\n".join(fr.lines))
    order = [h for h in KNOWN_HEADINGS if h in groups] + [h for h in groups if h not in KNOWN_HEADINGS]
    return "\n\n".join(f"### {h}\n\n" + "\n\n".join(groups[h]) for h in order)


def assemble(text: str, fragments: list[cc.Fragment], version: str, date: str) -> str:
    """The changelog with `[Unreleased]` emptied and `## [version] - date` below it."""
    if not SEMVER_RE.match(version):
        raise AssembleError(f"'{version}' is not a release version (MAJOR.MINOR.PATCH)")
    try:
        datetime.date.fromisoformat(date)
    except ValueError as err:
        raise AssembleError(f"date '{date}' is not YYYY-MM-DD") from err
    if re.search(r"^## \[" + re.escape(version) + r"\]", text, re.MULTILINE):
        raise AssembleError(f"the changelog already has a section for {version}")
    head, body, rest = split_unreleased(text)
    section = f"## [{version}] - {date}\n\n"
    content = section_body(body, fragments)
    if content:
        section += content + "\n\n"
    return head + "\n\n" + section + rest


def run(root: pathlib.Path, version: str, date: str, after_tag: str, consumed_out: pathlib.Path | None) -> list[str]:
    fragments, problems = load_fragments(root / cc.FRAGMENT_DIR)
    if problems:
        raise AssembleError("cannot release these fragments:\n  " + "\n  ".join(problems))
    # A fragment is the previous release's only when the tag holds that very file: a later fragment that
    # reuses the name of a released (and since deleted) one has other bytes and is released.
    already = blobs_at_tag(after_tag, cwd=root)
    chosen = [fr for fr in fragments if already.get(fr.path) != blob_id(root / fr.path)]
    changelog = root / "CHANGELOG.md"
    text = changelog.read_text(encoding="utf-8")
    # A change that a released section already describes is released once: a fragment that repeats
    # its lead-in (a re-added copy, a rebase onto a release that took the change) stops the release
    # instead of publishing the change twice. The gate rejects such a PR; this is the backstop.
    released = cc.released_lead_ins(text)
    twice = [fr.path for fr in chosen if fr.lead_in in released]
    if twice:
        raise AssembleError(
            "these fragments repeat the bold lead-in of a released CHANGELOG.md section; remove them "
            "(the change is already released): " + ", ".join(twice)
        )
    result = assemble(text, chosen, version, date)
    changelog.write_text(result, encoding="utf-8")
    for fr in chosen:
        (root / fr.path).unlink()
    consumed = [fr.path for fr in chosen]
    if consumed_out is not None:
        consumed_out.write_text("".join(p + "\n" for p in consumed), encoding="utf-8")
    return consumed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", default=str(ROOT), help="repository root (default: this checkout)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="validate every fragment in changelog.d/")
    prev = sub.add_parser("previous-tag", help="print the newest vMAJOR.MINOR.PATCH tag reachable from --rev")
    prev.add_argument("--rev", default="HEAD^")
    asm = sub.add_parser("assemble", help="build the release section from the fragments and remove them")
    asm.add_argument("--version", required=True, help="MAJOR.MINOR.PATCH (no leading v)")
    asm.add_argument("--date", help="YYYY-MM-DD (default: today, UTC)")
    asm.add_argument("--after-tag", default="", help="previous release tag: its fragments are not released again")
    asm.add_argument("--consumed-out", help="file that receives the released fragment paths, one per line")
    args = parser.parse_args(argv)
    root = pathlib.Path(args.root)
    try:
        if args.cmd == "check":
            fragments, problems = load_fragments(root / cc.FRAGMENT_DIR)
            if problems:
                raise AssembleError("\n  ".join(problems))
            print(f"{len(fragments)} changelog fragment(s), all valid")
            return 0
        if args.cmd == "previous-tag":
            print(cc.latest_release_tag(args.rev, cwd=root))
            return 0
        date = args.date or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        out = pathlib.Path(args.consumed_out) if args.consumed_out else None
        consumed = run(root, args.version, date, args.after_tag, out)
        print(f"assembled [{args.version}] - {date} from {len(consumed)} fragment(s)")
    except (AssembleError, OSError) as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
