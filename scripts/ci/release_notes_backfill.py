#!/usr/bin/env python3
"""Backfill the notes of past GitHub releases in the VictoriaLogs release style. Dry run unless told otherwise.

    release_notes_backfill.py --out-dir OUT                       # dry run: plan + report, edits nothing
    LH_BACKFILL_APPLY=yes release_notes_backfill.py --out-dir OUT --apply [--only TAG,...] [--max N]

For every release the notes are rendered with scripts/ci/release_notes.py (the renderer of the release
workflow) from the repository AT THE RELEASE TAG, never from today's main:

* `CHANGELOG.md` at the tag, and the `changelog.d/` fragments at the tag that the previous release tag
  did not already have (the same rule as the release job, `changelog_fragments.py assemble --after-tag`).
  No tag has its own `## [X.Y.Z]` section: the release-metadata pull request writes it after the tag. The
  entries of a release are therefore what `[Unreleased]` holds at the tag, minus what it held at the
  previous release tag (`release_notes.py --after-tag`). A `## [X.Y.Z]` section at the tag, if one exists,
  is used as it stands.
* The old body is kept from its `## Release Artifacts` marker on, byte for byte (binaries, images, Helm).
  Only the text before the marker is replaced, so the new body is `notes + "\\n\\n" + old[marker:]`.

A release is left alone, and reported with the reason, when it:

* is immutable (the API says so) or was published at or after the immutability cutoff;
* has no `## Release Artifacts` marker (a hand-written or GitHub-generated body), already starts with
  `Released at` (the new format), or has other text before the marker;
* has no entry in the changelog at its tag ("no changelog entry").

The risks of a rendered body are only reported, never fixed: unbalanced backticks or code fences (an open
fence would swallow the artifacts block), an unclosed HTML comment, a body above 60,000 bytes, @mentions
(they notify users), and `#N` references (those above the newest issue number of the repository, and
bare ones left unlinked, are the ones that point at unrelated issues).

Nothing is edited without BOTH `--apply` and the environment variable LH_BACKFILL_APPLY=yes. Apply
refuses an immutable release or one published after the cutoff, re-reads the live body and refuses it if it
differs from the planned one, writes the old body to OUT/backup/TAG.md before `gh release edit`, verifies
the body after the edit, and keeps OUT/apply.log.jsonl so a stopped run resumes where it ended. To undo:
`gh release edit TAG --notes-file OUT/backup/TAG.md`.
"""

import argparse
import datetime
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # also under `python3 -I`
import changelog_fragments as cf  # noqa: E402
import check_changelog_pr as cc  # noqa: E402
import release_notes as rn  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_REPO = "ReliablyObserve/victoria-lakehouse"
# Releases published at or after this moment are immutable (owner decision, 2026-10-23).
CUTOFF = "2026-10-23T06:00:00Z"
APPLY_ENV = "LH_BACKFILL_APPLY"
MAX_BODY_BYTES = 60000
MARKER = re.compile(r"^## Release Artifacts[ \t]*$", re.MULTILINE)
NEW_FORMAT = "Released at "
NO_CHANGES = "\n\nNo user-visible changes.\n"

# Statuses of the plan.
EDIT, IMMUTABLE, NO_ENTRY, NO_MARKER, NEW_STYLE, OTHER_TEXT, NO_TAG = (
    "eligible",
    "excluded-immutable",
    "skipped-no-changelog-entry",
    "skipped-no-artifacts-marker",
    "skipped-already-new-format",
    "skipped-text-before-marker",
    "skipped-no-tag",
)

CODE_SPAN = re.compile(r"`[^`\n]*`")
URL = re.compile(r"https?://\S+")
MENTION = re.compile(r"(?<![\w@/.`])@([A-Za-z0-9](?:[A-Za-z0-9-]{0,38})(?:/[A-Za-z0-9_.-]+)?)(?![\w@])")
LINKED_REF = re.compile(r"\[#(\d+)\]\(" + re.escape(rn.REPO_URL) + r"/issues/\d+\)")
OTHER_REPO_REF = re.compile(r"(?:Victoria\w*|upstream|VL|VT|Loki|Tempo|Jaeger)\s+\[#(\d+)\]\(")
BARE_REF = re.compile(r"(?<![\w&#/\[])#(\d+)(?!\w)")
COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)


class BackfillError(Exception):
    """The backfill cannot continue (bad input, refused apply, gh failed)."""


class NoEntry(Exception):
    """The changelog at the tag has nothing for this release."""


def sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def run_git(root, *args):
    """stdout of git, or None when it fails (a missing tag or file)."""
    try:
        return subprocess.run(
            ["git", *args], cwd=root, capture_output=True, text=True, encoding="utf-8", check=True
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return None


class Repo:
    """Read access to the repository at tags, with the small caches the plan needs."""

    def __init__(self, root):
        self.root = pathlib.Path(root)
        self._changelog = {}

    def has_tag(self, tag):
        return run_git(self.root, "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}^{{commit}}") is not None

    def changelog(self, tag):
        if tag not in self._changelog:
            self._changelog[tag] = run_git(self.root, "show", f"{tag}:CHANGELOG.md")
        return self._changelog[tag]

    def previous_tag(self, tag):
        return cc.latest_release_tag(f"{tag}^", cwd=self.root)

    def fragments(self, tag):
        """path -> blob id of every file of changelog.d/ at the tag."""
        return cf.blobs_at_tag(tag, cwd=self.root)

    def blob(self, blob_id):
        return run_git(self.root, "cat-file", "blob", blob_id)


def tag_fragments(repo, tag, previous):
    """(fragments the release adds, problems): the fragments at the tag that the previous tag did not have."""
    have = repo.fragments(previous) if previous else {}
    chosen, problems = [], []
    for path, blob_id in sorted(repo.fragments(tag).items()):
        if path.rpartition("/")[2] == cc.FRAGMENT_README:
            continue
        if have.get(path) == blob_id:
            continue
        text = repo.blob(blob_id)
        if text is None:
            problems.append(f"{path}: cannot be read at {tag}")
            continue
        fragment, why = cc.parse_fragment(path, text)
        problems.extend(why)
        if fragment is not None:
            chosen.append(fragment)
    return chosen, problems


def notes_for_tag(repo, tag, date):
    """(notes, warnings) of the release at `tag`; NoEntry when its changelog has nothing for it."""
    version = tag[1:]
    text = repo.changelog(tag)
    if text is None:
        raise NoEntry(f"no CHANGELOG.md at {tag}")
    warnings = []
    try:
        if re.search(r"^## \[" + re.escape(version) + r"\]", text, re.MULTILINE):
            notes, more = rn.render(text, version, date)
            return check_entry(notes), warnings + more
    except rn.ReleaseNotesError as err:
        raise NoEntry(str(err)) from err
    previous = repo.previous_tag(tag)
    already = set()
    if previous:
        before = repo.changelog(previous)
        if before is None:
            warnings.append(f"cannot read CHANGELOG.md at {previous}; entries of the previous release may be repeated")
        else:
            already = rn.bullet_keys(before)
    fragments, problems = tag_fragments(repo, tag, previous)
    warnings.extend(f"fragment skipped: {problem}" for problem in problems)
    try:
        assembled = cf.assemble(text, fragments, version, date)
        notes, more = rn.render(assembled, version, date, already)
    except cf.AssembleError as err:
        if fragments:
            raise NoEntry(f"cannot assemble the fragments at {tag}: {err}") from err
        warnings.append(f"[Unreleased] at {tag} rendered as it stands ({err})")
        try:
            notes, more = rn.render(text, "Unreleased", date, already)
        except rn.ReleaseNotesError as inner:
            raise NoEntry(str(inner)) from inner
    except rn.ReleaseNotesError as err:
        raise NoEntry(str(err)) from err
    return check_entry(notes), warnings + more


def check_entry(notes):
    if notes.count("\n\n") == 1 and notes.endswith(NO_CHANGES):
        raise NoEntry("no changelog entry")
    return notes


def split_body(body):
    """(text before the Release Artifacts marker, the rest from the marker on); None without a marker."""
    match = MARKER.search(body)
    if match is None:
        return None
    return body[: match.start()], body[match.start() :]


def new_body(notes, suffix):
    return notes.rstrip("\n") + "\n\n" + suffix


def risks(notes, body, max_ref):
    """Warnings (and counts) about the rendered notes. Reports only; the text is never changed."""
    found = []
    counts = {"mentions": 0, "refs": 0, "refs_unknown": 0, "refs_other_repo": 0, "refs_bare": 0, "backtick_lines": 0}
    in_fence = False
    odd = 0
    fences = 0
    for line in notes.split("\n"):
        if line.lstrip().startswith("```"):
            fences += 1
            in_fence = not in_fence
            continue
        if not in_fence and line.count("`") % 2:
            odd += 1
    counts["backtick_lines"] = odd
    if odd:
        found.append(f"{odd} line(s) with unbalanced backticks")
    if fences % 2:
        found.append("odd number of ``` fences: the artifacts block would render as code")
    if "<!--" in COMMENT.sub("", notes):
        found.append("unclosed HTML comment: it would hide the artifacts block")
    size = len(body.encode("utf-8"))
    if size > MAX_BODY_BYTES:
        found.append(f"body is {size} bytes (> {MAX_BODY_BYTES})")
    plain = URL.sub(" ", CODE_SPAN.sub(" ", notes))
    plain = re.sub(r"\[[^\]\n]*\]\([^)\n]*\)", " ", plain)
    mentions = sorted(set(MENTION.findall(plain)))
    counts["mentions"] = len(mentions)
    if mentions:
        found.append("@mentions that notify users: " + ", ".join("@" + m for m in mentions))
    linked = [int(n) for n in LINKED_REF.findall(notes)]
    counts["refs"] = len(linked)
    unknown = sorted({n for n in linked if n == 0 or (max_ref is not None and n > max_ref)})
    counts["refs_unknown"] = len(unknown)
    if unknown:
        found.append("#N links to numbers that are not issues or pull requests of the repository: " + ", ".join(f"#{n}" for n in unknown))
    other = sorted({int(n) for n in OTHER_REPO_REF.findall(notes)})
    counts["refs_other_repo"] = len(other)
    if other:
        found.append("#N after an upstream/other project name is linked to THIS repository: " + ", ".join(f"#{n}" for n in other))
    bare = sorted(set(BARE_REF.findall(URL.sub(" ", CODE_SPAN.sub(" ", LINKED_REF.sub(" ", notes))))))
    counts["refs_bare"] = len(bare)
    if bare:
        found.append("bare #N left unlinked, which GitHub auto-links to the repository: " + ", ".join(f"#{n}" for n in bare))
    return found, counts


def ref_context(notes, width=50):
    """(number, text around it) of every #N reference the renderer turned into a link."""
    plain = LINKED_REF.sub(lambda m: f"\x01{m.group(1)}", notes).replace("\n", " ")
    found = []
    for match in re.finditer(r"\x01(\d+)", plain):
        text = plain[max(0, match.start() - width) : match.end() + width // 2].replace("\x01", "#").strip()
        found.append((int(match.group(1)), text))
    return found


def release_date(release):
    return (release.get("published_at") or "")[:10]


def immutable_reason(release, cutoff=CUTOFF):
    """Why a release must not be edited, "" when it may."""
    if release.get("immutable"):
        return "immutable"
    if release.get("draft"):
        return "draft"
    published = release.get("published_at") or ""
    if not published or published >= cutoff:
        return f"published {published or 'never'} (cutoff {cutoff})"
    return ""


def entry_count(notes):
    """Number of entries (bullets and update notes) of rendered notes."""
    return len(re.findall(r"^(?:\* (?:SECURITY|FEATURE|BUGFIX): |\*\*Update note)", notes, re.MULTILINE))


def plan_release(repo, release, max_ref=None, cutoff=CUTOFF, compare=None):
    """One entry of the plan for a release object of the GitHub API."""
    tag = release["tag_name"]
    body = release.get("body") or ""
    entry = {
        "tag": tag,
        "published_at": release.get("published_at"),
        "status": EDIT,
        "reason": "",
        "old_bytes": len(body.encode("utf-8")),
        "new_bytes": None,
        "old_sha256": sha(body),
        "new_sha256": None,
        "warnings": [],
        "counts": {},
    }

    def skip(status, reason):
        entry.update(status=status, reason=reason)
        return entry

    why = immutable_reason(release, cutoff)
    if why:
        return skip(IMMUTABLE, why)
    parts = split_body(body)
    if parts is None:
        return skip(NO_MARKER, "no '## Release Artifacts' marker: hand-written or GitHub-generated body")
    head, suffix = parts
    if head.startswith(NEW_FORMAT):
        return skip(NEW_STYLE, "starts with 'Released at'")
    if head.strip():
        return skip(OTHER_TEXT, "text before the marker that the backfill did not write")
    if not repo.has_tag(tag):
        return skip(NO_TAG, f"tag {tag} is not in the local clone (git fetch --tags)")
    try:
        notes, warnings = notes_for_tag(repo, tag, release_date(release))
    except NoEntry as err:
        if compare is not None:
            entry["source"] = {"at_tag": 0, "in_main": len(rn.bullet_keys(compare, tag[1:]))}
        return skip(NO_ENTRY, str(err))
    result = new_body(notes, suffix)
    found, counts = risks(notes, result, max_ref)
    if compare is not None:
        entry["source"] = {"at_tag": entry_count(notes), "in_main": len(rn.bullet_keys(compare, tag[1:]))}
    entry.update(
        new_bytes=len(result.encode("utf-8")),
        new_sha256=sha(result),
        warnings=[f"render: {w}" for w in warnings] + [f"risk: {w}" for w in found],
        counts=counts,
        ref_context=ref_context(notes),
        notes=notes,
        new_body=result,
    )
    return entry


def version_key(tag):
    return tuple(int(x) for x in tag[1:].split("."))


def build_plan(repo, releases, max_ref=None, cutoff=CUTOFF, compare=None):
    return [plan_release(repo, r, max_ref, cutoff, compare) for r in sorted(releases, key=lambda r: version_key(r["tag_name"]))]


def write_plan(plan, releases, out_dir):
    """OUT/old/TAG.md (the current body), OUT/new/TAG.md (the planned body, eligible only) and OUT/plan.json."""
    by_tag = {r["tag_name"]: r for r in releases}
    for sub in ("old", "new"):
        (out_dir / sub).mkdir(parents=True, exist_ok=True)
    slim = []
    for entry in plan:
        (out_dir / "old" / f"{entry['tag']}.md").write_text(by_tag[entry["tag"]].get("body") or "", encoding="utf-8")
        if entry["status"] == EDIT:
            (out_dir / "new" / f"{entry['tag']}.md").write_text(entry["new_body"], encoding="utf-8")
        slim.append({k: v for k, v in entry.items() if k not in ("notes", "new_body")})
    (out_dir / "plan.json").write_text(json.dumps(slim, indent=1) + "\n", encoding="utf-8")


def pick_samples(plan):
    """Up to five eligible releases spread over the history: the first, a mid 0.1xx, a 0.14x, a 0.15x/0.16x
    and the newest. The mid ones are the middle of the eligible releases of their range that have a real entry."""
    eligible = [e for e in plan if e["status"] == EDIT]
    if not eligible:
        return []
    picks = [eligible[0]]
    for low, high in ((100, 139), (140, 149), (150, 169)):
        pool = [e for e in eligible if low <= version_key(e["tag"])[1] <= high and len(e["notes"]) > 200]
        if pool:
            picks.append(pool[(len(pool) - 1) // 2])
    picks.append(eligible[-1])
    seen, samples = set(), []
    for entry in picks:
        if entry["tag"] not in seen:
            seen.add(entry["tag"])
            samples.append(entry)
    return samples


def head_lines(text, count):
    lines = text.split("\n")
    shown = "\n".join(lines[:count])
    return shown + (f"\n... ({len(lines) - count} more lines)" if len(lines) > count else "")


def apply_command(out_dir, count):
    return (
        f"env -u GITHUB_TOKEN LH_BACKFILL_APPLY=yes python3 scripts/ci/release_notes_backfill.py "
        f"--out-dir {out_dir} --apply --pause 2 --max {count}"
    )


def render_report(plan, out_dir, repo_name, max_ref, samples_head=18):
    counts = {}
    for entry in plan:
        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
    editable = sum(1 for e in plan if e["status"] != IMMUTABLE)
    eligible = [e for e in plan if e["status"] == EDIT]
    out = [
        "# Release notes backfill: dry run",
        "",
        f"Repository {repo_name}. Immutability cutoff {CUTOFF}. Newest issue/PR number: {max_ref if max_ref is not None else 'unknown'}.",
        "",
        "## Summary",
        "",
        f"* considered: {len(plan)}",
        f"* editable (before the cutoff and not immutable): {editable}",
        f"* excluded as immutable or published after the cutoff: {counts.get(IMMUTABLE, 0)}",
        f"* eligible, notes rewritten on apply: {len(eligible)}",
    ]
    for status in (NO_ENTRY, NO_MARKER, NEW_STYLE, OTHER_TEXT, NO_TAG):
        out.append(f"* {status}: {counts.get(status, 0)}")
    warned = [e for e in eligible if e["warnings"]]
    out.append(f"* eligible releases with warnings: {len(warned)}")
    totals = {k: sum(e["counts"].get(k, 0) for e in eligible) for k in ("mentions", "refs", "refs_unknown", "refs_other_repo", "refs_bare", "backtick_lines")}
    out.append(
        "* risk counts over eligible releases: "
        + ", ".join(f"{k}={v}" for k, v in totals.items())
        + f", over {MAX_BODY_BYTES} bytes={sum(1 for e in eligible if e['new_bytes'] > MAX_BODY_BYTES)}"
    )
    out += ["", "## Releases", "", "| tag | published | status | old bytes | new bytes | note |", "|---|---|---|---|---|---|"]
    for entry in plan:
        note = entry["reason"] or ("; ".join(entry["warnings"])[:160] if entry["warnings"] else "")
        new = entry["new_bytes"] if entry["new_bytes"] is not None else ""
        out.append(
            f"| {entry['tag']} | {(entry['published_at'] or '')[:10]} | {entry['status']} | {entry['old_bytes']} | {new} | {note.replace('|', '/')} |"
        )
    out += ["", "## Diff samples (old head vs new head)", ""]
    for entry in pick_samples(plan):
        out += [
            f"### {entry['tag']} ({(entry['published_at'] or '')[:10]}, {entry['old_bytes']} -> {entry['new_bytes']} bytes)",
            "",
            "Old:",
            "",
            "```text",
            head_lines((out_dir / "old" / f"{entry['tag']}.md").read_text(encoding="utf-8"), 6),
            "```",
            "",
            "New:",
            "",
            "```text",
            head_lines(entry["new_body"], samples_head),
            "```",
            "",
        ]
    out += ["## Source cross-check: entries at the tag vs the `## [X]` section of the compare ref (main)", ""]
    compared = [e for e in plan if e.get("source") and e["status"] in (EDIT, NO_ENTRY)]
    differing = [e for e in compared if e["source"]["at_tag"] != e["source"]["in_main"]]
    out.append(
        f"{len(differing)} of {len(compared)} releases differ. The tag-time reading is the [Unreleased] delta "
        "against the previous release tag; main's section is what the release-metadata pull request wrote "
        "later (no tag has its own section)."
    )
    out += ["", "| tag | status | entries at the tag | entries in main's section |", "|---|---|---|---|"]
    for e in differing:
        out.append(f"| {e['tag']} | {e['status']} | {e['source']['at_tag']} | {e['source']['in_main'] or 'no section'} |")
    links = [(e["tag"], n, c) for e in eligible for n, c in e["ref_context"]]
    out += ["", "## #N references the notes link to this repository", "", f"{len(links)} links. Each must be an issue or pull request of THIS repository.", ""]
    out.extend(f"* {tag} #{number}: `{context.replace('`', '')}`" for tag, number, context in links)
    out += ["", "## Warnings", ""]
    if not warned:
        out.append("None.")
    for entry in warned:
        out.append(f"* {entry['tag']}")
        out.extend(f"  * {w}" for w in entry["warnings"])
    out += [
        "",
        "## Apply (not run)",
        "",
        "The bodies to write are in `new/`, the current ones in `old/`; apply needs this directory (plan.json, new/) to still be here "
        "and the dry run to match the live releases, otherwise it refuses. It needs the flag AND the variable, and it is the only apply "
        "path: it checks the cutoff, immutability, the live body and the artifacts part, writes backup/TAG.md before each edit, "
        "verifies the body afterwards and resumes from apply.log.jsonl:",
        "",
        "```bash",
        "# a batch of the oldest unedited eligible releases; repeat (resumes from apply.log.jsonl) or drop --max for all",
        apply_command(out_dir, 10),
        "```",
        "",
        f"Undo one release: `env -u GITHUB_TOKEN gh release edit TAG --repo {repo_name} --notes-file {out_dir}/backup/TAG.md`.",
        "",
    ]
    return "\n".join(out)


def gh(args, binary="gh", wait=60, attempts=3):
    """stdout of a gh call; a rate limit is waited out, any other failure raises."""
    for attempt in range(attempts):
        done = subprocess.run([binary, *args], capture_output=True, text=True, encoding="utf-8")
        if done.returncode == 0:
            return done.stdout
        if "rate limit" in (done.stderr or "").lower() and attempt + 1 < attempts:
            time.sleep(wait)
            continue
        raise BackfillError(f"gh {' '.join(args[:3])} failed: {(done.stderr or done.stdout).strip()[:300]}")
    raise BackfillError("gh: out of attempts")


def fetch_releases(repo_name, binary="gh"):
    out = gh(
        [
            "api", "-X", "GET", f"repos/{repo_name}/releases", "--paginate", "-F", "per_page=100",
            "--jq", ".[] | {tag_name,published_at,draft,prerelease,immutable,body}",
        ],
        binary,
    )
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def fetch_max_ref(repo_name, binary="gh"):
    try:
        return int(gh(["api", "-X", "GET", f"repos/{repo_name}/issues", "-F", "state=all", "-F", "per_page=1", "--jq", ".[0].number"], binary).strip())
    except (BackfillError, ValueError):
        return None


def load_releases(path):
    text = pathlib.Path(path).read_text(encoding="utf-8")
    try:
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        return [json.loads(line) for line in text.splitlines() if line.strip()]


def read_log(path):
    done = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                row = json.loads(line)
                done[row["tag"]] = row
    return done


def append_log(path, row):
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def apply_plan(plan, out_dir, repo_name, only=None, limit=None, pause=2.0, binary="gh", cutoff=CUTOFF, sleep=time.sleep):
    """Edit the eligible releases. Returns the tags edited. Stops at the first failure."""
    log_path = out_dir / "apply.log.jsonl"
    done = read_log(log_path)
    (out_dir / "backup").mkdir(parents=True, exist_ok=True)
    edited = []
    for entry in plan:
        tag = entry["tag"]
        if only and tag not in only:
            continue
        if entry["status"] != EDIT:
            if only:
                raise BackfillError(f"{tag} is not eligible: {entry['status']} ({entry['reason']})")
            continue
        if done.get(tag, {}).get("status") == "done":
            continue
        if limit is not None and len(edited) >= limit:
            break
        live = json.loads(gh(["api", f"repos/{repo_name}/releases/tags/{tag}"], binary))
        why = immutable_reason(live, cutoff)
        if why:
            raise BackfillError(f"{tag}: {why}; refusing")
        old = live.get("body") or ""
        if sha(old) != entry["old_sha256"]:
            raise BackfillError(f"{tag}: the live body differs from the planned one; run the dry run again")
        parts = split_body(old)
        replacement = entry["new_body"]
        if parts is None or not replacement.endswith(parts[1]) or parts[0].strip():
            raise BackfillError(f"{tag}: the new body does not keep the artifacts part of the live body; refusing")
        backup = out_dir / "backup" / f"{tag}.md"
        with open(backup, "w", encoding="utf-8") as handle:
            handle.write(old)
            handle.flush()
            os.fsync(handle.fileno())
        notes_file = out_dir / "new" / f"{tag}.md"
        if sha(notes_file.read_text(encoding="utf-8")) != entry["new_sha256"]:
            raise BackfillError(f"{tag}: {notes_file} is not the reviewed body; run the dry run again")
        gh(["release", "edit", tag, "--repo", repo_name, "--notes-file", str(notes_file)], binary)
        after = json.loads(gh(["api", f"repos/{repo_name}/releases/tags/{tag}"], binary))
        ok = sha(after.get("body") or "") == entry["new_sha256"]
        append_log(
            log_path,
            {"tag": tag, "status": "done" if ok else "verify-failed", "old_sha256": entry["old_sha256"], "new_sha256": entry["new_sha256"],
             "backup": str(backup), "at": datetime.datetime.now(datetime.timezone.utc).isoformat()},
        )
        if not ok:
            raise BackfillError(f"{tag}: the body read back after the edit is not the planned one; restore from {backup}")
        edited.append(tag)
        sleep(pause)
    return edited


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out-dir", required=True, help="plan, report, old and new bodies, backups and the apply log go here")
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--root", default=str(ROOT), help="git checkout that holds the tags (default: this one)")
    parser.add_argument("--releases-json", help="JSON or JSON-lines releases (gh api repos/R/releases); default: fetch with gh")
    parser.add_argument("--compare-ref", default="origin/main", help="ref whose CHANGELOG.md is only compared with the tag-time entries (report); '' to skip")
    parser.add_argument("--gh", default="gh", help="gh binary")
    parser.add_argument("--apply", action="store_true", help=f"edit the releases; also needs {APPLY_ENV}=yes")
    parser.add_argument("--only", default="", help="apply only these tags (comma separated)")
    parser.add_argument("--max", type=int, default=None, help="apply at most this many releases in this run")
    parser.add_argument("--pause", type=float, default=2.0, help="seconds between edits")
    args = parser.parse_args(argv)

    out_dir = pathlib.Path(args.out_dir)
    if args.apply and os.environ.get(APPLY_ENV) != "yes":
        print(f"error: --apply needs {APPLY_ENV}=yes in the environment; nothing was edited", file=sys.stderr)
        return 2
    try:
        releases = load_releases(args.releases_json) if args.releases_json else fetch_releases(args.repo, args.gh)
        max_ref = fetch_max_ref(args.repo, args.gh) if not args.releases_json else None
        repo = Repo(args.root)
        compare = run_git(repo.root, "show", f"{args.compare_ref}:CHANGELOG.md") if args.compare_ref else None
        plan = build_plan(repo, releases, max_ref, compare=compare)
        if not args.apply:
            write_plan(plan, releases, out_dir)
        if args.apply:
            # Apply what the owner reviewed: the files of the dry run, if the plan still matches the releases.
            if not (out_dir / "plan.json").exists():
                raise BackfillError(f"no plan.json in {out_dir}: run the dry run first and review it")
            reviewed = {e["tag"]: e for e in json.loads((out_dir / "plan.json").read_text(encoding="utf-8"))}
            for entry in plan:
                before = reviewed.get(entry["tag"])
                if entry["status"] == EDIT and (before is None or before.get("new_sha256") != entry["new_sha256"]):
                    raise BackfillError(f"{entry['tag']}: differs from the reviewed dry run in {out_dir}; run the dry run again")
            edited = apply_plan(plan, out_dir, args.repo, set(filter(None, args.only.split(","))), args.max, args.pause, args.gh)
            print(f"edited {len(edited)} release(s): {' '.join(edited)}")
            return 0
        report = render_report(plan, out_dir, args.repo, max_ref)
        (out_dir / "report.md").write_text(report, encoding="utf-8")
        print(report.split("## Releases")[0])
        print(f"report: {out_dir / 'report.md'}")
    except (BackfillError, OSError, json.JSONDecodeError) as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
