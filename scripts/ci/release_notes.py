#!/usr/bin/env python3
"""Render GitHub release notes from CHANGELOG.md in the VictoriaLogs release style.

    release_notes.py --version 0.157.6
    release_notes.py --version Unreleased --date 2026-10-22

The output is deterministic and needs no network. It follows
https://github.com/VictoriaMetrics/VictoriaLogs/releases:

    Released at 2026-10-22

    **Update note 1:** ...            (from the "### Update notes" subsection)

    * SECURITY: ...                   (### Security)

    * FEATURE: ...                    (### Added, Changed, Deprecated, Removed, Performance)

    * BUGFIX: ...                     (### Fixed)

Rules that keep it safe to run in the release job:

* No text of the section is ever dropped. A subsection this script does not know
  is rendered under FEATURE with a warning on stderr; text outside any
  subsection is kept (bullets under FEATURE, prose right after "Released at").
* A bullet is one line, as upstream: wrapped lines are joined. Nested bullets and
  further paragraphs of a bullet stay under it.
* `#123` becomes a link to the issue or pull request of this repository. Code
  spans, existing links, URLs and references to other repositories are left alone.
* The bold lead-in of a bullet stays its first words. Removed and deprecated
  items say so (`removed: `, `deprecated: `) right after the prefix.
"""

import argparse
import datetime
import re
import sys

REPO_URL = "https://github.com/ReliablyObserve/victoria-lakehouse"

SECURITY, FEATURE, BUGFIX = "SECURITY", "FEATURE", "BUGFIX"
GROUP_ORDER = (SECURITY, FEATURE, BUGFIX)

# Keep a Changelog section -> (upstream prefix, words that state the kind of change).
SECTIONS = {
    "security": (SECURITY, ""),
    "added": (FEATURE, ""),
    "changed": (FEATURE, ""),
    "performance": (FEATURE, ""),
    "deprecated": (FEATURE, "deprecated: "),
    "removed": (FEATURE, "removed: "),
    "fixed": (BUGFIX, ""),
}
UPDATE_NOTES = {"update notes", "update note"}

VERSION_HEADING = re.compile(r"^## \[(?P<version>[^\]]+)\](?:[ \t]*-[ \t]*(?P<date>\d{4}-\d{2}-\d{2}))?")
SUBSECTION_HEADING = re.compile(r"^### (?P<title>.*\S)\s*$")
DEEPER_HEADING = re.compile(r"^#{4,6}\s")
BULLET = re.compile(r"^[-*] ")
NESTED_BULLET = re.compile(r"^(?P<indent> {2,})[-*] (?P<text>.*)$")
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Spans that must not be touched, then the references to turn into links.
REFERENCE = re.compile(
    r"(?P<skip>`[^`\n]*`|\[[^\]\n]*\]\([^)\n]*\)|https?://\S+|[\w.-]+/[\w.-]+#\d+)"
    r"|(?<![\w&#/\[])#(?P<number>\d{1,5})(?!\w)"
)


class ReleaseNotesError(Exception):
    """The notes cannot be rendered (no such version, bad date)."""


def link_refs(text):
    """Turn `#123` into a link to the issue or pull request of this repository."""

    def replace(match):
        number = match.group("number")
        if number is None:
            return match.group(0)
        return f"[#{number}]({REPO_URL}/issues/{number})"

    return REFERENCE.sub(replace, text)


def find_section(lines, version):
    """Return (date or None, body lines) of `## [version]`."""
    start = None
    date = None
    for index, line in enumerate(lines):
        match = VERSION_HEADING.match(line)
        if match is None:
            continue
        if start is not None:
            return date, lines[start:index]
        if match.group("version") == version:
            start, date = index + 1, match.group("date")
    if start is None:
        raise ReleaseNotesError(f"no section [{version}] in the changelog")
    return date, lines[start:]


class Bullet:
    """One changelog bullet: the first line and the lines that belong to it."""

    def __init__(self, text):
        self.rows = [("item", 0, text)]
        self.after_blank = False

    def add(self, line):
        if not line.strip():
            self.after_blank = True
            return
        stripped = line.strip()
        nested = NESTED_BULLET.match(line)
        if self.after_blank:
            self.rows.append(("para", 0, stripped))
        elif nested:
            self.rows.append(("nested", len(nested.group("indent")), nested.group("text").strip()))
        else:
            kind, indent, text = self.rows[-1]
            self.rows[-1] = (kind, indent, f"{text} {stripped}")
            self.after_blank = False
            return
        self.after_blank = False

    def flat(self):
        return " ".join(text for _, _, text in self.rows)

    def render(self, prefix, label):
        lines = []
        for kind, indent, text in self.rows:
            text = link_refs(text)
            if kind == "item":
                lines.append(f"* {prefix}: {label}{text}")
            elif kind == "nested":
                lines.append(f"{' ' * indent}- {text}")
            else:
                lines.extend(["", f"  {text}"])
        return lines


def parse_section(body, version):
    """Split a section body into prose, update notes and bullets per upstream prefix."""
    prose = []
    notes = []
    groups = {prefix: [] for prefix in GROUP_ORDER}
    warnings = []

    heading = None  # normalised subsection title, None before the first ### line
    known = None
    warned = set()
    orphan_warned = False
    paragraph = []

    def flush_paragraph():
        if paragraph:
            prose.append(" ".join(paragraph))
            paragraph.clear()

    current = None

    def flush_bullet():
        nonlocal current, orphan_warned
        if current is None:
            return
        bullet, current = current, None
        if heading is None:
            if not orphan_warned:
                warnings.append(f"[{version}]: a bullet outside any ### subsection is rendered under {FEATURE}")
                orphan_warned = True
            groups[FEATURE].append((bullet, ""))
        elif heading in UPDATE_NOTES:
            notes.append(bullet)
        elif known is not None:
            prefix, label = known
            groups[prefix].append((bullet, label))
        else:
            groups[FEATURE].append((bullet, ""))

    for raw in body:
        line = raw.rstrip()
        sub = SUBSECTION_HEADING.match(line)
        if sub:
            flush_bullet()
            flush_paragraph()
            title = sub.group("title")
            heading = re.sub(r"\s*\(.*\)\s*$", "", title).strip().lower()
            known = SECTIONS.get(heading)
            if known is None and heading not in UPDATE_NOTES and title not in warned:
                warned.add(title)
                warnings.append(f"[{version}]: unknown subsection '### {title}' is rendered under {FEATURE}")
            continue
        if current is not None:
            if not line.strip():
                current.add(line)
                continue
            if BULLET.match(line):
                flush_bullet()
            elif DEEPER_HEADING.match(line):
                flush_bullet()
            elif line.startswith((" ", "\t")) or not current.after_blank:
                current.add(line)
                continue
            else:
                flush_bullet()
        if not line.strip():
            flush_paragraph()
            continue
        if BULLET.match(line):
            flush_paragraph()
            current = Bullet(line[2:].strip())
            continue
        paragraph.append(line.strip())
    flush_bullet()
    flush_paragraph()
    return prose, notes, groups, warnings


def render(changelog_text, version, date=None):
    """Return (notes markdown, warnings) for `## [version]` of the changelog text."""
    version = version[1:] if re.match(r"^v\d", version) else version
    lines = changelog_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    section_date, body = find_section(lines, version)
    date = date or section_date
    if date is None:
        raise ReleaseNotesError(f"[{version}] has no date in its heading: pass one with --date")
    if not DATE.match(date):
        raise ReleaseNotesError(f"date '{date}' is not YYYY-MM-DD")
    try:
        datetime.date.fromisoformat(date)
    except ValueError as err:
        raise ReleaseNotesError(f"date '{date}' is not a calendar date") from err

    prose, notes, groups, warnings = parse_section(body, version)
    blocks = [f"Released at {date}"]
    blocks.extend(link_refs(paragraph) for paragraph in prose)
    for number, note in enumerate(notes, 1):
        tag = "**Update note:**" if len(notes) == 1 else f"**Update note {number}:**"
        blocks.append(f"{tag} {link_refs(note.flat())}")
    for prefix in GROUP_ORDER:
        if groups[prefix]:
            lines_out = []
            for bullet, label in groups[prefix]:
                lines_out.extend(bullet.render(prefix, label))
            blocks.append("\n".join(lines_out))
    if len(blocks) == 1:
        blocks.append("No user-visible changes.")
    return "\n\n".join(blocks) + "\n", warnings


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--changelog", default="CHANGELOG.md")
    parser.add_argument("--version", required=True, help='e.g. 0.157.6, v0.157.6 or "Unreleased"')
    parser.add_argument(
        "--date",
        help="YYYY-MM-DD; default: the date in the section heading, or today (UTC) for Unreleased",
    )
    args = parser.parse_args(argv)

    date = args.date
    if date is None and args.version == "Unreleased":
        date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    try:
        with open(args.changelog, encoding="utf-8") as handle:
            text = handle.read()
        notes, warnings = render(text, args.version, date)
    except (OSError, ReleaseNotesError) as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    for warning in warnings:
        print(f"warning: {warning}", file=sys.stderr)
    sys.stdout.write(notes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
