"""GitHub release notes rendered from CHANGELOG.md in the VictoriaLogs release style."""

import collections
import contextlib
import io
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import release_notes as rn  # noqa: E402

HERE = os.path.dirname(__file__)
DATA = os.path.join(HERE, "testdata", "release_notes")
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
ISSUES = "https://github.com/ReliablyObserve/victoria-lakehouse/issues/"


def read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


FIXTURE = read(os.path.join(DATA, "changelog_fixture.md"))


def changelog(*sections):
    return "# Changelog\n\n" + "\n".join(sections)


class Golden(unittest.TestCase):
    """Real sections of CHANGELOG.md; the golden files are the reviewed output."""

    def check(self, version):
        body, _ = rn.render(FIXTURE, version)
        self.assertEqual(body, read(os.path.join(DATA, f"{version}.golden.md")))

    def test_0_157_6(self):
        self.check("0.157.6")

    def test_0_157_4_security_block(self):
        self.check("0.157.4")

    def test_0_157_2_changed_removed_fixed_security(self):
        self.check("0.157.2")

    def test_0_157_1(self):
        self.check("0.157.1")

    def test_0_157_0(self):
        self.check("0.157.0")

    def test_0_157_5_empty_section(self):
        self.check("0.157.5")

    def test_0_142_7_prose_before_bullets(self):
        self.check("0.142.7")


class UpstreamShape(unittest.TestCase):
    """The shape of https://github.com/VictoriaMetrics/VictoriaLogs/releases."""

    def test_first_line_and_group_order(self):
        body, _ = rn.render(FIXTURE, "0.157.2")
        lines = body.split("\n")
        self.assertEqual(lines[0], "Released at 2026-10-21")
        self.assertEqual(lines[1], "")
        prefixes = [re.match(r"\* ([A-Z]+): ", l).group(1) for l in lines if l.startswith("* ")]
        self.assertEqual(prefixes, ["SECURITY", "FEATURE", "FEATURE", "BUGFIX", "BUGFIX"])

    def test_blank_line_between_groups_none_inside(self):
        body, _ = rn.render(FIXTURE, "0.157.2")
        groups = body.rstrip("\n").split("\n\n")
        bullet_groups = [g for g in groups if g.startswith("* ")]
        self.assertEqual(len(bullet_groups), 3)
        for g in bullet_groups:
            self.assertEqual(len({l.split(":")[0] for l in g.split("\n")}), 1)
        self.assertTrue(body.endswith("\n"))
        self.assertFalse(body.endswith("\n\n"))

    def test_only_upstream_prefixes(self):
        for version in ("0.157.0", "0.157.1", "0.157.2", "0.157.4", "0.157.6"):
            body, _ = rn.render(FIXTURE, version)
            for line in body.split("\n"):
                if line.startswith("* "):
                    self.assertRegex(line, r"^\* (SECURITY|FEATURE|BUGFIX): ")

    def test_bold_lead_in_stays_first_words(self):
        body, _ = rn.render(FIXTURE, "0.157.6")
        self.assertRegex(body, r"\* BUGFIX: \*\*Only `FIPS=1` builds an image")
        self.assertRegex(body, r"\* SECURITY: \*\*The base images of the release Dockerfiles")

    def test_one_bullet_is_one_line(self):
        body, _ = rn.render(FIXTURE, "0.157.4")
        self.assertEqual(sum(1 for l in body.split("\n") if l.startswith("* SECURITY: ")), 5)

    def test_wrapped_bullet_joined(self):
        src = changelog("## [1.0.0] - 2026-01-02\n\n### Fixed\n\n- **A.** one\n  two\n  three\n")
        body, _ = rn.render(src, "1.0.0")
        self.assertIn("* BUGFIX: **A.** one two three\n", body)


class Mapping(unittest.TestCase):
    def test_keep_a_changelog_sections(self):
        src = changelog(
            "## [1.0.0] - 2026-01-02\n\n"
            "### Added\n\n- a\n\n### Changed\n\n- c\n\n### Deprecated\n\n- d\n\n"
            "### Removed\n\n- r\n\n### Fixed\n\n- f\n\n### Security\n\n- s\n\n"
            "### Performance\n\n- p\n"
        )
        body, warnings = rn.render(src, "1.0.0")
        self.assertEqual(warnings, [])
        self.assertEqual(
            body,
            "Released at 2026-01-02\n\n"
            "* SECURITY: s\n\n"
            "* FEATURE: a\n"
            "* FEATURE: c\n"
            "* FEATURE: deprecated: d\n"
            "* FEATURE: removed: r\n"
            "* FEATURE: p\n\n"
            "* BUGFIX: f\n",
        )

    def test_parenthesised_heading_suffix_ignored(self):
        src = changelog("## [1.0.0] - 2026-01-02\n\n### Fixed (UI internals)\n\n- x\n")
        body, warnings = rn.render(src, "1.0.0")
        self.assertIn("* BUGFIX: x", body)
        self.assertEqual(warnings, [])

    def test_unknown_subsection_renders_as_feature_with_warning(self):
        src = changelog("## [1.0.0] - 2026-01-02\n\n### Weird stuff\n\n- **Kept.** text (#7)\n")
        body, warnings = rn.render(src, "1.0.0")
        self.assertIn(f"* FEATURE: **Kept.** text ([#7]({ISSUES}7))", body)
        self.assertEqual(len(warnings), 1)
        self.assertIn("Weird stuff", warnings[0])

    def test_bullet_outside_any_subsection_is_kept_with_warning(self):
        src = changelog("## [1.0.0] - 2026-01-02\n\n- loose bullet\n")
        body, warnings = rn.render(src, "1.0.0")
        self.assertIn("* FEATURE: loose bullet", body)
        self.assertEqual(len(warnings), 1)

    def test_prose_kept_after_released_at(self):
        src = changelog("## [1.0.0] - 2026-01-02\n\nCut from a test-only change.\n\n### Fixed\n\n- f\n")
        body, warnings = rn.render(src, "1.0.0")
        self.assertTrue(body.startswith("Released at 2026-01-02\n\nCut from a test-only change.\n\n* BUGFIX: f\n"))
        self.assertEqual(warnings, [])

    def test_nested_bullets_and_paragraphs_stay_inside_their_bullet(self):
        src = changelog(
            "## [1.0.0] - 2026-01-02\n\n### Fixed\n\n- **A.** intro:\n  - first\n    wrapped\n  - second\n\n"
            "  Second paragraph.\n- **B.** next\n"
        )
        body, _ = rn.render(src, "1.0.0")
        self.assertEqual(
            body,
            "Released at 2026-01-02\n\n"
            "* BUGFIX: **A.** intro:\n  - first wrapped\n  - second\n\n  Second paragraph.\n"
            "* BUGFIX: **B.** next\n",
        )


class UpdateNotes(unittest.TestCase):
    def test_single_note_is_unnumbered(self):
        src = changelog(
            "## [1.0.0] - 2026-01-02\n\n### Update notes\n\n- the `x` flag now requires `POST` (#12).\n\n"
            "### Fixed\n\n- f\n"
        )
        body, warnings = rn.render(src, "1.0.0")
        self.assertEqual(warnings, [])
        self.assertEqual(
            body,
            "Released at 2026-01-02\n\n"
            f"**Update note:** the `x` flag now requires `POST` ([#12]({ISSUES}12)).\n\n"
            "* BUGFIX: f\n",
        )

    def test_several_notes_are_numbered_in_order(self):
        src = changelog(
            "## [1.0.0] - 2026-01-02\n\n### Update notes\n\n- first\n  wrapped\n\n- second\n\n### Added\n\n- a\n"
        )
        body, _ = rn.render(src, "1.0.0")
        self.assertEqual(
            body,
            "Released at 2026-01-02\n\n"
            "**Update note 1:** first wrapped\n\n"
            "**Update note 2:** second\n\n"
            "* FEATURE: a\n",
        )

    def test_update_note_without_other_changes(self):
        src = changelog("## [1.0.0] - 2026-01-02\n\n### Update notes\n\n- only this\n")
        body, _ = rn.render(src, "1.0.0")
        self.assertEqual(body, "Released at 2026-01-02\n\n**Update note:** only this\n")

    def test_singular_heading_accepted(self):
        src = changelog("## [1.0.0] - 2026-01-02\n\n### Update note\n\n- n\n")
        body, warnings = rn.render(src, "1.0.0")
        self.assertIn("**Update note:** n", body)
        self.assertEqual(warnings, [])


class Links(unittest.TestCase):
    def test_bare_reference_becomes_issue_link(self):
        self.assertEqual(rn.link_refs("fix (#685)."), f"fix ([#685]({ISSUES}685)).")

    def test_not_rewritten_in_code_links_urls_or_other_repos(self):
        for text in (
            "`#123` stays",
            "[#123](https://example.com/x) stays",
            "https://example.com/page#123 stays",
            "VictoriaMetrics/VictoriaLogs#1635 stays",
            "colour #123456 stays",
            "C#7 stays",
            "entity &#123; stays",
            "a path docs/#12 stays",
            "[#12] without a target stays",
            f"[#9]({ISSUES}9) already linked",
        ):
            self.assertEqual(rn.link_refs(text), text)

    def test_several_references_and_idempotent(self):
        once = rn.link_refs("see #1, #22 and #333")
        self.assertEqual(
            once,
            f"see [#1]({ISSUES}1), [#22]({ISSUES}22) and [#333]({ISSUES}333)",
        )
        self.assertEqual(rn.link_refs(once), once)

    def test_reference_at_start_and_end(self):
        self.assertEqual(rn.link_refs("#5"), f"[#5]({ISSUES}5)")


class EmptyAndErrors(unittest.TestCase):
    def test_empty_section_has_no_changes_line(self):
        body, warnings = rn.render(changelog("## [1.0.0] - 2026-01-02\n"), "1.0.0")
        self.assertEqual(body, "Released at 2026-01-02\n\nNo user-visible changes.\n")
        self.assertEqual(warnings, [])

    def test_empty_subsections_only(self):
        body, _ = rn.render(changelog("## [1.0.0] - 2026-01-02\n\n### Fixed\n\n### Security\n"), "1.0.0")
        self.assertEqual(body, "Released at 2026-01-02\n\nNo user-visible changes.\n")

    def test_missing_version_raises(self):
        with self.assertRaises(rn.ReleaseNotesError):
            rn.render(FIXTURE, "9.9.9")

    def test_version_is_not_matched_by_prefix(self):
        with self.assertRaises(rn.ReleaseNotesError):
            rn.render(FIXTURE, "0.157")

    def test_unreleased_needs_a_date(self):
        src = changelog("## [Unreleased]\n\n### Fixed\n\n- f\n\n## [1.0.0] - 2026-01-02\n")
        body, _ = rn.render(src, "Unreleased", date="2026-02-03")
        self.assertEqual(body, "Released at 2026-02-03\n\n* BUGFIX: f\n")

    def test_date_override_and_bad_date(self):
        body, _ = rn.render(FIXTURE, "0.157.5", date="2030-01-01")
        self.assertTrue(body.startswith("Released at 2030-01-01\n"))
        with self.assertRaises(rn.ReleaseNotesError):
            rn.render(FIXTURE, "0.157.5", date="1 Jan")

    def test_versioned_section_without_date_needs_override(self):
        src = changelog("## [1.0.0]\n\n### Fixed\n\n- f\n")
        with self.assertRaises(rn.ReleaseNotesError):
            rn.render(src, "1.0.0")

    def test_section_stops_at_the_next_version(self):
        body, _ = rn.render(FIXTURE, "0.157.5")
        self.assertNotIn("FIPS", body)


class Determinism(unittest.TestCase):
    def test_same_input_same_output(self):
        for version in ("0.157.0", "0.157.2", "0.157.4", "0.157.5", "0.157.6", "0.142.7"):
            a = rn.render(FIXTURE, version)
            b = rn.render(FIXTURE, version)
            self.assertEqual(a, b)

    def test_crlf_input_renders_the_same(self):
        a, _ = rn.render(FIXTURE, "0.157.2")
        b, _ = rn.render(FIXTURE.replace("\n", "\r\n"), "0.157.2")
        self.assertEqual(a, b)


class NeverDropsText(unittest.TestCase):
    """Every word of every released section of the real CHANGELOG.md reaches the notes."""

    @staticmethod
    def source_words(section_lines):
        counter = collections.Counter()
        for line in section_lines:
            s = line.strip()
            if not s or s.startswith("## ") or re.match(r"#{3} ", s):
                continue
            if s.startswith(("- ", "* ")):
                s = s[2:]
            counter.update(s.split())
        return counter

    def test_all_released_sections(self):
        text = read(os.path.join(ROOT, "CHANGELOG.md"))
        sections = re.split(r"(?m)^(?=## \[)", text)
        checked = 0
        for section in sections:
            m = re.match(r"## \[([^\]]+)\] - (\d{4}-\d{2}-\d{2})", section)
            if not m:
                continue
            version = m.group(1)
            body, _ = rn.render(text, version)
            self.assertRegex(body, r"^Released at \d{4}-\d{2}-\d{2}\n")
            plain = re.sub(r"\[(#\d+)\]\(https://github\.com/[^)]*\)", r"\1", body)
            have = collections.Counter(plain.split())
            missing = self.source_words(section.split("\n")[1:]) - have
            self.assertEqual(dict(missing), {}, f"text dropped from {version}")
            checked += 1
        self.assertGreater(checked, 100)


class Cli(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = self._tmp.name

    def run_main(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = rn.main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def fixture_path(self):
        return os.path.join(DATA, "changelog_fixture.md")

    def test_prints_notes(self):
        code, out, err = self.run_main("--changelog", self.fixture_path(), "--version", "0.157.0")
        self.assertEqual(code, 0)
        self.assertEqual(out, read(os.path.join(DATA, "0.157.0.golden.md")))
        self.assertEqual(err, "")

    def test_leading_v_accepted(self):
        _, out, _ = self.run_main("--changelog", self.fixture_path(), "--version", "v0.157.0")
        self.assertEqual(out, read(os.path.join(DATA, "0.157.0.golden.md")))

    def test_warnings_go_to_stderr(self):
        src = changelog("## [1.0.0] - 2026-01-02\n\n### Odd\n\n- text\n")
        path = os.path.join(self.tmp, "c.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write(src)
        code, out, err = self.run_main("--changelog", path, "--version", "1.0.0")
        self.assertEqual(code, 0)
        self.assertIn("* FEATURE: text", out)
        self.assertNotIn("warning", out)
        self.assertIn("warning: [1.0.0]: unknown subsection '### Odd'", err)

    def test_unknown_version_fails_without_output(self):
        code, out, err = self.run_main("--changelog", self.fixture_path(), "--version", "9.9.9")
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")
        self.assertIn("9.9.9", err)

    def test_missing_file_fails(self):
        code, out, err = self.run_main("--changelog", os.path.join(DATA, "nope.md"), "--version", "1.0.0")
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")

    def test_unreleased_defaults_to_today_utc(self):
        code, out, _ = self.run_main("--changelog", self.fixture_path(), "--version", "Unreleased")
        self.assertEqual(code, 0)
        self.assertRegex(out, r"^Released at \d{4}-\d{2}-\d{2}\n\nNo user-visible changes\.\n$")


class AfterTag(unittest.TestCase):
    """Entries the previous release already carried in [Unreleased] are not repeated."""

    PREVIOUS = changelog("## [Unreleased]\n\n### Fixed\n\n- **Old fix.** was\n  wrapped (#1)\n\n### Update notes\n\n- old note\n")
    CURRENT = changelog(
        "## [Unreleased]\n\n### Fixed\n\n- **Old fix.** was wrapped (#1)\n- **New fix.** (#2)\n\n"
        "### Update notes\n\n- old note\n- new note\n\n## [1.0.0] - 2026-01-02\n"
    )

    def test_bullet_keys_ignore_wrapping(self):
        self.assertEqual(
            rn.bullet_keys(self.PREVIOUS),
            {"**Old fix.** was wrapped (#1)", "old note"},
        )

    def test_old_entries_left_out(self):
        body, _ = rn.render(self.CURRENT, "Unreleased", "2026-02-03", rn.bullet_keys(self.PREVIOUS))
        self.assertEqual(
            body,
            "Released at 2026-02-03\n\n**Update note:** new note\n\n"
            f"* BUGFIX: **New fix.** ([#2]({ISSUES}2))\n",
        )

    def test_everything_old_means_no_changes(self):
        body, _ = rn.render(self.PREVIOUS, "Unreleased", "2026-02-03", rn.bullet_keys(self.PREVIOUS))
        self.assertEqual(body, "Released at 2026-02-03\n\nNo user-visible changes.\n")

    def test_no_previous_unreleased_section(self):
        self.assertEqual(rn.bullet_keys(FIXTURE, "9.9.9"), set())
        self.assertEqual(rn.bullet_keys(changelog("## [1.0.0] - 2026-01-01\n\n- x\n")), set())

    def git(self, cwd, *args):
        subprocess.run(
            ["git", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
            cwd=cwd, check=True, capture_output=True,
        )

    def run_in(self, cwd, *argv):
        old = os.getcwd()
        out, err = io.StringIO(), io.StringIO()
        os.chdir(cwd)
        try:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = rn.main(list(argv))
        finally:
            os.chdir(old)
        return code, out.getvalue(), err.getvalue()

    def test_cli_reads_the_previous_tag_from_git(self):
        with tempfile.TemporaryDirectory() as repo:
            self.git(repo, "init", "-q")
            with open(os.path.join(repo, "CHANGELOG.md"), "w", encoding="utf-8") as f:
                f.write(self.PREVIOUS)
            self.git(repo, "add", "CHANGELOG.md")
            self.git(repo, "commit", "-q", "-m", "previous")
            self.git(repo, "tag", "v1.0.0")
            with open(os.path.join(repo, "CHANGELOG.md"), "w", encoding="utf-8") as f:
                f.write(self.CURRENT)
            code, out, err = self.run_in(
                repo, "--version", "Unreleased", "--date", "2026-02-03", "--after-tag", "v1.0.0"
            )
        self.assertEqual(code, 0, err)
        self.assertEqual(err, "")
        self.assertIn("**New fix.**", out)
        self.assertNotIn("Old fix", out)
        self.assertNotIn("old note", out)

    def test_unreadable_tag_renders_everything_with_a_warning(self):
        with tempfile.TemporaryDirectory() as repo:
            self.git(repo, "init", "-q")
            with open(os.path.join(repo, "CHANGELOG.md"), "w", encoding="utf-8") as f:
                f.write(self.CURRENT)
            code, out, err = self.run_in(
                repo, "--version", "Unreleased", "--date", "2026-02-03", "--after-tag", "v9.9.9"
            )
        self.assertEqual(code, 0)
        self.assertIn("Old fix", out)
        self.assertIn("**New fix.**", out)
        self.assertIn("warning: cannot read CHANGELOG.md at v9.9.9", err)

    def test_empty_tag_is_ignored(self):
        code, out, err = self.run_in(
            DATA, "--changelog", "changelog_fixture.md", "--version", "0.157.0", "--after-tag", ""
        )
        self.assertEqual(code, 0)
        self.assertEqual(err, "")
        self.assertEqual(out, read(os.path.join(DATA, "0.157.0.golden.md")))


class WorkflowWiring(unittest.TestCase):
    """The release job puts the generated notes in front of the artifacts section."""

    def step(self):
        workflow = read(os.path.join(ROOT, ".github/workflows/auto-release.yaml"))
        step = workflow.split("      - name: Create release\n", 1)[1]
        return step.split("\n      - name:", 1)[0]

    def test_notes_come_from_the_generator(self):
        step = self.step()
        self.assertIn("scripts/ci/release_notes.py", step)
        self.assertIn("--version Unreleased", step)
        self.assertIn('--after-tag "$(git describe --tags --abbrev=0 "$VERSION^"', step)
        self.assertNotIn("extract_changelog_section.sh", step)

    def test_notes_precede_the_artifacts_section(self):
        step = self.step()
        heredoc = step.split("<<EOF\n", 1)[1].split("\n          EOF", 1)[0]
        lines = [l.strip() for l in heredoc.split("\n")]
        self.assertEqual(lines[0], "${NOTES}")
        self.assertEqual(lines.count("${NOTES}"), 1)
        self.assertLess(lines.index("${NOTES}"), lines.index("## Release Artifacts"))

    def test_assembled_body_runs_the_real_step_text(self):
        """Run the notes part of the step: shell metacharacters in a bullet stay text."""
        step = self.step()
        script = step.split("          # Release notes in the", 1)[1]
        script = "# Release notes in the" + script.split("\n          EOF\n", 1)[0] + "\n          EOF\n"
        script = "\n".join(l[10:] if l.startswith("          ") else l for l in script.split("\n"))
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "scripts", "ci"))
            shutil.copy(os.path.join(ROOT, "scripts/ci/release_notes.py"), os.path.join(tmp, "scripts/ci"))
            with open(os.path.join(tmp, "CHANGELOG.md"), "w", encoding="utf-8") as f:
                f.write(
                    "## [Unreleased]\n\n### Fixed\n\n"
                    "- **Quoting.** keeps `$(touch PWNED)`, $(touch PWNED2), `id` and $HOME (#5).\n\n"
                    "## [1.0.0] - 2026-01-01\n"
                )
            script = script.replace("/tmp/release-notes.md", os.path.join(tmp, "out.md"))
            script = script.replace("${{ github.repository_owner }}", "Owner")
            result = subprocess.run(
                ["bash", "-euo", "pipefail", "-c", 'VERSION=v1.2.3\nREPO=o/r\n' + script],
                cwd=tmp, capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            out = read(os.path.join(tmp, "out.md"))
            self.assertFalse(os.path.exists(os.path.join(tmp, "PWNED")))
            self.assertFalse(os.path.exists(os.path.join(tmp, "PWNED2")))
        self.assertRegex(out, r"^Released at \d{4}-\d{2}-\d{2}\n\n\* BUGFIX: \*\*Quoting\.\*\* keeps `\$\(touch PWNED\)`, \$\(touch PWNED2\), `id` and \$HOME ")
        self.assertIn("/issues/5)).\n\n## Release Artifacts\n", out)
        self.assertIn("docker pull ghcr.io/o/r/lakehouse-logs:v1.2.3\n", out)

    def test_no_context_expression_in_the_notes_command(self):
        for line in self.step().split("\n"):
            if "release_notes.py" in line:
                self.assertNotIn("${{", line)


if __name__ == "__main__":
    unittest.main()
