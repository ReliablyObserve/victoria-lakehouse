"""Changelog fragments: parsing, assembly into the release section, the release-metadata shape, the PR gate.

The release-metadata dry run builds a fixture repository the way the release job sees it: fragments on
main, a release commit, a tag, then more merges.
"""
import contextlib
import io
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
import unittest.mock

from scripts.ci import changelog_fragments as cf
from scripts.ci import check_changelog_pr as cc

ROOT = pathlib.Path(__file__).resolve().parents[3]

BASE_CHANGELOG = """# Changelog

preamble

## [Unreleased]

## [0.1.0] - 2026-01-01

### Fixed

- **Old fix.** text.
"""


def frag(path, text):
    fr, why = cc.parse_fragment(path, text)
    assert fr is not None, why
    return fr


class ParseTests(unittest.TestCase):
    def ok(self, text, name="changelog.d/1.fixed.md"):
        fr, why = cc.parse_fragment(name, text)
        self.assertEqual(why, [])
        return fr

    def bad(self, text, name="changelog.d/1.fixed.md", needle=""):
        fr, why = cc.parse_fragment(name, text)
        self.assertIsNone(fr, text)
        self.assertTrue(why and needle in why[0], why)

    def test_a_bullet_is_a_fragment(self):
        fr = self.ok("- **Lead.** body (#1).\n")
        self.assertEqual((fr.kind, fr.lead_in, fr.text), ("fixed", "Lead.", "- **Lead.** body (#1)."))

    def test_wrapped_and_indented_second_paragraph_is_the_house_style(self):
        fr = self.ok("- **Lead.** wrapped\n  body.\n\n  Second paragraph\n  of the same item.\n")
        self.assertEqual(len(fr.lines), 5)
        self.assertEqual(fr.text, "- **Lead.** wrapped body. Second paragraph of the same item.")

    def test_an_indented_sub_list_belongs_to_the_item(self):
        fr = self.ok("- **Lead.** body:\n\n  - first detail\n  - second detail\n")
        self.assertEqual(fr.text, "- **Lead.** body: - first detail - second detail")

    def test_every_kind_is_accepted_and_nothing_else(self):
        for kind in cc.FRAGMENT_KINDS:
            self.assertEqual(self.ok("- **A.** b\n", f"changelog.d/x.{kind}.md").kind, kind)
        for name in ("changelog.d/x.fix.md", "changelog.d/x.md", "changelog.d/x.performance.md",
                     "changelog.d/X.fixed.md", "changelog.d/.fixed.md", "changelog.d/a/b.fixed.md",
                     "changelog.d/x.fixed.txt", "docs/x.fixed.md", "changelog.d/x fixed.fixed.md",
                     "changelog.d/x.fixed.md.sh", "x/changelog.d/x.fixed.md", "changelog.d/-x.fixed.md"):
            self.bad("- **A.** b\n", name, needle="not a fragment name")

    def test_empty(self):
        self.bad("", needle="empty")
        self.bad("\n  \n", needle="empty")

    def test_not_a_bullet_or_no_bold_lead_in(self):
        self.bad("plain text\n", needle="must start with")
        self.bad("- no lead in\n", needle="must start with")
        self.bad("- **unclosed lead in\n", needle="must start with")
        self.bad("### Fixed\n\n- **A.** b\n", needle="must start with")

    def test_two_items_a_second_paragraph_or_a_heading_are_refused(self):
        self.bad("- **A.** b\n- **C.** d\n", needle="exactly one bullet")
        self.bad("- **A.** b\n\nUnindented second paragraph.\n", needle="exactly one bullet")
        self.bad("- **A.** b\n\n### Fixed\n", needle="exactly one bullet")
        self.bad("- **A.** b\n## [1.0.0]\n", needle="exactly one bullet")

    def test_star_and_plus_second_items_are_refused(self):
        self.bad("- **A.** b\n* **C.** d\n", needle="exactly one bullet")
        self.bad("- **A.** b\n+ **C.** d\n", needle="exactly one bullet")

    def test_the_name_cap_and_the_size_cap(self):
        stem = "a" * 81  # 1 + 80
        self.assertEqual(self.ok("- **A.** b\n", f"changelog.d/{stem}.fixed.md").kind, "fixed")
        self.bad("- **A.** b\n", f"changelog.d/{stem}a.fixed.md", needle="not a fragment name")
        self.ok("- **A.** " + "x" * (cc.FRAGMENT_MAX_BYTES - 20) + "\n")
        self.bad("- **A.** " + "x" * cc.FRAGMENT_MAX_BYTES + "\n", needle="larger than")

    def test_a_name_with_a_trailing_newline_is_not_a_fragment(self):
        self.assertIsNone(cc.fragment_kind("changelog.d/1.fixed.md\n"))
        self.bad("- **A.** b\n", "changelog.d/1.fixed.md\n", needle="not a fragment name")
        self.assertIsNone(cc.fragment_kind("x/changelog.d/1.fixed.md"))

    def test_an_unclosed_html_comment_is_refused(self):
        self.bad("- **A.** b <!-- hides the rest\n", needle="not closed")
        self.bad("- **A.** b\n\n  <!-- open\n  more\n", needle="not closed")
        self.assertEqual(self.ok("- **A.** b <!-- note --> c\n").text, "- **A.** b <!-- note --> c")

    def test_control_characters_are_refused(self):
        self.bad("- **A.** b\x00c\n", needle="carriage returns")
        self.bad("- **A.** b\r\n", needle="carriage returns")
        self.bad("- **A.**\tb\n", needle="tabs")

    def test_trailing_whitespace_and_blank_edges_are_normalised(self):
        fr = self.ok("\n\n- **A.** b   \n  c  \n\n\n")
        self.assertEqual(fr.lines, ("- **A.** b", "  c"))

    def test_duplicates(self):
        a = frag("changelog.d/1.fixed.md", "- **A.** b\n")
        same = frag("changelog.d/2.fixed.md", "- **A.**   b\n")
        lead = frag("changelog.d/3.changed.md", "- **A.** other\n")
        other = frag("changelog.d/4.fixed.md", "- **B.** c\n")
        self.assertEqual(cc.duplicate_fragment_problems([a, other]), [])
        self.assertIn("same bullet as changelog.d/1.fixed.md", cc.duplicate_fragment_problems([a, same])[0])
        self.assertIn("same bold lead-in as changelog.d/1.fixed.md", cc.duplicate_fragment_problems([a, lead])[0])


class AssembleTests(unittest.TestCase):
    def test_sections_follow_keep_a_changelog_order_and_files_sort_by_name(self):
        frs = [
            frag("changelog.d/20.fixed.md", "- **F2.** t\n"),
            frag("changelog.d/10.fixed.md", "- **F1.** t\n"),
            frag("changelog.d/5.security.md".replace("security.md", "security.md"), "- **S.** t\n"),
            frag("changelog.d/7.added.md", "- **A.** t\n"),
            frag("changelog.d/8.removed.md", "- **R.** t\n"),
            frag("changelog.d/9.changed.md", "- **C.** t\n"),
            frag("changelog.d/9.deprecated.md", "- **D.** t\n"),
        ]
        out = cf.assemble(BASE_CHANGELOG, frs, "0.2.0", "2026-02-02")
        want = (
            "## [Unreleased]\n\n## [0.2.0] - 2026-02-02\n\n"
            "### Added\n\n- **A.** t\n\n"
            "### Changed\n\n- **C.** t\n\n"
            "### Deprecated\n\n- **D.** t\n\n"
            "### Removed\n\n- **R.** t\n\n"
            "### Fixed\n\n- **F1.** t\n\n- **F2.** t\n\n"
            "### Security\n\n- **S.** t\n\n"
            "## [0.1.0] - 2026-01-01\n"
        )
        self.assertIn(want, out)
        self.assertTrue(out.startswith("# Changelog\n\npreamble\n\n"))
        self.assertTrue(out.endswith("- **Old fix.** text.\n"))

    def test_the_release_shape_the_gate_requires_follows(self):
        frs = [frag("changelog.d/1.fixed.md", "- **F.** t\n  more\n"), frag("changelog.d/2.added.md", "- **A.** t\n")]
        out = cf.assemble(BASE_CHANGELOG, frs, "0.2.0", "2026-02-02")
        self.assertEqual(cc.changelog_release_shape(BASE_CHANGELOG, out, frs), ("0.1.0", "0.2.0"))
        # without the fragments the same head is not a release: lines appeared from nowhere
        self.assertIsNone(cc.changelog_release_shape(BASE_CHANGELOG, out))
        # a fragment whose bullet differs from the one in the section is refused
        other = [frag("changelog.d/1.fixed.md", "- **F.** changed\n  more\n"), frs[1]]
        self.assertIsNone(cc.changelog_release_shape(BASE_CHANGELOG, out, other))

    def test_no_fragments_makes_an_empty_section(self):
        out = cf.assemble(BASE_CHANGELOG, [], "0.2.0", "2026-02-02")
        self.assertIn("## [Unreleased]\n\n## [0.2.0] - 2026-02-02\n\n## [0.1.0]", out)
        self.assertEqual(cc.changelog_release_shape(BASE_CHANGELOG, out), ("0.1.0", "0.2.0"))

    def test_legacy_unreleased_bullets_and_fragments_share_the_section(self):
        legacy = BASE_CHANGELOG.replace(
            "## [Unreleased]\n",
            "## [Unreleased]\n\n### Fixed\n\n- **Legacy fix.** long\n  wrapped.\n\n  Second para.\n\n### Performance\n\n- **Legacy perf.** t\n",
        )
        frs = [frag("changelog.d/1.fixed.md", "- **New fix.** t\n"), frag("changelog.d/2.added.md", "- **New feat.** t\n")]
        out = cf.assemble(legacy, frs, "0.2.0", "2026-02-02")
        sect = out.split("## [0.2.0] - 2026-02-02\n\n", 1)[1].split("## [0.1.0]", 1)[0]
        self.assertEqual(
            sect,
            "### Added\n\n- **New feat.** t\n\n"
            "### Fixed\n\n- **Legacy fix.** long\n  wrapped.\n\n  Second para.\n\n- **New fix.** t\n\n"
            "### Performance\n\n- **Legacy perf.** t\n\n",
        )
        self.assertTrue(out.split("## [0.2.0]")[0].endswith("## [Unreleased]\n\n"))
        # [Keep a Changelog headings only] the release shape holds for the legacy text without '### Performance'
        plain = BASE_CHANGELOG.replace("## [Unreleased]\n", "## [Unreleased]\n\n### Fixed\n\n- **Legacy fix.** long\n  wrapped.\n")
        out2 = cf.assemble(plain, frs, "0.2.0", "2026-02-02")
        self.assertEqual(cc.changelog_release_shape(plain, out2, frs), ("0.1.0", "0.2.0"))

    def test_text_that_is_not_a_bullet_under_unreleased_is_an_error_not_a_loss(self):
        for body in ("prose without a heading\n", "### Fixed\n\nprose in a section\n", "- **bullet before any heading.** t\n"):
            bad = BASE_CHANGELOG.replace("## [Unreleased]\n", "## [Unreleased]\n\n" + body)
            with self.assertRaises(cf.AssembleError):
                cf.assemble(bad, [], "0.2.0", "2026-02-02")

    def test_bad_version_date_or_repeated_version(self):
        for version, date in (("v0.2.0", "2026-02-02"), ("0.2", "2026-02-02"), ("0.1.0", "2026-02-02"), ("0.2.0", "2026-13-40")):
            with self.assertRaises(cf.AssembleError, msg=(version, date)):
                cf.assemble(BASE_CHANGELOG, [], version, date)
        with self.assertRaises(cf.AssembleError):
            cf.assemble("# C\n", [], "0.2.0", "2026-02-02")


class RepoFixture(unittest.TestCase):
    """A repository with CHANGELOG.md and changelog.d/, driven the way the release job drives it."""

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(self.tmp)]))
        self.repo = self.tmp / "repo"
        self.repo.mkdir()
        self.env = dict(os.environ, GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1")
        self.git("init", "-q", "-b", "main")
        self.write("CHANGELOG.md", BASE_CHANGELOG)
        self.commit("base")
        self.git("tag", "v0.1.0")  # a release from before fragments existed
        self.write("changelog.d/README.md", "# readme\n")
        self.commit("add changelog.d")

    def git(self, *args):
        return subprocess.run(
            ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", *args],
            cwd=self.repo, check=True, capture_output=True, text=True, env=self.env,
        ).stdout.strip()

    def write(self, rel, text):
        p = self.repo / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")

    def commit(self, msg):
        self.git("add", "-A")
        self.git("commit", "-q", "-m", msg)
        return self.git("rev-parse", "HEAD")

    def fragments(self):
        return sorted(p.name for p in (self.repo / "changelog.d").iterdir() if p.name != "README.md")

    def release(self, version, after_tag):
        """What the build job does at the release commit, then what the metadata job does."""
        consumed = self.tmp / "consumed.txt"
        code = cf.main(["--root", str(self.repo), "assemble", "--version", version, "--date", "2026-02-02",
                        "--after-tag", after_tag, "--consumed-out", str(consumed)])
        self.assertEqual(code, 0)
        return consumed.read_text().split()


class ReleaseDryRunTests(RepoFixture):
    def test_assembly_consumes_the_fragments_and_lists_them(self):
        self.write("changelog.d/1.fixed.md", "- **Fix.** t\n")
        self.write("changelog.d/2.added.md", "- **Feat.** t\n")
        self.commit("merge 1 and 2")
        consumed = self.release("0.2.0", "v0.1.0")
        self.assertEqual(consumed, ["changelog.d/1.fixed.md", "changelog.d/2.added.md"])
        self.assertEqual(self.fragments(), [])
        self.assertTrue((self.repo / "changelog.d/README.md").exists())
        text = (self.repo / "CHANGELOG.md").read_text()
        self.assertIn("## [0.2.0] - 2026-02-02\n\n### Added\n\n- **Feat.** t\n\n### Fixed\n\n- **Fix.** t\n\n## [0.1.0]", text)

    def test_a_fragment_merged_after_the_release_commit_lands_in_the_next_release(self):
        """The incident: a bullet merged between the release commit and its metadata PR sat under a tag without it."""
        self.write("changelog.d/1.fixed.md", "- **Before the tag.** t\n")
        release_commit = self.commit("merge 1")
        self.git("tag", "v0.2.0")
        # release v0.2.0 is assembled from the release commit, as the build job's checkout is
        self.git("checkout", "-q", "--detach", release_commit)
        consumed_0_2 = self.release("0.2.0", "v0.1.0")
        metadata_changelog = (self.repo / "CHANGELOG.md").read_text()
        self.git("checkout", "-q", "--", ".")
        self.git("clean", "-fdq")
        self.git("checkout", "-q", "main")
        # ... while 2 merges to main after the release commit and before the metadata PR
        self.write("changelog.d/2.fixed.md", "- **After the tag.** t\n")
        self.commit("merge 2 after the release commit")
        # the metadata PR: CHANGELOG.md from the build job, consumed fragments removed
        self.write("CHANGELOG.md", metadata_changelog)
        for rel in consumed_0_2:
            (self.repo / rel).unlink()
        self.commit("chore: release metadata for v0.2.0 [skip release]")
        self.assertEqual(self.fragments(), ["2.fixed.md"])
        self.assertIn("- **Before the tag.** t", metadata_changelog)
        self.assertNotIn("After the tag", metadata_changelog)
        # the next release carries it, under its own version
        consumed_0_3 = self.release("0.3.0", "v0.2.0")
        self.assertEqual(consumed_0_3, ["changelog.d/2.fixed.md"])
        text = (self.repo / "CHANGELOG.md").read_text()
        self.assertEqual(text.count("After the tag"), 1)
        self.assertLess(text.index("After the tag"), text.index("## [0.2.0]"))
        self.assertGreater(text.index("After the tag"), text.index("## [0.3.0]"))

    def test_back_to_back_releases_do_not_release_a_fragment_twice(self):
        """v0.2.0's metadata PR has not merged when v0.3.0 is cut: its fragment is still in the tree."""
        self.write("changelog.d/1.fixed.md", "- **First.** t\n")
        self.commit("merge 1")
        self.git("tag", "v0.2.0")
        self.write("changelog.d/2.fixed.md", "- **Second.** t\n")
        self.commit("merge 2")
        consumed = self.release("0.3.0", "v0.2.0")
        self.assertEqual(consumed, ["changelog.d/2.fixed.md"])
        self.assertEqual(self.fragments(), ["1.fixed.md"])
        self.assertNotIn("**First.**", (self.repo / "CHANGELOG.md").read_text())

    def test_a_new_fragment_that_reuses_the_name_of_a_released_one_is_released(self):
        """foo.fixed.md was released and deleted; a later PR adds another foo.fixed.md: it must not be skipped."""
        self.write("changelog.d/foo.fixed.md", "- **Old.** t\n")
        self.commit("merge old foo")
        self.git("tag", "v0.2.0")
        self.git("rm", "-q", "changelog.d/foo.fixed.md")
        self.commit("chore: release metadata for v0.2.0 [skip release]")
        self.write("changelog.d/foo.fixed.md", "- **New under the old name.** t\n")
        self.commit("merge new foo")
        self.assertEqual(self.release("0.3.0", "v0.2.0"), ["changelog.d/foo.fixed.md"])
        self.assertIn("**New under the old name.**", (self.repo / "CHANGELOG.md").read_text())

    def test_a_fragment_whose_lead_in_is_already_released_stops_the_release(self):
        """A re-added copy, or a branch rebased onto a release that took the change: never published twice."""
        self.write("changelog.d/1.fixed.md", "- **Old fix.** reworded (the section of 0.1.0 has this lead-in)\n")
        self.commit("re-added")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = cf.main(["--root", str(self.repo), "assemble", "--version", "0.2.0", "--date", "2026-02-02"])
        self.assertEqual(code, 1)
        self.assertIn("repeat the bold lead-in of a released", err.getvalue())
        self.assertIn("changelog.d/1.fixed.md", err.getvalue())
        self.assertEqual((self.repo / "CHANGELOG.md").read_text(), BASE_CHANGELOG)
        self.assertEqual(self.fragments(), ["1.fixed.md"])

    def test_the_same_fragment_released_and_added_again_is_stopped(self):
        """An identical fragment re-added after its release (its metadata PR merged) is not released a second time."""
        released = BASE_CHANGELOG.replace("## [0.1.0]", "## [0.2.0] - 2026-02-01\n\n### Fixed\n\n- **Once.** t\n\n## [0.1.0]")
        self.write("CHANGELOG.md", released)
        self.commit("chore: release metadata for v0.2.0 [skip release]")
        self.git("tag", "v0.2.0")
        self.write("changelog.d/2.fixed.md", "- **Once.** t\n")
        self.commit("a copy of the released fragment under a new name")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = cf.main(["--root", str(self.repo), "assemble", "--version", "0.3.0", "--date", "2026-02-03", "--after-tag", "v0.2.0"])
        self.assertEqual(code, 1)
        self.assertIn("repeat the bold lead-in of a released", err.getvalue())

    def test_previous_tag_ignores_stray_tags(self):
        self.git("tag", "v0.2.0")
        self.write("x.txt", "x")
        self.commit("later")
        for stray in ("foo-stray", "v1abc", "v0.3.0-rc1", "v01.2.3", "release-9.9.9"):
            self.git("tag", stray)
        self.assertEqual(cc.latest_release_tag("HEAD", cwd=self.repo), "v0.2.0")
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(cf.main(["--root", str(self.repo), "previous-tag", "--rev", "HEAD"]), 0)
        self.assertEqual(out.getvalue().strip(), "v0.2.0")
        # a fragment is not lost because a stray tag sat between two releases
        self.write("changelog.d/1.fixed.md", "- **After the stray.** t\n")
        self.commit("merge 1")
        prev = cc.latest_release_tag("HEAD^", cwd=self.repo)
        self.assertEqual(prev, "v0.2.0")
        self.assertEqual(self.release("0.3.0", prev), ["changelog.d/1.fixed.md"])

    def test_previous_tag_is_the_highest_version_reachable_not_the_last_created(self):
        self.git("tag", "v0.10.0")
        self.git("tag", "v0.9.0")
        self.assertEqual(cc.latest_release_tag("HEAD", cwd=self.repo), "v0.10.0")
        self.assertEqual(cc.latest_release_tag("no-such-rev", cwd=self.repo), "")

    def test_an_unreadable_previous_tag_releases_everything(self):
        self.write("changelog.d/1.fixed.md", "- **First.** t\n")
        self.commit("merge 1")
        self.assertEqual(self.release("0.2.0", "v9.9.9"), ["changelog.d/1.fixed.md"])

    def test_an_invalid_fragment_stops_the_release_and_changes_nothing(self):
        self.write("changelog.d/1.fixed.md", "- **Fine.** t\n")
        self.write("changelog.d/2.fixed.md", "not a bullet\n")
        self.write("changelog.d/stray.txt", "x\n")
        self.commit("bad")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = cf.main(["--root", str(self.repo), "assemble", "--version", "0.2.0", "--date", "2026-02-02"])
        self.assertEqual(code, 1)
        self.assertIn("2.fixed.md", err.getvalue())
        self.assertIn("stray.txt", err.getvalue())
        self.assertEqual((self.repo / "CHANGELOG.md").read_text(), BASE_CHANGELOG)
        self.assertEqual(self.fragments(), ["1.fixed.md", "2.fixed.md", "stray.txt"])

    def test_a_symlink_or_a_directory_in_changelog_d_is_not_released(self):
        self.write("changelog.d/1.fixed.md", "- **Fine.** t\n")
        os.symlink("../CHANGELOG.md", self.repo / "changelog.d" / "3.fixed.md")
        (self.repo / "changelog.d" / "4.fixed.md").mkdir()
        _, problems = cf.load_fragments(self.repo / "changelog.d")
        self.assertEqual(sorted(p.split(":")[0] for p in problems), ["changelog.d/3.fixed.md", "changelog.d/4.fixed.md"])
        self.assertTrue(all("not a regular file" in p for p in problems))

    def test_check_reports_every_problem(self):
        self.write("changelog.d/1.fixed.md", "- **Fine.** t\n")
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(cf.main(["--root", str(self.repo), "check"]), 0)
        self.assertIn("1 changelog fragment(s)", out.getvalue())
        self.write("changelog.d/2.fixed.md", "- **Fine.** t\n")
        with contextlib.redirect_stderr(err):
            self.assertEqual(cf.main(["--root", str(self.repo), "check"]), 1)
        self.assertIn("same bullet", err.getvalue())

    def test_the_real_repository_has_only_valid_fragments(self):
        fragments, problems = cf.load_fragments(ROOT / "changelog.d")
        self.assertEqual(problems, [])

    def test_the_assembled_metadata_is_recognised_as_a_release(self):
        """base = main with the fragments, head = the metadata PR: CHANGELOG assembled, fragments deleted."""
        self.write("changelog.d/1.fixed.md", "- **Fix.** t\n  more\n")
        self.write("changelog.d/2.added.md", "- **Feat.** t\n")
        base = self.commit("merge")
        self.release("0.2.0", "v0.1.0")
        head = self.commit("chore: release metadata for v0.2.0 [skip release]")
        files = self.git("diff", "--name-only", f"{base}..{head}").split()
        self.assertEqual(sorted(files), ["CHANGELOG.md", "changelog.d/1.fixed.md", "changelog.d/2.added.md"])

        def show(rev):
            def read(path):
                try:
                    return self.git("show", f"{rev}:{path}") + "\n"
                except subprocess.CalledProcessError:
                    return None
            return read

        deleted = cc.metadata_fragment_deletions(files, show(base), show(head))
        self.assertEqual([f.path for f in deleted], ["changelog.d/1.fixed.md", "changelog.d/2.added.md"])
        shape = cc.changelog_release_shape(show(base)("CHANGELOG.md"), show(head)("CHANGELOG.md"), deleted)
        self.assertEqual(shape, ("0.1.0", "0.2.0"))
        self.assertTrue(cc.is_release_metadata_sync(files, fragment_paths=frozenset(f.path for f in deleted)))
        self.assertFalse(cc.is_release_metadata_sync(files))


class GateTests(RepoFixture):
    """check_changelog_pr.main on a fixture: a PR is a branch from main."""

    def gate(self, base="main", head="HEAD", env=None):
        out, err = io.StringIO(), io.StringIO()
        with unittest.mock.patch("scripts.ci.check_changelog_pr.ROOT", self.repo), \
                unittest.mock.patch.object(sys, "argv", ["x", "--base", base, "--head", head]), \
                unittest.mock.patch.dict(os.environ, env or {}, clear=False), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cc.main()
        return code, out.getvalue() + err.getvalue()

    def pr(self, files, subject="fix: something"):
        self.git("checkout", "-q", "-b", "pr")
        for rel, text in files.items():
            self.write(rel, text)
        self.commit(subject)

    def test_a_release_impacting_change_without_a_fragment_fails(self):
        self.pr({"internal/x/x.go": "package x\n"})
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("must add a changelog fragment", out)

    def test_a_release_impacting_change_with_a_fragment_passes(self):
        self.pr({"internal/x/x.go": "package x\n", "changelog.d/12.fixed.md": "- **Fix.** t (#12).\n"})
        code, out = self.gate()
        self.assertEqual((code, out.strip()), (0, "changelog gate: ok"))

    def test_docs_and_test_only_changes_need_no_fragment(self):
        self.pr({"docs/x.md": "x\n", "internal/x/x_test.go": "package x\n"}, subject="docs: x")
        code, out = self.gate()
        self.assertEqual(code, 0, out)
        self.assertIn("skipped", out)

    def test_ci_only_changes_need_no_fragment(self):
        self.pr({"scripts/ci/x.py": "x\n", ".github/workflows/x.yaml": "on: push\n"}, subject="ci: x")
        code, out = self.gate()
        self.assertEqual(code, 0, out)

    def test_an_invalid_fragment_fails_even_when_none_is_required(self):
        self.pr({"docs/x.md": "x\n", "changelog.d/12.fixed.md": "not a bullet\n"}, subject="docs: x")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("must start with", out)

    def test_a_badly_named_fragment_and_a_stray_file_fail(self):
        for rel in ("changelog.d/12.fix.md", "changelog.d/notes.txt", "changelog.d/Upper.fixed.md"):
            with self.subTest(rel=rel):
                self.git("checkout", "-q", "main")
                self.git("branch", "-q", "-D", "pr") if "pr" in self.git("branch").split() else None
                self.pr({"internal/x/x.go": "package x\n", rel: "- **Fix.** t\n"})
                code, out = self.gate()
                self.assertEqual(code, 1, out)
                self.assertIn("not a fragment name", out)

    def test_a_fragment_that_repeats_an_existing_one_fails(self):
        self.write("changelog.d/1.fixed.md", "- **Fix.** t\n")
        self.commit("main has 1")
        self.pr({"internal/x/x.go": "package x\n", "changelog.d/2.fixed.md": "- **Fix.** t\n"})
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("the same bullet as changelog.d/1.fixed.md", out)

    def test_a_fragment_already_on_main_does_not_satisfy_a_new_pr(self):
        self.write("changelog.d/1.fixed.md", "- **Fix.** t\n")
        self.commit("main has 1")
        self.pr({"internal/x/x.go": "package x\n"})
        code, out = self.gate()
        self.assertEqual(code, 1)

    def test_the_legacy_unreleased_bullet_is_accepted_only_until_a_release_contains_fragments(self):
        legacy = BASE_CHANGELOG.replace("## [Unreleased]\n", "## [Unreleased]\n\n### Fixed\n\n- **Legacy.** t\n")
        self.git("checkout", "-q", "-b", "pr")
        self.write("internal/x/x.go", "package x\n")
        self.write("CHANGELOG.md", legacy)
        self.commit("fix: legacy")
        code, out = self.gate()
        self.assertEqual((code, out.strip()), (0, "changelog gate: ok"), out)  # v0.1.0 has no changelog.d/README.md
        # a release whose tag contains changelog.d/README.md closes the window
        self.git("checkout", "-q", "main")
        self.git("tag", "v0.2.0")
        self.git("branch", "-q", "-D", "pr")
        self.git("checkout", "-q", "-b", "pr")
        self.write("internal/x/x.go", "package x\n")
        self.write("CHANGELOG.md", legacy)
        self.commit("fix: legacy again")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("[Unreleased] in CHANGELOG.md stays empty", out)

    def test_the_window_is_judged_from_the_base_not_from_todays_tags(self):
        self.assertIs(cc.legacy_unreleased_window_open("HEAD", cwd=self.repo), True)
        self.git("tag", "v0.2.0")
        self.assertIs(cc.legacy_unreleased_window_open("HEAD", cwd=self.repo), False)
        self.assertIs(cc.legacy_unreleased_window_open("v0.1.0", cwd=self.repo), True)

    def test_a_dependency_only_commit_with_an_invalid_fragment_is_not_skipped(self):
        self.pr({"go.sum": "x\n", "changelog.d/12.fixed.md": "not a bullet\n"}, subject="build(deps): bump x from 1 to 2")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("must start with", out)

    def test_a_dependency_only_commit_without_a_fragment_is_still_skipped(self):
        self.pr({"go.sum": "x\n"}, subject="build(deps): bump x from 1 to 2")
        code, out = self.gate()
        self.assertEqual((code, "dependency-only" in out), (0, True), out)

    def test_a_symlink_fragment_is_refused(self):
        self.write("note.txt", "- **Via a link.** t\n")
        self.git("checkout", "-q", "-b", "pr")
        os.symlink("../note.txt", self.repo / "changelog.d" / "12.fixed.md")
        self.write("internal/x/x.go", "package x\n")
        self.commit("fix: link")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("regular file", out)

    def test_an_executable_fragment_is_refused(self):
        self.git("checkout", "-q", "-b", "pr")
        self.write("changelog.d/12.fixed.md", "- **Exec.** t\n")
        os.chmod(self.repo / "changelog.d" / "12.fixed.md", 0o755)
        self.write("internal/x/x.go", "package x\n")
        self.commit("fix: exec")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("regular file", out)

    def test_a_file_name_with_a_newline_is_refused(self):
        self.pr({"internal/x/x.go": "package x\n", "changelog.d/12.fixed.md\n": "- **Nl.** t\n"})
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("not a fragment name", out)

    def test_a_lead_in_that_a_released_section_has_is_refused(self):
        self.pr({"internal/x/x.go": "package x\n", "changelog.d/12.fixed.md": "- **Old fix.** same change again\n"})
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("already in a released section", out)

    def test_an_unclosed_comment_in_a_fragment_is_refused(self):
        self.pr({"internal/x/x.go": "package x\n", "changelog.d/12.fixed.md": "- **Hides.** t <!-- oops\n"})
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("not closed", out)

    def tag_with_fragment(self):
        """main has fragment 1 released in v0.2.0 (its metadata pull request has not merged), and fragment 2 after it."""
        self.write("changelog.d/1.fixed.md", "- **Released one.** t\n")
        self.commit("merge 1")
        self.git("tag", "v0.2.0")
        self.write("changelog.d/2.fixed.md", "- **After the tag.** t\n")
        self.commit("merge 2")

    def test_editing_or_deleting_a_fragment_the_newest_release_took_is_refused(self):
        self.tag_with_fragment()
        for how in ("edit", "delete"):
            with self.subTest(how=how):
                self.git("checkout", "-q", "main")
                if "pr" in self.git("branch").split():
                    self.git("branch", "-q", "-D", "pr")
                self.git("checkout", "-q", "-b", "pr")
                if how == "edit":
                    self.write("changelog.d/1.fixed.md", "- **Released one.** reworded\n")
                else:
                    (self.repo / "changelog.d/1.fixed.md").unlink()
                self.write("docs/x.md", "x\n")
                self.commit("docs: touch")
                code, out = self.gate()
                self.assertEqual(code, 1, out)
                self.assertIn("already released in v0.2.0", out)

    def test_a_fragment_merged_after_the_tag_may_still_be_edited(self):
        self.tag_with_fragment()
        self.git("checkout", "-q", "-b", "pr")
        self.write("changelog.d/2.fixed.md", "- **After the tag.** reworded\n")
        self.write("docs/x.md", "x\n")
        self.commit("docs: reword")
        code, out = self.gate()
        self.assertEqual(code, 0, out)

    def test_an_edit_of_a_fragment_must_still_be_valid(self):
        self.tag_with_fragment()
        self.git("checkout", "-q", "-b", "pr")
        self.write("changelog.d/2.fixed.md", "no longer a bullet\n")
        self.commit("docs: break it")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("must start with", out)

    def test_the_window_is_judged_from_the_base_tip_so_a_stale_branch_cannot_keep_it_open(self):
        legacy = BASE_CHANGELOG.replace("## [Unreleased]\n", "## [Unreleased]\n\n### Fixed\n\n- **Legacy.** t\n")
        self.git("checkout", "-q", "-b", "stale")  # cut while the window is open
        self.write("internal/x/x.go", "package x\n")
        self.write("CHANGELOG.md", legacy)
        self.commit("fix: legacy")
        self.git("checkout", "-q", "main")
        self.write("y.txt", "y")
        self.commit("main moves on")
        self.git("tag", "v0.2.0")  # the release that closes the window; the stale branch was cut before it
        self.write("x.txt", "x")
        self.commit("main moves on again")
        self.git("checkout", "-q", "stale")
        code, out = self.gate()  # base = the main tip; the merge base predates v0.2.0
        self.assertEqual(code, 1, out)
        self.assertIn("stays empty", out)

    def test_stray_tags_do_not_move_the_window(self):
        self.git("tag", "foo-stray")
        self.git("tag", "v0.2.0-rc1")
        self.assertIs(cc.legacy_unreleased_window_open("HEAD", cwd=self.repo), True)
        self.git("tag", "v0.2.0")
        self.git("tag", "zzz-latest-stray")
        self.assertIs(cc.legacy_unreleased_window_open("HEAD", cwd=self.repo), False)

    def test_a_release_metadata_pr_with_fragment_deletions_is_a_metadata_sync(self):
        self.write("changelog.d/1.fixed.md", "- **Fix.** t\n")
        self.commit("merge 1")
        self.git("checkout", "-q", "-b", "pr")
        self.release("0.2.0", "v0.1.0")
        self.commit("chore: release metadata for v0.2.0 [skip release]")
        code, out = self.gate()
        self.assertEqual((code, out.strip()), (0, "changelog gate: ok (release metadata sync)"))

    def test_a_pr_that_edits_a_fragment_while_changing_the_changelog_is_not_metadata(self):
        self.write("changelog.d/1.fixed.md", "- **Fix.** t\n")
        self.commit("merge 1")
        self.git("checkout", "-q", "-b", "pr")
        self.release("0.2.0", "v0.1.0")
        self.write("changelog.d/9.fixed.md", "- **New.** t\n")
        self.commit("chore: release metadata for v0.2.0 [skip release]")
        code, out = self.gate()
        self.assertNotIn("release metadata sync", out)


class WorkflowUsesTheTagHelper(unittest.TestCase):
    def test_no_step_of_the_release_asks_git_describe_for_the_previous_release(self):
        text = (ROOT / ".github" / "workflows" / "auto-release.yaml").read_text()
        step = text.split("      - name: Assemble the changelog\n", 1)[1].split("\n      - name:", 1)[0]
        self.assertIn("changelog_fragments.py previous-tag", step)
        self.assertNotIn("git describe", step)
        self.assertNotIn("git describe", text.split("      - name: Render release notes\n", 1)[1].split("\n      - name:", 1)[0])


class ClassifierLockTests(unittest.TestCase):
    """changelog.d/ is a non-product path: a lone fragment change never triggers a release or the registry rules."""

    def test_a_fragment_path_is_not_product(self):
        sys.path.insert(0, str(ROOT / "scripts" / "ci"))
        try:
            import pr_classify
        finally:
            sys.path.pop(0)
        for path in ("changelog.d/1.fixed.md", "changelog.d/README.md", "CHANGELOG.md"):
            self.assertIsNone(pr_classify.product_reason(path), path)
        self.assertFalse(cc.is_release_path("changelog.d/1.fixed.md"))
        self.assertFalse(cc.should_require_changelog(["docs: x"], ["changelog.d/1.fixed.md"]))


class ClassifierTests(RepoFixture):
    """pr_classify's exact release-metadata shape accepts fragment deletions and nothing looser."""

    def test_the_classifier_recognises_the_fragment_release_shape(self):
        sys.path.insert(0, str(ROOT / "scripts" / "ci"))
        try:
            import pr_classify
        finally:
            sys.path.pop(0)
        self.write("changelog.d/1.fixed.md", "- **Fix.** t\n")
        self.write("charts/victoria-lakehouse/Chart.yaml", 'version: 0.1.0\nappVersion: "0.1.0"\n')
        base = self.commit("merge 1")
        self.release("0.2.0", "v0.1.0")
        self.write("charts/victoria-lakehouse/Chart.yaml", 'version: 0.2.0\nappVersion: "0.2.0"\n')
        head = self.commit("chore: release metadata for v0.2.0 [skip release]")
        files = self.git("diff", "--name-only", f"{base}..{head}").split()
        cwd = os.getcwd()
        os.chdir(self.repo)
        try:
            self.git("tag", "v0.2.0")
            self.assertTrue(pr_classify.release_metadata(files, base, head, pr_classify.RELEASE_BOT, set()))
            # a kept-back fragment line in the new section, or a fragment edited instead of deleted, is not the shape
            self.write("changelog.d/1.fixed.md", "- **Fix.** t edited\n")
            edited = self.commit("edit instead of delete")
            files2 = self.git("diff", "--name-only", f"{base}..{edited}").split()
            self.assertFalse(pr_classify.release_metadata(files2, base, edited, pr_classify.RELEASE_BOT, set()))
            # a bullet in the new section that no deleted fragment holds is not the shape either
            self.git("reset", "-q", "--hard", head)
            text = (self.repo / "CHANGELOG.md").read_text().replace("- **Fix.** t\n", "- **Fix.** t\n\n- **Smuggled.** t\n")
            self.write("CHANGELOG.md", text)
            smuggled = self.commit("smuggle a bullet")
            files3 = self.git("diff", "--name-only", f"{base}..{smuggled}").split()
            self.assertFalse(pr_classify.release_metadata(files3, base, smuggled, pr_classify.RELEASE_BOT, set()))
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
