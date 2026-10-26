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

    def test_the_size_cap_boundary_is_exact(self):
        head = "- **A.** "
        exactly = head + "x" * (cc.FRAGMENT_MAX_BYTES - len(head) - 1) + "\n"
        self.assertEqual(len(exactly.encode()), cc.FRAGMENT_MAX_BYTES)
        self.ok(exactly)
        self.bad(exactly + "x", needle="larger than")

    def test_characters_on_which_line_splitters_disagree_are_refused(self):
        for ch in ("\x0b", "\x0c", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029"):
            with self.subTest(ch=repr(ch)):
                self.bad(f"- **A.** b{ch}c\n", needle="line separator")

    def test_unclosed_fences_and_pre_are_refused(self):
        self.bad("- **A.** b\n\n  ```\n  code\n", needle="fenced")
        self.bad("- **A.** b\n\n  ~~~\n  code\n", needle="fenced")
        self.bad("- **A.** b <pre>x\n", needle="<pre>")
        self.bad("- **A.** b </pre>\n", needle="<pre>")
        self.ok("- **A.** b\n\n  ```\n  code\n  ```\n")
        self.ok("- **A.** b <pre>x</pre> c\n")

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

    def test_lead_ins_are_compared_without_whitespace_case_or_the_closing_dot(self):
        a = frag("changelog.d/1.fixed.md", "- **Same lead.** one\n")
        for variant in ("- **Same lead. ** two\n", "- **same  LEAD.** two\n", "- **Same lead**. two\n", "- ** Same lead ** two\n"):
            with self.subTest(variant=variant):
                b = frag("changelog.d/2.fixed.md", variant)
                self.assertIn("same bold lead-in", cc.duplicate_fragment_problems([a, b])[0])
        self.assertEqual(cc.duplicate_fragment_problems([a, frag("changelog.d/2.fixed.md", "- **Other lead.** x\n")]), [])

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

    def test_a_hand_written_bullet_under_unreleased_stops_the_release(self):
        legacy = BASE_CHANGELOG.replace("## [Unreleased]\n", "## [Unreleased]\n\n### Fixed\n\n- **Legacy fix.** t\n")
        with self.assertRaises(cf.AssembleError) as cm:
            cf.assemble(legacy, [frag("changelog.d/1.fixed.md", "- **New.** t\n")], "0.2.0", "2026-02-02")
        self.assertIn("[Unreleased]", str(cm.exception))
        self.assertIn("fragments", str(cm.exception))

    def test_a_section_too_large_for_a_release_body_stops_the_release(self):
        big = [frag(f"changelog.d/{i}.fixed.md", f"- **F{i}.** " + "x" * 10000 + "\n") for i in range(11)]
        with self.assertRaises(cf.AssembleError) as cm:
            cf.assemble(BASE_CHANGELOG, big, "0.2.0", "2026-02-02")
        self.assertIn("at most", str(cm.exception))
        cf.assemble(BASE_CHANGELOG, big[:9], "0.2.0", "2026-02-02")

    def test_the_section_cap_counts_bytes_and_names_the_files(self):
        wide = [frag(f"changelog.d/{i}.fixed.md", f"- **W{i}.** " + "\u00e9" * 8000 + "\n") for i in range(7)]
        chars = sum(len("\n".join(f.lines)) for f in wide)
        self.assertLess(chars, cf.SECTION_MAX_BYTES)  # under the cap in characters ...
        with self.assertRaises(cf.AssembleError) as cm:  # ... over it in bytes
            cf.assemble(BASE_CHANGELOG, wide, "0.2.0", "2026-02-02")
        self.assertIn("changelog.d/0.fixed.md (", str(cm.exception))
        self.assertIn("largest first", str(cm.exception))

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

    def test_a_fragment_left_behind_by_a_merged_metadata_pr_stops_the_next_release(self):
        """The section of v0.2.0 is in CHANGELOG.md, yet the fragment it was built from is still here: skipped for ever otherwise."""
        self.write("changelog.d/1.fixed.md", "- **Left.** t\n")
        self.commit("merge 1")
        self.git("tag", "v0.2.0")
        stranded = BASE_CHANGELOG.replace("## [0.1.0]", "## [0.2.0] - 2026-02-01\n\n### Fixed\n\n- **Hand filed.** t\n\n## [0.1.0]")
        self.write("CHANGELOG.md", stranded)
        self.commit("the section was filed, the file was not removed")
        self.write("changelog.d/2.fixed.md", "- **Next.** t\n")
        self.commit("merge 2")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = cf.main(["--root", str(self.repo), "assemble", "--version", "0.3.0", "--date", "2026-02-03", "--after-tag", "v0.2.0"])
        self.assertEqual(code, 1)
        self.assertIn("changelog.d/1.fixed.md (released in v0.2.0;", err.getvalue())
        self.assertEqual(self.fragments(), ["1.fixed.md", "2.fixed.md"])

    def test_every_left_behind_fragment_is_listed(self):
        self.write("changelog.d/1.fixed.md", "- **Left one.** t\n")
        self.write("changelog.d/2.fixed.md", "- **Left two.** t\n")
        self.commit("merge 1 and 2")
        self.git("tag", "v0.2.0")
        stranded = BASE_CHANGELOG.replace("## [0.1.0]", "## [0.2.0] - 2026-02-01\n\n### Fixed\n\n- **Hand filed.** t\n\n## [0.1.0]")
        self.write("CHANGELOG.md", stranded)
        self.commit("sections filed")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = cf.main(["--root", str(self.repo), "assemble", "--version", "0.3.0", "--date", "2026-02-03", "--after-tag", "v0.2.0"])
        self.assertEqual(code, 1)
        self.assertIn("changelog.d/1.fixed.md (released in v0.2.0; its bullet is NOT in the section: file it under [0.2.0] and delete the file)", err.getvalue())
        self.assertIn("changelog.d/2.fixed.md (released in v0.2.0; its bullet is NOT in the section: file it under [0.2.0] and delete the file)", err.getvalue())

    def test_the_left_behind_error_says_delete_only_when_the_bullet_is_in_the_section(self):
        self.write("changelog.d/1.fixed.md", "- **Left one.** t\n")
        self.commit("merge 1")
        self.git("tag", "v0.2.0")
        filed = BASE_CHANGELOG.replace("## [0.1.0]", "## [0.2.0] - 2026-02-01\n\n### Fixed\n\n- **Left one.** t\n\n## [0.1.0]")
        self.write("CHANGELOG.md", filed)
        self.commit("section filed")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = cf.main(["--root", str(self.repo), "assemble", "--version", "0.3.0", "--date", "2026-02-03", "--after-tag", "v0.2.0"])
        self.assertEqual(code, 1)
        self.assertIn("its bullet is in the section: delete the file", err.getvalue())
        self.assertNotIn("NOT in the section", err.getvalue())

    def test_a_dry_run_runs_every_check_and_changes_nothing(self):
        self.write("changelog.d/1.fixed.md", "- **Fine.** t\n")
        self.commit("merge 1")
        before = (self.repo / "CHANGELOG.md").read_text()
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cf.main(["--root", str(self.repo), "assemble", "--dry-run", "--version", "0.2.0", "--date", "2026-02-02",
                            "--after-tag", "v0.1.0", "--consumed-out", str(self.tmp / "c.txt")])
        self.assertEqual(code, 0)
        self.assertIn("checked [0.2.0]", out.getvalue())
        self.assertEqual((self.repo / "CHANGELOG.md").read_text(), before)
        self.assertEqual(self.fragments(), ["1.fixed.md"])
        self.assertFalse((self.tmp / "c.txt").exists())
        # and it fails exactly where the real assembly fails
        self.write("changelog.d/2.fixed.md", "not a bullet\n")
        self.commit("bad")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(cf.main(["--root", str(self.repo), "assemble", "--dry-run", "--version", "0.2.0", "--date", "2026-02-02"]), 1)

    def test_a_lead_in_that_differs_only_in_case_or_dot_is_still_released_twice(self):
        self.write("changelog.d/1.fixed.md", "- **old FIX**. reworded\n")
        self.commit("copy")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = cf.main(["--root", str(self.repo), "assemble", "--version", "0.2.0", "--date", "2026-02-02"])
        self.assertEqual(code, 1)
        self.assertIn("repeat the bold lead-in of a released", err.getvalue())

    def test_a_pending_metadata_pr_is_not_left_behind(self):
        """No section for v0.2.0 yet: its fragment is skipped by the next release, as before."""
        self.write("changelog.d/1.fixed.md", "- **Pending.** t\n")
        self.commit("merge 1")
        self.git("tag", "v0.2.0")
        self.write("changelog.d/2.fixed.md", "- **Next.** t\n")
        self.commit("merge 2")
        self.assertEqual(self.release("0.3.0", "v0.2.0"), ["changelog.d/2.fixed.md"])

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

    def test_a_hand_written_unreleased_bullet_is_refused(self):
        legacy = BASE_CHANGELOG.replace("## [Unreleased]\n", "## [Unreleased]\n\n### Fixed\n\n- **Legacy.** t\n")
        self.git("checkout", "-q", "-b", "pr")
        self.write("internal/x/x.go", "package x\n")
        self.write("CHANGELOG.md", legacy)
        self.write("changelog.d/12.fixed.md", "- **With a fragment too.** t\n")
        self.commit("fix: legacy")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("stays empty", out)

    def test_a_bullet_filed_by_hand_under_a_released_section_is_refused(self):
        filed = BASE_CHANGELOG.replace("## [0.1.0] - 2026-01-01\n", "## [0.1.0] - 2026-01-01\n\n### Added\n\n- **Hand filed.** t\n")
        self.pr({"docs/x.md": "x\n", "CHANGELOG.md": filed}, subject="docs: file a bullet")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("by hand under a released section", out)

    def test_two_fragments_with_the_same_bullet_fail_whichever_one_the_pr_adds(self):
        self.write("changelog.d/940.fixed.md", "- **Dup.** same text\n")
        self.commit("main has 940")
        self.pr({"internal/x/x.go": "package x\n", "changelog.d/940-a.fixed.md": "- **Dup.** same text\n"})
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("the same bullet as changelog.d/940-a.fixed.md", out)  # sorted first: 940-a < 940.

    def test_the_other_ordering_of_a_duplicate_pair_fails_too(self):
        self.write("changelog.d/940-copy.fixed.md", "- **Dup.** same text\n")
        self.commit("main has 940-copy")
        self.pr({"internal/x/x.go": "package x\n", "changelog.d/940.fixed.md": "- **Dup.** same text\n"})
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("the same bullet as", out)

    def test_a_regular_fragment_turned_into_a_symlink_is_refused(self):
        self.tag_with_fragment()
        self.git("checkout", "-q", "-b", "pr")
        target = self.repo / "changelog.d" / "2.fixed.md"
        target.unlink()
        os.symlink("../README.md", target)  # a link to a file that is not a bullet
        self.write("docs/x.md", "x\n")
        self.commit("docs: retype")
        code, out = self.gate()
        self.assertEqual(code, 1, out)
        self.assertIn("regular file", out)

    def test_a_released_fragment_turned_into_a_symlink_is_refused_as_released(self):
        self.tag_with_fragment()
        self.git("checkout", "-q", "-b", "pr")
        target = self.repo / "changelog.d" / "1.fixed.md"
        target.unlink()
        os.symlink("2.fixed.md", target)
        self.write("docs/x.md", "x\n")
        self.commit("docs: retype a released one")
        code, out = self.gate()
        self.assertEqual(code, 1, out)
        self.assertIn("already released in v0.2.0", out)

    def test_a_real_git_mv_of_a_released_fragment_is_refused(self):
        self.tag_with_fragment()
        self.git("checkout", "-q", "-b", "pr")
        self.git("mv", "changelog.d/1.fixed.md", "changelog.d/1b.fixed.md")
        self.commit("docs: rename")
        self.assertIn("R", self.git("diff", "--name-status", "main..HEAD").split()[0])  # git sees a rename
        code, out = self.gate()
        self.assertEqual(code, 1, out)
        self.assertIn("already released in v0.2.0", out)

    def test_the_readme_cannot_be_deleted_or_replaced_by_a_link(self):
        # no cwd changes: all paths are absolute
        cases = {
            "deleted": lambda: (self.repo / "changelog.d" / "README.md").unlink(),
            "symlink to a file": lambda: (self.repo / "changelog.d" / "README.md").unlink() or os.symlink("../CHANGELOG.md", self.repo / "changelog.d" / "README.md"),
            "symlink to a directory": lambda: (self.repo / "changelog.d" / "README.md").unlink() or os.symlink("../internal", self.repo / "changelog.d" / "README.md"),
            "dangling symlink": lambda: (self.repo / "changelog.d" / "README.md").unlink() or os.symlink("nowhere", self.repo / "changelog.d" / "README.md"),
            "executable": lambda: os.chmod(self.repo / "changelog.d" / "README.md", 0o755),
        }
        for name, mutate in cases.items():
            with self.subTest(name):
                self.git("checkout", "-q", "main")
                if "pr" in self.git("branch").split():
                    self.git("branch", "-q", "-D", "pr")
                self.git("checkout", "-q", "-b", "pr")
                (self.repo / "internal").mkdir(exist_ok=True)
                mutate()
                self.write("docs/x.md", name)
                self.commit("docs: " + name)
                code, out = self.gate()
                self.assertEqual(code, 1, out)
                self.assertIn("changelog.d/README.md: must stay a regular file", out)

    def test_the_readme_cannot_become_a_gitlink(self):
        self.git("checkout", "-q", "-b", "pr")
        sha = self.git("rev-parse", "HEAD")
        self.git("rm", "-q", "--cached", "changelog.d/README.md")
        self.git("update-index", "--add", "--cacheinfo", f"160000,{sha},changelog.d/README.md")
        self.git("commit", "-q", "-m", "docs: gitlink")
        code, out = self.gate()
        self.assertEqual(code, 1, out)
        self.assertIn("must stay a regular file", out)

    def stranded(self):
        """main: fragment 1 released in v0.2.0 (tag), then its section was filed in CHANGELOG.md by hand, the file left behind."""
        self.write("changelog.d/1.fixed.md", "- **Stranded.** t\n")
        self.commit("merge 1")
        self.git("tag", "v0.2.0")
        filed = BASE_CHANGELOG.replace("## [0.1.0]", "## [0.2.0] - 2026-02-01\n\n### Fixed\n\n- **Stranded.** t\n\n## [0.1.0]")
        self.write("CHANGELOG.md", filed)
        self.commit("chore: release metadata for v0.2.0 [skip release] (merged without the fragment removal)")

    def test_a_stranded_fragment_whose_bullet_is_in_a_released_section_can_be_deleted(self):
        self.stranded()
        self.git("checkout", "-q", "-b", "pr")
        (self.repo / "changelog.d" / "1.fixed.md").unlink()
        self.commit("chore: remove a fragment its release already took")
        code, out = self.gate()
        self.assertEqual(code, 0, out)

    def test_filing_the_bullet_by_hand_and_deleting_its_fragment_in_one_pr_is_allowed(self):
        self.write("changelog.d/1.fixed.md", "- **Stranded.** t\n")
        self.commit("merge 1")
        self.git("tag", "v0.2.0")
        self.git("checkout", "-q", "-b", "pr")
        filed = BASE_CHANGELOG.replace("## [0.1.0]", "## [0.2.0] - 2026-02-01\n\n### Fixed\n\n- **Stranded.** t\n\n## [0.1.0]")
        self.write("CHANGELOG.md", filed)
        (self.repo / "changelog.d" / "1.fixed.md").unlink()
        self.commit("chore: file the bullet of v0.2.0 and remove its fragment")
        code, out = self.gate()
        self.assertEqual(code, 0, out)

    def test_the_deleted_fragments_bullet_may_be_filed_even_when_the_pr_is_not_a_plain_sync(self):
        """With another file in the PR the strict-shape shortcut does not apply: the allowed-bullets rule decides."""
        self.write("changelog.d/1.fixed.md", "- **Stranded.** t\n")
        self.commit("merge 1")
        self.git("tag", "v0.2.0")
        self.git("checkout", "-q", "-b", "pr")
        filed = BASE_CHANGELOG.replace("## [0.1.0]", "## [0.2.0] - 2026-02-01\n\n### Fixed\n\n- **Stranded.** t\n\n## [0.1.0]")
        self.write("CHANGELOG.md", filed)
        self.write("docs/x.md", "a note\n")
        (self.repo / "changelog.d" / "1.fixed.md").unlink()
        self.commit("docs: file the bullet of v0.2.0, remove its fragment and add a note")
        code, out = self.gate()
        self.assertEqual(code, 0, out)

    def test_filing_a_different_bullet_while_deleting_a_fragment_is_refused(self):
        self.write("changelog.d/1.fixed.md", "- **Stranded.** t\n")
        self.commit("merge 1")
        self.git("tag", "v0.2.0")
        self.git("checkout", "-q", "-b", "pr")
        filed = BASE_CHANGELOG.replace("## [0.1.0]", "## [0.2.0] - 2026-02-01\n\n### Fixed\n\n- **Something else.** t\n\n## [0.1.0]")
        self.write("CHANGELOG.md", filed)
        (self.repo / "changelog.d" / "1.fixed.md").unlink()
        self.commit("chore: file another bullet")
        code, out = self.gate()
        self.assertEqual(code, 1, out)

    def test_deleting_a_released_fragment_whose_bullet_is_nowhere_is_refused_with_an_honest_message(self):
        self.tag_with_fragment()
        self.git("checkout", "-q", "-b", "pr")
        (self.repo / "changelog.d" / "1.fixed.md").unlink()
        self.commit("chore: delete")
        code, out = self.gate()
        self.assertEqual(code, 1, out)
        self.assertIn("If that pull request cannot merge, file the bullet under the section [0.2.0] and delete this file", out)

    def _pr_changelog(self, text, extra=None, subject="docs: x"):
        files = {"CHANGELOG.md": text, **(extra or {})}
        self.pr(files, subject=subject)
        return self.gate()

    def test_a_changelog_only_pr_cannot_add_text_under_unreleased_even_as_a_fix(self):
        legacy = BASE_CHANGELOG.replace("## [Unreleased]\n", "## [Unreleased]\n\n### Fixed\n\n- **Hotfix.** t\n")
        code, out = self._pr_changelog(legacy, subject="fix: emergency")
        self.assertEqual(code, 1)
        self.assertNotIn("release metadata sync", out)
        self.assertIn("stays empty", out)

    def test_chart_plus_unreleased_text_under_a_feat_subject_is_no_metadata_sync(self):
        legacy = BASE_CHANGELOG.replace("## [Unreleased]\n", "## [Unreleased]\n\n### Added\n\n- **Feature.** t\n")
        code, out = self._pr_changelog(legacy, {"charts/victoria-lakehouse/Chart.yaml": "version: 9.9.9\n"}, subject="feat: x")
        self.assertEqual(code, 1)
        self.assertNotIn("release metadata sync", out)

    def test_a_hand_made_section_holding_a_bullet_is_no_metadata_sync(self):
        hand = BASE_CHANGELOG.replace("## [0.1.0]", "## [9.9.9] - 2026-02-01\n\n### Fixed\n\n- **Hand made.** t\n\n## [0.1.0]")
        code, out = self._pr_changelog(hand, {"charts/victoria-lakehouse/Chart.yaml": "version: 9.9.9\n"})
        self.assertEqual(code, 1)
        self.assertNotIn("release metadata sync", out)

    def test_a_hand_bullet_under_an_old_release_plus_a_dummy_section_is_no_metadata_sync(self):
        hand = BASE_CHANGELOG.replace("## [Unreleased]\n", "## [Unreleased]\n\n## [0.0.0] - 2026-01-01\n").replace(
            "- **Old fix.** text.", "- **Old fix.** text.\n\n- **Slipped in.** t")
        code, out = self._pr_changelog(hand)
        self.assertEqual(code, 1)
        self.assertNotIn("release metadata sync", out)

    def test_a_metadata_pr_that_keeps_its_fragments_is_no_metadata_sync(self):
        self.write("changelog.d/1.fixed.md", "- **Fix.** t\n")
        self.commit("merge 1")
        self.git("checkout", "-q", "-b", "pr")
        self.release("0.2.0", "v0.1.0")
        self.git("checkout", "--", "changelog.d/1.fixed.md")  # the fragment comes back: assembled, not removed
        self.commit("chore: release metadata for v0.2.0 [skip release]")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertNotIn("release metadata sync", out)

    def test_a_fragment_bullet_bare_under_unreleased_above_an_empty_section_is_no_metadata_sync(self):
        self.write("changelog.d/1.fixed.md", "- **Fix.** t\n")
        self.commit("merge 1")
        self.git("checkout", "-q", "-b", "pr")
        self.release("0.2.0", "v0.1.0")
        text = (self.repo / "CHANGELOG.md").read_text()
        text = text.replace("- **Fix.** t\n\n", "").replace("### Fixed\n\n## [0.1.0]", "## [0.1.0]")
        text = text.replace("## [Unreleased]\n", "## [Unreleased]\n\n- **Fix.** t\n", 1)
        self.write("CHANGELOG.md", text)
        self.commit("chore: release metadata for v0.2.0 [skip release]")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertNotIn("release metadata sync", out)

    def test_a_release_without_fragments_is_a_valid_metadata_sync(self):
        self.git("tag", "v0.2.0")
        self.git("checkout", "-q", "-b", "pr")
        self.release("0.2.0", "v0.1.0")
        self.write("charts/victoria-lakehouse/Chart.yaml", "version: 0.2.0\n")
        self.commit("chore: release metadata for v0.2.0 [skip release]")
        code, out = self.gate()
        self.assertEqual((code, out.strip()), (0, "changelog gate: ok (release metadata sync)"))

    def test_unreleased_text_is_refused_before_the_dependency_only_skip(self):
        legacy = BASE_CHANGELOG.replace("## [Unreleased]\n", "## [Unreleased]\n\n### Fixed\n\n- **Hotfix.** t\n")
        self.pr({"go.sum": "x\n", "CHANGELOG.md": legacy}, subject="build(deps): bump x from 1 to 2")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("stays empty", out)

    def test_an_unrelated_pr_fails_while_main_has_text_under_unreleased(self):
        legacy = BASE_CHANGELOG.replace("## [Unreleased]\n", "## [Unreleased]\n\n### Fixed\n\n- **On main.** t\n")
        self.write("CHANGELOG.md", legacy)
        self.commit("main has text under unreleased")
        self.pr({"docs/x.md": "x\n"}, subject="docs: unrelated")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("stays empty", out)

    def test_released_text_of_every_kind_is_guarded(self):
        cases = {
            "star item": "* Hand star item",
            "numbered item": "1. Hand numbered item",
            "prose": "Hand written prose line",
            "plus item": "+ Hand plus item",
        }
        for name, line in cases.items():
            with self.subTest(name):
                self.git("checkout", "-q", "main")
                if "pr" in self.git("branch").split():
                    self.git("branch", "-q", "-D", "pr")
                filed = BASE_CHANGELOG.replace("- **Old fix.** text.", "- **Old fix.** text.\n\n" + line)
                code, out = self._pr_changelog(filed)
                self.assertEqual(code, 1, out)
                self.assertIn("by hand under a released section", out)

    def test_deleting_or_rewording_a_released_bullet_is_refused_but_a_reflow_is_not(self):
        self.git("checkout", "-q", "-b", "pr")
        self.write("CHANGELOG.md", BASE_CHANGELOG.replace("- **Old fix.** text.", "- **Old fix.**\n  text."))  # re-wrapped only
        self.write("docs/x.md", "x\n")
        self.commit("docs: reflow")
        code, out = self.gate()
        self.assertEqual(code, 0, out)
        self.git("checkout", "-q", "main")
        self.git("branch", "-q", "-D", "pr")
        code, out = self._pr_changelog(BASE_CHANGELOG.replace("\n- **Old fix.** text.\n", "\n"))  # deleted
        self.assertEqual(code, 1, out)
        self.git("checkout", "-q", "main")
        self.git("branch", "-q", "-D", "pr")
        code, out = self._pr_changelog(BASE_CHANGELOG.replace("text.", "other words."))  # reworded
        self.assertEqual(code, 1, out)

    def test_a_lead_in_that_differs_only_in_case_or_dot_from_a_released_one_is_refused(self):
        self.pr({"internal/x/x.go": "package x\n", "changelog.d/12.fixed.md": "- **old FIX**. reworded\n"})
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("already in a released section", out)

    def test_an_unreleased_fragment_cannot_be_deleted_while_its_bullet_is_filed_under_a_release(self):
        self.tag_with_fragment()  # fragment 2 is merged after v0.2.0: no release took it
        self.git("checkout", "-q", "-b", "pr")
        filed = BASE_CHANGELOG.replace("## [0.1.0]", "## [0.2.0] - 2026-02-01\n\n### Fixed\n\n- **After the tag.** t\n\n## [0.1.0]")
        self.write("CHANGELOG.md", filed)
        (self.repo / "changelog.d" / "2.fixed.md").unlink()
        self.write("docs/x.md", "x\n")
        self.commit("docs: file it under 0.2.0")
        code, out = self.gate()
        self.assertEqual(code, 1, out)
        self.assertIn("by hand under a released section", out)

    def test_a_released_fragment_may_not_have_its_bullet_filed_under_another_release(self):
        self.write("changelog.d/1.fixed.md", "- **Stranded.** t\n")
        self.commit("merge 1")
        self.git("tag", "v0.2.0")
        self.git("checkout", "-q", "-b", "pr")
        filed = BASE_CHANGELOG.replace("- **Old fix.** text.", "- **Old fix.** text.\n\n- **Stranded.** t")  # under 0.1.0, not 0.2.0
        self.write("CHANGELOG.md", filed)
        (self.repo / "changelog.d" / "1.fixed.md").unlink()
        self.commit("chore: wrong section")
        code, out = self.gate()
        self.assertEqual(code, 1, out)

    def test_the_stranded_refusal_names_the_releasing_tag_not_the_newest(self):
        self.tag_with_fragment()
        self.git("tag", "v0.3.0")  # a later release, whose tree still holds fragment 1
        self.git("checkout", "-q", "-b", "pr")
        (self.repo / "changelog.d" / "1.fixed.md").unlink()
        self.commit("chore: delete")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("already released in v0.2.0", out)
        self.assertNotIn("v0.3.0", out)

    def test_a_delete_only_recovery_with_a_fix_subject_is_told_to_use_docs_or_chore(self):
        self.stranded()
        self.git("checkout", "-q", "-b", "pr")
        (self.repo / "changelog.d" / "1.fixed.md").unlink()
        self.commit("fix(changelog): remove a fragment its release already took")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("docs: or chore: subject", out)

    def test_a_release_metadata_pr_with_fragment_deletions_is_a_metadata_sync(self):
        self.write("changelog.d/1.fixed.md", "- **Fix.** t\n")
        self.commit("merge 1")
        self.git("tag", "v0.2.0")
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


class ConsumedListChecks(unittest.TestCase):
    """The list is checked in the build job (before publish pushes anything) and again before the tag is pushed."""

    def run_check(self, job, lines):
        from scripts.ci.tests.test_auto_release_jobs import step_run
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "release-meta"))
            with open(os.path.join(tmp, "release-meta", "consumed.txt"), "w") as fh:
                fh.write(lines)
            return subprocess.run(["bash", "-e", "-c", step_run(job, "Check the released fragment list")],
                                  env=dict(os.environ, RUNNER_TEMP=tmp), capture_output=True, text=True)

    def test_both_jobs_run_the_same_path_check(self):
        from scripts.ci.tests.test_auto_release_jobs import step_run
        pick = lambda job: [l for l in step_run(job, "Check the released fragment list").split("\n") if "grep -Eqx" in l]
        self.assertEqual(len(pick("build")), 1)
        self.assertEqual(pick("build"), pick("metadata"))

    def test_the_build_job_checks_before_anything_is_published(self):
        from scripts.ci.tests.test_auto_release_jobs import JOBS
        names = [step.get("name") for step in JOBS["build"]["steps"]]
        self.assertLess(names.index("Assemble the changelog"), names.index("Check the released fragment list"))
        self.assertLess(names.index("Check the released fragment list"), names.index("Render release notes"))
        # the publish job needs the build job, so a failure here ships nothing
        self.assertIn("build", JOBS["publish"]["needs"])

    def test_the_assembly_is_checked_before_anything_is_built_and_the_notes_size_after_they_render(self):
        from scripts.ci.tests.test_auto_release_jobs import JOBS
        names = [step.get("name") for step in JOBS["build"]["steps"]]
        self.assertLess(names.index("Check the changelog assembles"), names.index("Prepare VL deps (logs)"))
        self.assertLess(names.index("Check the changelog assembles"), names.index("Build binaries"))
        self.assertLess(names.index("Render release notes"), names.index("Check the release notes size"))
        self.assertLess(names.index("Check the release notes size"), names.index("Prepare release metadata"))

    def test_the_notes_size_check_refuses_over_125000_bytes(self):
        from scripts.ci.tests.test_auto_release_jobs import step_run
        for size, want in ((125000, 0), (125001, 1)):
            with tempfile.TemporaryDirectory() as tmp:
                os.makedirs(os.path.join(tmp, "release-notes"))
                with open(os.path.join(tmp, "release-notes", "release-notes.md"), "w") as fh:
                    fh.write("x" * size)
                r = subprocess.run(["bash", "-e", "-c", step_run("build", "Check the release notes size")],
                                   env=dict(os.environ, RUNNER_TEMP=tmp), capture_output=True, text=True)
                self.assertEqual(r.returncode, want, (size, r.stdout))

    def test_good_and_bad_lists_in_both_jobs(self):
        for job in ("build", "metadata"):
            with self.subTest(job=job):
                self.assertEqual(self.run_check(job, "").returncode, 0)
                self.assertEqual(self.run_check(job, "changelog.d/1.fixed.md\nchangelog.d/2.added.md\n").returncode, 0)
                for bad in ("CHANGELOG.md\n", "changelog.d/1.fixed.md\n\n", "changelog.d/1.fixed.md.sh\n", "changelog.d/../x.fixed.md\n",
                            "changelog.d/1.fixed.md\nchangelog.d/2.fixed.md \n"):
                    self.assertNotEqual(self.run_check(job, bad).returncode, 0, bad)


class WorkflowUsesTheTagHelper(unittest.TestCase):
    def test_no_step_of_the_release_asks_git_describe_for_the_previous_release(self):
        text = (ROOT / ".github" / "workflows" / "auto-release.yaml").read_text()
        step = text.split("      - name: Assemble the changelog\n", 1)[1].split("\n      - name:", 1)[0]
        self.assertIn("changelog_fragments.py previous-tag", step)
        self.assertNotIn("git describe", step)
        self.assertNotIn("git describe", text.split("      - name: Render release notes\n", 1)[1].split("\n      - name:", 1)[0])


class MetadataTieTests(RepoFixture):
    """A release-metadata sync is tied to its tag: the fragments it deletes are the ones that release took."""

    gate = GateTests.gate

    def setUp(self):
        super().setUp()
        self.write("changelog.d/1.fixed.md", "- **One.** t\n")
        self.write("changelog.d/2.added.md", "- **Two.** t\n")
        self.commit("merge 1 and 2")
        self.git("tag", "v0.2.0")  # the release commit of 0.2.0

    def pr(self, version="0.2.0", prev="v0.1.0", chart=True, mutate=None, subject=None, at=None, merge_main=False):
        """The metadata PR as the release job builds it (at the release commit `at`, default main's tip),
        optionally hand-edited and with main merged in afterwards, on branch pr."""
        self.git("checkout", "-q", "-b", "pr", *([at] if at else []))
        self.release(version, prev)
        if chart:
            self.write("charts/victoria-lakehouse/Chart.yaml", f"version: {version}\n")
        if mutate:
            mutate()
        self.commit(subject or f"chore: release metadata for v{version} [skip release]")
        if merge_main:
            self.git("merge", "-q", "main", "-m", "merge main")
        return self.gate()

    def land(self):
        """The PR merges into main."""
        self.git("checkout", "-q", "main")
        self.git("merge", "-q", "--ff-only", "pr")
        self.git("branch", "-q", "-D", "pr")

    def cl(self):
        return (self.repo / "CHANGELOG.md").read_text()

    def set_cl(self, text):
        self.write("CHANGELOG.md", text)

    def ok_sync(self, result):
        self.assertEqual(result[0], 0, result[1])
        self.assertIn("release metadata sync", result[1])

    def refused(self, result, needle=None):
        code, out = result
        self.assertEqual(code, 1, out)
        self.assertNotIn("release metadata sync", out)
        if needle:
            self.assertIn(needle, out)

    # ---- accepted

    def test_a_normal_release_is_a_sync(self):
        self.ok_sync(self.pr())

    def test_a_release_without_fragments_is_a_sync(self):
        self.git("checkout", "-q", "main")
        self.git("rm", "-q", "changelog.d/1.fixed.md", "changelog.d/2.added.md")
        self.commit("chore: nothing pending")
        self.git("tag", "v0.2.1")
        self.ok_sync(self.pr("0.2.1", "v0.2.0"))

    def test_two_pending_releases_in_order(self):
        self.write("changelog.d/3.fixed.md", "- **Three.** t\n")
        self.commit("merge 3")
        self.git("tag", "v0.3.0")
        self.ok_sync(self.pr("0.2.0", "v0.1.0", at="v0.2.0", merge_main=True))
        self.land()
        self.ok_sync(self.pr("0.3.0", "v0.2.0"))  # built on the tip, where the first one has landed

    def test_two_pending_releases_out_of_order(self):
        self.write("changelog.d/3.fixed.md", "- **Three.** t\n")
        self.commit("merge 3")
        self.git("tag", "v0.3.0")
        self.ok_sync(self.pr("0.3.0", "v0.2.0", at="v0.3.0", merge_main=True))  # deletes 3 only (1 and 2 belong to v0.2.0)
        self.land()
        # the earlier one second: a section under the newer one, so the strict shape does not apply; the
        # normal path accepts it because the tag exists and the PR deletes what v0.2.0 took
        code, out = self.pr("0.2.0", "v0.1.0", chart=False)
        self.assertEqual(code, 0, out)

    def test_main_moved_after_the_cut(self):
        self.git("checkout", "-q", "-b", "pr")
        self.release("0.2.0", "v0.1.0")
        self.commit("chore: release metadata for v0.2.0 [skip release]")
        self.git("checkout", "-q", "main")
        self.write("changelog.d/9.fixed.md", "- **Nine.** t\n")  # merged after the release commit
        self.commit("merge 9")
        self.git("checkout", "-q", "pr")
        self.git("merge", "-q", "main", "-m", "merge main")
        code, out = self.gate()
        self.assertEqual(code, 0, out)

    def test_recovery_filing_the_bullet_by_hand_and_deleting_the_file(self):
        self.git("checkout", "-q", "-b", "pr")
        filed = BASE_CHANGELOG.replace("## [0.1.0]", "## [0.2.0] - 2026-02-01\n\n### Added\n\n- **Two.** t\n\n### Fixed\n\n- **One.** t\n\n## [0.1.0]")
        self.set_cl(filed)
        (self.repo / "changelog.d/1.fixed.md").unlink()
        (self.repo / "changelog.d/2.added.md").unlink()
        self.commit("chore: file the bullets of v0.2.0 and remove their fragments")
        code, out = self.gate()
        self.assertEqual(code, 0, out)

    def test_recovery_dropping_an_empty_junk_heading(self):
        self.git("checkout", "-q", "main")
        self.set_cl(BASE_CHANGELOG.replace("## [0.1.0]", "## [0.0.1] - 2026-01-01\n\n## [0.1.0]"))
        self.commit("junk heading on main")
        self.git("checkout", "-q", "-b", "pr")
        self.set_cl(BASE_CHANGELOG)
        self.commit("docs: drop an empty heading")
        code, out = self.gate()
        self.assertEqual(code, 0, out)

    # ---- refused

    def test_a_hand_bullet_in_the_new_section(self):
        self.refused(self.pr(mutate=lambda: self.set_cl(self.cl().replace("- **Two.** t", "- **Two.** t\n\n- **Slipped in.** t"))))

    def test_a_hand_bullet_in_the_previous_section(self):
        self.refused(self.pr(mutate=lambda: self.set_cl(self.cl().replace("- **Old fix.** text.", "- **Old fix.** text.\n\n- **Slipped in.** t"))))

    def test_a_hand_made_section(self):
        def mutate():
            self.set_cl(self.cl().replace("## [0.2.0] - ", "## [9.9.9] - 2026-02-02\n\n- **Hand.** t\n\n## [0.2.0] - ", 1))
        self.refused(self.pr(mutate=mutate))

    def test_a_bare_bullet_under_unreleased_above_an_empty_new_section(self):
        def mutate():
            t = self.cl().replace("### Added\n\n- **Two.** t\n\n", "").replace("### Fixed\n\n- **One.** t\n\n", "")
            self.set_cl(t.replace("## [Unreleased]\n", "## [Unreleased]\n\n- **One.** t\n- **Two.** t\n", 1))
        self.refused(self.pr(mutate=mutate))

    def test_a_metadata_pr_that_keeps_all_its_fragments(self):
        self.refused(self.pr(mutate=self._restore_fragments))

    def _restore_fragments(self):
        self.write("changelog.d/1.fixed.md", "- **One.** t\n")
        self.write("changelog.d/2.added.md", "- **Two.** t\n")

    def test_feat_chart_and_an_unreleased_bullet(self):
        def mutate():
            self.set_cl(self.cl().replace("## [Unreleased]\n", "## [Unreleased]\n\n- **Feature.** t\n", 1))
        self.refused(self.pr(mutate=mutate, subject="feat: metadata"))

    def test_prose_in_the_new_section(self):
        self.refused(self.pr(mutate=lambda: self.set_cl(self.cl().replace("- **Two.** t", "- **Two.** t\n\nProse nobody wrote."))))

    def test_a_stray_heading(self):
        self.refused(self.pr(mutate=lambda: self.set_cl(self.cl().replace("### Added", "### Weird\n\n### Added", 1))))

    def test_a_re_indented_fragment_line(self):
        self.refused(self.pr(mutate=lambda: self.set_cl(self.cl().replace("- **Two.** t", "  - **Two.** t"))))

    def test_a_sync_shaped_zero_section(self):
        self.refused(self.pr(mutate=lambda: self.set_cl(self.cl().replace("## [Unreleased]\n", "## [Unreleased]\n\n## [0.0.0] - 2026-01-01\n", 1))))

    def test_w7c_one_fragment_kept_and_its_bullet_dropped(self):
        def mutate():
            self.write("changelog.d/1.fixed.md", "- **One.** t\n")
            self.set_cl(self.cl().replace("### Fixed\n\n- **One.** t\n\n", ""))
        self.refused(self.pr(mutate=mutate), None)

    def test_w7c_without_chart(self):
        def mutate():
            self.write("changelog.d/1.fixed.md", "- **One.** t\n")
            self.set_cl(self.cl().replace("### Fixed\n\n- **One.** t\n\n", ""))
        self.refused(self.pr(chart=False, mutate=mutate), "without deleting the fragments release v0.2.0 took")

    def test_empty_dated_sections(self):
        for name, tagged in (("0.2.0", True), ("99.0.0", False), ("foo", False)):
            with self.subTest(name):
                self.git("checkout", "-q", "main")
                if "pr" in self.git("branch").split():
                    self.git("branch", "-q", "-D", "pr")
                self.git("checkout", "-q", "-b", "pr")
                self.set_cl(BASE_CHANGELOG.replace("## [0.1.0]", f"## [{name}] - 2026-02-02\n\n## [0.1.0]"))
                self.commit("docs: an empty section")
                self.refused(self.gate())

    def test_a_renamed_unreleased_heading(self):
        self.git("checkout", "-q", "-b", "pr")
        self.set_cl(BASE_CHANGELOG.replace("## [Unreleased]", "## [Pending]"))
        self.commit("docs: rename")
        self.refused(self.gate(), "exactly one '## [Unreleased]'")

    def test_deleting_the_unreleased_heading(self):
        self.git("checkout", "-q", "-b", "pr")
        self.set_cl(BASE_CHANGELOG.replace("## [Unreleased]\n\n", ""))
        self.commit("docs: drop the heading")
        self.refused(self.gate(), "exactly one '## [Unreleased]'")

    def test_a_section_with_text_cannot_be_removed(self):
        self.git("checkout", "-q", "-b", "pr")
        self.set_cl(BASE_CHANGELOG.replace("\n## [0.1.0] - 2026-01-01\n\n### Fixed\n\n- **Old fix.** text.\n", ""))
        self.commit("docs: drop a release")
        self.refused(self.gate())

    def test_a_bullet_moved_from_the_previous_section_into_the_new_one(self):
        def mutate():
            t = self.cl().replace("\n- **Old fix.** text.\n", "\n", 1)
            self.set_cl(t.replace("- **Two.** t", "- **Two.** t\n\n- **Old fix.** text.", 1))
        self.refused(self.pr(mutate=mutate))

    def test_the_same_version_heading_twice(self):
        self.git("checkout", "-q", "-b", "pr")
        self.set_cl(BASE_CHANGELOG.rstrip("\n") + "\n\n## [0.1.0] - 2026-01-01\n")
        self.commit("docs: duplicate a heading")
        self.refused(self.gate(), "the same version heading twice")

    def test_a_heading_cannot_be_dropped_while_its_text_stays(self):
        self.git("checkout", "-q", "-b", "pr")
        self.set_cl(BASE_CHANGELOG.replace("## [0.1.0] - 2026-01-01\n\n", "", 1))
        self.commit("docs: drop a heading, keep its text")
        self.refused(self.gate(), "is removed but it holds text")

    # ---- the two mutants that survived the fourth review

    def test_the_preamble_cannot_change_during_a_sync(self):
        self.refused(self.pr(mutate=lambda: self.set_cl(self.cl().replace("preamble", "a different preamble", 1))))

    def test_the_previous_newest_section_cannot_change_during_a_sync(self):
        self.refused(self.pr(mutate=lambda: self.set_cl(self.cl().replace("- **Old fix.** text.", "- **Old fix.** reworded."))))

    def test_a_release_whose_tag_does_not_exist_is_not_a_sync(self):
        self.git("tag", "-d", "v0.2.0")
        self.git("tag", "v0.2.9")
        code, out = self.pr("0.2.0", "v0.1.0")
        self.refused((code, out))

    def test_the_assembler_checks_the_first_tag_of_a_fragment_not_the_previous_release(self):
        """Out of order: v0.2.0 took fragments 1 and 2 (its metadata PR is pending); v0.3.0's section was merged first.
        The next release must not call 1 and 2 left behind just because v0.3.0, their 'previous release', has a section."""
        self.write("changelog.d/3.fixed.md", "- **Three.** t\n")
        self.commit("merge 3")
        self.git("tag", "v0.3.0")
        self.git("checkout", "-q", "-b", "m3")
        self.release("0.3.0", "v0.2.0")
        self.commit("chore: release metadata for v0.3.0 [skip release]")
        self.git("checkout", "-q", "main")
        self.git("merge", "-q", "--ff-only", "m3")
        self.write("changelog.d/4.fixed.md", "- **Four.** t\n")
        self.commit("merge 4")
        self.assertEqual(self.release("0.4.0", "v0.3.0"), ["changelog.d/4.fixed.md"])


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
