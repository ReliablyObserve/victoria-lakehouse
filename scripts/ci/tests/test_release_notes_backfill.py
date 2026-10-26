"""The release notes backfill: planning, the risk report and the guarded apply (scripts/ci/release_notes_backfill.py)."""

import contextlib
import hashlib
import inspect
import io
import json
import os
import pathlib
import stat
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import changelog_fragments as cf  # noqa: E402
import release_notes_backfill as bf  # noqa: E402

ARTIFACTS = (
    "## Release Artifacts\n\n### Binaries\n| Binary | Platform | File |\n|---|---|---|\n"
    "| **lakehouse-logs** | Linux amd64 | `lakehouse-logs-linux-amd64` |\n\n"
    "### Docker Images\n```bash\ndocker pull ghcr.io/x/lakehouse-logs:v1.0.1\n```\n"
)
BEFORE = (
    "# Changelog\n\n## [Unreleased]\n\n### Fixed\n\n- **Old fix (#10).** released by v1.0.0.\n\n"
    "## [0.9.0] - 2025-12-01\n\n### Added\n\n- **Ancient.** x\n"
)
AFTER = (
    "# Changelog\n\n## [Unreleased]\n\n### Fixed\n\n- **Old fix (#10).** released by v1.0.0.\n"
    "- **New fix (#11).** It wraps\n  over two lines.\n\n### Added\n\n- **New feature.** `code` here.\n\n"
    "## [0.9.0] - 2025-12-01\n\n### Added\n\n- **Ancient.** x\n"
)
OLD_PUBLISHED = "2026-05-01T10:00:00Z"
BEFORE_CUTOFF = "2026-10-23T05:59:59Z"


def rel(tag="v1.0.1", published=OLD_PUBLISHED, body=ARTIFACTS, **extra):
    release = {"tag_name": tag, "published_at": published, "draft": False, "prerelease": False, "immutable": False, "body": body}
    release.update(extra)
    return release


class FakeRepo:
    """Tags as dicts: changelog text, previous tag and fragments (path -> text)."""

    def __init__(self, tags):
        self.tags = tags
        self.blobs = {}

    def has_tag(self, tag):
        return tag in self.tags

    def changelog(self, tag):
        return self.tags.get(tag, {}).get("changelog")

    def previous_tag(self, tag):
        return self.tags[tag].get("prev", "")

    def fragments(self, tag):
        out = {}
        for path, text in self.tags.get(tag, {}).get("fragments", {}).items():
            blob = hashlib.sha1(text.encode()).hexdigest()
            self.blobs[blob] = text
            out[path] = blob
        return out

    def blob(self, blob_id):
        return self.blobs.get(blob_id)


def two_tags(**second):
    tags = {"v1.0.0": {"changelog": BEFORE}, "v1.0.1": {"changelog": AFTER, "prev": "v1.0.0"}}
    tags["v1.0.1"].update(second)
    return FakeRepo(tags)


class Planning(unittest.TestCase):
    def test_rewrites_only_the_part_before_the_marker(self):
        entry = bf.plan_release(two_tags(), rel())
        self.assertEqual(entry["status"], bf.EDIT)
        self.assertTrue(entry["new_body"].endswith("\n\n" + ARTIFACTS))
        self.assertEqual(entry["new_body"][entry["new_body"].index("## Release Artifacts") :], ARTIFACTS)
        self.assertTrue(entry["new_body"].startswith("Released at 2026-05-01\n\n"))
        self.assertIn("* BUGFIX: **New fix ([#11](", entry["new_body"])
        self.assertIn("It wraps over two lines.", entry["new_body"])
        self.assertIn("* FEATURE: **New feature.** `code` here.", entry["new_body"])
        self.assertEqual(entry["old_bytes"], len(ARTIFACTS))
        self.assertEqual(entry["new_bytes"], len(entry["new_body"].encode()))

    def test_entries_of_the_previous_release_are_not_repeated(self):
        entry = bf.plan_release(two_tags(), rel())
        self.assertNotIn("Old fix", entry["new_body"])

    def test_first_release_has_no_previous_tag(self):
        repo = FakeRepo({"v0.1.0": {"changelog": BEFORE}})
        entry = bf.plan_release(repo, rel("v0.1.0"))
        self.assertEqual(entry["status"], bf.EDIT)
        self.assertIn("Old fix", entry["new_body"])

    def test_section_at_the_tag_is_used_as_it_stands(self):
        text = "# Changelog\n\n## [Unreleased]\n\n## [1.0.1] - 2026-05-01\n\n### Added\n\n- **Own section.** y\n"
        entry = bf.plan_release(FakeRepo({"v1.0.1": {"changelog": text}}), rel())
        self.assertEqual(entry["status"], bf.EDIT)
        self.assertIn("* FEATURE: **Own section.** y", entry["new_body"])

    def test_no_entry_is_left_alone(self):
        same = {"v1.0.0": {"changelog": BEFORE}, "v1.0.1": {"changelog": BEFORE, "prev": "v1.0.0"}}
        entry = bf.plan_release(FakeRepo(same), rel())
        self.assertEqual((entry["status"], entry["new_sha256"]), (bf.NO_ENTRY, None))
        self.assertNotIn("new_body", entry)

    def test_empty_unreleased_and_missing_changelog_are_no_entry(self):
        empty = "# Changelog\n\n## [Unreleased]\n\n## [0.9.0] - 2025-12-01\n"
        self.assertEqual(bf.plan_release(FakeRepo({"v1.0.1": {"changelog": empty}}), rel())["status"], bf.NO_ENTRY)
        self.assertEqual(bf.plan_release(FakeRepo({"v1.0.1": {}}), rel())["status"], bf.NO_ENTRY)
        no_unreleased = "# Changelog\n\n## [0.9.0] - 2025-12-01\n\n### Added\n\n- x\n"
        self.assertEqual(bf.plan_release(FakeRepo({"v1.0.1": {"changelog": no_unreleased}}), rel())["status"], bf.NO_ENTRY)

    def test_unknown_tag(self):
        self.assertEqual(bf.plan_release(FakeRepo({}), rel())["status"], bf.NO_TAG)

    def test_cutoff_is_exclusive_of_the_cutoff_second(self):
        self.assertEqual(bf.plan_release(two_tags(), rel(published=BEFORE_CUTOFF))["status"], bf.EDIT)
        for published in (bf.CUTOFF, "2026-10-23T06:00:01Z", "2027-01-01T00:00:00Z"):
            entry = bf.plan_release(two_tags(), rel(published=published))
            self.assertEqual(entry["status"], bf.IMMUTABLE, published)

    def test_immutable_flag_and_draft_exclude(self):
        self.assertEqual(bf.plan_release(two_tags(), rel(immutable=True))["status"], bf.IMMUTABLE)
        self.assertEqual(bf.plan_release(two_tags(), rel(draft=True))["status"], bf.IMMUTABLE)
        self.assertEqual(bf.plan_release(two_tags(), rel(published=None))["status"], bf.IMMUTABLE)

    def test_bodies_that_are_not_ours_to_replace(self):
        generated = "## What's Changed\n* feat: x by @someone in https://example.invalid/pull/4\n"
        self.assertEqual(bf.plan_release(two_tags(), rel(body=generated))["status"], bf.NO_MARKER)
        self.assertEqual(bf.plan_release(two_tags(), rel(body="notes ## Release Artifacts\n"))["status"], bf.NO_MARKER)
        self.assertEqual(bf.plan_release(two_tags(), rel(body=""))["status"], bf.NO_MARKER)
        self.assertEqual(bf.plan_release(two_tags(), rel(body="Released at 2026-05-01\n\n* FEATURE: x\n\n" + ARTIFACTS))["status"], bf.NEW_STYLE)
        self.assertEqual(bf.plan_release(two_tags(), rel(body="Hand written.\n\n" + ARTIFACTS))["status"], bf.OTHER_TEXT)

    def test_marker_must_be_a_heading_line(self):
        entry = bf.plan_release(two_tags(), rel(body="\n\n" + ARTIFACTS))
        self.assertEqual(entry["status"], bf.EDIT)
        self.assertTrue(entry["new_body"].endswith("\n\n" + ARTIFACTS))

    def test_first_marker_wins_and_the_rest_is_kept_verbatim(self):
        body = ARTIFACTS + "\nmore\n## Release Artifacts\nagain\r\n  trailing  \n"
        entry = bf.plan_release(two_tags(), rel(body=body))
        self.assertTrue(entry["new_body"].endswith("\n\n" + body))

    def test_planning_is_deterministic(self):
        a = bf.plan_release(two_tags(), rel())
        b = bf.plan_release(two_tags(), rel())
        self.assertEqual(a["new_sha256"], b["new_sha256"])

    def test_unassemblable_unreleased_is_rendered_as_it_stands_with_a_warning(self):
        text = "# Changelog\n\n## [Unreleased]\n\n**Loose prose.**\n\n### Added\n\n- **Item.** a\n"
        entry = bf.plan_release(FakeRepo({"v1.0.1": {"changelog": text}}), rel())
        self.assertEqual(entry["status"], bf.EDIT)
        self.assertTrue(any("rendered as it stands" in w for w in entry["warnings"]))
        self.assertIn("* FEATURE: **Item.** a", entry["new_body"])

    def test_fragments_of_the_tag_minus_those_of_the_previous_tag(self):
        old = "- **Released fragment.** r\n"
        new = "- **New fragment.** n\n"
        text = "# Changelog\n\n## [Unreleased]\n\n## [0.9.0] - 2025-12-01\n"
        repo = FakeRepo(
            {
                "v1.0.0": {"changelog": text, "fragments": {"changelog.d/1.fixed.md": old}},
                "v1.0.1": {"changelog": text, "prev": "v1.0.0", "fragments": {"changelog.d/1.fixed.md": old, "changelog.d/2.added.md": new}},
            }
        )
        entry = bf.plan_release(repo, rel())
        self.assertEqual(entry["status"], bf.EDIT)
        self.assertIn("**New fragment.**", entry["new_body"])
        self.assertNotIn("Released fragment", entry["new_body"])
        # same name, other bytes: released again
        repo.tags["v1.0.1"]["fragments"]["changelog.d/1.fixed.md"] = "- **Reused name.** z\n"
        self.assertIn("**Reused name.**", bf.plan_release(repo, rel())["new_body"])

    def test_invalid_fragment_is_reported_not_rendered(self):
        text = "# Changelog\n\n## [Unreleased]\n\n### Added\n\n- **Item.** a\n"
        repo = FakeRepo({"v1.0.1": {"changelog": text, "fragments": {"changelog.d/1.fixed.md": "no bullet\n"}}})
        entry = bf.plan_release(repo, rel())
        self.assertTrue(any("fragment skipped" in w for w in entry["warnings"]))
        self.assertNotIn("no bullet", entry["new_body"])

    def test_source_cross_check(self):
        compare = "# Changelog\n\n## [1.0.1] - 2026-05-01\n\n### Fixed\n\n- **a.** x\n- **b.** y\n- **c.** z\n"
        entry = bf.plan_release(two_tags(), rel(), compare=compare)
        self.assertEqual(entry["source"], {"at_tag": 2, "in_main": 3})
        self.assertFalse(any("source" in w for w in entry["warnings"]))


class Risks(unittest.TestCase):
    def check(self, notes, body=None, max_ref=100):
        return bf.risks(notes, body if body is not None else notes, max_ref)

    def test_clean(self):
        found, counts = self.check("Released at 2026-01-01\n\n* FEATURE: **A.** `x` ([#5](%s/issues/5))\n" % bf.rn.REPO_URL)
        self.assertEqual(found, [])
        self.assertEqual((counts["refs"], counts["mentions"], counts["backtick_lines"]), (1, 0, 0))

    def test_unbalanced_backticks_and_fences(self):
        found, counts = self.check("* FEATURE: a ` b\n* FEATURE: `ok`\n```bash\nit's `\n")
        self.assertEqual(counts["backtick_lines"], 1)
        self.assertTrue(any("unbalanced backticks" in w for w in found))
        self.assertTrue(any("fences" in w for w in found))

    def test_balanced_fence_is_fine(self):
        found, _ = self.check("```bash\nx ` y\n```\n")
        self.assertEqual(found, [])

    def test_unclosed_comment(self):
        found, _ = self.check("* FEATURE: a <!-- b\n")
        self.assertTrue(any("HTML comment" in w for w in found))
        self.assertEqual(self.check("* FEATURE: a <!-- b --> c\n")[0], [])

    def test_big_body(self):
        found, _ = self.check("x", body="y" * (bf.MAX_BODY_BYTES + 1))
        self.assertTrue(any("bytes" in w for w in found))
        self.assertEqual(self.check("x", body="y" * bf.MAX_BODY_BYTES)[0], [])

    def test_mentions_outside_code_only(self):
        found, counts = self.check("* FEATURE: thanks @szibis and @org/team, mail a@b.com, `@code`, https://x/@url, [l](https://y/@z)\n")
        self.assertEqual(counts["mentions"], 2)
        self.assertTrue(any("@org/team" in w and "@szibis" in w for w in found))

    def test_refs_above_the_newest_number_and_other_projects(self):
        link = lambda n: "[#%d](%s/issues/%d)" % (n, bf.rn.REPO_URL, n)
        found, counts = self.check("* FEATURE: VictoriaLogs %s and %s and %s\n" % (link(7), link(1603), link(0)), max_ref=800)
        self.assertEqual(counts["refs"], 3)
        self.assertEqual(counts["refs_unknown"], 2)
        self.assertEqual(counts["refs_other_repo"], 1)
        self.assertTrue(any("#0, #1603" in w for w in found))
        self.assertTrue(any("other project" in w for w in found))

    def test_unknown_max_ref_does_not_flag(self):
        link = "[#9999](%s/issues/9999)" % bf.rn.REPO_URL
        self.assertEqual(self.check("* FEATURE: %s\n" % link, max_ref=None)[1]["refs_unknown"], 0)

    def test_bare_refs_left_unlinked(self):
        found, counts = self.check("* FEATURE: see #123456 and `#5`\n")
        self.assertEqual(counts["refs_bare"], 1)
        self.assertTrue(any("#123456" in w for w in found))

    def test_ref_context(self):
        notes = "* FEATURE: this was fixed upstream in issue [#12](%s/issues/12) long ago\n" % bf.rn.REPO_URL
        self.assertEqual(bf.ref_context(notes), [(12, "* FEATURE: this was fixed upstream in issue #12 long ago")])
        self.assertEqual(bf.ref_context("* FEATURE: `#5` and #6 stay out\n"), [])

    def test_risks_are_reported_in_the_plan_without_changing_the_text(self):
        changelog = "# Changelog\n\n## [Unreleased]\n\n### Added\n\n- **A @szibis.** unbalanced ` tick (#5)\n"
        entry = bf.plan_release(FakeRepo({"v1.0.1": {"changelog": changelog}}), rel(), max_ref=3)
        self.assertEqual(entry["status"], bf.EDIT)
        text = " ".join(entry["warnings"])
        for needle in ("unbalanced backticks", "@szibis", "#5"):
            self.assertIn(needle, text)
        self.assertIn("**A @szibis.** unbalanced ` tick", entry["new_body"])


FAKE_GH = """#!{python}
import json, os, sys
state_path, log_path = os.environ["FAKE_GH_STATE"], os.environ["FAKE_GH_LOG"]
state = json.load(open(state_path))
args = sys.argv[1:]
with open(log_path, "a") as log:
    log.write(json.dumps(args) + "\\n")
if args[0] == "api":
    tag = args[1].rsplit("/", 1)[1]
    print(json.dumps(state[tag]))
elif args[:2] == ["release", "edit"]:
    tag = args[2]
    if os.environ.get("FAKE_GH_FAIL") == tag:
        sys.stderr.write("HTTP 500 boom")
        sys.exit(1)
    notes = open(args[args.index("--notes-file") + 1]).read()
    backup = os.path.join(os.environ["FAKE_GH_BACKUP"], tag + ".md")
    with open(log_path, "a") as log:
        log.write(json.dumps({{"backup_exists_at_edit": os.path.exists(backup), "backup_equals_live": os.path.exists(backup) and open(backup).read() == state[tag]["body"]}}) + "\\n")
    state[tag]["body"] = "corrupted" if os.environ.get("FAKE_GH_CORRUPT") == tag else notes
    json.dump(state, open(state_path, "w"))
else:
    sys.exit(3)
"""


class Applying(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        tmp = pathlib.Path(self.tmp.name)
        self.out = tmp / "out"
        self.state = tmp / "state.json"
        self.calls = tmp / "calls.log"
        self.gh = tmp / "gh"
        self.gh.write_text(FAKE_GH.format(python=sys.executable))
        self.gh.chmod(self.gh.stat().st_mode | stat.S_IXUSR)
        self.tags = {"v1.0.0": {"changelog": BEFORE}, "v1.0.1": {"changelog": AFTER, "prev": "v1.0.0"}}
        self.tags["v1.0.2"] = {"changelog": AFTER.replace("- **New feature.**", "- **Third.** t\n- **New feature.**"), "prev": "v1.0.1"}
        self.repo = FakeRepo(self.tags)
        self.releases = [rel("v1.0.0", body=ARTIFACTS), rel("v1.0.1"), rel("v1.0.2")]
        self.live = {r["tag_name"]: dict(r) for r in self.releases}
        self.env = {"FAKE_GH_STATE": str(self.state), "FAKE_GH_LOG": str(self.calls), "FAKE_GH_BACKUP": str(self.out / "backup")}
        self.saved = {k: os.environ.get(k) for k in (*self.env, bf.APPLY_ENV, "FAKE_GH_FAIL", "FAKE_GH_CORRUPT")}
        self.addCleanup(self.restore_env)
        os.environ.update(self.env)
        for key in (bf.APPLY_ENV, "FAKE_GH_FAIL", "FAKE_GH_CORRUPT"):
            os.environ.pop(key, None)
        self.save_state()

    def restore_env(self):
        for key, value in self.saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def save_state(self):
        self.state.write_text(json.dumps(self.live))

    def plan(self):
        plan = bf.build_plan(self.repo, self.releases)
        bf.write_plan(plan, self.releases, self.out)
        return plan

    def gh_calls(self):
        if not self.calls.exists():
            return []
        return [json.loads(line) for line in self.calls.read_text().splitlines()]

    def edits(self):
        return [c for c in self.gh_calls() if isinstance(c, list) and c[:2] == ["release", "edit"]]

    def apply(self, plan=None, **kw):
        kw.setdefault("pause", 0)
        kw.setdefault("sleep", lambda s: None)
        return bf.apply_plan(plan or self.plan(), self.out, "o/r", binary=str(self.gh), **kw)

    def test_the_plan_edits_nothing_and_writes_old_new_and_plan(self):
        plan = self.plan()
        self.assertEqual([e["status"] for e in plan], [bf.EDIT] * 3)
        self.assertEqual(self.gh_calls(), [])
        self.assertEqual((self.out / "old" / "v1.0.1.md").read_text(), ARTIFACTS)
        self.assertEqual((self.out / "new" / "v1.0.1.md").read_text(), next(e for e in plan if e["tag"] == "v1.0.1")["new_body"])
        saved = json.loads((self.out / "plan.json").read_text())
        self.assertNotIn("new_body", saved[0])
        self.assertEqual({e["tag"] for e in saved}, {"v1.0.0", "v1.0.1", "v1.0.2"})

    def test_apply_edits_eligible_releases_with_a_backup_first(self):
        plan = self.plan()
        edited = self.apply(plan)
        self.assertEqual(edited, ["v1.0.0", "v1.0.1", "v1.0.2"])
        live = json.loads(self.state.read_text())
        for entry in plan:
            if entry["status"] == bf.EDIT:
                self.assertEqual(live[entry["tag"]]["body"], entry["new_body"])
                self.assertEqual((self.out / "backup" / f"{entry['tag']}.md").read_text(), ARTIFACTS)
        checks = [c for c in self.gh_calls() if isinstance(c, dict)]
        self.assertEqual(len(checks), len(edited))
        self.assertTrue(all(c["backup_exists_at_edit"] and c["backup_equals_live"] for c in checks))
        log = [json.loads(l) for l in (self.out / "apply.log.jsonl").read_text().splitlines()]
        self.assertEqual([r["status"] for r in log], ["done"] * len(edited))

    def test_resumes_from_the_log_and_honours_max(self):
        plan = self.plan()
        first = self.apply(plan, limit=1)
        self.assertEqual(len(first), 1)
        second = self.apply(plan)
        self.assertNotIn(first[0], second)
        self.assertEqual(len(self.edits()), len(first) + len(second))
        self.assertEqual(self.apply(plan), [])
        self.assertEqual(len(self.edits()), len(first) + len(second))

    def test_only_selects_tags_and_refuses_ineligible_ones(self):
        plan = self.plan()
        self.assertEqual(self.apply(plan, only={"v1.0.1"}), ["v1.0.1"])
        self.releases.append(rel("v1.0.3", published=bf.CUTOFF))
        self.live["v1.0.3"] = dict(self.releases[-1])
        self.save_state()
        plan = bf.build_plan(self.repo, self.releases)
        with self.assertRaises(bf.BackfillError):
            self.apply(plan, only={"v1.0.3"})

    def test_refuses_when_the_live_body_changed_since_the_plan(self):
        plan = self.plan()
        self.live["v1.0.1"]["body"] = ARTIFACTS + "\nedited by hand\n"
        self.save_state()
        with self.assertRaisesRegex(bf.BackfillError, "differs from the planned"):
            self.apply(plan, only={"v1.0.1"})
        self.assertEqual(self.edits(), [])

    def test_refuses_a_release_that_became_immutable_or_a_draft_or_post_cutoff(self):
        for change in ({"immutable": True}, {"draft": True}, {"published_at": bf.CUTOFF}, {"published_at": "2026-12-01T00:00:00Z"}):
            plan = self.plan()
            self.live["v1.0.1"] = dict(self.releases[1], **change)
            self.save_state()
            with self.assertRaisesRegex(bf.BackfillError, "refusing"):
                self.apply(plan, only={"v1.0.1"})
            self.assertEqual(self.edits(), [], change)

    def test_refuses_a_new_body_that_loses_the_artifacts_part(self):
        plan = self.plan()
        entry = next(e for e in plan if e["tag"] == "v1.0.1")
        entry["new_body"] = "Released at 2026-05-01\n\n* FEATURE: x\n"
        with self.assertRaisesRegex(bf.BackfillError, "artifacts part"):
            self.apply(plan, only={"v1.0.1"})
        self.assertEqual(self.edits(), [])

    def test_refuses_when_the_reviewed_file_changed(self):
        plan = self.plan()
        (self.out / "new" / "v1.0.1.md").write_text("tampered\n")
        with self.assertRaisesRegex(bf.BackfillError, "reviewed body"):
            self.apply(plan, only={"v1.0.1"})
        self.assertEqual(self.edits(), [])

    def test_stops_when_the_body_read_back_is_not_the_planned_one(self):
        plan = self.plan()
        os.environ["FAKE_GH_CORRUPT"] = "v1.0.1"
        with self.assertRaisesRegex(bf.BackfillError, "restore from"):
            self.apply(plan)
        log = [json.loads(l) for l in (self.out / "apply.log.jsonl").read_text().splitlines()]
        self.assertEqual(log[-1]["status"], "verify-failed")
        self.assertEqual((self.out / "backup" / "v1.0.1.md").read_text(), ARTIFACTS)
        # nothing after the failed release was edited
        self.assertEqual([c[2] for c in self.edits()][-1], "v1.0.1")

    def test_a_gh_failure_stops_the_run(self):
        plan = self.plan()
        os.environ["FAKE_GH_FAIL"] = "v1.0.1"
        with self.assertRaisesRegex(bf.BackfillError, "boom"):
            self.apply(plan, only={"v1.0.1"})
        self.assertFalse((self.out / "apply.log.jsonl").exists())

    def test_pause_between_edits(self):
        slept = []
        self.apply(sleep=slept.append, pause=1.5)
        self.assertEqual(slept, [1.5] * len(self.edits()))

    def run_main(self, *args, env=None):
        stderr, stdout = io.StringIO(), io.StringIO()
        old = dict(os.environ)
        os.environ.update(env or {})
        try:
            with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(stdout):
                code = bf.main(["--out-dir", str(self.out), "--releases-json", str(self.tmp_json), "--root", ".", "--gh", str(self.gh), "--compare-ref", "", *args])
        finally:
            os.environ.clear()
            os.environ.update(old)
        return code, stdout.getvalue(), stderr.getvalue()

    @property
    def tmp_json(self):
        path = pathlib.Path(self.tmp.name) / "releases.json"
        path.write_text(json.dumps(self.releases))
        return path

    def test_apply_flag_without_the_environment_variable_does_nothing(self):
        self.plan()
        for value in (None, "", "no", "YES", "true"):
            env = {} if value is None else {bf.APPLY_ENV: value}
            os.environ.pop(bf.APPLY_ENV, None)
            code, _, err = self.run_main("--apply", env=env)
            self.assertEqual(code, 2, value)
            self.assertIn(bf.APPLY_ENV, err)
        self.assertEqual(self.gh_calls(), [])

    def test_the_environment_variable_without_the_flag_does_nothing(self):
        # build_plan needs a real repo through main(); run it against this checkout's tags is not needed:
        # main() reads tags with git, so an empty temp repo makes every release "no tag", which edits nothing.
        with tempfile.TemporaryDirectory() as empty:
            subprocess.run(["git", "init", "-q", empty], check=True)
            code, out, _ = self.run_main("--root", empty, env={bf.APPLY_ENV: "yes"})
        self.assertEqual(code, 0)
        self.assertEqual(self.gh_calls(), [])
        self.assertIn("considered: 3", out)
        self.assertTrue((self.out / "report.md").exists())

    def test_apply_needs_a_reviewed_plan(self):
        code, _, err = self.run_main("--apply", env={bf.APPLY_ENV: "yes"})
        self.assertEqual(code, 1)
        self.assertIn("run the dry run first", err)
        self.assertEqual(self.gh_calls(), [])


class WithGit(unittest.TestCase):
    """Real tags: the previous release tag, the fragments at a tag and the notes of a tag."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.git("init", "-q", "-b", "main")

    def git(self, *args):
        env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
        return subprocess.run(["git", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", *args], cwd=self.root, env=env, check=True, capture_output=True, text=True).stdout

    def commit(self, tag, changelog, fragments=None):
        (self.root / "CHANGELOG.md").write_text(changelog)
        frag_dir = self.root / "changelog.d"
        frag_dir.mkdir(exist_ok=True)
        for old in frag_dir.iterdir():
            old.unlink()
        (frag_dir / "README.md").write_text("readme\n")
        for name, text in (fragments or {}).items():
            (frag_dir / name).write_text(text)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", tag)
        self.git("tag", tag)

    def test_notes_of_a_tag_from_the_changelog_and_the_fragments_at_the_tag(self):
        self.commit("v1.0.0", BEFORE, {"1.fixed.md": "- **Frag one.** a\n"})
        self.commit("v1.0.1", AFTER, {"1.fixed.md": "- **Frag one.** a\n", "2.added.md": "- **Frag two.** b\n"})
        self.commit("v1.0.2", AFTER + "\n", {})
        repo = bf.Repo(self.root)
        self.assertTrue(repo.has_tag("v1.0.1"))
        self.assertFalse(repo.has_tag("v9.9.9"))
        self.assertEqual(repo.previous_tag("v1.0.1"), "v1.0.0")
        self.assertEqual(repo.previous_tag("v1.0.0"), "")
        notes, _ = bf.notes_for_tag(repo, "v1.0.1", "2026-05-01")
        self.assertIn("**New fix (#11).**", notes.replace("[#11](https://github.com/ReliablyObserve/victoria-lakehouse/issues/11)", "#11"))
        self.assertIn("**Frag two.**", notes)
        self.assertNotIn("Frag one", notes)
        self.assertNotIn("Old fix", notes)
        with self.assertRaises(bf.NoEntry):
            bf.notes_for_tag(repo, "v1.0.2", "2026-05-02")

    def test_planner_on_real_tags_with_legacy_text_and_with_fragments(self):
        empty = "# Changelog\n\n## [Unreleased]\n\n## [0.9.0] - 2025-12-01\n"
        self.commit("v1.0.0", BEFORE)
        self.commit("v1.0.1", AFTER)  # hand-written [Unreleased], as every release before v0.167.12
        self.commit("v1.0.2", empty, {"7.added.md": "- **Fragment feature.** f\n"})  # fragments, empty [Unreleased]
        self.commit("v1.0.3", empty, {"7.added.md": "- **Fragment feature.** f\n", "8.fixed.md": "- **Fragment fix.** g\n"})
        repo = bf.Repo(self.root)
        legacy = bf.plan_release(repo, rel("v1.0.1"))
        self.assertEqual(legacy["status"], bf.EDIT, legacy["reason"])
        self.assertIn("**New fix", legacy["new_body"])
        self.assertNotIn("Old fix", legacy["new_body"])
        fragment = bf.plan_release(repo, rel("v1.0.2"))
        self.assertEqual(fragment["status"], bf.EDIT, fragment["reason"])
        self.assertIn("* FEATURE: **Fragment feature.** f", fragment["new_body"])
        after = bf.plan_release(repo, rel("v1.0.3"))
        self.assertIn("**Fragment fix.**", after["new_body"])
        self.assertNotIn("Fragment feature", after["new_body"])

    def test_plan_through_the_real_repo_and_main_dry_run(self):
        self.commit("v1.0.0", BEFORE)
        self.commit("v1.0.1", AFTER)
        releases = self.root / "releases.json"
        releases.write_text(json.dumps([rel("v1.0.0"), rel("v1.0.1"), rel("v1.0.2", published="2026-10-24T00:00:00Z", immutable=True)]))
        out = self.root / "out"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = bf.main(["--out-dir", str(out), "--releases-json", str(releases), "--root", str(self.root), "--gh", "/nonexistent/gh", "--compare-ref", "main"])
        self.assertEqual(code, 0)
        plan = {e["tag"]: e for e in json.loads((out / "plan.json").read_text())}
        self.assertEqual(plan["v1.0.0"]["status"], bf.EDIT)
        self.assertEqual(plan["v1.0.1"]["status"], bf.EDIT)
        self.assertEqual(plan["v1.0.2"]["status"], bf.IMMUTABLE)
        report = (out / "report.md").read_text()
        for needle in ("considered: 3", "excluded as immutable or published after the cutoff: 1", "eligible, notes rewritten on apply: 2", "LH_BACKFILL_APPLY=yes", "--apply"):
            self.assertIn(needle, report)
        self.assertTrue((out / "new" / "v1.0.1.md").read_text().endswith(ARTIFACTS))


class LegacyAssembly(unittest.TestCase):
    """Historical tags were cut with hand-written [Unreleased] text, which the release assembler refuses.

    The backfill merges it itself (bf.legacy_assemble); the release path stays strict and cannot reach it.
    """

    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

    def test_the_release_assembler_still_refuses_legacy_text(self):
        with self.assertRaisesRegex(cf.AssembleError, "text under '## \\[Unreleased\\]'"):
            cf.assemble(AFTER, [], "1.0.1", "2026-05-01")

    def test_the_backfill_assembles_the_same_text(self):
        assembled = bf.legacy_assemble(AFTER, [], "1.0.1", "2026-05-01")
        self.assertIn("## [1.0.1] - 2026-05-01\n\n### Added\n\n- **New feature.**", assembled)
        self.assertIn("### Fixed\n\n- **Old fix (#10).**", assembled)
        self.assertTrue(assembled.index("### Added") < assembled.index("### Fixed"))  # Keep a Changelog order

    def test_legacy_text_and_fragments_merge_in_one_section(self):
        fragment, _ = bf.cc.parse_fragment("changelog.d/9.fixed.md", "- **Fragment fix.** f\n")
        assembled = bf.legacy_assemble(AFTER, [fragment], "1.0.1", "2026-05-01")
        self.assertIn("- **New fix (#11).** It wraps\n  over two lines.\n\n- **Fragment fix.** f", assembled)

    def test_bad_legacy_text_and_bad_arguments_are_errors(self):
        for text in (
            "# C\n\n## [Unreleased]\n\nloose prose\n",
            "# C\n\n## [Unreleased]\n\n- bullet outside a heading\n",
            "# C\n\n## [0.9.0] - 2025-01-01\n",
        ):
            with self.assertRaises(cf.AssembleError, msg=text):
                bf.legacy_assemble(text, [], "1.0.1", "2026-05-01")
        for version, date in (("v1.0.1", "2026-05-01"), ("1.0", "2026-05-01"), ("1.0.1", "2026-13-01")):
            with self.assertRaises(cf.AssembleError):
                bf.legacy_assemble(AFTER, [], version, date)

    def test_the_release_path_cannot_reach_the_legacy_merge(self):
        self.assertFalse(hasattr(cf, "legacy_assemble"))
        self.assertFalse(hasattr(cf, "legacy_groups"))
        self.assertEqual(list(inspect.signature(cf.assemble).parameters), ["text", "fragments", "version", "date"])
        self.assertNotIn("legacy_assemble", inspect.getsource(cf))
        out = io.StringIO()
        with contextlib.redirect_stdout(out), self.assertRaises(SystemExit):
            cf.main(["assemble", "--help"])
        self.assertNotIn("legacy", out.getvalue().lower())
        for relative in (".github/workflows/auto-release.yaml", "scripts/ci/changelog_fragments.py", "scripts/ci/release_notes.py"):
            text = pathlib.Path(self.ROOT, relative).read_text(encoding="utf-8")
            self.assertNotIn("release_notes_backfill", text, relative)
            self.assertNotIn("legacy_assemble", text, relative)

    def test_the_planner_handles_a_tag_with_legacy_text(self):
        entry = bf.plan_release(two_tags(), rel())
        self.assertEqual(entry["status"], bf.EDIT)
        self.assertIn("**New feature.**", entry["new_body"])
        self.assertNotIn("Old fix", entry["new_body"])

    def test_the_planner_handles_a_tag_with_fragments_and_an_empty_unreleased(self):
        empty = "# Changelog\n\n## [Unreleased]\n\n## [0.9.0] - 2025-12-01\n"
        repo = FakeRepo(
            {
                "v1.0.1": {
                    "changelog": empty,
                    "fragments": {"changelog.d/2.added.md": "- **Frag.** a\n", "changelog.d/1.fixed.md": "- **Fix.** b\n"},
                }
            }
        )
        entry = bf.plan_release(repo, rel())
        self.assertEqual(entry["status"], bf.EDIT)
        self.assertLess(entry["new_body"].index("* FEATURE: **Frag.**"), entry["new_body"].index("* BUGFIX: **Fix.**"))

    def test_the_planner_handles_legacy_text_next_to_fragments(self):
        repo = FakeRepo({"v1.0.1": {"changelog": AFTER, "fragments": {"changelog.d/1.fixed.md": "- **Frag fix.** b\n"}}})
        entry = bf.plan_release(repo, rel())
        self.assertEqual(entry["status"], bf.EDIT)
        for lead_in in ("**New fix", "**New feature.**", "**Frag fix.**"):
            self.assertIn(lead_in, entry["new_body"])


class ReleasedEntries(unittest.TestCase):
    def render(self, text, version="Unreleased"):
        return bf.rn.render(text, version, "2026-05-01")[0]

    def test_nothing_released_changes_nothing(self):
        notes = self.render(AFTER)
        self.assertEqual(bf.without_released(notes, set()), notes)

    def test_entries_with_nested_items_and_paragraphs_are_kept_whole(self):
        text = "# C\n\n## [Unreleased]\n\n### Fixed\n\n- **A.** one\n  - nested\n\n  second paragraph\n- **B.** two\n"
        notes = self.render(text)
        self.assertEqual(bf.without_released(notes, set()), notes)
        _, items = bf.notes_items(notes)
        self.assertEqual(len(items), 2)
        self.assertIn("  - nested", items[0][1])
        self.assertIn("  second paragraph", items[0][1])
        dropped = bf.without_released(notes, {items[1][1]})
        self.assertIn("**A.** one", dropped)
        self.assertNotIn("**B.**", dropped)

    def test_dropping_everything_leaves_no_entry(self):
        notes = self.render(AFTER)
        released = {text for _, text in bf.notes_items(notes)[1]}
        left = bf.without_released(notes, released)
        self.assertEqual(left, "Released at 2026-05-01\n\nNo user-visible changes.\n")
        with self.assertRaises(bf.NoEntry):
            bf.check_entry(left)

    def test_prose_and_update_notes_before_the_entries_stay(self):
        text = "# C\n\n## [Unreleased]\n\nSome prose.\n\n### Update notes\n\n- change the flag\n\n### Added\n\n- **A.** a\n- **B.** b\n"
        notes = self.render(text)
        released = {text for _, text in bf.notes_items(notes)[1] if "**A.**" in text}
        left = bf.without_released(notes, released)
        self.assertIn("Some prose.", left)
        self.assertIn("**Update note:** change the flag", left)
        self.assertNotIn("**A.**", left)
        self.assertIn("* FEATURE: **B.** b", left)

    def test_groups_keep_the_upstream_order(self):
        text = "# C\n\n## [Unreleased]\n\n### Fixed\n\n- **F.** f\n\n### Security\n\n- **S.** s\n\n### Added\n\n- **A.** a\n"
        left = bf.without_released(self.render(text), set())
        self.assertEqual([l.split(":")[0] for l in left.split("\n") if l.startswith("* ")], ["* SECURITY", "* FEATURE", "* BUGFIX"])

    def test_count_entries(self):
        self.assertEqual(bf.count_entries(AFTER, "0.9.0"), 1)
        self.assertEqual(bf.count_entries(AFTER, "9.9.9"), 0)
        self.assertEqual(bf.count_entries(AFTER, "Unreleased"), 3)

    def test_unreleased_items_of_a_changelog_without_the_section(self):
        self.assertEqual(bf.unreleased_items("# C\n\n## [0.9.0] - 2025-01-01\n"), set())
        self.assertEqual(len(bf.unreleased_items(AFTER)), 3)


class Report(unittest.TestCase):
    def test_samples_are_spread_and_distinct(self):
        plan = []
        for tag in ("v0.15.0", "v0.121.0", "v0.121.1", "v0.143.5", "v0.156.2", "v0.157.6", "v0.167.2"):
            notes = "Released at 2026-01-01\n\n* FEATURE: " + "x" * 300 + "\n"
            status = bf.EDIT if tag != "v0.167.2" else bf.NEW_STYLE
            plan.append({"tag": tag, "status": status, "notes": notes, "new_body": notes + ARTIFACTS})
        tags = [e["tag"] for e in bf.pick_samples(plan)]
        self.assertEqual(tags, ["v0.15.0", "v0.121.0", "v0.143.5", "v0.156.2", "v0.157.6"])
        self.assertEqual(bf.pick_samples([]), [])


if __name__ == "__main__":
    unittest.main()
