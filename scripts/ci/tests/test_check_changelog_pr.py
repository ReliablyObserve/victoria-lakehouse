import unittest

from scripts.ci.check_changelog_pr import (
    extract_unreleased_section,
    has_genuinely_new_unreleased_entries,
    has_meaningful_changelog_content,
    has_new_versioned_entries,
    is_dependency_only_pr,
    is_release_commit,
    is_release_metadata_sync,
    should_require_changelog,
    versioned_bullets,
)


class CheckChangelogPRTests(unittest.TestCase):
    def test_extract_unreleased_section(self):
        text = """# Changelog

## [Unreleased]

### Features

- add thing

## [0.1.0] - 2026-01-01
"""
        self.assertEqual(
            extract_unreleased_section(text),
            "### Features\n\n- add thing",
        )

    def test_has_meaningful_changelog_content(self):
        self.assertFalse(has_meaningful_changelog_content(""))
        self.assertFalse(has_meaningful_changelog_content("### Features"))
        self.assertTrue(has_meaningful_changelog_content("### Features\n\n- add thing"))

    def test_should_require_changelog_for_release_commits(self):
        self.assertTrue(should_require_changelog(["feat: add delete mode"], ["docs/readme.md"]))
        self.assertTrue(should_require_changelog(["fix(delete): handle Glacier files"], ["README.md"]))

    def test_should_require_changelog_for_impactful_paths(self):
        self.assertTrue(should_require_changelog(["test: add coverage"], ["internal/delete/handler.go"]))
        self.assertTrue(should_require_changelog(["docs: mention thing"], ["go.mod"]))

    def test_should_skip_for_unit_test_only_changes(self):
        self.assertFalse(
            should_require_changelog(
                ["test: add coverage"],
                ["internal/delete/handler_test.go", "internal/storage/parquets3/storage_test.go"],
            )
        )

    def test_should_skip_for_docs_only(self):
        self.assertFalse(should_require_changelog(["docs: update guide"], ["docs/getting-started.md"]))

    def test_should_skip_for_github_workflow_only_changes(self):
        self.assertFalse(should_require_changelog(["ci: tune workflow"], [".github/workflows/ci.yaml"]))
        self.assertFalse(
            should_require_changelog(
                ["fix(ci): update release skip"], [".github/workflows/auto-release.yaml"]
            )
        )

    def test_should_require_for_deployment_changes(self):
        self.assertTrue(should_require_changelog(["feat: update compose"], ["deployment/docker/docker-compose-e2e.yml"]))

    def test_should_skip_for_changelog_gate_policy_only_changes(self):
        self.assertFalse(
            should_require_changelog(
                ["test: refine changelog gate"],
                [
                    "scripts/ci/check_changelog_pr.py",
                    "scripts/ci/tests/test_check_changelog_pr.py",
                ],
            )
        )

    def test_is_release_commit_ignores_ci_scope(self):
        self.assertFalse(is_release_commit("feat(ci): consolidate post-release updates"))
        self.assertFalse(is_release_commit("fix(ci): exempt paths from changelog"))
        self.assertFalse(is_release_commit("fix(build): update makefile"))
        self.assertFalse(is_release_commit("feat(deps): bump go version"))

    def test_is_release_commit_allows_code_scopes(self):
        self.assertTrue(is_release_commit("feat(delete): add cost-aware deletion"))
        self.assertTrue(is_release_commit("fix(storage): handle nil pointer"))
        self.assertTrue(is_release_commit("feat: add new feature"))
        self.assertTrue(is_release_commit("fix: correct bug"))

    def test_should_skip_for_ci_commits_with_readme(self):
        self.assertFalse(
            should_require_changelog(
                ["feat(ci): consolidate post-release updates", "fix(ci): exempt paths"],
                [".github/workflows/auto-release.yaml", "README.md", "scripts/ci/check_changelog_pr.py"],
            )
        )

    def test_should_skip_for_website_only_changes(self):
        self.assertFalse(
            should_require_changelog(
                ["feat: update website"],
                ["website/src/pages/index.tsx", "website/docusaurus.config.ts"],
            )
        )

    def test_has_genuinely_new_unreleased_entries_detects_new(self):
        base_changelog = "## [0.12.0]\n\n- fix: old bug\n"
        head_unreleased = "- fix: brand new fix\n"
        self.assertTrue(has_genuinely_new_unreleased_entries(head_unreleased, base_changelog))

    def test_has_genuinely_new_unreleased_entries_rejects_stale_branch(self):
        base_changelog = (
            "## [Unreleased]\n\n"
            "## [0.12.0] - 2026-05-05\n\n"
            "- feat: cost-aware deletion\n"
            "- feat: tombstone persistence\n"
        )
        head_unreleased = (
            "### Added\n\n"
            "- feat: cost-aware deletion\n"
            "- feat: tombstone persistence\n"
        )
        self.assertFalse(has_genuinely_new_unreleased_entries(head_unreleased, base_changelog))

    def test_has_genuinely_new_unreleased_entries_mixed(self):
        base_changelog = (
            "## [0.12.0] - 2026-05-05\n\n"
            "- feat: old feature\n"
        )
        head_unreleased = (
            "- feat: old feature\n"
            "- feat: traces delete support\n"
        )
        self.assertTrue(has_genuinely_new_unreleased_entries(head_unreleased, base_changelog))

    def test_dependency_only_pr_go_modules(self):
        self.assertTrue(
            is_dependency_only_pr(
                ["build(deps): bump github.com/klauspost/compress from 1.18.5 to 1.18.6"],
                ["go.mod", "go.sum"],
            )
        )

    def test_dependency_only_pr_rejects_app_code(self):
        self.assertFalse(
            is_dependency_only_pr(
                ["build(deps): bump X"],
                ["go.mod", "go.sum", "internal/delete/handler.go"],
            )
        )

    def test_dependency_only_pr_rejects_empty(self):
        self.assertFalse(is_dependency_only_pr([], ["go.mod"]))
        self.assertFalse(is_dependency_only_pr(["build(deps): bump X"], []))

    def test_versioned_bullets_excludes_unreleased(self):
        text = (
            "## [Unreleased]\n\n"
            "- feat: only in unreleased\n\n"
            "## [0.39.0] - 2026-06-07\n\n"
            "### Fixed\n\n"
            "- fix: backfilled entry\n"
        )
        self.assertEqual(versioned_bullets(text), {"- fix: backfilled entry"})

    def test_has_new_versioned_entries_detects_backfill(self):
        base_changelog = (
            "## [Unreleased]\n\n"
            "## [0.49.0] - 2026-06-07\n\n"
            "- feat: lifecycle\n"
        )
        head_changelog = (
            "## [Unreleased]\n\n"
            "## [0.49.0] - 2026-06-07\n\n"
            "- feat: lifecycle\n\n"
            "## [0.39.0] - 2026-06-07\n\n"
            "- fix: cold jaeger partial-hit narrowing\n"
        )
        self.assertTrue(has_new_versioned_entries(head_changelog, base_changelog))

    def test_has_new_versioned_entries_rejects_no_new_bullets(self):
        base_changelog = (
            "## [0.49.0] - 2026-06-07\n\n"
            "- feat: lifecycle\n"
        )
        # head only reshuffles the same bullet under a renamed heading
        head_changelog = (
            "## [0.48.0] - 2026-06-06\n\n"
            "- feat: lifecycle\n"
        )
        self.assertFalse(has_new_versioned_entries(head_changelog, base_changelog))

    def test_has_new_versioned_entries_ignores_unreleased_only_additions(self):
        base_changelog = "## [Unreleased]\n\n## [0.49.0] - 2026-06-07\n\n- feat: lifecycle\n"
        # the only new bullet lives under [Unreleased]; the versioned path must
        # not claim it (that is the Unreleased path's job)
        head_changelog = (
            "## [Unreleased]\n\n"
            "- fix: brand new\n\n"
            "## [0.49.0] - 2026-06-07\n\n"
            "- feat: lifecycle\n"
        )
        self.assertFalse(has_new_versioned_entries(head_changelog, base_changelog))

    def test_release_metadata_sync_detection(self):
        self.assertTrue(
            is_release_metadata_sync(
                ["CHANGELOG.md", "README.md", "charts/victoria-lakehouse/Chart.yaml"]
            )
        )
        self.assertFalse(is_release_metadata_sync(["README.md"]))
        self.assertFalse(is_release_metadata_sync(["CHANGELOG.md", "internal/delete/handler.go"]))


if __name__ == "__main__":
    unittest.main()
