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
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@e", "-c", "commit.gpgsign=false", *args],
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


if __name__ == "__main__":
    unittest.main()
