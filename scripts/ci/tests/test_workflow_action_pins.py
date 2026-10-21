"""Static check: every external action reference is pinned to a full commit SHA.

Scans .github/workflows/*.y*ml and every YAML file under .github/actions. A `uses:`
reference must be

    uses: owner/repo[/path]@<40 hex> # v1.2.3

Local actions (`./...`) and reusable workflows of this repository need no pin. A
`docker://` reference must carry an `@sha256:<64 hex>` digest. The trailing comment must
name the version the SHA stands for, so a reader (and Dependabot) can tell what is pinned.

Run: python3 -I -m unittest discover -s scripts/ci/tests -p test_workflow_action_pins.py -v
"""

import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
OWN_REPO = "reliablyobserve/victoria-lakehouse"

USES = re.compile(r"^\s*(?:-\s+)?uses:\s*(?P<ref>\S+)(?P<rest>.*)$")
SHA_REF = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_./-]+)?@[0-9a-f]{40}$")
DOCKER_REF = re.compile(r"^docker://\S+@sha256:[0-9a-f]{64}$")
VERSION_COMMENT = re.compile(r"^\s*#.*\bv\d+(?:\.\d+)*")


def workflow_files(root: pathlib.Path):
    wf = root / ".github" / "workflows"
    files = sorted(list(wf.glob("*.yaml")) + list(wf.glob("*.yml")))
    actions = root / ".github" / "actions"
    if actions.is_dir():
        files += sorted(list(actions.rglob("*.yaml")) + list(actions.rglob("*.yml")))
    return files


def check_text(name: str, text: str) -> list:
    """Return one message per violation in the YAML text of `name`."""
    problems = []
    for number, line in enumerate(text.splitlines(), 1):
        match = USES.match(line)
        if not match:
            continue
        ref = match.group("ref").strip("'\"")
        where = f"{name}:{number}"
        if ref.startswith("./"):
            continue
        if ref.lower().startswith(OWN_REPO + "/"):
            continue
        if ref.startswith("docker://"):
            if not DOCKER_REF.match(ref):
                problems.append(f"{where}: {ref} is not pinned to an image digest")
            continue
        if not SHA_REF.match(ref):
            problems.append(f"{where}: {ref} is not pinned to a 40-character commit SHA")
            continue
        if not VERSION_COMMENT.match(match.group("rest")):
            problems.append(f"{where}: {ref} has no trailing `# vX.Y.Z` version comment")
    return problems


def scan(root: pathlib.Path) -> list:
    problems = []
    for path in workflow_files(root):
        problems += check_text(str(path.relative_to(root)), path.read_text())
    return problems


class CheckTextTest(unittest.TestCase):
    """The checker itself, on fixtures (fails on an unpinned reference)."""

    SHA = "3d3c42e5aac5ba805825da76410c181273ba90b1"

    def test_tag_is_rejected(self):
        self.assertEqual(len(check_text("w.yaml", "      - uses: actions/checkout@v7\n")), 1)

    def test_branch_is_rejected(self):
        self.assertEqual(len(check_text("w.yaml", "        uses: aquasecurity/trivy-action@master\n")), 1)

    def test_short_sha_is_rejected(self):
        self.assertEqual(
            len(check_text("w.yaml", "      - uses: actions/checkout@3d3c42e # v7.0.1\n")), 1
        )

    def test_uppercase_sha_is_rejected(self):
        line = f"      - uses: actions/checkout@{self.SHA.upper()} # v7.0.1\n"
        self.assertEqual(len(check_text("w.yaml", line)), 1)

    def test_pinned_with_version_comment_passes(self):
        line = f"      - uses: actions/checkout@{self.SHA} # v7.0.1\n"
        self.assertEqual(check_text("w.yaml", line), [])

    def test_pinned_subpath_action_passes(self):
        line = f"        uses: github/codeql-action/init@{self.SHA} # v4.39.0\n"
        self.assertEqual(check_text("w.yaml", line), [])

    def test_pinned_without_version_comment_is_rejected(self):
        self.assertEqual(len(check_text("w.yaml", f"      - uses: actions/checkout@{self.SHA}\n")), 1)
        self.assertEqual(
            len(check_text("w.yaml", f"      - uses: actions/checkout@{self.SHA} # latest\n")), 1
        )

    def test_quoted_reference_is_checked(self):
        self.assertEqual(len(check_text("w.yaml", "      - uses: 'actions/checkout@v7'\n")), 1)

    def test_local_action_and_own_reusable_workflow_need_no_pin(self):
        text = (
            "      - uses: ./.github/actions/thing\n"
            "    uses: ReliablyObserve/victoria-lakehouse/.github/workflows/build.yaml@main\n"
        )
        self.assertEqual(check_text("w.yaml", text), [])

    def test_other_repo_reusable_workflow_is_checked(self):
        line = "    uses: other/repo/.github/workflows/build.yaml@main\n"
        self.assertEqual(len(check_text("w.yaml", line)), 1)

    def test_docker_reference_needs_a_digest(self):
        self.assertEqual(len(check_text("w.yaml", "      - uses: docker://alpine:3.20\n")), 1)
        digest = "sha256:" + "a" * 64
        self.assertEqual(check_text("w.yaml", f"      - uses: docker://alpine@{digest} # v3.20\n"), [])

    def test_text_that_is_not_a_uses_key_is_ignored(self):
        text = '        run: echo "uses: foo/bar@v1"\n      - name: uses\n'
        self.assertEqual(check_text("w.yaml", text), [])


class RepositoryPinsTest(unittest.TestCase):
    def test_workflow_files_are_found(self):
        self.assertTrue(workflow_files(ROOT), "no workflow files found under .github/workflows")

    def test_every_action_reference_is_pinned_with_a_version_comment(self):
        problems = scan(ROOT)
        self.assertEqual(problems, [], "\n" + "\n".join(problems))


if __name__ == "__main__":
    unittest.main()
