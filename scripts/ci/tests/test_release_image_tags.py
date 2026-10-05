"""Execute the actual release publisher with Docker mocked; no registry writes."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[3]


def publisher_script():
    workflow = (ROOT / ".github/workflows/auto-release.yaml").read_text()
    step = workflow.split("      - name: Build and push Docker images\n", 1)[1]
    run = step.split("        run: |\n", 1)[1].split("\n      - name:", 1)[0]
    script = textwrap.dedent(run)
    for expression, value in {
        "steps.version.outputs.next": "v9.8.7",
        "github.repository": "ExampleOrg/victoria-lakehouse",
        "secrets.GITHUB_TOKEN": "test-token",
        "github.actor": "test-actor",
    }.items():
        script = script.replace("${{ " + expression + " }}", value)
    if "${{" in script:
        raise AssertionError("unsubstituted workflow expression")
    return script


class PublisherTests(unittest.TestCase):
    def run_publisher(self, hub=False, fail=False):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            docker = temporary / "docker"
            docker.write_text("#!/usr/bin/env python3\n"
                              "import json, os, sys\n"
                              "with open(os.environ['CALLS'], 'a') as f:\n"
                              "    f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                              "if sys.argv[1:3] == ['buildx', 'build'] and os.environ['FAIL_BUILD'] == '1':\n"
                              "    sys.exit(19)\n")
            docker.chmod(0o755)
            calls = temporary / "calls.jsonl"
            environment = dict(os.environ, PATH=f"{temporary}:{os.environ['PATH']}",
                               CALLS=str(calls), FAIL_BUILD=str(int(fail)),
                               DOCKERHUB_ENABLED=str(hub).lower(), DOCKERHUB_USERNAME="publisher",
                               DOCKERHUB_NAMESPACE="customhub", VL_VERSION_LOGS="vl",
                               VL_COMMIT_TRACES="vlpin", VT_VERSION="vt")
            result = subprocess.run(["bash", "-euo", "pipefail", "-c", publisher_script()],
                                    cwd=ROOT, env=environment, capture_output=True, text=True)
            records = [json.loads(line) for line in calls.read_text().splitlines()]
            return result, [call for call in records if call[:2] == ["buildx", "build"]]

    def assert_publish(self, hub):
        result, builds = self.run_publisher(hub=hub)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(builds), 4, "one build per module/variant, no alias rebuilds")
        for arguments, (module, suffix, fips) in zip(builds, [
            ("logs", "", "0"), ("logs", "-fips", "1"),
            ("traces", "", "0"), ("traces", "-fips", "1"),
        ]):
            tags = [arguments[index + 1] for index, value in enumerate(arguments) if value == "-t"]
            expected = [f"ghcr.io/{repository}/lakehouse-{module}:{tag}{suffix}"
                        for repository in ["exampleorg/victoria-lakehouse", "exampleorg"]
                        for tag in ["v9.8.7", "9.8.7", "latest"]]
            if hub:
                expected += [f"docker.io/customhub/lakehouse-{module}:{tag}{suffix}"
                             for tag in ["v9.8.7", "latest"]]
            self.assertCountEqual(tags, expected)
            self.assertEqual(len(tags), len(set(tags)))
            self.assertEqual(arguments[arguments.index("--platform") + 1], "linux/amd64,linux/arm64")
            self.assertEqual(arguments[arguments.index("-f") + 1], f"Dockerfile.{module}")
            self.assertIn(f"FIPS={fips}", arguments)
            self.assertIn("VERSION=v9.8.7", arguments)
            self.assertIn("type=image,compression=zstd,compression-level=3,oci-mediatypes=true,push=true", arguments)

    def test_ghcr_only(self):
        self.assert_publish(False)

    def test_dockerhub_mirror_unchanged(self):
        self.assert_publish(True)

    def test_build_failure_propagates(self):
        result, builds = self.run_publisher(fail=True)
        self.assertEqual(result.returncode, 19)
        self.assertEqual(len(builds), 1)


if __name__ == "__main__":
    unittest.main()
