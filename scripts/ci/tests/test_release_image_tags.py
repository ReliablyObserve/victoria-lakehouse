"""Execute the actual release image push script with skopeo mocked; no registry writes."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/ci/release_push_images.sh"
ARCHIVES = ["lakehouse-logs.tar", "lakehouse-logs-fips.tar", "lakehouse-traces.tar", "lakehouse-traces-fips.tar"]


class PublisherTests(unittest.TestCase):
    def run_publisher(self, hub=False, fail=False, username="publisher", namespace="customhub",
                      version="v9.8.7", archives=ARCHIVES, extra=None):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            images = temporary / "images"
            images.mkdir()
            for name in archives:
                (images / name).write_bytes(b"archive")
            skopeo = temporary / "skopeo"
            skopeo.write_text("#!/usr/bin/env python3\n"
                              "import json, os, sys\n"
                              "with open(os.environ['CALLS'], 'a') as f:\n"
                              "    f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                              "if os.environ['FAIL_COPY'] == '1':\n"
                              "    sys.exit(19)\n")
            skopeo.chmod(0o755)
            calls = temporary / "calls.jsonl"
            environment = dict(os.environ, PATH=f"{temporary}:{os.environ['PATH']}",
                               CALLS=str(calls), FAIL_COPY=str(int(fail)),
                               VERSION=version, GITHUB_REPOSITORY="ExampleOrg/victoria-lakehouse",
                               IMAGES_DIR=str(images),
                               DOCKERHUB_ENABLED=str(hub).lower(), DOCKERHUB_USERNAME=username,
                               DOCKERHUB_NAMESPACE=namespace)
            environment.update(extra or {})
            result = subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, env=environment, capture_output=True, text=True)
            records = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
            return result, records

    def assert_publish(self, hub, username="publisher", namespace="customhub"):
        result, copies = self.run_publisher(hub=hub, username=username, namespace=namespace)
        self.assertEqual(result.returncode, 0, result.stderr)
        by_source = {}
        for arguments in copies:
            self.assertEqual(arguments[0], "copy")
            for flag in ("--all", "--retry-times"):
                self.assertIn(flag, arguments)
            source, destination = arguments[-2:]
            by_source.setdefault(source, []).append(destination)
        self.assertEqual(len(by_source), 4, "one archive per module/variant, no alias rebuilds")
        for source, (module, suffix) in zip(by_source, [
            ("logs", ""), ("logs", "-fips"), ("traces", ""), ("traces", "-fips"),
        ]):
            self.assertTrue(source.startswith("oci-archive:") and source.endswith(f"lakehouse-{module}{suffix}.tar"), source)
            expected = [f"docker://ghcr.io/{repository}/lakehouse-{module}:{tag}{suffix}"
                        for repository in ["exampleorg/victoria-lakehouse", "exampleorg"]
                        for tag in ["v9.8.7", "9.8.7", "latest"]]
            if hub and username:
                owner = namespace or "reliablyobserve"
                expected += [f"docker://docker.io/{owner}/lakehouse-{module}:{tag}{suffix}"
                             for tag in ["v9.8.7", "latest"]]
            self.assertCountEqual(by_source[source], expected)
            self.assertEqual(len(by_source[source]), len(set(by_source[source])))

    def test_ghcr_only(self):
        self.assert_publish(False)

    def test_dockerhub_mirror_unchanged(self):
        self.assert_publish(True)

    def test_dockerhub_default_namespace_unchanged(self):
        self.assert_publish(True, namespace="")

    def test_dockerhub_without_username_stays_disabled(self):
        self.assert_publish(True, username="")

    def test_copy_failure_propagates(self):
        result, copies = self.run_publisher(fail=True)
        self.assertEqual(result.returncode, 19)
        self.assertEqual(len(copies), 1)

    def test_missing_archive_stops_before_any_push(self):
        result, copies = self.run_publisher(archives=ARCHIVES[:3])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("lakehouse-traces-fips.tar not found", result.stderr)
        self.assertEqual(len(copies), 18, "the first three archives are pushed in order, the fourth is missing")

    def test_registry_host_and_flags_come_from_the_environment(self):
        result, copies = self.run_publisher(extra={"GHCR_HOST": "localhost:5000",
                                                    "SKOPEO_DEST_FLAGS": "--dest-tls-verify=false"})
        self.assertEqual(result.returncode, 0, result.stderr)
        for arguments in copies:
            self.assertIn("--dest-tls-verify=false", arguments)
            self.assertTrue(arguments[-1].startswith("docker://localhost:5000/"), arguments[-1])

    def test_hostile_versions_are_refused_before_any_push(self):
        for version in ["", "v1.2.3; touch x", "v1.2.3\nv1.2.4", "$(id)", "v1.2", "../v1.2.3", "v1.2.3-rc1",
                        "v01.2.3", "v1.2.3`id`", "v1.2.3 ", "v１.2.3", "v9999999999.0.0"]:
            with self.subTest(version=version):
                result, copies = self.run_publisher(version=version)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(copies, [])


class WorkflowWiring(unittest.TestCase):
    """The build job exports archives and pushes nothing; the publish job copies them."""

    def setUp(self):
        self.workflow = yaml.safe_load((ROOT / ".github/workflows/auto-release.yaml").read_text())

    def run_text(self, job, name):
        for step in self.workflow["jobs"][job]["steps"]:
            if step.get("name") == name:
                return step["run"]
        self.fail(f"step {name!r} not found in job {job}")

    def test_build_exports_oci_archives_and_never_pushes(self):
        script = self.run_text("build", "Build images")
        self.assertIn("buildx build", script)
        self.assertIn("type=oci,dest=", script)
        self.assertIn("compression=zstd,compression-level=3,oci-mediatypes=true", script)
        self.assertNotIn("push=true", script)
        self.assertNotIn("--push", script)
        self.assertNotIn("docker login", script)
        self.assertEqual(script.count("--platform linux/amd64,linux/arm64"), 1)
        for argument in ("-f Dockerfile.${MODULE}", "--build-arg VERSION=${VERSION}", "--build-arg FIPS=${FIPS_ARG}",
                         "--build-arg VL_VERSION=${VL_VERSION_LOGS}", "--build-arg VL_COMMIT=${VL_COMMIT_TRACES}",
                         "--build-arg VT_VERSION=${VT_VERSION}"):
            self.assertIn(argument, script)

    def test_publish_runs_the_push_script_on_the_downloaded_archives(self):
        self.assertEqual(self.run_text("publish", "Push Docker images").strip(), "bash scripts/ci/release_push_images.sh")
        job = self.workflow["jobs"]["publish"]
        self.assertEqual(job["permissions"], {"contents": "write", "packages": "write"})

    def test_release_binaries_are_built_with_trimpath_and_no_vcs_stamp(self):
        script = self.run_text("build", "Build binaries")
        self.assertEqual(script.count("go build -trimpath -buildvcs=false -ldflags"), 2)
        self.assertIn("lib/buildinfo.Version=${VERSION}", script)


if __name__ == "__main__":
    unittest.main()
