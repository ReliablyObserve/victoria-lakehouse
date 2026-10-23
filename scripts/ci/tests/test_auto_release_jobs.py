"""Static and executed checks of the job split of .github/workflows/auto-release.yaml.

The release is four jobs (decide, build, publish, metadata) plus a read-only dry run. These
tests keep the split from eroding:

* every job states its permissions, and the workflow grants none by default;
* a job that can write (a write permission or any secret) runs no build tool and uses only
  the two actions that move data (checkout, download-artifact);
* only `publish` holds `packages: write`, only `metadata` reads TOKEN_GITHUB and it runs in
  the `release` environment, and `build` has no write permission, no secret and no stored credential;
* no `${{ }}` expression appears inside a `run:` script (values reach scripts through `env:`);
* the version that every job turns into tag, image and file names is validated, and hostile
  tags, versions and pull-request inputs are refused before anything is written;
* a dry run cannot publish: publish and metadata require a real run on main.

Run: python3 -I -m unittest discover -s scripts/ci/tests -p test_auto_release_jobs.py -v
"""

import json
import os
import pathlib
import re
import stat
import subprocess
import tempfile
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github" / "workflows" / "auto-release.yaml"
REQUIRED_CHECKS = ROOT / "scripts" / "ci" / "tests" / "testdata" / "required_checks.txt"

TEXT = WORKFLOW.read_text()
DOC = yaml.safe_load(TEXT)
JOBS = DOC["jobs"]

# Build tools: a job that can write must never run one (they execute repository, dependency
# and upstream code).
BUILD_TOOL = re.compile(
    r"(?<![\w./$-])(go|make|npm|npx|pip3?|yarn|pnpm|cargo|python3?\s+-m\s+pip|docker\s+(?:buildx\s+)?build)(?![\w./-])"
)
DATA_ACTIONS = {"actions/checkout", "actions/download-artifact"}


def is_sensitive(job: dict) -> bool:
    """A job that can write through GITHUB_TOKEN or can read any secret."""
    permissions = job.get("permissions") or {}
    writes = any(value == "write" for value in permissions.values())
    return writes or "secrets." in json.dumps(job)


def run_scripts(job: dict):
    for step in job["steps"]:
        if "run" in step:
            yield step.get("name", step.get("id", "?")), step["run"]


def code_lines(script: str):
    for line in script.split("\n"):
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            yield line


def step_run(job: str, name: str) -> str:
    for step in JOBS[job]["steps"]:
        if step.get("name") == name:
            return step["run"]
    raise AssertionError(f"step {name!r} not found in job {job!r}")


class JobShape(unittest.TestCase):
    def test_the_split_is_five_jobs(self):
        self.assertEqual(list(JOBS), ["decide", "build", "publish", "metadata", "dry-run"])

    def test_workflow_grants_nothing_by_default_and_every_job_states_its_permissions(self):
        self.assertEqual(DOC["permissions"], {})
        for name, job in JOBS.items():
            self.assertIsInstance(job.get("permissions"), dict, f"{name} must declare permissions")
            self.assertTrue(job["permissions"], f"{name} has an empty permissions block")

    def test_only_publish_and_metadata_can_write(self):
        writers = {n for n, j in JOBS.items() if any(v == "write" for v in j["permissions"].values())}
        self.assertEqual(writers, {"publish", "metadata"})
        self.assertEqual(JOBS["publish"]["permissions"], {"contents": "write", "packages": "write"})
        self.assertEqual(JOBS["metadata"]["permissions"], {"contents": "write", "pull-requests": "write"})
        packages = {n for n, j in JOBS.items() if j["permissions"].get("packages") == "write"}
        self.assertEqual(packages, {"publish"})
        for name in ("decide", "build", "dry-run"):
            self.assertTrue(all(v == "read" for v in JOBS[name]["permissions"].values()), name)
        self.assertEqual(JOBS["build"]["permissions"], {"contents": "read"})

    def test_build_has_no_secret_no_environment_and_no_stored_credential(self):
        build = JOBS["build"]
        self.assertNotIn("secrets.", json.dumps(build))
        self.assertNotIn("environment", build)
        for step in build["steps"]:
            if step.get("uses", "").startswith("actions/checkout@"):
                self.assertIs(step["with"].get("persist-credentials"), False)

    def test_only_metadata_reads_the_personal_token_and_runs_in_the_release_environment(self):
        for name, job in JOBS.items():
            has = "TOKEN_GITHUB" in json.dumps(job)
            self.assertEqual(has, name == "metadata", name)
        self.assertEqual(JOBS["metadata"]["environment"], "release")
        for name in ("decide", "build", "publish", "dry-run"):
            self.assertNotIn("environment", JOBS[name], name)
        # Only metadata may keep a pushed credential.
        for name, job in JOBS.items():
            for step in job["steps"]:
                if step.get("uses", "").startswith("actions/checkout@"):
                    persists = step["with"].get("persist-credentials")
                    self.assertIs(persists, name == "metadata", f"{name}: persist-credentials")

    def test_jobs_that_can_write_run_no_build_tool(self):
        sensitive = [n for n, j in JOBS.items() if is_sensitive(j)]
        self.assertTrue({"decide", "publish", "metadata"} <= set(sensitive), sensitive)
        for name in sensitive:
            for step_name, script in run_scripts(JOBS[name]):
                for line in code_lines(script):
                    self.assertIsNone(BUILD_TOOL.search(line), f"{name}/{step_name}: {line.strip()}")

    def test_jobs_that_can_write_use_only_data_actions_and_checked_in_scripts(self):
        for name, job in JOBS.items():
            if not is_sensitive(job):
                continue
            for step in job["steps"]:
                if "uses" in step:
                    self.assertIn(step["uses"].split("@")[0], DATA_ACTIONS, f"{name}: {step['uses']}")
            for step_name, script in run_scripts(job):
                for line in code_lines(script):
                    for interpreter in re.findall(r"(?:^|[;&|(]\s*|\s)(python3?|bash|sh)\s+(\S+)", line):
                        if interpreter[1].startswith("-") or interpreter[1] in ('"$', "-c"):
                            continue
                        self.assertTrue(interpreter[1].startswith("scripts/ci/"),
                                        f"{name}/{step_name}: runs {interpreter[1]}")

    def test_no_expression_inside_any_run_script(self):
        for name, job in JOBS.items():
            for step_name, script in run_scripts(job):
                self.assertNotIn("${{", script, f"{name}/{step_name}")

    def test_every_job_name_is_distinct_from_the_required_checks(self):
        required = {l.strip() for l in REQUIRED_CHECKS.read_text().splitlines() if l.strip() and not l.startswith("#")}
        for name, job in JOBS.items():
            self.assertNotIn(job.get("name", name), required)
            self.assertNotIn(name, required)

    def test_release_jobs_follow_the_decision(self):
        self.assertEqual(JOBS["build"]["needs"], "decide")
        self.assertIn("needs.decide.outputs.release == 'true'", JOBS["build"]["if"])
        for name in ("publish", "metadata"):
            condition = JOBS[name]["if"]
            self.assertIn("github.ref == 'refs/heads/main'", condition)
            self.assertIn("needs.decide.outputs.release == 'true'", condition)
            self.assertIn("needs.decide.outputs.dry_run == 'false'", condition)
        self.assertEqual(JOBS["publish"]["needs"], ["decide", "build"])
        self.assertEqual(JOBS["metadata"]["needs"], ["decide", "publish"])


class DryRun(unittest.TestCase):
    def test_the_input_is_a_boolean_that_defaults_to_false(self):
        dispatch = DOC[True]["workflow_dispatch"] if True in DOC else DOC["on"]["workflow_dispatch"]
        self.assertEqual(dispatch["inputs"]["dry_run"]["type"], "boolean")
        self.assertIs(dispatch["inputs"]["dry_run"]["default"], False)

    def test_a_dry_run_cannot_reach_publish_or_metadata(self):
        for name in ("publish", "metadata"):
            self.assertIn("needs.decide.outputs.dry_run == 'false'", JOBS[name]["if"])
        self.assertIn("needs.decide.outputs.dry_run == 'true'", JOBS["dry-run"]["if"])

    def test_the_dry_run_job_has_no_secret_no_environment_and_a_pinned_local_registry(self):
        job = JOBS["dry-run"]
        self.assertNotIn("secrets.", json.dumps(job))
        self.assertNotIn("environment", job)
        self.assertRegex(job["services"]["registry"]["image"], r"^registry:[\w.]+@sha256:[0-9a-f]{64}$")
        script = step_run("dry-run", "Verify the artifacts and rehearse the image push")
        self.assertIn("GHCR_HOST=localhost:5000", script)
        self.assertIn("DOCKERHUB_ENABLED=false", script)
        for forbidden in ("login", "gh release", "git push", "helm push", "docker push"):
            self.assertNotIn(forbidden, script)

    def test_a_dry_run_has_its_own_concurrency_group_and_only_decide_may_leave_main(self):
        self.assertIn("inputs.dry_run &&", DOC["concurrency"]["group"])
        self.assertIn("github.run_id", DOC["concurrency"]["group"])
        self.assertEqual(JOBS["decide"]["if"],
                         "github.ref == 'refs/heads/main' || (github.event_name == 'workflow_dispatch' && inputs.dry_run)")
        self.assertNotIn("if", JOBS["build"]["steps"][0])

    def test_a_dry_run_is_a_manual_run_and_the_range_check_never_skips_it(self):
        # the range check (release_product_check.py) returns "release" for workflow_dispatch before any
        # ref or tag test, so a dry run from any branch always builds; there is no separate skip step
        step = next(s for s in JOBS["decide"]["steps"] if s.get("id") == "skip")
        self.assertNotIn("if", step)
        self.assertEqual(step["env"]["EVENT_NAME"], "${{ github.event_name }}")
        self.assertNotIn("skip_release", [s.get("id") for s in JOBS["decide"]["steps"]])


TAG_RE = re.search(r"TAG_RE='([^']+)'", step_run("decide", "Determine version bump")).group(1)

HOSTILE = [
    "", "v1.2.3; touch x", "v1.2.3`touch x`", "v1.2.3$(touch x)", "v1.2.3\ntouch x", "v1.2.3\n", "\nv1.2.3",
    "../v1.2.3", "v1.2.3/../x", "v1.2.3 ", " v1.2.3", "v1.2", "v1.2.3.4", "v1.2.3-rc1", "v1.2.3+build",
    "v01.2.3", "v1.02.3", "v1.2.03", "V1.2.3", "1.2.3", "v１.２.３", "v1.2.٣", "v1.2.3​",
    "v9999999999.0.0", "v-1.2.3", "v1.2.3|id", "v1.2.3&&id", "v1.2.3>x", "v${IFS}.0.0", "v999$(echo${IFS}INJECTED>&2).0.0",
    "v1.2.3\x00", "v1.2.3%0a", "v1.2.3'", 'v1.2.3"',
]
VALID = ["v0.0.0", "v0.157.8", "v1.2.3", "v10.20.30", "v123456789.0.1"]


class VersionValidation(unittest.TestCase):
    def match(self, value: str) -> bool:
        # The shell's own matcher, exactly as the workflow uses it.
        result = subprocess.run(["bash", "-c", '[[ "$1" =~ $2 ]]', "x", value, TAG_RE],
                                capture_output=True, env=dict(os.environ, LC_ALL="C"))
        return result.returncode == 0

    def test_valid_versions_pass(self):
        for value in VALID:
            self.assertTrue(self.match(value), value)

    def test_hostile_tag_names_fail(self):
        for value in HOSTILE:
            if "\x00" in value:
                continue  # an environment variable cannot hold a NUL byte
            self.assertFalse(self.match(value), repr(value))

    def test_every_job_that_reads_the_version_checks_it_with_the_same_pattern(self):
        for name in ("build", "publish", "metadata", "dry-run"):
            script = step_run(name, "Check the version")
            self.assertIn(r"^v(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})$", script, name)
        self.assertIn(r"^v(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})$", TAG_RE and
                      step_run("decide", "Publish the decision"))
        self.assertEqual(TAG_RE, r"^v(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})$")

    def test_the_checking_steps_refuse_hostile_versions(self):
        for name in ("build", "publish", "metadata", "dry-run"):
            script = step_run(name, "Check the version")
            for value in HOSTILE:
                if "\x00" in value:
                    continue
                with self.subTest(job=name, version=value):
                    marker = pathlib.Path(tempfile.mkdtemp()) / "x"
                    result = subprocess.run(["bash", "-e", "-c", script], capture_output=True, text=True,
                                            env=dict(os.environ, VERSION=value, LC_ALL="C"), cwd=marker.parent)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse(marker.exists())
            result = subprocess.run(["bash", "-e", "-c", script], capture_output=True, text=True,
                                    env=dict(os.environ, VERSION="v1.2.3"))
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_the_final_decision_step_refuses_a_hostile_version_and_writes_exact_booleans(self):
        script = step_run("decide", "Publish the decision")
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp) / "out"

            def run(skip_product="", nxt="v1.2.3", dry=""):
                out.write_text("")
                env = dict(os.environ, SKIP_PRODUCT=skip_product, NEXT=nxt,
                           DRY_RUN_INPUT=dry, GITHUB_OUTPUT=str(out))
                result = subprocess.run(["bash", "-e", "-c", script], capture_output=True, text=True, env=env, cwd=tmp)
                return result, dict(l.split("=", 1) for l in out.read_text().splitlines())

            result, got = run()
            self.assertEqual((result.returncode, got), (0, {"release": "true", "dry_run": "false", "next": "v1.2.3"}))
            result, got = run(dry="true")
            self.assertEqual(got["dry_run"], "true")
            for dry in ("True", "1", "yes", "false", " true"):
                self.assertEqual(run(dry=dry)[1]["dry_run"], "false", dry)
            result, got = run(skip_product="true", nxt="")
            self.assertEqual((result.returncode, got["release"]), (0, "false"))
            result, got = run(skip_product="true")
            self.assertEqual((result.returncode, got["release"]), (0, "false"))
            for hostile in HOSTILE:
                if "\x00" in hostile:
                    continue
                result, got = run(nxt=hostile)
                self.assertNotEqual(result.returncode, 0, repr(hostile))
                self.assertEqual(got, {})


class VersionStep(unittest.TestCase):
    """The decide job's version step, run against a scratch repository with hostile tags."""

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(self.tmp)]))
        self.repo = self.tmp / "repo"
        self.repo.mkdir()
        git = ["git", "-C", str(self.repo), "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"]
        isolated = dict(os.environ, GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1")
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True, env=isolated)
        subprocess.run(git + ["commit", "-q", "--allow-empty", "-m", "base"], check=True, env=isolated)
        self.git = git
        bindir = self.tmp / "bin"
        bindir.mkdir()
        gh = bindir / "gh"
        gh.write_text("#!/usr/bin/env bash\necho \"$*\" >> \"$GH_CALLS\"\n"
                      "case \"$*\" in *additions,deletions,files*) echo '0 0' ;; esac\n")
        gh.chmod(gh.stat().st_mode | stat.S_IEXEC)
        self.bindir = bindir
        self.script = self.tmp / "step.sh"
        self.script.write_text("set -e\n" + step_run("decide", "Determine version bump"))

    def tag(self, name):
        return subprocess.run(self.git + ["tag", name], check=True, capture_output=True,
                              env=dict(os.environ, GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1"))

    def run_step(self, event="workflow_dispatch", pr_input="", message="fix: x (#1)"):
        out = self.tmp / "github_output"
        out.write_text("")
        calls = self.tmp / "gh_calls"
        calls.write_text("")
        env = dict(os.environ, PATH=f"{self.bindir}{os.pathsep}{os.environ['PATH']}", COMMIT_MSG=message,
                   EVENT_NAME=event, PR_INPUT=pr_input, GITHUB_OUTPUT=str(out), GH_CALLS=str(calls),
                   GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1")
        result = subprocess.run(["bash", str(self.script)], cwd=self.repo, env=env, capture_output=True, text=True)
        values = dict(l.split("=", 1) for l in out.read_text().splitlines() if "=" in l)
        return result, values, calls.read_text()

    def test_tags_that_are_not_plain_versions_are_ignored_and_never_executed(self):
        for name in ["v0.1.0", "v0.2.0", "v9.9.9$(touch${IFS}pwned)", "v9.9.9;touch${IFS}pwned", "v9.9.9`touch${IFS}pwned`",
                     "v9.9.9-rc1", "v99999999999.0.0", "v1.0.0|touch", "v${IFS}9.0.0", "v999$(echo${IFS}INJECTED>&2).0.0"]:
            self.tag(name)
        result, values, _ = self.run_step()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((values["latest_tag"], values["next"]), ("v0.2.0", "v0.2.1"))
        self.assertFalse((self.repo / "pwned").exists())
        self.assertFalse((self.tmp / "pwned").exists())
        self.assertNotIn("INJECTED", result.stderr + result.stdout)
        self.assertIn("7 tag(s) starting with v are not vMAJOR.MINOR.PATCH and are ignored", result.stdout)
        # the names themselves are never printed (a name could carry a workflow command)
        self.assertNotIn("touch", result.stdout)
        self.assertNotIn("INJECTED", result.stdout)

    def test_without_valid_tags_the_first_version_is_computed(self):
        self.tag("v1.2.3;touch${IFS}pwned")
        result, values, _ = self.run_step()
        self.assertEqual((values["latest_tag"], values["next"]), ("v0.0.0", "v0.0.1"))

    def test_a_pr_input_that_is_not_a_number_is_refused_before_gh_runs(self):
        for hostile in ["1; touch pwned", "-R other/repo", "1 --repo other/repo", "$(touch pwned)", "`touch pwned`",
                        "12345678901", "abc", "1\n2", "１２", "../1", "0x1", "1.5"]:
            with self.subTest(pr=hostile):
                result, values, calls = self.run_step(pr_input=hostile)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(calls, "")
                self.assertEqual(values.get("next"), None)
                self.assertFalse((self.repo / "pwned").exists())

    def test_a_numeric_pr_input_reaches_gh_as_one_argument(self):
        result, values, calls = self.run_step(pr_input="123")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("pr view 123 --json", calls)
        self.assertEqual(values["next"], "v0.0.1")


class MetadataStep(unittest.TestCase):
    """The metadata job's pull-request step, run with a local origin and a fake gh."""

    FILES = {
        "CHANGELOG.md": "# changelog\n",
        "docs/features.md": "features\n",
        "README.md": "readme\n",
        "charts/victoria-lakehouse/Chart.yaml": "version: 1.2.3\n",
    }

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(self.tmp)]))
        self.origin = self.tmp / "origin.git"
        subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(self.origin)], check=True)
        self.work = self.tmp / "work"
        subprocess.run(["git", "clone", "-q", str(self.origin), str(self.work)], check=True, capture_output=True)
        self.env = dict(os.environ, GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1")
        for name, content in self.FILES.items():
            path = self.work / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("old\n")
        self.git("add", "-A")
        self.git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "base")
        self.git("push", "-q", "origin", "HEAD:main")
        bindir = self.tmp / "bin"
        bindir.mkdir()
        gh = bindir / "gh"
        gh.write_text("#!/usr/bin/env bash\necho \"$*\" >> \"$GH_CALLS\"\necho https://example.invalid/pull/1\n")
        gh.chmod(gh.stat().st_mode | stat.S_IEXEC)
        self.bindir = bindir
        self.runner = self.tmp / "runner"
        self.meta = self.runner / "release-meta"
        for name, content in self.FILES.items():
            path = self.meta / "files" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        (self.meta / "pr-body.md").write_text("body\n")
        (self.meta / "consumed.txt").write_text("")

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.work), *args], check=True, capture_output=True, text=True, env=self.env)

    def run_step(self):
        calls = self.tmp / "gh_calls"
        calls.write_text("")
        env = dict(self.env, PATH=f"{self.bindir}{os.pathsep}{os.environ['PATH']}", VERSION="v1.2.3",
                   RUNNER_TEMP=str(self.runner), GH_CALLS=str(calls), GH_TOKEN="x")
        # the list is checked by its own step, before the tag is pushed and the release published
        check = subprocess.run(["bash", "-e", "-c", step_run("metadata", "Check the released fragment list")],
                               cwd=self.work, env=env, capture_output=True, text=True)
        if check.returncode != 0:
            return check, calls.read_text()
        script = step_run("metadata", "Create release metadata PR")
        result = subprocess.run(["bash", "-e", "-c", script], cwd=self.work, env=env, capture_output=True, text=True)
        return result, calls.read_text()

    def pushed_branches(self):
        out = subprocess.run(["git", "-C", str(self.origin), "branch", "--list"], capture_output=True, text=True).stdout
        return [l.strip("* ").strip() for l in out.splitlines()]

    def test_the_four_files_become_one_commit_and_one_pull_request(self):
        result, calls = self.run_step()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("chore/release-metadata-v1.2.3", self.pushed_branches())
        changed = subprocess.run(["git", "-C", str(self.origin), "diff", "--name-only", "main", "chore/release-metadata-v1.2.3"],
                                 capture_output=True, text=True).stdout.split()
        self.assertCountEqual(changed, list(self.FILES))
        self.assertIn("pr create --title chore: release metadata for v1.2.3 [skip release] --body-file", calls)
        self.assertIn("--label release:patch", calls)

    def test_a_regeneration_failure_opens_the_pull_request_and_then_fails(self):
        (self.meta / "regen-failed").write_text("")
        result, calls = self.run_step()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("docs/features.md was NOT regenerated", result.stdout)
        self.assertIn("pr create", calls)
        self.assertIn("chore/release-metadata-v1.2.3", self.pushed_branches())

    def refused(self, mutate):
        mutate()
        result, calls = self.run_step()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(calls, "")
        self.assertEqual(self.pushed_branches(), ["main"])

    def test_an_extra_file_in_the_artifact_is_refused(self):
        self.refused(lambda: (self.meta / "files" / ".github" / "workflows").mkdir(parents=True) or
                     (self.meta / "files" / ".github" / "workflows" / "x.yaml").write_text("on: push\n"))

    def test_a_symlink_in_the_artifact_is_refused(self):
        def mutate():
            target = self.meta / "files" / "README.md"
            target.unlink()
            target.symlink_to("/etc/passwd")
        self.refused(mutate)

    def test_a_missing_file_is_refused(self):
        self.refused(lambda: (self.meta / "files" / "docs" / "features.md").unlink())

    def test_the_released_fragments_are_removed_in_the_same_pull_request(self):
        # two fragments on main; the release consumed the first, the second was merged after the release commit
        self.git("rm", "-q", "-r", "--cached", ".")
        for name in ("changelog.d/1.fixed.md", "changelog.d/2.added.md"):
            path = self.work / name
            path.parent.mkdir(exist_ok=True)
            path.write_text("- **F.** t\n")
        self.git("add", "-A")
        self.git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "fragments")
        self.git("push", "-q", "origin", "HEAD:main")
        (self.meta / "consumed.txt").write_text("changelog.d/1.fixed.md\n")
        result, calls = self.run_step()
        self.assertEqual(result.returncode, 0, result.stderr)
        changed = subprocess.run(["git", "-C", str(self.origin), "diff", "--name-status", "main", "chore/release-metadata-v1.2.3"],
                                 capture_output=True, text=True).stdout.split("\n")
        self.assertIn("D\tchangelog.d/1.fixed.md", changed)
        self.assertNotIn("D\tchangelog.d/2.added.md", changed)

    def test_a_consumed_list_with_anything_but_a_fragment_path_is_refused(self):
        for line in ("CHANGELOG.md", "changelog.d/../.github/workflows/x.yaml", "changelog.d/x.fix.md",
                     "/etc/passwd", "changelog.d/README.md", "changelog.d/a/b.fixed.md",
                     "x/changelog.d/1.fixed.md", "changelog.d/1.fixed.md.sh", "changelog.d/-a.fixed.md",
                     "changelog.d/" + "a" * 82 + ".fixed.md", "changelog.d/1.fixed.md ", " changelog.d/1.fixed.md",
                     "changelog.d/1.fixed.md\nCHANGELOG.md", "changelog.d/UP.fixed.md"):
            with self.subTest(line=line):
                (self.meta / "consumed.txt").write_text(f"changelog.d/1.fixed.md\n{line}\n")
                result, calls = self.run_step()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("not a changelog fragment", result.stdout)  # refused by the check, not by a later git failure
                self.assertEqual(calls, "")
                self.assertEqual(self.pushed_branches(), ["main"])

    def test_a_bad_list_stops_the_release_before_the_tag_is_pushed(self):
        """The list is its own step, ahead of 'Push the tag' and 'Publish the release'."""
        names = [step.get("name") for step in JOBS["metadata"]["steps"]]
        self.assertLess(names.index("Check the released fragment list"), names.index("Push the tag"))
        self.assertLess(names.index("Check the released fragment list"), names.index("Publish the release"))

    def test_a_symlink_in_the_checkout_is_not_removed(self):
        self.git("rm", "-q", "-r", "--cached", ".")
        (self.work / "changelog.d").mkdir(exist_ok=True)
        (self.work / "changelog.d" / "1.fixed.md").symlink_to("../README.md")
        self.git("add", "-A")
        self.git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "a link")
        self.git("push", "-q", "origin", "HEAD:main")
        (self.meta / "consumed.txt").write_text("changelog.d/1.fixed.md\n")
        result, calls = self.run_step()
        self.assertEqual(result.returncode, 0, result.stderr)
        changed = subprocess.run(["git", "-C", str(self.origin), "diff", "--name-status", "main", "chore/release-metadata-v1.2.3"],
                                 capture_output=True, text=True).stdout.split("\n")
        self.assertNotIn("D\tchangelog.d/1.fixed.md", changed)

    def test_a_missing_consumed_list_is_refused(self):
        self.refused(lambda: (self.meta / "consumed.txt").unlink())

    def test_a_missing_or_linked_body_is_refused(self):
        self.refused(lambda: (self.meta / "pr-body.md").unlink())

    def test_the_other_files_of_the_artifact_are_not_copied(self):
        (self.meta / "files" / "docs" / "evil.md").write_text("x")
        result, calls = self.run_step()
        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
