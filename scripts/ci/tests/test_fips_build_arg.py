"""Static and behaviour checks: only `--build-arg FIPS=1` builds an image with the FIPS 140 module.

`ARG FIPS=0` followed by `ENV GOFIPS140=${FIPS:+v1.0.0}` put the module into the default image too:
in Docker's (and the shell's) `${VAR:+word}`, a variable set to `0` is still set and non-empty. The
release Dockerfiles select the module in the build `RUN`, with an exact `= "1"` comparison. This test
fails when

  env-or-arg   GOFIPS140 is set by an `ENV` or `ARG` instruction (no exact comparison is possible there)
  unguarded    GOFIPS140 appears in a `RUN` without `if [ "$FIPS" = "1" ]; then export GOFIPS140=v1.0.0; fi`
               in front of the `go build`, or the build `RUN` has no such guard at all
  behaviour    running the guard from the Dockerfile with FIPS set to 0, "", 01, true, yes ... exports
               GOFIPS140 (only the exact value 1 may)

and, for the build scripts, when scripts/ci/check_fips_variant.sh accepts the wrong variant or the
trivy-image jobs stop running it on the built binary of every matrix entry. Comment lines are ignored,
so a comment may still explain the old mistake.

Run: python3 -I -m unittest discover -s scripts/ci/tests -p test_fips_build_arg.py -v
"""

import os
import pathlib
import re
import stat
import subprocess
import tempfile
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[3]
DOCKERFILES = ("Dockerfile.logs", "Dockerfile.traces")
CHECK = ROOT / "scripts" / "ci" / "check_fips_variant.sh"
SECURITY = ROOT / ".github" / "workflows" / "security.yaml"

GUARD = 'if [ "$FIPS" = "1" ]; then export GOFIPS140=v1.0.0; fi'
INSTRUCTION = re.compile(r"^(?P<name>[A-Za-z]+)\s")


def logical_lines(text: str):
    """Yield (first line number, joined text); a trailing `\\` joins the next line, comment lines are dropped."""
    pending, start = [], 0
    for number, raw in enumerate(text.splitlines(), 1):
        if not pending:
            start = number
        line = raw.rstrip()
        if not pending and line.lstrip().startswith("#"):
            continue
        if line.endswith("\\"):
            pending.append(line[:-1].strip())
            continue
        pending.append(line.strip())
        yield start, " ".join(p for p in pending if p)
        pending = []
    if pending:
        yield start, " ".join(p for p in pending if p)


def problems(name: str, text: str) -> list:
    """One message per way the Dockerfile text lets a non-1 FIPS value select the FIPS module."""
    found, built = [], False
    for number, line in logical_lines(text):
        match = INSTRUCTION.match(line)
        instruction = match.group("name").upper() if match else ""
        where = f"{name}:{number}"
        if "go build" in line and instruction == "RUN":
            built = True
            guard_at, build_at = line.find(GUARD), line.find("go build")
            if guard_at < 0 or guard_at > build_at:
                found.append(f"{where}: the build RUN does not run `{GUARD}` before `go build`")
        if "GOFIPS140" in line:
            if instruction in ("ENV", "ARG"):
                found.append(f"{where}: {instruction} sets GOFIPS140; use the exact-match guard in the build RUN instead")
            elif instruction == "RUN" and "GOFIPS140" in line.replace(GUARD, ""):
                found.append(f"{where}: GOFIPS140 is used in a RUN outside the exact-match guard")
    if not built:
        found.append(f"{name}: no RUN with `go build` was found")
    return found


def run_guard(value):
    """The GOFIPS140 value the Dockerfile guard leaves in the environment of the build, or None."""
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
    if value is not None:
        env["FIPS"] = value
    script = f'{GUARD}; printf "%s" "${{GOFIPS140-}}"; [ -z "${{GOFIPS140+x}}" ] || printf "|set"'
    out = subprocess.run(["sh", "-c", script], env=env, capture_output=True, text=True, check=True).stdout
    return out.split("|")[0] if out.endswith("|set") else None


class DockerfileTests(unittest.TestCase):
    def test_only_the_exact_value_one_selects_the_module(self):
        for name in DOCKERFILES:
            with self.subTest(dockerfile=name):
                self.assertEqual(problems(name, (ROOT / name).read_text()), [])

    def test_the_old_expansion_is_rejected(self):
        # The text origin/main had: the checker must flag it (this is the failing-first case).
        old = ('ARG FIPS=0\nENV GOWORK=off\nENV GOFIPS140=${FIPS:+v1.0.0}\n'
               'RUN CGO_ENABLED=0 go build -o /x ./cmd/x\n')
        found = problems("old", old)
        self.assertTrue(any("ENV sets GOFIPS140" in f for f in found), found)
        self.assertTrue(any("does not run" in f for f in found), found)

    def test_other_non_exact_forms_are_rejected(self):
        for body in ("ENV GOFIPS140=${FIPS:-v1.0.0}", "ARG GOFIPS140=v1.0.0", "ENV GOFIPS140=$FIPS",
                     'RUN [ -n "$FIPS" ] && export GOFIPS140=v1.0.0 && go build -o /x .',
                     'RUN go build -o /x . && GOFIPS140=v1.0.0 true',
                     f'RUN go build -o /x . && {GUARD}'):
            with self.subTest(body=body):
                text = "ARG FIPS=0\n" + body + "\n" + ("RUN go build -o /x .\n" if body.startswith(("ENV", "ARG")) else "")
                self.assertTrue(problems("x", text), body)

    def test_a_comment_may_name_the_old_expansion(self):
        text = "# was: ENV GOFIPS140=${FIPS:+v1.0.0}\nRUN " + GUARD + " && go build -o /x .\n"
        self.assertEqual(problems("x", text), [])

    def test_guard_behaviour(self):
        for value in ("0", "", "01", "1 ", "true", "yes", "v1.0.0", "off", "-1"):
            with self.subTest(fips=value):
                self.assertIsNone(run_guard(value), f"FIPS={value!r} must not export GOFIPS140")
        self.assertIsNone(run_guard(None))
        self.assertEqual(run_guard("1"), "v1.0.0")


STUB_GO = """#!/bin/sh
# go version -m <binary>: print the canned build information named by the binary's content
echo "$3: go1.27.3"
echo "	path	example.com/lakehouse"
cat "$3"
"""

FIPS_ON = "\tbuild\t-tags=fips140v1.0\n\tbuild\tGOFIPS140=v1.0.0-c2097c7c\n\tbuild\tDefaultGODEBUG=fips140=on\n"
FIPS_OFF = "\tbuild\t-trimpath=true\n\tbuild\tCGO_ENABLED=0\n"


class CheckScriptTests(unittest.TestCase):
    def check(self, build_info, expected):
        with tempfile.TemporaryDirectory() as directory:
            tmp = pathlib.Path(directory)
            go = tmp / "go"
            go.write_text(STUB_GO)
            go.chmod(go.stat().st_mode | stat.S_IEXEC)
            binary = tmp / "binary"
            binary.write_text(build_info)
            env = dict(os.environ, PATH=f"{tmp}:{os.environ['PATH']}")
            return subprocess.run(["bash", str(CHECK), str(binary), expected], env=env, capture_output=True, text=True)

    def test_matching_variants_pass(self):
        self.assertEqual(self.check(FIPS_OFF, "0").returncode, 0)
        self.assertEqual(self.check(FIPS_ON, "1").returncode, 0)

    def test_swapped_variants_fail(self):
        # The #685 symptom: the default binary carries the FIPS settings.
        wrong_default = self.check(FIPS_ON, "0")
        self.assertEqual(wrong_default.returncode, 1, wrong_default.stdout)
        for text in ("GOFIPS140", "fips140 build tag", "DefaultGODEBUG"):
            self.assertIn(text, wrong_default.stderr)
        self.assertEqual(self.check(FIPS_OFF, "1").returncode, 1)

    def test_a_partly_fips_binary_fails_both_ways(self):
        for partial in ("\tbuild\tGOFIPS140=v1.0.0-c2097c7c\n", "\tbuild\t-tags=fips140v1.0\n",
                        "\tbuild\tDefaultGODEBUG=fips140=on\n"):
            with self.subTest(partial=partial):
                self.assertEqual(self.check(partial, "0").returncode, 1)
                self.assertEqual(self.check(partial, "1").returncode, 1)

    def test_bad_usage_exits_two(self):
        result = subprocess.run(["bash", str(CHECK), "x", "2"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)


class WorkflowTests(unittest.TestCase):
    def test_every_release_image_has_its_binary_checked(self):
        job = yaml.safe_load(SECURITY.read_text())["jobs"]["trivy-image"]
        variants = {(e["module"], e["fips"]) for e in job["strategy"]["matrix"]["include"]}
        self.assertEqual(variants, {("logs", "0"), ("logs", "1"), ("traces", "0"), ("traces", "1")})
        steps = [s for s in job["steps"] if "check_fips_variant.sh" in str(s.get("run", ""))]
        self.assertEqual(len(steps), 1, "one step runs scripts/ci/check_fips_variant.sh on the built binary")
        self.assertIn("${FIPS_ARG}", steps[0]["run"])
        self.assertEqual(steps[0]["env"]["FIPS_ARG"], "${{ matrix.fips }}")
        names = [s.get("name") for s in job["steps"]]
        self.assertLess(names.index("Build image"), names.index(steps[0]["name"]))


if __name__ == "__main__":
    unittest.main()
