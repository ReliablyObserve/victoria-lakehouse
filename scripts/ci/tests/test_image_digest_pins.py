"""Static check: every container image the repository references is pinned by digest.

Scans the tracked Dockerfiles (`Dockerfile*`, `*.Dockerfile`), the Compose files (a YAML file whose
name or directory contains `compose`) and .github/workflows/*.y*ml. It fails on

  from            a Dockerfile `FROM` whose image (a stage name and `scratch` are exempt; a `${ARG}`
                  is resolved through the ARG default declared before the first FROM) has no digest
  compose-image   a Compose `image:` value without a digest
  workflow-image  a workflow `image:` / `container:` value, or the image argument of a literal
                  `docker run|create|pull`, without a digest (a GitHub expression such as
                  `${{ matrix.image }}` is not judged here: the matrix `image:` line is)

A reference is pinned when it reads `name:tag@sha256:<64 hex>`: the digest makes the pull immutable and
the tag stays for people. The digest must be the one of the multi-arch index, so every platform resolves.

Exceptions live in scripts/ci/image_pins_allowlist.txt, one per line:

    <path> | <rule> | <substring of the offending line> | <one-line reason>

An entry that matches nothing fails the test, so the list cannot rot.

Run: python3 -I -m unittest discover -s scripts/ci/tests -p test_image_digest_pins.py -v
"""

import os
import pathlib
import re
import shlex
import subprocess
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
ALLOWLIST = "scripts/ci/image_pins_allowlist.txt"
RULES = ("from", "compose-image", "workflow-image")

# `name:tag@sha256:<64 hex>`: a human tag followed by the immutable digest.
PINNED = re.compile(r":[\w][\w.-]*@sha256:[0-9a-f]{64}(?![0-9a-f])")

FROM_LINE = re.compile(r"^\s*FROM\s+(?P<flags>(?:--\S+\s+)*)(?P<ref>\S+)(?:\s+AS\s+(?P<alias>\S+))?\s*$", re.IGNORECASE)
ARG_LINE = re.compile(r"^\s*ARG\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?:=(?P<value>\S*))?\s*$", re.IGNORECASE)
VAR = re.compile(r"\$\{(?P<braced>[A-Za-z_][A-Za-z0-9_]*)(?::?-(?P<default>[^}]*))?\}|\$(?P<bare>[A-Za-z_][A-Za-z0-9_]*)")
IMAGE_KEY = re.compile(r"^\s*(?:-\s+)?image:\s*(?P<value>.*)$")
CONTAINER_KEY = re.compile(r"^\s*container:\s*(?P<value>[^\s|>#].*)$")
DOCKER_CMD = re.compile(r"\bdocker\s+(?P<sub>run|create|pull)\b(?P<rest>.*)$")

# `docker run|create` options that take a value as the next word (`--opt=value` needs nothing).
VALUE_FLAGS = frozenset(
    "-e --env -p --publish -v --volume --name --network --net -u --user -w --workdir --entrypoint --platform "
    "-l --label --add-host --env-file --mount --cap-add --cap-drop --tmpfs --restart -m --memory --cpus "
    "--hostname -h --device --dns --ulimit --log-driver --log-opt --pull --health-cmd --security-opt --shm-size "
    "--group-add --ip --pid --ipc --cidfile --stop-signal --stop-timeout".split()
)


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


def strip_value(value: str) -> str:
    """A YAML scalar without its trailing comment and quotes."""
    value = re.sub(r"\s+#.*$", "", value).strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    return value


def is_pinned(ref: str) -> bool:
    return bool(PINNED.search(ref))


def expand(ref: str, args: dict) -> str:
    """Replace `$NAME` / `${NAME}` / `${NAME:-default}` with the ARG default (or the inline default)."""

    def repl(match):
        name = match.group("braced") or match.group("bare")
        if args.get(name) not in (None, ""):
            return args[name]
        if match.group("default") is not None:
            return match.group("default")
        return match.group(0)

    for _ in range(8):  # a default may itself name an ARG; the bound stops a cycle
        expanded = VAR.sub(repl, ref)
        if expanded == ref:
            break
        ref = expanded
    return ref


def check_dockerfile(name: str, text: str) -> list:
    """Return (rule, 'name:line', message, line) for each unpinned FROM."""
    found, args, stages, seen_from = [], {}, set(), False
    for number, line in logical_lines(text):
        arg = ARG_LINE.match(line)
        if arg and not seen_from:
            args[arg.group("name")] = arg.group("value") or ""
            continue
        match = FROM_LINE.match(line)
        if not match:
            continue
        seen_from = True
        ref = match.group("ref")
        if ref.lower() == "scratch" or ref.lower() in stages:
            pass
        elif not is_pinned(expand(ref, args)):
            found.append(("from", f"{name}:{number}", f"FROM {ref} is not pinned as name:tag@sha256:<digest>", line))
        if match.group("alias"):
            stages.add(match.group("alias").lower())
    return found


def check_compose(name: str, text: str) -> list:
    found = []
    for number, line in logical_lines(text):
        match = IMAGE_KEY.match(line)
        if not match:
            continue
        ref = strip_value(match.group("value"))
        if ref and not is_pinned(ref):
            found.append(("compose-image", f"{name}:{number}", f"image {ref} is not pinned as name:tag@sha256:<digest>", line))
    return found


def docker_command_image(rest: str, sub: str):
    """The image argument of the words following `docker run|create|pull`, or None."""
    try:
        words = shlex.split(rest, posix=True)
    except ValueError:
        words = rest.split()
    skip = False
    for word in words:
        if skip:
            skip = False
            continue
        if word in ("&&", "||", ";", "|"):
            return None
        if word.startswith("-"):
            if sub != "pull" and word in VALUE_FLAGS:
                skip = True
            continue
        return word
    return None


def check_workflow(name: str, text: str) -> list:
    found = []

    def add(number, ref, line):
        found.append(("workflow-image", f"{name}:{number}", f"image {ref} is not pinned as name:tag@sha256:<digest>", line))

    for number, line in logical_lines(text):
        for key in (IMAGE_KEY, CONTAINER_KEY):
            match = key.match(line)
            if match:
                ref = strip_value(match.group("value"))
                if ref and not ref.startswith(("[", "{", "|", ">")) and "${{" not in ref and not is_pinned(ref):
                    add(number, ref, line)
        for cmd in DOCKER_CMD.finditer(line):
            ref = docker_command_image(cmd.group("rest"), cmd.group("sub"))
            if ref and "${{" not in ref and not is_pinned(ref):
                add(number, ref, line)
    return found


def parse_allowlist(text: str) -> list:
    """Return (path, rule, substring, reason) for each entry; raise ValueError on a malformed one."""
    entries = []
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(" | ")]
        if len(parts) != 4 or not all(parts) or parts[1] not in RULES:
            raise ValueError(f"allowlist line {number}: want `path | rule | substring | reason`, rule one of {RULES}")
        entries.append(tuple(parts))
    return entries


def apply_allowlist(found: list, entries: list):
    """Return (remaining violations, entries that matched nothing)."""
    used, remaining = set(), []
    for rule, where, message, line in found:
        path = where.rsplit(":", 1)[0]
        hit = [e for e in entries if e[0] == path and e[1] == rule and e[2] in line]
        if hit:
            used.update(hit)
        else:
            remaining.append((rule, where, message, line))
    return remaining, [e for e in entries if e not in used]


SKIP_DIRS = {".git", "node_modules", "deps", "vendor", ".claude"}


def tracked_files(root: pathlib.Path) -> list:
    """Repository-relative paths of the tracked files (git), else every file below root."""
    try:
        out = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"], check=True, capture_output=True, text=True
        ).stdout
        return sorted(p for p in out.split("\0") if p)
    except (OSError, subprocess.CalledProcessError):
        paths = []
        for base, dirs, files in os.walk(root):
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
            paths += [str((pathlib.Path(base) / f).relative_to(root)) for f in files]
        return sorted(paths)


def classify(path: str):
    """'dockerfile', 'compose', 'workflow' or None for a repository-relative path."""
    parts = pathlib.PurePosixPath(path).parts
    base = parts[-1]
    if base.startswith("Dockerfile") or base.endswith(".Dockerfile"):
        return "dockerfile"
    if path.startswith(".github/workflows/") and base.endswith((".yaml", ".yml")):
        return "workflow"
    if base.endswith((".yaml", ".yml")) and path.split("/", 1)[0] not in ("charts", ".github", "docs", "website"):
        if "compose" in base or "compose" in parts[:-1]:
            return "compose"
    return None


CHECKERS = {"dockerfile": check_dockerfile, "compose": check_compose, "workflow": check_workflow}


def scanned_files(root: pathlib.Path) -> list:
    return [(p, classify(p)) for p in tracked_files(root) if classify(p) and (root / p).is_file()]


def scan(root: pathlib.Path):
    found = []
    for path, kind in scanned_files(root):
        found += CHECKERS[kind](path, (root / path).read_text(encoding="utf-8"))
    allow = root / ALLOWLIST
    entries = parse_allowlist(allow.read_text(encoding="utf-8")) if allow.is_file() else []
    return apply_allowlist(found, entries)


DIGEST_A = "@sha256:" + "a" * 64


def rules_of(kind: str, text: str) -> list:
    return [rule for rule, *_ in CHECKERS[kind]("f", text)]


class DockerfileTest(unittest.TestCase):
    def test_tag_only_from_is_rejected(self):
        self.assertEqual(rules_of("dockerfile", "FROM golang:1.27.3-alpine3.23 AS builder\n"), ["from"])

    def test_platform_flag_is_skipped(self):
        self.assertEqual(rules_of("dockerfile", "FROM --platform=$BUILDPLATFORM golang:1.27.3 AS b\n"), ["from"])
        self.assertEqual(rules_of("dockerfile", f"FROM --platform=$BUILDPLATFORM golang:1.27.3{DIGEST_A} AS b\n"), [])

    def test_pinned_from_passes(self):
        self.assertEqual(rules_of("dockerfile", f"FROM gcr.io/distroless/static-debian12:nonroot{DIGEST_A}\n"), [])

    def test_digest_without_a_tag_is_rejected(self):
        self.assertEqual(rules_of("dockerfile", f"FROM alpine{DIGEST_A}\n"), ["from"])

    def test_short_or_malformed_digest_is_rejected(self):
        self.assertEqual(rules_of("dockerfile", "FROM alpine:3.21@sha256:abc\n"), ["from"])
        self.assertEqual(rules_of("dockerfile", f"FROM alpine:3.21{DIGEST_A}ff\n"), ["from"])
        self.assertEqual(rules_of("dockerfile", "FROM alpine:3.21@sha256:" + "A" * 64 + "\n"), ["from"])
        self.assertEqual(rules_of("dockerfile", "FROM alpine:3.21@sha512:" + "a" * 64 + "\n"), ["from"])

    def test_stage_names_and_scratch_are_exempt(self):
        text = f"FROM alpine:3.21{DIGEST_A} AS build\nFROM scratch\nFROM build\nFROM BUILD AS final\n"
        self.assertEqual(rules_of("dockerfile", text), [])

    def test_a_later_stage_name_does_not_exempt_an_earlier_use(self):
        self.assertEqual(rules_of("dockerfile", f"FROM later\nFROM alpine:3.21{DIGEST_A} AS later\n"), ["from"])

    def test_lowercase_from_and_continuation_are_read(self):
        self.assertEqual(rules_of("dockerfile", "from alpine:3.21\n"), ["from"])
        self.assertEqual(rules_of("dockerfile", "FROM \\\n  alpine:3.21\n"), ["from"])

    def test_comment_lines_are_ignored(self):
        self.assertEqual(rules_of("dockerfile", "# FROM alpine:3.21\n"), [])

    def test_arg_default_with_a_digest_passes(self):
        text = f"ARG BASE=alpine:3.21{DIGEST_A}\nFROM ${{BASE}}\nFROM $BASE\n"
        self.assertEqual(rules_of("dockerfile", text), [])

    def test_arg_default_without_a_digest_is_rejected(self):
        self.assertEqual(rules_of("dockerfile", "ARG BASE=alpine:3.21\nFROM ${BASE}\n"), ["from"])

    def test_arg_without_a_default_is_rejected(self):
        self.assertEqual(rules_of("dockerfile", "ARG UPSTREAM\nFROM ${UPSTREAM} AS upstream\n"), ["from"])

    def test_arg_after_the_first_from_does_not_count(self):
        text = f"FROM alpine:3.21{DIGEST_A}\nARG BASE=alpine:3.21{DIGEST_A}\nFROM ${{BASE}}\n"
        self.assertEqual(rules_of("dockerfile", text), ["from"])

    def test_non_from_lines_with_images_are_not_judged(self):
        self.assertEqual(rules_of("dockerfile", "COPY --from=alpine:3.21 /x /x\nRUN echo FROM alpine:3.21\n"), [])


class ComposeTest(unittest.TestCase):
    def test_tag_only_image_is_rejected(self):
        self.assertEqual(rules_of("compose", "services:\n  a:\n    image: nginx:alpine\n"), ["compose-image"])

    def test_pinned_image_passes_with_quotes_and_comment(self):
        self.assertEqual(rules_of("compose", f'    image: "nginx:alpine{DIGEST_A}"  # note\n'), [])
        self.assertEqual(rules_of("compose", f"    image: nginx:alpine{DIGEST_A}\n"), [])

    def test_anchor_level_image_is_checked(self):
        self.assertEqual(rules_of("compose", "x-lh: &lh\n  image: alpine:3\n"), ["compose-image"])

    def test_list_item_image_is_checked(self):
        self.assertEqual(rules_of("compose", "  - image: alpine:3\n"), ["compose-image"])

    def test_variable_image_is_rejected_until_allowlisted(self):
        self.assertEqual(rules_of("compose", "    image: ${LOGS_IMAGE}\n"), ["compose-image"])

    def test_variable_default_with_a_digest_passes(self):
        self.assertEqual(rules_of("compose", f"    image: ${{X:-nginx:alpine{DIGEST_A}}}\n"), [])

    def test_commented_out_image_and_other_keys_pass(self):
        self.assertEqual(rules_of("compose", "    # image: nginx:alpine\n    container_name: image\n    build: .\n"), [])


class WorkflowTest(unittest.TestCase):
    def test_service_container_image_is_checked(self):
        text = "    services:\n      db:\n        image: postgres:16\n"
        self.assertEqual(rules_of("workflow", text), ["workflow-image"])
        self.assertEqual(rules_of("workflow", f"        image: postgres:16{DIGEST_A}\n"), [])

    def test_container_scalar_and_mapping_are_checked(self):
        self.assertEqual(rules_of("workflow", "    container: node:22\n"), ["workflow-image"])
        self.assertEqual(rules_of("workflow", "    container:\n      image: node:22\n"), ["workflow-image"])
        self.assertEqual(rules_of("workflow", f"    container: node:22{DIGEST_A}\n"), [])

    def test_expression_and_flow_list_values_are_not_judged(self):
        self.assertEqual(rules_of("workflow", "        image: ${{ matrix.image }}\n        image: [minio, mc]\n"), [])

    def test_matrix_image_line_is_judged(self):
        self.assertEqual(rules_of("workflow", "          - image: rustfs/rustfs:1.0.0\n"), ["workflow-image"])

    def test_docker_run_image_is_found_after_flags_with_values(self):
        text = "docker run -d --name s3 -p 127.0.0.1:9000:9000 -e A=b alpine:3.21 sleep 1\n"
        self.assertEqual(rules_of("workflow", text), ["workflow-image"])
        self.assertEqual(rules_of("workflow", f"docker run -d --name s3 -p 9000:9000 -e A=b alpine:3.21{DIGEST_A} sleep 1\n"), [])

    def test_docker_run_continuation_with_an_expression_image_passes(self):
        text = 'docker run -d --name s3 \\\n  -e A=b \\\n  "${{ matrix.image }}" ${{ matrix.cmd }}\n'
        self.assertEqual(rules_of("workflow", text), [])

    def test_docker_pull_is_checked(self):
        self.assertEqual(rules_of("workflow", "docker pull ghcr.io/${REPO}/app:${VERSION}\n"), ["workflow-image"])
        self.assertEqual(rules_of("workflow", f"docker pull ghcr.io/o/app:1{DIGEST_A}\n"), [])

    def test_other_docker_subcommands_and_comments_pass(self):
        self.assertEqual(rules_of("workflow", "docker build -f Dockerfile .\ndocker compose up -d\n# docker run alpine\n"), [])


class AllowlistTest(unittest.TestCase):
    ENTRY = "f | compose-image | alpine:3 | built locally"

    def test_entry_suppresses_a_matching_violation(self):
        found = check_compose("f", "  image: alpine:3\n")
        self.assertEqual(apply_allowlist(found, parse_allowlist(self.ENTRY)), ([], []))

    def test_entry_for_another_file_rule_or_text_does_not_suppress(self):
        found = check_compose("f", "  image: alpine:3\n")
        for entry in (
            "g | compose-image | alpine:3 | r",
            "f | from | alpine:3 | r",
            "f | compose-image | nginx | r",
        ):
            remaining, stale = apply_allowlist(found, parse_allowlist(entry))
            self.assertEqual((len(remaining), len(stale)), (1, 1), entry)

    def test_unused_entry_is_stale(self):
        remaining, stale = apply_allowlist([], parse_allowlist(self.ENTRY))
        self.assertEqual((remaining, len(stale)), ([], 1))

    def test_malformed_entries_are_refused(self):
        for bad in ("f | compose-image | alpine:3", "f | nonsense | x | r", "f | compose-image | | r"):
            with self.assertRaises(ValueError):
                parse_allowlist(bad)

    def test_comments_and_blank_lines_are_ignored(self):
        self.assertEqual(parse_allowlist("# c\n\n" + self.ENTRY + "\n"), [tuple(p.strip() for p in self.ENTRY.split(" | "))])


class ClassifyTest(unittest.TestCase):
    def test_kinds(self):
        cases = {
            "Dockerfile.logs": "dockerfile",
            "deployment/docker/minio-src/mc.Dockerfile": "dockerfile",
            "deployment/docker/Dockerfile.loki": "dockerfile",
            "deployment/docker/docker-compose-e2e.yml": "compose",
            "scripts/bench/compaction/compose.yml": "compose",
            "scripts/ci/tests/pre1970_hot_compare.compose.yml": "compose",
            "tests/s3compat/compose/garage.yml": "compose",
            ".github/workflows/ci.yaml": "workflow",
            "charts/victoria-lakehouse/values.yaml": None,
            "docs/docker-compose-setup.md": None,
            "deployment/docker/lakehouse-e2e-config.yml": None,
        }
        for path, want in cases.items():
            self.assertEqual(classify(path), want, path)


class RepositoryImagePinsTest(unittest.TestCase):
    def test_files_are_found(self):
        kinds = {kind for _, kind in scanned_files(ROOT)}
        self.assertEqual(kinds, {"dockerfile", "compose", "workflow"}, "a kind of file is no longer found")

    def test_images_are_pinned_by_digest(self):
        remaining, stale = scan(ROOT)
        problems = [f"{where}: [{rule}] {message}: {line.strip()}" for rule, where, message, line in remaining]
        problems += [f"{ALLOWLIST}: stale entry (matches nothing): {' | '.join(e)}" for e in stale]
        self.assertEqual(problems, [], "\n" + "\n".join(problems))


if __name__ == "__main__":
    unittest.main()
