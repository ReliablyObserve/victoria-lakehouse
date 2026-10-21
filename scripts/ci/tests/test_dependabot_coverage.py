"""Static check: .github/dependabot.yml keeps every dependency manifest in the repository updated.

Reads the tracked files (git) and .github/dependabot.yml. It fails on

  uncovered     a dependency manifest whose directory has no `updates:` entry of a matching ecosystem:
                  go.mod                                         gomod
                  a *requirements*.txt / *.in file               pip
                  package.json next to a package-lock.json       npm
                  a Dockerfile (name has dockerfile/containerfile) docker
                  a Compose file (name has compose)              docker-compose, or docker (it reads every
                                                                 non-dot YAML file of its directory too)
                  a YAML file in a directory named compose       docker (docker-compose only reads names with compose)
                  .github/workflows/*.y*ml                       github-actions, directory `/`
  stale         an `updates:` entry whose directory holds no file its ecosystem reads (a typo or a removed
                directory), or an allowlist line that is no longer needed
  schedule      an entry without `schedule.interval`
  cooldown      an entry without `cooldown.default-days` of at least 7 days (new releases wait before they
                are proposed; security updates are not delayed)
  own-image     an entry that reads a file referencing an image this repository builds itself
                (`ghcr.io/reliablyobserve/mc`, `.../minio`) without an `ignore:` for it

The file name rules mirror the file fetchers of the Dependabot ecosystems, so a directory that the
fetcher would reject is reported instead of silently producing nothing.

Exceptions live in scripts/ci/dependabot_allowlist.txt, one per line:

    <directory> | <ecosystem> | <one-line reason>

Run: python3 -I -m unittest discover -s scripts/ci/tests -p test_dependabot_coverage.py -v
"""

import fnmatch
import pathlib
import posixpath
import re
import subprocess
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[3]
CONFIG = ".github/dependabot.yml"
ALLOWLIST = "scripts/ci/dependabot_allowlist.txt"
ECOSYSTEMS = ("gomod", "pip", "npm", "docker", "docker-compose", "github-actions")
MIN_COOLDOWN_DAYS = 7

# File name rules of the Dependabot file fetchers.
DOCKERFILE = re.compile(r"dockerfile|containerfile", re.IGNORECASE)
COMPOSE = re.compile(r"(docker-)?compose(-[\w]+)?(?:\.[\w-]+)?\.ya?ml", re.IGNORECASE)
YAML_FILE = re.compile(r"^[^.].*\.ya?ml$", re.IGNORECASE)
NPM_LOCKS = ("package-lock.json", "npm-shrinkwrap.json")

# Images that this repository builds and publishes itself; their tags are not upstream releases.
OWN_IMAGES = ("reliablyobserve/mc", "reliablyobserve/minio")
OWN_IMAGE_REF = re.compile(r"reliablyobserve/(?:mc|minio)\b")


def directory_of(path: str) -> str:
    return "/" + posixpath.dirname(path).strip("/") if posixpath.dirname(path) else "/"


def normalize(directory: str) -> str:
    return "/" + directory.strip("/")


def required(files: list) -> set:
    """The (ecosystem, directory) pairs that a tracked manifest asks for. docker-compose stands for
    docker-compose or docker."""
    need, names = set(), set(files)
    for path in files:
        base, where = posixpath.basename(path), directory_of(path)
        if base == "go.mod":
            need.add(("gomod", where))
        elif "requirements" in base and base.endswith((".txt", ".in")):
            need.add(("pip", where))
        elif base == "package.json" and any(posixpath.join(posixpath.dirname(path), lock) in names for lock in NPM_LOCKS):
            need.add(("npm", where))
        elif DOCKERFILE.search(base):
            need.add(("docker", where))
        elif COMPOSE.search(base) and YAML_FILE.match(base):
            need.add(("docker-compose", where))
        elif YAML_FILE.match(base) and "compose" in posixpath.dirname(path).lower().split("/") and not path.startswith(("charts/", ".github/", "docs/", "website/")):
            need.add(("docker", where))  # a Compose stack whose files carry no `compose` in their names: only docker reads them
        elif path.startswith(".github/workflows/") and base.endswith((".yaml", ".yml")):
            need.add(("github-actions", "/"))
    return need


def read_by(ecosystem: str, files: list, where: str) -> list:
    """The files of directory `where` that the file fetcher of `ecosystem` reads."""
    found = []
    for path in files:
        base = posixpath.basename(path)
        if ecosystem == "github-actions":
            ok = where == "/" and path.startswith(".github/workflows/") and base.endswith((".yaml", ".yml"))
        elif directory_of(path) != where:
            continue
        elif ecosystem == "gomod":
            ok = base == "go.mod"
        elif ecosystem == "pip":
            ok = "requirements" in base and base.endswith((".txt", ".in"))
        elif ecosystem == "npm":
            ok = base == "package.json"
        elif ecosystem == "docker":
            ok = bool(DOCKERFILE.search(base)) or bool(YAML_FILE.match(base))
        elif ecosystem == "docker-compose":
            ok = bool(COMPOSE.search(base)) and bool(YAML_FILE.match(base))
        else:
            ok = False
        if ok:
            found.append(path)
    return found


def entry_directories(entry: dict, files: list) -> list:
    """The directories an `updates:` entry names, with `directories:` globs expanded over the tracked files."""
    names = []
    if "directory" in entry:
        names.append(entry["directory"])
    names += list(entry.get("directories") or [])
    known = {directory_of(p) for p in files} - {"/"}
    out = []
    for name in names:
        name = normalize(str(name))
        pattern = re.compile("".join("[^/]*" if c == "*" else re.escape(c) for c in name) + r"\Z")
        out += sorted(d for d in known if pattern.match(d)) if "*" in name else [name]
    return out


def parse_allowlist(text: str) -> list:
    entries = []
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(" | ")]
        if len(parts) != 3 or not all(parts) or parts[1] not in ECOSYSTEMS:
            raise ValueError(f"allowlist line {number}: want `directory | ecosystem | reason`, ecosystem one of {ECOSYSTEMS}")
        entries.append((normalize(parts[0]), parts[1], parts[2]))
    return entries


def check(config: dict, files: list, allow: list, read=lambda path: "") -> list:
    """Return the problems (an empty list is a pass). `read` returns the text of a tracked file."""
    problems, covered, entries = [], set(), config.get("updates") or []

    for index, entry in enumerate(entries):
        eco = entry.get("package-ecosystem")
        name = f"updates[{index}] {eco}"
        if eco not in ECOSYSTEMS:
            problems.append(f"{name}: ecosystem is not one this repository uses ({', '.join(ECOSYSTEMS)})")
            continue
        dirs = entry_directories(entry, files)
        if not dirs:
            problems.append(f"{name}: no directory")
        if not (entry.get("schedule") or {}).get("interval"):
            problems.append(f"schedule: {name} {dirs} has no schedule.interval")
        days = (entry.get("cooldown") or {}).get("default-days")
        if not isinstance(days, int) or days < MIN_COOLDOWN_DAYS:
            problems.append(f"cooldown: {name} {dirs} needs cooldown.default-days of at least {MIN_COOLDOWN_DAYS}")
        ignored = [i.get("dependency-name", "") for i in entry.get("ignore") or [] if not i.get("update-types") and not i.get("versions")]
        for where in dirs:
            seen = read_by(eco, files, where)
            if not seen:
                problems.append(f"stale: {name} directory {where} holds no file that {eco} reads")
            covered.add((eco, where))
            if eco in ("docker", "docker-compose"):
                for path in seen:
                    if OWN_IMAGE_REF.search(read(path)):
                        for image in OWN_IMAGES:
                            if image in read(path) and not any(fnmatch.fnmatchcase(image, pat) for pat in ignored):
                                problems.append(f"own-image: {name} reads {path}, which uses {image}; add it to `ignore:`")

    allowed = {(e[1], e[0]) for e in allow}
    used = set()
    for eco, where in sorted(required(files)):
        # A Compose file is read by docker-compose and by docker; docker-compose is not required of a docker entry.
        if eco == "docker-compose":
            ok = ("docker-compose", where) in covered or ("docker", where) in covered
        else:
            ok = (eco, where) in covered
        if ok:
            continue
        if (eco, where) in allowed:
            used.add((eco, where))
        else:
            problems.append(f"uncovered: {where} has {eco} manifests but .github/dependabot.yml has no `{eco}` entry for it")
    for eco, where in sorted(allowed - used):
        problems.append(f"{ALLOWLIST}: stale entry (nothing is uncovered there): {where} | {eco}")
    return problems


def tracked_files(root: pathlib.Path) -> list:
    out = subprocess.run(["git", "-C", str(root), "ls-files", "-z"], check=True, capture_output=True, text=True).stdout
    return sorted(p for p in out.split("\0") if p and (root / p).is_file())


def reader(root: pathlib.Path):
    def read(path: str) -> str:
        try:
            return (root / path).read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""

    return read


GOOD = {"schedule": {"interval": "weekly"}, "cooldown": {"default-days": 7}}


def entry(eco: str, *dirs: str, **extra) -> dict:
    key = {"directory": dirs[0]} if len(dirs) == 1 else {"directories": list(dirs)}
    return {"package-ecosystem": eco, **key, **GOOD, **extra}


def problems_of(updates: list, files: list, allow: str = "", read=lambda path: "") -> list:
    return check({"version": 2, "updates": updates}, files, parse_allowlist(allow), read)


def kinds(problems: list) -> list:
    return [p.split(":", 1)[0] for p in problems]


class RequiredTest(unittest.TestCase):
    def test_manifest_kinds(self):
        files = [
            "go.mod",
            "lakehouse-traces/go.mod",
            "scripts/proof/requirements.txt",
            "scripts/proof/requirements.in",
            ".github/workflow-lint-requirements.txt",
            "website/package.json",
            "tests/playwright/proof/package.json",
            "tests/playwright/proof/package-lock.json",
            "Dockerfile.logs",
            "deployment/docker/minio-src/mc.Dockerfile",
            "deployment/docker/docker-compose-e2e.yml",
            "scripts/bench/compaction/compose.yml",
            "scripts/ci/tests/pre1970_hot_compare.compose.yml",
            "tests/s3compat/compose/garage.yml",
            ".github/workflows/ci.yaml",
            "docs/setup.md",
        ]
        self.assertEqual(
            required(files),
            {
                ("gomod", "/"),
                ("gomod", "/lakehouse-traces"),
                ("pip", "/scripts/proof"),
                ("pip", "/.github"),
                ("npm", "/tests/playwright/proof"),
                ("docker", "/"),
                ("docker", "/deployment/docker/minio-src"),
                ("docker", "/tests/s3compat/compose"),
                ("docker-compose", "/deployment/docker"),
                ("docker-compose", "/scripts/bench/compaction"),
                ("docker-compose", "/scripts/ci/tests"),
                ("github-actions", "/"),
            },
        )

    def test_package_json_without_a_lock_is_not_required(self):
        self.assertEqual(required(["website/package.json"]), set())

    def test_yaml_without_compose_in_its_name_needs_docker(self):
        self.assertEqual(required(["tests/s3compat/compose/garage.yml", "charts/x/values.yaml"]), {("docker", "/tests/s3compat/compose")})


class CheckTest(unittest.TestCase):
    FILES = ["go.mod", "scripts/proof/requirements.txt", "Dockerfile.logs", ".github/workflows/ci.yaml"]
    FULL = [entry("gomod", "/"), entry("pip", "/scripts/proof"), entry("docker", "/"), entry("github-actions", "/")]

    def test_complete_config_passes(self):
        self.assertEqual(problems_of(self.FULL, self.FILES), [])

    def test_missing_ecosystem_is_reported_with_its_directory(self):
        got = problems_of(self.FULL[:2] + self.FULL[3:], self.FILES)
        self.assertEqual(kinds(got), ["uncovered"])
        self.assertIn("/ has docker manifests", got[0])

    def test_directories_list_and_glob_cover(self):
        files = ["a/go.mod", "b/go.mod", "b/c/go.mod"]
        self.assertEqual(problems_of([entry("gomod", "/a", "/b", "/b/c")], files), [])
        self.assertEqual(problems_of([entry("gomod", "/*")], files[:2]), [])
        self.assertEqual(kinds(problems_of([entry("gomod", "/*")], files)), ["uncovered"])

    def test_wrong_directory_is_uncovered_and_stale(self):
        got = problems_of([entry("gomod", "/nope")], ["go.mod"])
        self.assertEqual(sorted(kinds(got)), ["stale", "uncovered"])

    def test_docker_entry_covers_compose_files_and_the_reverse_is_not_assumed(self):
        files = ["d/docker-compose.yml"]
        self.assertEqual(problems_of([entry("docker", "/d")], files), [])
        self.assertEqual(problems_of([entry("docker-compose", "/d")], files), [])
        files = ["d/Dockerfile"]
        self.assertEqual(kinds(problems_of([entry("docker-compose", "/d")], files)), ["stale", "uncovered"])

    def test_docker_compose_entry_for_a_directory_without_compose_names_is_stale(self):
        files = ["s/garage.yml"]
        self.assertEqual(kinds(problems_of([entry("docker-compose", "/s")], files)), ["stale"])
        self.assertEqual(problems_of([entry("docker", "/s")], files), [])

    def test_unknown_ecosystem_is_reported(self):
        self.assertEqual(kinds(problems_of([entry("cargo", "/")], [])), ["updates[0] cargo"])

    def test_schedule_and_cooldown_are_required(self):
        bare = {"package-ecosystem": "gomod", "directory": "/"}
        self.assertEqual(kinds(problems_of([bare], ["go.mod"])), ["schedule", "cooldown"])
        short = entry("gomod", "/", cooldown={"default-days": 3})
        self.assertEqual(kinds(problems_of([short], ["go.mod"])), ["cooldown"])
        semver_only = entry("gomod", "/", cooldown={"semver-major-days": 30})
        self.assertEqual(kinds(problems_of([semver_only], ["go.mod"])), ["cooldown"])

    def test_own_images_need_an_ignore(self):
        files = ["d/docker-compose.yml"]
        text = "services:\n  mc:\n    image: ghcr.io/reliablyobserve/mc:RELEASE.1\n"
        read = lambda path: text  # noqa: E731
        self.assertEqual(kinds(problems_of([entry("docker-compose", "/d")], files, read=read)), ["own-image"])
        for pattern in ("*reliablyobserve/mc", "reliablyobserve/*"):
            ignored = entry("docker-compose", "/d", ignore=[{"dependency-name": pattern}])
            self.assertEqual(problems_of([ignored], files, read=read), [], pattern)

    def test_a_partial_ignore_does_not_count(self):
        files = ["d/docker-compose.yml"]
        read = lambda path: "image: ghcr.io/reliablyobserve/minio:RELEASE.1\n"  # noqa: E731
        partial = entry("docker-compose", "/d", ignore=[{"dependency-name": "*reliablyobserve/minio", "update-types": ["version-update:semver-major"]}])
        self.assertEqual(kinds(problems_of([partial], files, read=read)), ["own-image"])


class AllowlistTest(unittest.TestCase):
    def test_entry_covers_an_uncovered_manifest(self):
        got = problems_of([], ["x/go.mod"], "/x | gomod | tracked elsewhere")
        self.assertEqual(got, [])

    def test_unused_entry_is_stale(self):
        got = problems_of([entry("gomod", "/x")], ["x/go.mod"], "/x | gomod | no longer needed")
        self.assertEqual(kinds(got), [ALLOWLIST])

    def test_malformed_entries_are_refused(self):
        for bad in ("/x | gomod", "/x | cargo | r", "/x | gomod | "):
            with self.assertRaises(ValueError):
                parse_allowlist(bad)

    def test_comments_and_blank_lines_are_ignored(self):
        self.assertEqual(parse_allowlist("# c\n\n/x | gomod | r\n"), [("/x", "gomod", "r")])


class RepositoryDependabotTest(unittest.TestCase):
    def test_every_manifest_is_covered(self):
        files = tracked_files(ROOT)
        config = yaml.safe_load((ROOT / CONFIG).read_text(encoding="utf-8"))
        self.assertEqual(config.get("version"), 2)
        allow_file = ROOT / ALLOWLIST
        allow = parse_allowlist(allow_file.read_text(encoding="utf-8")) if allow_file.is_file() else []
        problems = check(config, files, allow, reader(ROOT))
        self.assertEqual(problems, [], "\n" + "\n".join(problems))


if __name__ == "__main__":
    unittest.main()
