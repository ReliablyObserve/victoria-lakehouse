"""Static check: tools installed in CI are pinned and their downloads are verified.

Scans .github/workflows/*.y*ml, every YAML file under .github/actions, the shell scripts in
scripts/ci and the Makefile (comment lines are ignored; a trailing `\\` joins the next line).
It fails on

  floating-ref   a tool command (go install/get/run, pip, pipx, npm, npx, uvx, cargo install) that
                 names `@latest`, `@master`, `@main` or `@HEAD`; and a `go install|get|run
                 module@ref` whose ref is not a `vX.Y.Z` version
  download       a `curl`/`wget` of an external URL with no checksum or signature verification
                 (`sha256sum -c`, `shasum -c`, `gpg --verify`, `cosign verify`, `gh attestation
                 verify`) in the same or the next few lines. Loopback hosts and single-label
                 service names (compose or cluster services) are exempt
  pipe-to-shell  `| sh` / `| bash` (also `sudo`, `zsh`) and `bash <(curl ...)`
  pip-hashes     `pip install` without `--require-hashes` (the requirements file must be a hashed lock)
  npm-install    `npm install` / `npm i` instead of `npm ci` (a global install of an exactly
                 versioned package, `npm install -g name@1.2.3`, is allowed)

Exceptions live in scripts/ci/tool_pins_allowlist.txt, one per line:

    <path> | <rule> | <substring of the offending line> | <one-line reason>

An entry that matches nothing fails the test, so the list cannot rot.

Run: python3 -I -m unittest discover -s scripts/ci/tests -p test_ci_tool_pins.py -v
"""

import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
ALLOWLIST = "scripts/ci/tool_pins_allowlist.txt"
RULES = ("floating-ref", "download", "pipe-to-shell", "pip-hashes", "npm-install")

# How many logical lines after a download may carry its verification.
VERIFY_WINDOW = 8

FLOATING = re.compile(r"@(latest|master|main|HEAD)\b")
TOOL_CMD = re.compile(
    r"(?:\bgo\s+(?:install|get|run)\b|\bpipx?\s+(?:install|run)\b|\bpip3?\s+install\b"
    r"|\bnpm\s+(?:install|i|exec)\b|\bnpx\b|\buvx?\b|\bcargo\s+install\b)"
)
GO_CMD = re.compile(r"\bgo\s+(?:install|get|run)\b")
GO_MODULE_REF = re.compile(r"(?<![\w.])((?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}/[\w./~-]*)@([\w.+-]+)")
GO_VERSION = re.compile(r"^v\d+\.\d+\.\d+(?:[-+][\w.+-]*)?$")
FETCH = re.compile(r"(?<![\w./-])(?:curl|wget)\b")
URL_HOST = re.compile(r"https?://([^/\s\"'`]+)")
VERIFY = re.compile(
    r"sha256sum\s+(?:-\w+\s+)*(?:-c|--check)\b|shasum\s+(?:-\w+\s+\d+\s+)?(?:-c|--check)\b"
    r"|gpg\s+--verify|cosign\s+verify|gh\s+attestation\s+verify|minisign\s+-V"
)
PIPE_SHELL = re.compile(r"\|\s*(?:sudo\s+(?:-\w+\s+)*)?(?:ba|z|da)?sh\b(?!\w)|\b(?:ba|z)?sh\s+<\(\s*(?:curl|wget)\b")
PIP_INSTALL = re.compile(r"\bpip3?\s+install\b")
NPM_INSTALL = re.compile(r"\bnpm\s+(?:install|i)\b(?P<rest>[^|;&]*)")
NPM_GLOBAL = re.compile(r"(?:^|\s)(?:-g|--global)\b")
EXACT_NPM_PKG = re.compile(r"^(?:@[\w.-]+/)?[\w.-]+@\d+\.\d+\.\d+\S*$")


def is_local_host(host: str) -> bool:
    host = host.split("@")[-1].split(":")[0].lower()
    if host.startswith("$") or "${" in host:
        return True  # a variable: nothing to judge statically
    if host in ("localhost", "0.0.0.0") or host.startswith("127.") or host.endswith(".local"):
        return True
    return "." not in host  # single-label compose or service names


def logical_lines(text: str):
    """Yield (first line number, joined text) with `\\` continuations joined and comment lines dropped."""
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


def check_text(name: str, text: str) -> list:
    """Return (rule, 'name:line', message, logical line) for each violation in `text`."""
    lines = list(logical_lines(text))
    found = []

    def add(rule, number, message, line):
        found.append((rule, f"{name}:{number}", message, line))

    for index, (number, line) in enumerate(lines):
        stripped = line.lstrip("- ").lstrip()
        if stripped.startswith(("uses:", "runs-on:")):
            continue
        if TOOL_CMD.search(line):
            if FLOATING.search(line):
                add("floating-ref", number, "tool reference follows a moving ref (latest/master/main/HEAD)", line)
            elif GO_CMD.search(line):
                for module, ref in GO_MODULE_REF.findall(line):
                    if not GO_VERSION.match(ref):
                        add("floating-ref", number, f"{module}@{ref} is not an exact vX.Y.Z version", line)
        if FETCH.search(line):
            hosts = URL_HOST.findall(line)
            if any(not is_local_host(h) for h in hosts):
                window = " ".join(text_ for _, text_ in lines[index : index + 1 + VERIFY_WINDOW])
                if not VERIFY.search(window):
                    add("download", number, "download without a checksum or signature check nearby", line)
        if PIPE_SHELL.search(line):
            add("pipe-to-shell", number, "output piped into a shell", line)
        if PIP_INSTALL.search(line) and "--require-hashes" not in line:
            add("pip-hashes", number, "pip install without --require-hashes (use a hashed lock file)", line)
        for match in NPM_INSTALL.finditer(line):
            rest = match.group("rest")
            packages = [t for t in rest.split() if not t.startswith("-")]
            if NPM_GLOBAL.search(rest) and packages and all(EXACT_NPM_PKG.match(p) for p in packages):
                continue
            add("npm-install", number, "npm install instead of npm ci (needs a lock file)", line)
    return found


def parse_allowlist(text: str) -> list:
    """Return (path, rule, substring, reason) for each entry; raise ValueError on a malformed one."""
    entries = []
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split("|")]
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


def scanned_files(root: pathlib.Path):
    wf = root / ".github" / "workflows"
    files = sorted(list(wf.glob("*.yaml")) + list(wf.glob("*.yml")))
    actions = root / ".github" / "actions"
    if actions.is_dir():
        files += sorted(list(actions.rglob("*.yaml")) + list(actions.rglob("*.yml")))
    files += sorted((root / "scripts" / "ci").glob("*.sh"))
    if (root / "Makefile").is_file():
        files.append(root / "Makefile")
    return files


def scan(root: pathlib.Path):
    found = []
    for path in scanned_files(root):
        found += check_text(str(path.relative_to(root)), path.read_text())
    allow = root / ALLOWLIST
    entries = parse_allowlist(allow.read_text()) if allow.is_file() else []
    return apply_allowlist(found, entries)


def rules_of(text: str) -> list:
    return [rule for rule, *_ in check_text("w.yaml", text)]


class CheckTextTest(unittest.TestCase):
    """The checker itself, on fixtures."""

    def test_go_install_latest_is_rejected(self):
        self.assertEqual(rules_of("        run: go install golang.org/x/vuln/cmd/govulncheck@latest\n"), ["floating-ref"])

    def test_go_install_exact_version_passes(self):
        self.assertEqual(rules_of("          go install golang.org/x/vuln/cmd/govulncheck@v1.9.0\n"), [])

    def test_go_install_non_version_ref_is_rejected(self):
        self.assertEqual(rules_of("          go install golang.org/x/vuln/cmd/govulncheck@v1\n"), ["floating-ref"])
        self.assertEqual(rules_of("          go install example.com/tool/cmd/tool@abc1234\n"), ["floating-ref"])

    def test_go_get_continuation_with_latest_is_rejected(self):
        text = "  && go get github.com/securego/gosec/v2/cmd/gosec@v2.29.0 golang.org/x/tools@latest \\\n  && go build ./...\n"
        self.assertEqual(rules_of(text), ["floating-ref"])

    def test_go_get_pinned_continuation_passes(self):
        text = "  && go get github.com/securego/gosec/v2/cmd/gosec@v2.29.0 golang.org/x/tools@v0.52.0 \\\n  && go build ./...\n"
        self.assertEqual(rules_of(text), [])

    def test_local_go_commands_pass(self):
        self.assertEqual(rules_of("        run: go run ./scripts/ci/helmdrift\n        run: go install ./cmd/x\n"), [])

    def test_master_and_main_refs_are_rejected(self):
        self.assertEqual(rules_of("  run: npx some-tool@master\n"), ["floating-ref"])
        self.assertEqual(rules_of("  run: pip install git+https://example.com/x.git@main --require-hashes\n"), ["floating-ref"])

    def test_runs_on_and_uses_lines_are_not_tool_commands(self):
        self.assertEqual(rules_of("    runs-on: ubuntu-latest\n      - uses: actions/checkout@v7\n"), [])

    def test_comment_lines_are_ignored(self):
        self.assertEqual(rules_of("          # go install x.dev/y@latest ; curl https://x.dev/y | sh\n"), [])

    def test_unverified_external_download_is_rejected(self):
        text = '          curl -sSfL "https://github.com/o/r/releases/download/v1/t.tar.gz" -o t.tar.gz\n          tar xzf t.tar.gz\n'
        self.assertEqual(rules_of(text), ["download"])

    def test_download_piped_to_tar_is_rejected(self):
        self.assertEqual(rules_of('curl -sSfL "https://github.com/o/r/t.tar.gz" | tar xz\n'), ["download"])

    def test_wget_is_checked_like_curl(self):
        self.assertEqual(rules_of("  wget -q https://example.com/tool\n"), ["download"])

    def test_download_with_checksum_in_next_lines_passes(self):
        text = (
            '  curl --proto "=https" -fsSL -o "$d/a.tgz" \\\n    "https://github.com/o/r/a.tgz"\n'
            '  echo "${SHA}  $d/a.tgz" | sha256sum -c -\n'
        )
        self.assertEqual(rules_of(text), [])

    def test_checksum_far_away_does_not_count(self):
        text = "  curl -fsSL -o a https://example.com/a\n" + "  echo x\n" * 12 + "  sha256sum -c sums\n"
        self.assertEqual(rules_of(text), ["download"])

    def test_gh_attestation_and_cosign_count_as_verification(self):
        self.assertEqual(rules_of("curl -fsSL -o a https://example.com/a\ngh attestation verify a --repo o/r\n"), [])
        self.assertEqual(rules_of("curl -fsSL -o a https://example.com/a\ncosign verify-blob a\n"), [])

    def test_local_and_service_hosts_need_no_checksum(self):
        text = (
            "  curl -sf http://localhost:29428/health\n"
            "  curl -sf http://127.0.0.1:9000/minio/health/live > /dev/null\n"
            '  "$BUSYBOX" wget -qO- "http://victorialogs:9428/health"\n'
            '  curl -s "http://${HOST}:8080/x"\n'
        )
        self.assertEqual(rules_of(text), [])

    def test_release_asset_download_through_the_api_host_is_rejected(self):
        text = 'curl -sSfL -H "Accept: application/octet-stream" "https://api.github.com/repos/o/r/releases/assets/1" -o a\n'
        self.assertEqual(rules_of(text), ["download"])

    def test_pipe_to_shell_is_rejected(self):
        self.assertEqual(rules_of("curl -sSL http://localhost/i.sh | sh\n"), ["pipe-to-shell"])
        self.assertEqual(rules_of("echo x | sudo bash -s\n"), ["pipe-to-shell"])
        self.assertEqual(rules_of("bash <(curl -s http://localhost/i.sh)\n"), ["pipe-to-shell"])

    def test_words_that_only_contain_sh_pass(self):
        self.assertEqual(rules_of("echo a | shuf\nx | sha256sum\nrun: scripts/ci/x.sh\n"), [])

    def test_pip_install_without_hashes_is_rejected(self):
        for line in (
            "run: pip install pyyaml\n",
            "run: python -m pip install --disable-pip-version-check pyyaml==6.0.2\n",
            "run: pip install -r requirements.txt\n",
            "- run: pip3 install pyarrow duckdb\n",
        ):
            self.assertEqual(rules_of(line), ["pip-hashes"], line)

    def test_pip_install_with_hashes_passes(self):
        self.assertEqual(rules_of("run: pip install --require-hashes -r scripts/ci/requirements.txt\n"), [])

    def test_pip_install_hashes_on_a_continuation_passes(self):
        text = '"$venv/bin/pip" install --quiet --require-hashes --only-binary=:all: \\\n  -r .github/workflow-lint-requirements.txt\n'
        self.assertEqual(rules_of(text), [])

    def test_npm_install_is_rejected_and_npm_ci_passes(self):
        self.assertEqual(rules_of("        run: npm install\n"), ["npm-install"])
        self.assertEqual(rules_of("        run: npm i left-pad\n"), ["npm-install"])
        self.assertEqual(rules_of("        run: npm ci\n"), [])

    def test_global_npm_install_needs_an_exact_version(self):
        self.assertEqual(rules_of("run: npm install -g typescript\n"), ["npm-install"])
        self.assertEqual(rules_of("run: npm install -g typescript@latest\n"), ["floating-ref", "npm-install"])
        self.assertEqual(rules_of("run: npm install -g typescript@5.9.2\n"), [])

    def test_one_offending_line_per_rule(self):
        text = "run: curl -sSL https://example.com/i.sh | bash\n"
        self.assertEqual(sorted(rules_of(text)), ["download", "pipe-to-shell"])


class AllowlistTest(unittest.TestCase):
    ENTRY = "w.yaml | npm-install | npm install | no lock file yet"

    def test_entry_suppresses_a_matching_violation(self):
        found = check_text("w.yaml", "run: npm install\n")
        remaining, stale = apply_allowlist(found, parse_allowlist(self.ENTRY))
        self.assertEqual((remaining, stale), ([], []))

    def test_entry_for_another_file_rule_or_text_does_not_suppress(self):
        found = check_text("w.yaml", "run: npm install\n")
        for entry in (
            "other.yaml | npm-install | npm install | r",
            "w.yaml | pip-hashes | npm install | r",
            "w.yaml | npm-install | npm i left | r",
        ):
            remaining, stale = apply_allowlist(found, parse_allowlist(entry))
            self.assertEqual(len(remaining), 1, entry)
            self.assertEqual(len(stale), 1, entry)

    def test_unused_entry_is_stale(self):
        remaining, stale = apply_allowlist([], parse_allowlist(self.ENTRY))
        self.assertEqual((remaining, len(stale)), ([], 1))

    def test_malformed_entries_are_refused(self):
        for bad in ("w.yaml | npm-install | npm install", "w.yaml | nonsense | x | r", "w.yaml | npm-install | | r"):
            with self.assertRaises(ValueError):
                parse_allowlist(bad)

    def test_comments_and_blank_lines_are_ignored(self):
        self.assertEqual(parse_allowlist("# c\n\n" + self.ENTRY + "\n"), [tuple(p.strip() for p in self.ENTRY.split("|"))])


class RepositoryToolPinsTest(unittest.TestCase):
    def test_files_are_found(self):
        self.assertTrue(scanned_files(ROOT), "no workflow files found under .github/workflows")

    def test_tools_are_pinned_and_downloads_verified(self):
        remaining, stale = scan(ROOT)
        problems = [f"{where}: [{rule}] {message}: {line.strip()}" for rule, where, message, line in remaining]
        problems += [f"{ALLOWLIST}: stale entry (matches nothing): {' | '.join(e)}" for e in stale]
        self.assertEqual(problems, [], "\n" + "\n".join(problems))


if __name__ == "__main__":
    unittest.main()
