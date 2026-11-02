#!/usr/bin/env python3
"""The API matrix as a producer of the `PR proof` workflow (.github/workflows/pr-proof.yaml, job `api`).

  api_proof.py scope  --event pull_request --base origin/main --head HEAD
        prints `api=true|false` and `runner_fit=true|false` lines for $GITHUB_OUTPUT: does the pull request touch what the API matrix
        exercises (product code, build inputs, the proof harness, the registry's rows) and what the runner-fit measurement measures.

  api_proof.py run    --out OUT -- <pr_smoke.py options>
        runs `pr_smoke.py --api-only` on a port block chosen by probing (stack.pick_port_base), and once more on another block when the first
        attempt stopped because its ports were in use (exit 4): the single classified re-attempt. Any other result is final. The exit code is
        pr_smoke's; OUT/reattempt.json records a re-attempt, and `bundle` says so in the comment. A crash of this wrapper is exit 3 with
        "did not complete" files (never exit 1, a regression, or 2, an invalid run).

  api_proof.py bundle --out-dir lh-proof-comment --data-dir api-matrix-data --api-dir OUT --pr-number N --head-sha SHA --base-sha SHA \\
                      [--repo OWNER/NAME --run-id ID] [--skipped --changed N] [--reason TEXT]
        writes the commenter's artifact (manifest.json + api-matrix.md, docs/ci-proof-comment.md) and the data artifact:
          * pr_smoke.py --api-only left api-matrix.md and api-verdict.json in --api-dir -> that comment, verdict 0 pass, 1 fail,
            2 (invalid run) and 3/4 (did not complete) incomplete: the manifest has no `invalid`, the body says it;
          * --skipped (nothing the matrix exercises changed) -> the `skipped` comment;
          * anything else (no verdict file: the run died, timed out or was cancelled mid-way) -> a "did not complete" comment, so the
            verdict of an earlier head never stays on the pull request.

The artifact is untrusted data to the commenter, which sanitises it again; the manifest and the file set written here are checked with the
commenter's own parser (`proof_comment.parse_manifest`, `check_files`) before they are written.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import comment  # noqa: E402 - imports abreg, which puts the repository root on sys.path for scripts.ci below
import selection  # noqa: E402
from scripts.ci import proof_comment as pc  # noqa: E402

KIND = "api-matrix"
PRODUCER = ".github/workflows/pr-proof.yaml"
MANIFEST_FILE, BODY_FILE = "manifest.json", f"{KIND}.md"
EXIT_BUSY = 4  # stack.EXIT_BUSY and stack.PORTS_BUSY (a test pins both); stack.py is imported only by `run`, which has the harness dependencies
PORTS_BUSY = "ports already in use"
PR_SMOKE = os.path.join(HERE, "pr_smoke.py")
REATTEMPT_FILE = "reattempt.json"
DATA_FILES = ("api-matrix.json", "known-api.json", "api-verdict.json", "meta.json")
BODY_CAP = 60000  # GitHub rejects a comment above 65536 characters; the commenter cuts at 65000
REPO_RE = re.compile(r"[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}")
THIS_REPO_ISSUE = re.compile(r"victoria-lakehouse#(\d{1,7})$")

# What the API matrix exercises and what the runner-fit measurement measures (the paths its pull request trigger used to carry).
API_ALSO = (".github/workflows/pr-proof.yaml", "tests/conformance/registry/rows/")
RUNNER_FIT_PATHS = (".github/workflows/pr-proof.yaml", "deployment/docker/docker-compose-proof.yml", "scripts/bench/ab/", "scripts/proof/runner_fit.py")


def under(path: str, prefixes) -> bool:
    return any(path == p or (p.endswith("/") and path.startswith(p)) for p in prefixes)


def api_scope(files: list[str]) -> bool:
    """True when the matrix has something to say: Go code or an asset of a runtime tree, a build input, the proof harness (but not the perf
    shapes alone), the workflow itself, or a registry row. Docs, CI of other workflows, tests, charts and the website are not."""
    for f in files:
        kind = selection.classify(f)
        if kind in ("code", "asset", "runtime", "harness") or under(f, API_ALSO):
            return True
    return False


def runner_fit_scope(files: list[str]) -> bool:
    return any(under(f, RUNNER_FIT_PATHS) for f in files)


def cmd_scope(a) -> int:
    if a.event == "pull_request":
        files = selection.changed_files(a.base, a.head)
        api, fit = api_scope(files), runner_fit_scope(files)
    else:  # a manual run measures the runner fit; the matrix needs a pull request to comment on
        api, fit = False, True
    print(f"api={'true' if api else 'false'}")
    print(f"runner_fit={'true' if fit else 'false'}")
    return 0


# ------------------------------------------------------------------------------------------------ the run
def ports_busy(out: str) -> list[int] | None:
    """The ports a busy attempt reported, from the comment it left (None when it stopped for another reason: a stack of the project is
    already running, or the lock is held; another port block would not help)."""
    try:
        with open(os.path.join(out, "api-matrix.md"), encoding="utf-8") as f:
            text = f.read()
    except OSError:
        return None
    m = re.search(re.escape(PORTS_BUSY) + r" on 127\.0\.0\.1: \[([0-9, ]*)\]", text)
    return None if not m else [int(x) for x in re.findall(r"\d+", m.group(1))]


def write_infra_state(out: str, reason: str, infra: bool = True) -> None:
    """The run stopped before an attempt could measure anything, and the files the bundle and the gate read say so. infra=True (no port block is
    free): the files a busy pr_smoke leaves, exit 4. infra=False (the wrapper itself crashed): "did not complete", exit 3, never a regression."""
    meta: dict = {}
    text, verdict = comment.errored({}, meta, ("StackBusy: " if infra else "") + reason, comment.API_MARKER, comment.API_TITLE, infra=infra)
    verdict["exit"] = EXIT_BUSY if infra else 3
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "api-matrix.md"), "w", encoding="utf-8") as f:
        f.write(text)
    with open(os.path.join(out, "api-verdict.json"), "w", encoding="utf-8") as f:
        json.dump(verdict, f)


def run_with_retry(out: str, pick, attempt) -> int:
    """`attempt(base)` runs the matrix on a port block and returns its exit code; `pick(exclude)` returns a free block or raises stack.NoFreeBlock.
    Exit 4 because the ports were in use is re-attempted ONCE on another block (a collision with something on the host says nothing about the pull
    request); a regression, an invalid run or any other stop is never re-attempted."""
    import stack  # noqa: PLC0415 - the harness dependencies are installed where `run` is used
    try:
        first = pick(())
    except stack.NoFreeBlock as e:
        write_infra_state(out, str(e))
        return EXIT_BUSY
    rc = attempt(first)
    busy = ports_busy(out) if rc == EXIT_BUSY else None
    if busy is None:
        return rc
    report_holders(busy)
    rec = {"first_base": first, "busy_ports": busy, "second_base": None, "outcome": "no other free block"}
    try:
        rec["second_base"] = pick((first,))
    except stack.NoFreeBlock:
        return finish_reattempt(out, rec, rc)  # nowhere else to go: the first stop stands
    for name in ("api-matrix.md", *DATA_FILES):  # the busy first attempt's files must not stand in for a second attempt that dies without writing
        try:
            os.remove(os.path.join(out, name))
        except FileNotFoundError:
            pass
    rc = attempt(rec["second_base"])
    rec["outcome"] = "busy again" if rc == EXIT_BUSY else f"second attempt exit {rc}"
    return finish_reattempt(out, rec, rc)


def report_holders(ports: list[int]) -> None:
    """Best effort, to the log only: who holds a port that was found in use (`ss` may be missing or show nothing for another user's socket)."""
    for p in ports[:8]:
        try:
            r = subprocess.run(["ss", "-Htanp", f"( sport = :{int(p)} )"], capture_output=True, text=True, timeout=10, check=False)
            print(f"[api-proof] holder of port {p}: {(r.stdout or '').strip() or 'not shown by ss'}", file=sys.stderr, flush=True)
        except Exception as e:  # noqa: BLE001 - a diagnostic never fails the step
            print(f"[api-proof] holder of port {p}: ss unavailable ({type(e).__name__})", file=sys.stderr, flush=True)


def finish_reattempt(out: str, rec: dict, rc: int) -> int:
    with open(os.path.join(out, REATTEMPT_FILE), "w", encoding="utf-8") as f:
        json.dump(rec, f)
    return rc


def cmd_run(a) -> int:
    def attempt(base: int) -> int:
        print(f"[api-proof] API matrix on the port block starting at {base}", file=sys.stderr, flush=True)
        return subprocess.run([sys.executable, PR_SMOKE, "--api-only", "--out", a.out, *a.smoke], env=dict(os.environ, PROOF_PORT_BASE=str(base)), check=False).returncode
    try:
        import stack  # noqa: PLC0415 - inside the try: its import reads files and needs PyYAML, and a failure there is a crash like any other
        rc = run_with_retry(a.out, lambda exclude: stack.pick_port_base(exclude), attempt)
    except BaseException as e:  # a crash exits 1 (a regression to the gate): it is "did not complete", exit 3, whatever it was
        traceback.print_exc()
        try:
            write_infra_state(a.out, f"the API matrix wrapper crashed: {type(e).__name__}: {e}", infra=False)
        except OSError:
            pass
        if isinstance(e, KeyboardInterrupt):
            raise  # the process dies by the interrupt (the gate's catch-all reads that as "did not complete"), with the files in place
        rc = 3  # including SystemExit: a stray sys.exit(0) must not make the step report a pass while the files say exit 3
    print(f"[api-proof] exit {rc}", file=sys.stderr, flush=True)
    return rc


# ------------------------------------------------------------------------------------------------ the artifact
def verdict_of(exit_code: int) -> str:
    return {0: "pass", 1: "fail"}.get(exit_code, "incomplete")


def issue_numbers(known: dict | None) -> list[int]:
    """The issues of this repository that the known differences cite, for allowed_issue_refs (other repositories' links are neutralised)."""
    out: set[int] = set()
    for it in (known or {}).get("items", []):
        for k in it.get("tracked_by", []):
            for ref in k.get("issues", []):
                m = THIS_REPO_ISSUE.match(ref)
                if m and 1 <= int(m.group(1)) <= 9_999_999:
                    out.add(int(m.group(1)))
    return sorted(out)[:pc.MAX_ISSUE_REFS]


def cap_body(text: str, cap: int = BODY_CAP) -> str:
    if len(text) <= cap:
        return text
    note = "\n\n_The report was cut to fit the size limit of a PR comment; the full data is in the `api-matrix-data` artifact of the run._\n"
    cut = text[: cap - len(note)]
    return (cut[: cut.rfind("\n")] if "\n" in cut else cut) + note


def run_line(repo: str, run_id: str) -> str:
    if repo and run_id.isdigit() and REPO_RE.fullmatch(repo):
        return f"[The run and its job log](https://github.com/{repo}/actions/runs/{run_id}) have the details."
    return "The job log has the details."


def reattempt_note(api_dir: str) -> str:
    """One line when the run was re-attempted on another port block (the comment records every re-attempt)."""
    try:
        with open(os.path.join(api_dir, REATTEMPT_FILE), encoding="utf-8") as f:
            r = json.load(f)
        ports = ", ".join(str(int(p)) for p in r["busy_ports"])
        second = "no other block was free" if r.get("second_base") is None else f"the second attempt ran on the block starting at {int(r['second_base'])}"
        return (f"\nRe-attempted once: the ports {ports} were in use on this runner (the block starting at {int(r['first_base'])}); {second} "
                f"({str(r['outcome'])[:40].replace('`', '')}).\n")
    except (OSError, ValueError, KeyError, TypeError):
        return ""


def with_note(body: str, note: str) -> str:
    """The note goes before the `<sub>` footer: the comment keeps its fixed section order, the footer last."""
    i = body.rfind("<sub>")
    if not note or i < 0:
        return body + note
    return body[:i] + note.strip("\n") + "\n\n" + body[i:]


def body_for(args, api_dir: str) -> tuple[str, str, dict | None]:
    """(body, verdict, known differences) for what the run left in api_dir."""
    meta = {"base": args.base_sha, "head": args.head_sha}
    md, vf = os.path.join(api_dir, "api-matrix.md"), os.path.join(api_dir, "api-verdict.json")
    if args.skipped:
        body, _ = comment.skipped({"changed": args.changed}, comment.API_MARKER, comment.API_TITLE)
        return body, "skipped", None
    if os.path.isfile(md) and os.path.isfile(vf):
        with open(vf, encoding="utf-8") as f:
            exit_code = int(json.load(f).get("exit", 3))
        with open(md, encoding="utf-8") as f:
            body = f.read()
        body = with_note(body, reattempt_note(api_dir))
        known = None
        kf = os.path.join(api_dir, "known-api.json")
        if os.path.isfile(kf):
            with open(kf, encoding="utf-8") as f:
                known = json.load(f)
        return body, verdict_of(exit_code), known
    reason = args.reason or "the matrix run stopped before it wrote a verdict (it timed out, was cancelled or crashed)"
    body, _ = comment.errored({}, meta, reason, comment.API_MARKER, comment.API_TITLE)
    body = with_note(body, reattempt_note(api_dir))
    return body.rstrip("\n") + "\n\n" + run_line(args.repo, args.run_id) + " The figures of an earlier run do not apply to this head.\n", "incomplete", None


def cmd_bundle(a) -> int:
    body, verdict, known = body_for(a, a.api_dir)
    body = cap_body(body)
    refs = issue_numbers(known)
    manifest = {"schema_version": pc.SCHEMA_VERSION, "pr_number": a.pr_number, "head_sha": a.head_sha,
                "comments": [{"kind": KIND, "verdict": verdict}], "images": [], "allowed_issue_refs": refs}
    raw = (json.dumps(manifest, indent=1) + "\n").encode()
    pc.parse_manifest(raw)  # raises pc.Reject on anything the commenter would refuse
    pc.check_files({MANIFEST_FILE: raw, BODY_FILE: body.encode()}, manifest, PRODUCER)
    os.makedirs(a.out_dir, exist_ok=True)
    with open(os.path.join(a.out_dir, MANIFEST_FILE), "wb") as f:
        f.write(raw)
    with open(os.path.join(a.out_dir, BODY_FILE), "w", encoding="utf-8") as f:
        f.write(body)
    if a.data_dir:
        os.makedirs(a.data_dir, exist_ok=True)
        for name in DATA_FILES:
            src = os.path.join(a.api_dir, name)
            if os.path.isfile(src):
                shutil.copyfile(src, os.path.join(a.data_dir, name))
    print(f"api-matrix comment: verdict {verdict}, {len(body)} characters, {len(refs)} issue reference(s)")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sc = sub.add_parser("scope")
    sc.add_argument("--event", required=True, choices=("pull_request", "workflow_dispatch", "push", "schedule"))
    sc.add_argument("--base", default="origin/main")
    sc.add_argument("--head", default="HEAD")
    ru = sub.add_parser("run")
    ru.add_argument("--out", required=True)
    ru.add_argument("smoke", nargs=argparse.REMAINDER, help="options of pr_smoke.py after --")
    bu = sub.add_parser("bundle")
    bu.add_argument("--out-dir", required=True)
    bu.add_argument("--data-dir", default="")
    bu.add_argument("--api-dir", required=True)
    bu.add_argument("--pr-number", type=int, required=True)
    bu.add_argument("--head-sha", required=True)
    bu.add_argument("--base-sha", default="")
    bu.add_argument("--repo", default="")
    bu.add_argument("--run-id", default="")
    bu.add_argument("--skipped", action="store_true")
    bu.add_argument("--changed", type=int, default=0)
    bu.add_argument("--reason", default="")
    try:
        a = ap.parse_args(argv)
    except SystemExit as e:
        if e.code == 2 and (sys.argv[1:] if argv is None else argv)[:1] == ["run"]:
            return 3  # an argument error would be exit 2, "invalid run", to the gate; the bundle writes "did not complete" for the missing verdict
        raise
    if a.cmd == "run":
        a.smoke = a.smoke[1:] if a.smoke[:1] == ["--"] else a.smoke
        return cmd_run(a)
    return cmd_scope(a) if a.cmd == "scope" else cmd_bundle(a)


if __name__ == "__main__":
    sys.exit(main())
