#!/usr/bin/env python3
"""Deterministic test sharding for one Go package, with completeness guards.

Why: `internal/storage/parquets3` (root module and lakehouse-traces) runs
~1200 tests under `-short -race`. On a 2-core runner it takes 350-480s
against an 8m (480s) -timeout, with ~120s run-to-run spread, so the headroom
gate in gotest_report.py (fail at 90%) tripped at random. Splitting the
package into N separate `go test` runs gives each shard its own -timeout and
its own headroom measurement, at ~1/N of the work each.

Assignment is a hash of the top-level test name (sha1 mod N): stable across
runs and machines, independent of the other tests, so adding or removing a
test never moves any other test. No duration table to keep in sync. The
package has hundreds of similar-sized tests (the slowest is ~7% of the total),
so hashing balances the shards well; measured sizes are in the PR.

Subcommands:
  regex   --pkg DIR --shards N --index I
            print the `go test -run` regexp for shard I. Lists the package's
            top-level Test/Fuzz/Example functions with `go test -list`, so a
            new test lands in exactly one shard with no manual step. Fails if
            the shards do not partition the list exactly or a shard is empty.
  verify  --pkg DIR --shards N JSON...
            after the shards ran with `-json`: every listed test must have a
            result (pass/fail/skip) in exactly the shard it was assigned to.
            Fails on a test that ran in no shard, in two, or in the wrong one.
  cover-merge OUT PROFILE...
            merge per-shard coverprofiles (union of covered blocks) into OUT
            and print `coverage: X% of statements`, the figure `go test
            -cover` would print for one unsharded run.
"""
import argparse
import hashlib
import json
import re
import subprocess
import sys

NAME = re.compile(r"^(Test|Fuzz|Example)[A-Za-z0-9_]*$")
# `Test` alone, `Testfoo`: go only treats Test followed by a non-lowercase
# rune as a test, and -list applies the same rule, so no extra filtering.


def shard_of(name, n):
    return int(hashlib.sha1(name.encode()).hexdigest(), 16) % n


def assign(names, n):
    """Partition names into n lists by hash; each list sorted."""
    if n < 1:
        raise ValueError("shards must be >= 1")
    shards = [[] for _ in range(n)]
    for name in sorted(set(names)):
        shards[shard_of(name, n)].append(name)
    return shards


def parse_list(output):
    """Top-level Test/Fuzz/Example names from `go test -list .` output."""
    return sorted({ln.strip() for ln in output.splitlines() if NAME.match(ln.strip())})


def guard_partition(names, shards):
    """Raise ValueError unless shards are a disjoint cover of names, all non-empty."""
    seen = {}
    for i, sh in enumerate(shards):
        if not sh:
            raise ValueError(f"shard {i} is empty: pick fewer shards")
        for t in sh:
            if t in seen:
                raise ValueError(f"{t} is in shards {seen[t]} and {i}")
            seen[t] = i
    missing = set(names) - set(seen)
    extra = set(seen) - set(names)
    if missing or extra:
        raise ValueError(f"shards differ from the test list: missing={sorted(missing)} extra={sorted(extra)}")


def list_tests(pkg):
    r = subprocess.run(["go", "test", "-list", ".", pkg], capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(r.stdout + r.stderr)
        raise SystemExit(f"go test -list failed for {pkg}")
    names = parse_list(r.stdout)
    if not names:
        raise SystemExit(f"no tests listed for {pkg}")
    return names


def regex_for(tests):
    return "^(" + "|".join(tests) + ")$"


def ran_in(path):
    """Top-level tests with a final result in a `go test -json` file."""
    out = set()
    with open(path) as f:
        for line in f:
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            t = ev.get("Test", "")
            if t and "/" not in t and ev.get("Action") in ("pass", "fail", "skip"):
                out.add(t)
    return out


def verify(names, n, json_files):
    """Return a list of problems (empty when every test ran in its own shard)."""
    if len(json_files) != n:
        return [f"expected {n} shard result files, got {len(json_files)}"]
    expect = assign(names, n)
    problems = []
    got = [ran_in(p) for p in json_files]
    ran_by = {}
    for i, g in enumerate(got):
        for t in g:
            ran_by.setdefault(t, []).append(i)
    for i, sh in enumerate(expect):
        for t in sh:
            where = ran_by.get(t, [])
            if where == [i]:
                continue
            if not where:
                problems.append(f"{t}: ran in no shard (assigned to shard {i})")
            else:
                problems.append(f"{t}: ran in shard(s) {where}, assigned to shard {i}")
    return problems


def read_profile(path):
    mode, blocks = None, {}
    with open(path) as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            if ln.startswith("mode:"):
                mode = ln.split(":", 1)[1].strip()
                continue
            key, count = ln.rsplit(" ", 1)
            blocks.setdefault(key, 0)
            blocks[key] += int(count)
    return mode, blocks


def merge_profiles(paths):
    """(mode, {block: summed count}) over all profiles; modes must agree."""
    mode, merged = None, {}
    for p in paths:
        m, blocks = read_profile(p)
        if m is None:
            raise ValueError(f"{p}: no mode line")
        if mode is not None and m != mode:
            raise ValueError(f"{p}: mode {m} != {mode}")
        mode = m
        for k, c in blocks.items():
            merged[k] = merged.get(k, 0) + c
    if mode is None:
        raise ValueError("no profiles")
    return mode, merged


def percent(merged):
    """Covered statements / total statements, as go tool cover computes it."""
    total = covered = 0
    for key, count in merged.items():
        n = int(key.rsplit(" ", 1)[1])
        total += n
        if count > 0:
            covered += n
    return 100.0 * covered / total if total else 0.0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("regex")
    r.add_argument("--pkg", required=True)
    r.add_argument("--shards", type=int, required=True)
    r.add_argument("--index", type=int, required=True)
    v = sub.add_parser("verify")
    v.add_argument("--pkg", required=True)
    v.add_argument("--shards", type=int, required=True)
    v.add_argument("files", nargs="+")
    c = sub.add_parser("cover-merge")
    c.add_argument("out")
    c.add_argument("profiles", nargs="+")
    a = ap.parse_args(argv)

    if a.cmd == "regex":
        if not 0 <= a.index < a.shards:
            raise SystemExit("index out of range")
        names = list_tests(a.pkg)
        shards = assign(names, a.shards)
        try:
            guard_partition(names, shards)
        except ValueError as e:
            raise SystemExit(f"shard guard: {e}")
        print(regex_for(shards[a.index]))
        sys.stderr.write(f"shard {a.index + 1}/{a.shards}: {len(shards[a.index])} of {len(names)} tests\n")
    elif a.cmd == "verify":
        names = list_tests(a.pkg)
        problems = verify(names, a.shards, a.files)
        if problems:
            for p in problems[:50]:
                print(f"::error::shard guard: {p}")
            return 1
        print(f"shard guard: all {len(names)} tests ran exactly once, in their own shard")
    else:
        mode, merged = merge_profiles(a.profiles)
        with open(a.out, "w") as f:
            f.write(f"mode: {mode}\n")
            for k in sorted(merged):
                f.write(f"{k} {merged[k]}\n")
        print(f"coverage: {percent(merged):.1f}% of statements")
    return 0


if __name__ == "__main__":
    sys.exit(main())
