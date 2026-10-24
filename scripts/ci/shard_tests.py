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
test never moves any other test. Hashing balances the COUNT of tests, not their
time: with a long tail of slow tests (the slowest ten are ~30% of
lakehouse-traces/internal/storage/parquets3 under -race) one shard drew 468 s
and another 321 s from the same 500-test share. So an optional weights file
(--weights, scripts/ci/shard_weights/*.json) lists the slow tests with a
relative cost: those are placed greedily (heaviest first, onto the shard with
the least load) on top of the hash-placed rest, which count at the file's
default cost. A test missing from the file is simply hash-placed at the default
cost (it still lands in exactly one shard); a stale or missing weight costs
balance, never coverage, and the headroom gate in gotest_report.py shows it.
Regenerate the file from fresh `go test -json` output with the `weights`
subcommand.

Subcommands:
  regex   --pkg DIR --shards N --index I [--go-flags "-race"] [--run-filter RE]
            print the `go test -run` regexp for shard I. Lists the package's
            top-level Test/Fuzz/Example functions with `go test -list`, so a
            new test lands in exactly one shard with no manual step. Fails if
            the shards do not partition the list exactly or a shard is empty.
  verify  --pkg DIR --shards N [--go-flags "-race"] [--run-filter RE] JSON...
            after the shards ran with `-json`: every listed test must have a
            result (pass/fail/skip) in exactly the shard it was assigned to.
            Fails on a test that ran in no shard, in two, or in the wrong one.

--run-filter RE restricts both to the listed tests whose name matches RE (the
heavy CI job shards only the testing.Short()-gated tests): the partition and
the verify guard are computed over the filtered list, so a filtered-in test
that fails to run is still caught.
  weights OUT [--min-seconds S] JSON...
            write a weights file from `go test -json` output (the longest time
            seen per top-level test; tests >= S seconds listed, the rest
            averaged into "default"). Shard with it via --weights.
  cover-merge OUT PROFILE...
            merge per-shard coverprofiles (union of covered blocks) into OUT
            and print `coverage: X% of statements`, the figure `go test
            -cover` would print for one unsharded run.
"""
import argparse
import hashlib
import json
import re
import shlex
import subprocess
import sys

NAME = re.compile(r"^(Test|Fuzz|Example)[A-Za-z0-9_]*$")
# `Test` alone, `Testfoo`: go only treats Test followed by a non-lowercase
# rune as a test, and -list applies the same rule, so no extra filtering.


def shard_of(name, n):
    return int(hashlib.sha1(name.encode()).hexdigest(), 16) % n


def assign(names, n, weights=None):
    """Partition names into n lists; each list sorted.

    Without weights: by hash of the name. With weights (default, {test: cost}):
    tests not in the table are hash-placed at the default cost, then the
    tests in the table are placed heaviest first onto the least-loaded shard
    (ties: lowest shard index; equal weights: by name), so the result is a
    pure function of (names, n, weights) on every machine."""
    if n < 1:
        raise ValueError("shards must be >= 1")
    shards = [[] for _ in range(n)]
    if weights is None:
        for name in sorted(set(names)):
            shards[shard_of(name, n)].append(name)
        return shards
    default, table = weights
    loads = [0.0] * n
    listed = []
    for name in sorted(set(names)):
        if name in table:
            listed.append(name)
        else:
            i = shard_of(name, n)
            shards[i].append(name)
            loads[i] += default
    for name in sorted(listed, key=lambda t: (-table[t], t)):
        i = min(range(n), key=lambda k: (loads[k], k))
        shards[i].append(name)
        loads[i] += table[name]
    return [sorted(sh) for sh in shards]


def loads_of(shards, weights):
    """Estimated cost of each shard under the weights (default for unlisted tests)."""
    default, table = weights
    return [sum(table.get(t, default) for t in sh) for sh in shards]


def load_weights(path):
    """(default, {test: cost}) from a weights file: {"default": x, "tests": {name: cost}}."""
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    default, table = doc.get("default"), doc.get("tests")
    if not isinstance(default, (int, float)) or isinstance(default, bool) or default <= 0:
        raise ValueError(f"{path}: default must be a positive number")
    if not isinstance(table, dict) or not table:
        raise ValueError(f"{path}: tests must be a non-empty object")
    for name, w in table.items():
        if not NAME.match(name):
            raise ValueError(f"{path}: {name!r} is not a Test/Fuzz/Example name")
        if not isinstance(w, (int, float)) or isinstance(w, bool) or w <= 0:
            raise ValueError(f"{path}: weight of {name} must be a positive number")
    return float(default), {k: float(v) for k, v in table.items()}


def build_weights(json_files, min_seconds):
    """(default, {test: seconds}) from `go test -json` files: the longest
    elapsed time seen per top-level test over all files; tests of at least
    min_seconds are listed, the rest are averaged into the default."""
    elapsed = {}
    for path in json_files:
        with open(path) as f:
            for line in f:
                try:
                    ev = json.loads(line)
                except ValueError:
                    continue
                t = ev.get("Test", "")
                if t and "/" not in t and ev.get("Action") == "pass":
                    elapsed[t] = max(elapsed.get(t, 0.0), float(ev.get("Elapsed", 0.0)))
    table = {t: round(e, 1) for t, e in elapsed.items() if e >= min_seconds and round(e, 1) > 0}
    rest = [e for t, e in elapsed.items() if t not in table]
    if not table or not rest:
        raise ValueError("no tests above the threshold, or none below it")
    return round(max(sum(rest) / len(rest), 0.01), 2), dict(sorted(table.items(), key=lambda kv: (-kv[1], kv[0])))


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


def list_tests(pkg, go_flags=()):
    # go_flags must carry every build-affecting flag of the real run (-race,
    # -tags ...): a `//go:build !race` test is listed without -race but never
    # runs with it, which would fail verify (or silently fall out of a shard).
    r = subprocess.run(["go", "test", "-list", ".", *go_flags, pkg], capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(r.stdout + r.stderr)
        raise SystemExit(f"go test -list failed for {pkg}")
    names = parse_list(r.stdout)
    if not names:
        raise SystemExit(f"no tests listed for {pkg}")
    return names


def filter_tests(names, run_filter):
    """names that match run_filter (a `go test -run`-style regexp, searched per
    name); all of names when run_filter is empty. An empty result is an error:
    a filter that selects nothing would shard (and verify) nothing."""
    if not run_filter:
        return names
    rx = re.compile(run_filter)
    kept = [n for n in names if rx.search(n)]
    if not kept:
        raise SystemExit(f"--run-filter {run_filter!r} matches none of the {len(names)} listed tests")
    return kept


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


def verify(names, n, json_files, weights=None):
    """Return a list of problems (empty when every test ran in its own shard)."""
    if len(json_files) != n:
        return [f"expected {n} shard result files, got {len(json_files)}"]
    expect = assign(names, n, weights)
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
    r.add_argument("--go-flags", default="", help="build-affecting flags of the real run, e.g. -race")
    r.add_argument("--run-filter", default="", help="only shard the listed tests matching this regexp")
    r.add_argument("--weights", default="", help="weights file: balance the shards by cost, not count")
    v = sub.add_parser("verify")
    v.add_argument("--pkg", required=True)
    v.add_argument("--shards", type=int, required=True)
    v.add_argument("--go-flags", default="", help="build-affecting flags of the real run, e.g. -race")
    v.add_argument("--run-filter", default="", help="only verify the listed tests matching this regexp")
    v.add_argument("--weights", default="", help="the weights file the shards ran with")
    v.add_argument("files", nargs="+")
    w = sub.add_parser("weights")
    w.add_argument("out")
    w.add_argument("--min-seconds", type=float, default=1.0, help="list tests at least this slow (default 1.0)")
    w.add_argument("files", nargs="+", help="`go test -json` output files")
    c = sub.add_parser("cover-merge")
    c.add_argument("out")
    c.add_argument("profiles", nargs="+")
    a = ap.parse_args(argv)

    if a.cmd == "regex":
        if not 0 <= a.index < a.shards:
            raise SystemExit("index out of range")
        listed = list_tests(a.pkg, shlex.split(a.go_flags))
        names = filter_tests(listed, a.run_filter)
        weights = load_weights(a.weights) if a.weights else None
        if weights and not a.run_filter:
            stale = sorted(set(weights[1]) - set(listed))
            if stale:  # a renamed or deleted test: costs balance only, so warn, never fail
                sys.stderr.write("::warning title=Shard weights::%d weights entries match no test (%s); regenerate the file "
                                 "with shard_tests.py weights\n" % (len(stale), ", ".join(stale[:5])))
        shards = assign(names, a.shards, weights)
        try:
            guard_partition(names, shards)
        except ValueError as e:
            raise SystemExit(f"shard guard: {e}")
        print(regex_for(shards[a.index]))
        est = ""
        if weights:
            loads = loads_of(shards, weights)
            est = f", estimated load {loads[a.index]:.0f} of {sum(loads):.0f} (heaviest shard {max(loads):.0f})"
        sys.stderr.write(f"shard {a.index + 1}/{a.shards}: {len(shards[a.index])} of {len(names)} tests{est}\n")
    elif a.cmd == "verify":
        names = filter_tests(list_tests(a.pkg, shlex.split(a.go_flags)), a.run_filter)
        problems = verify(names, a.shards, a.files, load_weights(a.weights) if a.weights else None)
        if problems:
            for p in problems[:50]:
                print(f"::error::shard guard: {p}")
            return 1
        print(f"shard guard: all {len(names)} tests ran exactly once, in their own shard")
    elif a.cmd == "weights":
        default, table = build_weights(a.files, a.min_seconds)
        with open(a.out, "w", encoding="utf-8") as f:
            json.dump({"default": default, "tests": table}, f, indent=1)
            f.write("\n")
        print(f"{len(table)} tests listed, default {default}s for the rest")
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
