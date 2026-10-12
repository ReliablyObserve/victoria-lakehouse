#!/usr/bin/env python3
"""Pick the A/B shape sets a change needs, from the files it touches, through the conformance registry.

Nothing maps files to shapes by hand except the coarse `areas` table of shapes.json. Every shape declares the
registry rows and features it measures (`covers`); a feature names the tests that prove it (`tests:
internal/x/y_test.go#TestZ`) and the rows it owns (`rows:`), a row names its tests (`refs.tests`); a test sits
in a package directory, and that directory is the code the item is about. A changed file selects every shape whose
covered items (and the rows those features list, one hop) sit in the file's package directory.

Rules, per changed path:
  cmd/, internal/, lakehouse-traces/, patches/ (non-test code)   the shapes of the matching items, plus the
                                                                 control smoke subset
  go.mod, go.sum, Dockerfile*, charts' images                    the whole control sets (the module graph and the
                                                                 build reach every query)
  scripts/bench/ab, scripts/proof, the proof compose file        the control smoke subset (the harness itself)
  scripts/bench/ab/shapes.json                                   the shapes added or edited, plus smoke
  *_test.go, docs, CI, registry text, website, everything else   nothing: the PR comment says "skipped"

  selection.py --base origin/main              files from git diff base...HEAD
  selection.py --files internal/x/y.go ...     explicit file list
  selection.py --check                         gate (make conformance-check): no unknown id, every shape covers
                                               something reachable, sets and layers well formed

Prints JSON: {"run": bool, "sets": {set: [shape, ...]}, "why": {...}, ...}.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import abreg  # noqa: E402

CONTROL = ("control-logs", "control-traces")
API_TYPES = ("logsql", "jaeger", "tempo")
PICKS = ("trace_id",)
WINDOWS = ("start-end", "none", "micros", "end-ms")
RUNTIME_ALL = ("go.mod", "go.sum")
HARNESS = ("scripts/bench/ab/", "scripts/proof/", "deployment/docker/docker-compose-proof.yml", "deployment/docker/toxiproxy-proxies.json")
RUNTIME_PREFIXES = ("cmd/", "internal/", "lakehouse-traces/", "patches/")


def package_dir(test_ref: str) -> str:
    """internal/x/y_test.go#TestZ -> internal/x"""
    return os.path.dirname(test_ref.split("#", 1)[0])


def build_index(rows: list[dict], features: list[dict]) -> dict:
    """id -> {"dirs": package directories its tests sit in, "links": ids it owns (a feature's rows)}; features and rows alike."""
    idx: dict[str, dict] = {}
    for r in rows:
        tests = (r.get("refs") or {}).get("tests") or []
        idx[r["id"]] = {"kind": "row", "dirs": {package_dir(t) for t in tests if "/" in t}, "links": set()}
    for f in features:
        idx[f["id"]] = {"kind": "feature", "dirs": {package_dir(t) for t in (f.get("tests") or []) if "/" in t},
                        "links": set(f.get("rows") or [])}
    return idx


def shape_dirs(shape: dict, idx: dict) -> set[str]:
    """Package directories behind a shape: its covered items, and the rows those features list."""
    dirs: set[str] = set()
    for item in shape.get("covers") or []:
        e = idx.get(item)
        if not e:
            continue
        dirs |= e["dirs"]
        for linked in e["links"]:
            if linked in idx:
                dirs |= idx[linked]["dirs"]
    return {d for d in dirs if d}


def classify(path: str) -> str:
    if path.endswith("_test.go") or "/testdata/" in path or path.startswith(("docs/", "website/", ".github/", "charts/", "tests/")):
        return "ignored"
    if path == "scripts/bench/ab/shapes.json":
        return "shapes"
    if path.startswith(HARNESS):
        return "harness"
    if path in RUNTIME_ALL or os.path.basename(path).startswith("Dockerfile"):
        return "runtime"
    if path.startswith(RUNTIME_PREFIXES) and not path.startswith("lakehouse-traces/deps/") and "/deps/" not in path:
        return "code" if path.endswith(".go") or path.startswith("patches/") else "ignored"
    return "ignored"


def under(path: str, directory: str) -> bool:
    return path == directory or path.startswith(directory.rstrip("/") + "/")


def shapes_at(ref: str) -> dict:
    try:
        text = subprocess.run(["git", "show", f"{ref}:scripts/bench/ab/shapes.json"], cwd=abreg.ROOT, capture_output=True, text=True, check=True).stdout
        return json.loads(text)["sets"]
    except (subprocess.CalledProcessError, ValueError, KeyError):
        return {}


def changed_shapes(spec: dict, base: str | None, head: str | None = None) -> list[tuple[str, str]]:
    """(set, shape) pairs shapes.json adds or edits between base and head (default: the working tree)."""
    if not base:
        return []
    before = {(s, sh["name"]): sh for s, v in shapes_at(base).items() for sh in v["shapes"]}
    after_sets = shapes_at(head) if head else spec["sets"]
    return [(s, sh["name"]) for s, v in after_sets.items() for sh in v["shapes"] if before.get((s, sh["name"])) != sh]


def select(files: list[str], spec: dict, idx: dict, base: str | None = None, head: str | None = None) -> dict:
    chosen: dict[tuple[str, str], list[str]] = {}

    def add(set_name: str, shape_name: str, why: str) -> None:
        reasons = chosen.setdefault((set_name, shape_name), [])
        if why not in reasons:
            reasons.append(why)

    kinds = {f: classify(f) for f in files}
    relevant = [f for f, k in kinds.items() if k != "ignored"]
    smoke = [(s, sh) for s in CONTROL for sh in spec["sets"].get(s, {"shapes": []})["shapes"] if sh.get("smoke")]
    code = [f for f, k in kinds.items() if k == "code"]
    for set_name, sset in spec["sets"].items():
        for shape in sset["shapes"]:
            dirs = shape_dirs(shape, idx)
            for f in code:
                if any(under(f, d) for d in dirs):
                    add(set_name, shape["name"], f)
    for f in code:
        for prefix, sets in spec.get("areas", {}).items():
            if under(f, prefix.rstrip("/")):
                for set_name in sets:
                    for shape in spec["sets"][set_name]["shapes"]:
                        add(set_name, shape["name"], f"{f} (area {prefix})")
    for f, kind in kinds.items():
        if kind == "runtime":
            for set_name in CONTROL:
                for shape in spec["sets"][set_name]["shapes"]:
                    add(set_name, shape["name"], f)
        elif kind == "shapes":
            for set_name, shape_name in changed_shapes(spec, base, head):
                add(set_name, shape_name, "shape added or edited")
    if relevant:
        for set_name, shape in smoke:
            add(set_name, shape["name"], "control smoke")
    sets: dict[str, list[str]] = {}
    for set_name, sset in spec["sets"].items():
        names = [s["name"] for s in sset["shapes"] if (set_name, s["name"]) in chosen]
        if names:
            sets[set_name] = names
    return {"run": bool(sets), "changed": len(files), "relevant": relevant, "ignored": [f for f, k in kinds.items() if k == "ignored"],
            "sets": sets, "why": {f"{s}/{n}": r for (s, n), r in chosen.items()}}


def check(spec: dict, rows: list[dict], features: list[dict]) -> list[str]:
    """Problems of shapes.json against the registry; empty means the gate passes."""
    problems = []
    ids = {r["id"] for r in rows} | {f["id"] for f in features}
    idx = build_index(rows, features)
    import validate  # noqa: PLC0415
    ranges, layers = spec.get("ranges", {}), spec.get("layers", {})
    for need in ("control-logs", "control-traces"):
        sset = spec["sets"].get(need)
        if not sset:
            problems.append(f"shapes.json: the control set '{need}' is missing")
        elif not any(s.get("smoke") for s in sset["shapes"]):
            problems.append(f"shapes.json: the control set '{need}' must mark at least one shape \"smoke\": true")
    for prefix, sets in spec.get("areas", {}).items():
        for s in sets:
            if s not in spec["sets"]:
                problems.append(f"shapes.json: area '{prefix}' names the unknown set '{s}'")
    for set_name, sset in spec["sets"].items():
        names = set()
        for lay in sset.get("layers", []):
            if lay not in layers:
                problems.append(f"shapes.json: {set_name} uses the unknown layer '{lay}'")
        for rng in sset.get("ranges", []):
            if rng not in ranges:
                problems.append(f"shapes.json: {set_name} uses the unknown range '{rng}'")
        for shape in sset["shapes"]:
            where = f"shapes.json: {set_name}/{shape.get('name')}"
            if shape["name"] in names:
                problems.append(f"{where} is defined twice in the set")
            names.add(shape["name"])
            if shape.get("smoke") and set_name not in CONTROL:
                problems.append(f"{where} is marked smoke outside the control sets")
            if shape.get("signal") not in ("logs", "traces"):
                problems.append(f"{where} has no signal (logs or traces)")
            if shape.get("api", "logsql") not in API_TYPES:
                problems.append(f"{where} has an unknown api '{shape.get('api')}'")
            if shape.get("kind") not in validate.EXTRACTORS:
                problems.append(f"{where} has an unknown kind '{shape.get('kind')}'")
            if shape.get("pick") and shape["pick"] not in PICKS:
                problems.append(f"{where} has an unknown pick '{shape['pick']}'")
            if shape.get("window", "start-end") not in WINDOWS:
                problems.append(f"{where} has an unknown window '{shape.get('window')}'")
            for lay in shape.get("layers", []):
                if lay not in layers:
                    problems.append(f"{where} uses the unknown layer '{lay}'")
            for rng in shape.get("ranges", []):
                if rng not in ranges:
                    problems.append(f"{where} uses the unknown range '{rng}'")
            if shape.get("tenant", "numeric") not in ("numeric", "numeric1001", "alias"):
                problems.append(f"{where} has an unknown tenant form '{shape.get('tenant')}'")
            covers = shape.get("covers") or []
            if not covers:
                problems.append(f"{where} covers nothing: name the registry row or feature it measures")
            for item in covers:
                if item not in ids:
                    problems.append(f"{where} covers the unknown registry id '{item}'")
            if set_name not in CONTROL and covers and not shape_dirs(shape, idx) and not any(set_name in v for v in spec.get("areas", {}).values()):
                problems.append(f"{where} cannot be selected by any change: none of its covered items names a test package and no "
                                f"area lists the set '{set_name}'; cover a feature with tests or add the set to `areas`")
    return problems


def changed_files(base: str, head: str = "HEAD") -> list[str]:
    out = subprocess.run(["git", "diff", "--name-only", f"{base}...{head}"], cwd=abreg.ROOT, capture_output=True, text=True, check=True).stdout
    return [line for line in out.splitlines() if line]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default="")
    ap.add_argument("--head", default="HEAD")
    ap.add_argument("--files", nargs="*")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--out", default="")
    a = ap.parse_args(argv)
    spec = abreg.load_json(abreg.SHAPES)
    rows, features = abreg.load_rows(), abreg.load_features()
    if a.check:
        problems = check(spec, rows, features)
        for p in problems:
            print(p)
        sets = len(spec["sets"])
        shapes = sum(len(s["shapes"]) for s in spec["sets"].values())
        print(f"scripts/bench/ab selection: {'FAILED' if problems else 'ok'} ({len(problems)} problem(s); {shapes} shapes in {sets} sets)")
        return 1 if problems else 0
    base = a.base or "origin/main"
    files = a.files if a.files is not None else changed_files(base, a.head)
    result = select(files, spec, build_index(rows, features), base=base, head=None if a.head == "HEAD" else a.head)
    text = json.dumps(result, indent=1)
    if a.out:
        abreg.write_text(a.out, text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
