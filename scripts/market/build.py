#!/usr/bin/env python3
"""Build the market comparison from docs/market/data.

The data is one YAML file per system (docs/market/data/systems/*.yaml) plus
meta.yaml (sections, dimensions, groups, icon legend, source labels) and
summary.yaml (Lakehouse strengths, weaknesses, threats, fix list). Everything
a reader sees is generated from it:

  docs/market-comparison.md            the docs page (matrices + footnotes + prose)
  website/static/market/index.html     the interactive matrix (filters, sources, changes)
  docs/market/snapshots/<date>.json    a frozen copy, written with --snapshot

Commands:
  build.py                 validate and regenerate the page and the HTML
  build.py --check         validate and fail if the generated files are out of date
  build.py --snapshot      also freeze today's data as docs/market/snapshots/<date>.json
  build.py --diff A B      print the cell changes between two snapshots (paths or dates)
  build.py --stale         list cells whose `checked` date is older than meta.stale_after_days
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import re
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
DATA = ROOT / "docs" / "market" / "data"
PROSE = ROOT / "docs" / "market" / "prose"
SNAPSHOTS = ROOT / "docs" / "market" / "snapshots"
DOC_OUT = ROOT / "docs" / "market-comparison.md"
HTML_OUT = ROOT / "website" / "static" / "market" / "index.html"
TEMPLATE = pathlib.Path(__file__).with_name("page.html")

REQUIRED_CELL_KEYS = ("label", "checked")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class DataError(Exception):
    pass


def load(data_dir: pathlib.Path = DATA) -> dict:
    """Load and validate the data set; return one dict ready for rendering."""
    meta = yaml.safe_load((data_dir / "meta.yaml").read_text())
    summary = yaml.safe_load((data_dir / "summary.yaml").read_text())
    systems = {}
    for path in sorted((data_dir / "systems").glob("*.yaml")):
        doc = yaml.safe_load(path.read_text())
        systems[path.stem] = doc
    validate(meta, systems, summary)
    return {"meta": meta, "systems": systems, "summary": summary}


def validate(meta: dict, systems: dict, summary: dict) -> None:
    errors: list[str] = []
    icons = set(meta["icons"])
    labels = set(meta["labels"])
    dims = {d["id"] for d in meta["dimensions"]}
    sections = list(meta["sections"])
    for d in meta["dimensions"]:
        if d["section"] not in sections:
            errors.append(f"dimension {d['id']}: unknown section {d['section']!r}")
    grouped = [s for g in meta["groups"] for s in g["systems"]]
    for s in grouped:
        if s not in systems:
            errors.append(f"group lists {s!r} but docs/market/data/systems/{s}.yaml does not exist")
    for s in systems:
        if s not in grouped:
            errors.append(f"systems/{s}.yaml is not listed in any group in meta.yaml")
    if len(grouped) != len(set(grouped)):
        errors.append("a system is listed in more than one group")
    for sid, doc in systems.items():
        for key in ("name", "group", "reviewed", "cells"):
            if key not in doc:
                errors.append(f"{sid}: missing {key!r}")
        if not DATE_RE.match(str(doc.get("reviewed", ""))):
            errors.append(f"{sid}: reviewed must be YYYY-MM-DD")
        for dim, cell in (doc.get("cells") or {}).items():
            where = f"{sid}.{dim}"
            if dim not in dims:
                errors.append(f"{where}: unknown dimension")
                continue
            for key in REQUIRED_CELL_KEYS:
                if not cell.get(key):
                    errors.append(f"{where}: missing {key!r}")
            if not cell.get("icon") and not cell.get("text"):
                errors.append(f"{where}: a cell needs an icon, a text, or both")
            if cell.get("icon") and cell["icon"] not in icons:
                errors.append(f"{where}: icon {cell['icon']!r} is not in the legend")
            if cell.get("label") and cell["label"] not in labels:
                errors.append(f"{where}: label {cell['label']!r} is not one of {sorted(labels)}")
            if cell.get("label") != "unverified" and not cell.get("source"):
                errors.append(f"{where}: a {cell.get('label')} cell needs a source")
            if cell.get("checked") and not DATE_RE.match(str(cell["checked"])):
                errors.append(f"{where}: checked must be YYYY-MM-DD")
    for key in ("strengths", "weaknesses", "threats", "fix_list"):
        if key not in summary:
            errors.append(f"summary.yaml: missing {key!r}")
    if errors:
        raise DataError("market data is invalid:\n  " + "\n  ".join(errors))


def flatten(ds: dict) -> dict:
    """The structure the HTML page and the snapshots use."""
    meta, systems = ds["meta"], ds["systems"]
    return {
        "generated": max(str(s["reviewed"]) for s in systems.values()),
        "icons": meta["icons"],
        "labels": meta["labels"],
        "stale_after_days": meta.get("stale_after_days", 120),
        "dimensions": meta["dimensions"],
        "groups": [{"name": g["name"], "systems": [systems[s]["name"] for s in g["systems"]]} for g in meta["groups"]],
        "cells": {systems[sid]["name"]: systems[sid]["cells"] for g in meta["groups"] for sid in g["systems"]},
        "summary": ds["summary"],
    }


def md_escape(text: str) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def render_markdown(ds: dict) -> str:
    flat = flatten(ds)
    meta = ds["meta"]
    out: list[str] = []
    out.append("<!-- Generated by scripts/market/build.py from docs/market/data. Edit the data, not this file. -->\n")
    out.append(f"# {meta['title']}\n")
    out.append(f"Data reviewed up to **{flat['generated']}**. Every cell carries its source and one of these labels:\n")
    for k, v in meta["labels"].items():
        out.append(f"- **{k}**: {v}")
    out.append("\nIcons: " + " · ".join(f"{k} {v}" for k, v in meta["icons"].items()) + "\n")
    out.append("The interactive version, with filters, sources and changes since the last snapshot, is at "
               "[the market matrix](https://reliablyobserve.github.io/victoria-lakehouse/market/) "
               "(source: `website/static/market/index.html`).\n")
    for name in ("summary", "interfaces"):
        p = PROSE / f"{name}.md"
        if p.exists():
            out.append(p.read_text().rstrip() + "\n")
    s = ds["summary"]
    out.append("## Summary\n")
    for title, key in (("Where Lakehouse leads", "strengths"), ("Where Lakehouse falls short", "weaknesses"),
                       ("Competitive threats", "threats")):
        out.append(f"### {title}\n")
        out.extend(f"- {md_escape(x)}" for x in s[key])
        out.append("")
    out.append("### Fix list, in order\n")
    for i, x in enumerate(s["fix_list"], 1):
        issue = f" ({x['issue']})" if x.get("issue") else ""
        out.append(f"{i}. {md_escape(x['item'])}{issue}")
    out.append("")
    notes: list[str] = []
    for section in meta["sections"]:
        dims = [d for d in meta["dimensions"] if d["section"] == section]
        out.append(f"## {section}\n")
        rules = [d for d in dims if d.get("counting_rule")]
        for d in rules:
            out.append(f"- **{d['label']}**: {md_escape(d['counting_rule'])}")
        if rules:
            out.append("")
        out.append("| System | " + " | ".join(md_escape(d["label"]) for d in dims) + " |")
        out.append("|---" * (len(dims) + 1) + "|")
        for g in flat["groups"]:
            out.append(f"| **{md_escape(g['name'])}** |" + " |" * len(dims))
            for name in g["systems"]:
                row = []
                for d in dims:
                    c = flat["cells"][name].get(d["id"])
                    if not c:
                        row.append("—")
                        continue
                    notes.append(f"{md_escape(name)} · {md_escape(d['label'])}: {md_escape(c.get('note') or c.get('text') or c.get('icon'))} "
                                 f"[{c['label']}; checked {c['checked']}]" + (f" Source: {c['source']}" if c.get("source") else ""))
                    row.append(f"{c.get('icon', '')} {md_escape(c.get('text', ''))}".strip() + f"[^{len(notes)}]")
                out.append(f"| {md_escape(name)} | " + " | ".join(row) + " |")
        out.append("")
    for name in ("positions", "method"):
        p = PROSE / f"{name}.md"
        if p.exists():
            out.append(p.read_text().rstrip() + "\n")
    out.append("## Notes and sources\n")
    out.extend(f"[^{i}]: {n}" for i, n in enumerate(notes, 1))
    return "\n".join(out) + "\n"


def snapshot_list() -> list[dict]:
    snaps = []
    for p in sorted(SNAPSHOTS.glob("*.json")):
        snaps.append({"date": p.stem, "cells": json.loads(p.read_text())["cells"]})
    return snaps


def render_html(ds: dict) -> str:
    flat = flatten(ds)
    flat["snapshots"] = [s for s in snapshot_list() if s["date"] < flat["generated"]] or []
    blob = json.dumps(flat, ensure_ascii=False, sort_keys=True).replace("</", "<\\/")
    return TEMPLATE.read_text().replace("/*__DATA__*/null", blob)


def diff(a: dict, b: dict) -> list[str]:
    """Human-readable cell changes from snapshot a to snapshot b."""
    lines = []
    for name in sorted(set(a["cells"]) | set(b["cells"])):
        if name not in a["cells"]:
            lines.append(f"+ {name}: added")
            continue
        if name not in b["cells"]:
            lines.append(f"- {name}: removed")
            continue
        ca, cb = a["cells"][name], b["cells"][name]
        for dim in sorted(set(ca) | set(cb)):
            x, y = ca.get(dim), cb.get(dim)
            key = lambda c: (c.get("icon", ""), c.get("text", ""))
            if x and y and key(x) == key(y):
                continue
            fx = " ".join(key(x)).strip() if x else "(none)"
            fy = " ".join(key(y)).strip() if y else "(none)"
            lines.append(f"~ {name} · {dim}: {fx} → {fy}")
    return lines


def stale(ds: dict, today: dt.date | None = None) -> list[str]:
    today = today or dt.date.today()
    limit = int(ds["meta"].get("stale_after_days", 120))
    out = []
    for sid, doc in ds["systems"].items():
        for dim, c in doc["cells"].items():
            age = (today - dt.date.fromisoformat(str(c["checked"]))).days
            if age > limit:
                out.append(f"{doc['name']} · {dim}: checked {c['checked']} ({age} days ago)")
    return out


def load_snapshot(ref: str) -> dict:
    p = pathlib.Path(ref)
    if not p.exists():
        p = SNAPSHOTS / f"{ref}.json"
    return json.loads(p.read_text())


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--snapshot", action="store_true")
    ap.add_argument("--diff", nargs=2, metavar=("A", "B"))
    ap.add_argument("--stale", action="store_true")
    args = ap.parse_args(argv)

    if args.diff:
        changes = diff(load_snapshot(args.diff[0]), load_snapshot(args.diff[1]))
        print("\n".join(changes) if changes else "no cell changes")
        return 0
    try:
        ds = load()
    except DataError as e:
        print(e, file=sys.stderr)
        return 1
    if args.stale:
        rows = stale(ds)
        print("\n".join(rows) if rows else "no stale cells")
        return 1 if rows else 0
    if args.snapshot:
        flat = flatten(ds)
        SNAPSHOTS.mkdir(parents=True, exist_ok=True)
        (SNAPSHOTS / f"{flat['generated']}.json").write_text(
            json.dumps({"generated": flat["generated"], "cells": flat["cells"]}, ensure_ascii=False, indent=1, sort_keys=True) + "\n")
    md, html = render_markdown(ds), render_html(ds)
    if args.check:
        stale_files = [str(p.relative_to(ROOT)) for p, want in ((DOC_OUT, md), (HTML_OUT, html))
                       if not p.exists() or p.read_text() != want]
        if stale_files:
            print("generated market files are out of date; run python3 scripts/market/build.py:\n  "
                  + "\n  ".join(stale_files), file=sys.stderr)
            return 1
        print(f"market data ok: {len(ds['systems'])} systems, "
              f"{sum(len(s['cells']) for s in ds['systems'].values())} cells")
        return 0
    DOC_OUT.write_text(md)
    HTML_OUT.parent.mkdir(parents=True, exist_ok=True)
    HTML_OUT.write_text(html)
    print(f"wrote {DOC_OUT.relative_to(ROOT)} and {HTML_OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
