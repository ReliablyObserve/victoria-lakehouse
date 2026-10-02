import copy
import datetime as dt
import pathlib
import tempfile
import unittest

import yaml

from scripts.market import build


def write_dataset(root: pathlib.Path, meta: dict, systems: dict, summary: dict) -> pathlib.Path:
    data = root / "data"
    (data / "systems").mkdir(parents=True)
    (data / "meta.yaml").write_text(yaml.safe_dump(meta, allow_unicode=True))
    (data / "summary.yaml").write_text(yaml.safe_dump(summary, allow_unicode=True))
    for sid, doc in systems.items():
        (data / "systems" / f"{sid}.yaml").write_text(yaml.safe_dump(doc, allow_unicode=True))
    return data


META = {
    "title": "t",
    "icons": {"✅": "yes", "❌": "no"},
    "labels": {"docs": "d", "unverified": "u", "repo": "r"},
    "stale_after_days": 30,
    "sections": ["Ingest"],
    "dimensions": [{"id": "in_otlp", "section": "Ingest", "label": "OTLP"}],
    "groups": [{"name": "G", "systems": ["a", "b"]}],
}
SYSTEMS = {
    "a": {"name": "A", "group": "G", "reviewed": "2026-10-02",
          "cells": {"in_otlp": {"icon": "✅", "text": "yes", "label": "docs", "source": "https://a", "checked": "2026-10-02"}}},
    "b": {"name": "B", "group": "G", "reviewed": "2026-10-02",
          "cells": {"in_otlp": {"icon": "❌", "label": "unverified", "checked": "2026-10-02"}}},
}
SUMMARY = {"strengths": ["s"], "weaknesses": ["w"], "threats": ["t"], "fix_list": [{"item": "f", "issue": "#1"}]}


class ValidateTests(unittest.TestCase):
    def load(self, meta=META, systems=SYSTEMS, summary=SUMMARY):
        with tempfile.TemporaryDirectory() as d:
            return build.load(write_dataset(pathlib.Path(d), meta, systems, summary))

    def test_valid_dataset_loads(self):
        ds = self.load()
        self.assertEqual(set(ds["systems"]), {"a", "b"})

    def test_icon_outside_the_legend_is_rejected(self):
        s = copy.deepcopy(SYSTEMS)
        s["a"]["cells"]["in_otlp"]["icon"] = "🟣"
        with self.assertRaisesRegex(build.DataError, "not in the legend"):
            self.load(systems=s)

    def test_sourced_cell_without_source_is_rejected(self):
        s = copy.deepcopy(SYSTEMS)
        del s["a"]["cells"]["in_otlp"]["source"]
        with self.assertRaisesRegex(build.DataError, "needs a source"):
            self.load(systems=s)

    def test_unknown_dimension_label_and_bad_date_are_rejected(self):
        s = copy.deepcopy(SYSTEMS)
        s["a"]["cells"]["bogus"] = {"icon": "✅", "label": "docs", "source": "x", "checked": "2026-10-02"}
        s["b"]["cells"]["in_otlp"]["label"] = "rumour"
        s["b"]["cells"]["in_otlp"]["checked"] = "Oct 2"
        with self.assertRaises(build.DataError) as e:
            self.load(systems=s)
        msg = str(e.exception)
        self.assertIn("a.bogus: unknown dimension", msg)
        self.assertIn("label 'rumour'", msg)
        self.assertIn("checked must be YYYY-MM-DD", msg)

    def test_system_missing_from_groups_and_group_without_file_are_rejected(self):
        m = copy.deepcopy(META)
        m["groups"] = [{"name": "G", "systems": ["a", "ghost"]}]
        with self.assertRaises(build.DataError) as e:
            self.load(meta=m)
        self.assertIn("'ghost'", str(e.exception))
        self.assertIn("systems/b.yaml is not listed", str(e.exception))

    def test_empty_cell_is_rejected(self):
        s = copy.deepcopy(SYSTEMS)
        s["b"]["cells"]["in_otlp"].pop("icon")
        with self.assertRaisesRegex(build.DataError, "needs an icon, a text, or both"):
            self.load(systems=s)


class DiffAndStaleTests(unittest.TestCase):
    def test_diff_reports_changed_added_and_removed(self):
        a = {"cells": {"A": {"x": {"icon": "❌", "text": "no"}}, "Gone": {}}}
        b = {"cells": {"A": {"x": {"icon": "✅", "text": "yes"}, "y": {"icon": "✅"}}, "New": {}}}
        lines = build.diff(a, b)
        self.assertIn("~ A · x: ❌ no → ✅ yes", lines)
        self.assertIn("~ A · y: (none) → ✅", lines)
        self.assertIn("+ New: added", lines)
        self.assertIn("- Gone: removed", lines)

    def test_identical_snapshots_have_no_diff(self):
        a = {"cells": {"A": {"x": {"icon": "✅", "text": "yes", "note": "n1"}}}}
        b = {"cells": {"A": {"x": {"icon": "✅", "text": "yes", "note": "n2"}}}}
        self.assertEqual(build.diff(a, b), [])

    def test_stale_lists_cells_older_than_the_window(self):
        with tempfile.TemporaryDirectory() as d:
            ds = build.load(write_dataset(pathlib.Path(d), META, SYSTEMS, SUMMARY))
        self.assertEqual(build.stale(ds, today=dt.date(2026, 10, 20)), [])
        rows = build.stale(ds, today=dt.date(2026, 12, 1))
        self.assertEqual(len(rows), 2)
        self.assertIn("A · in_otlp: checked 2026-10-02", rows[0] + rows[1])


class RepositoryDataTests(unittest.TestCase):
    """The committed data validates and the generated files match it."""

    def test_committed_data_is_valid_and_generated_files_are_current(self):
        self.assertEqual(build.main(["--check"]), 0)

    def test_lakehouse_is_listed_first(self):
        ds = build.load()
        self.assertEqual(ds["meta"]["groups"][0]["name"], "Victoria Lakehouse")

    def test_markdown_has_a_footnote_for_every_cell(self):
        ds = build.load()
        md = build.render_markdown(ds)
        cells = sum(len(s["cells"]) for s in ds["systems"].values())
        self.assertEqual(md.count("\n[^"), cells)


if __name__ == "__main__":
    unittest.main()
