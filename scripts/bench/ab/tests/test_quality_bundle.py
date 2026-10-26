"""quality_bundle.py: the schema validator, the artifact manifest and its verification, the Markdown helpers."""

import copy
import json
import os
import pathlib

import pytest

import quality
import quality_bundle as qb
from test_quality import snap

SHA = "a" * 40


def doc(**kw):
    return quality.build_doc(snap(), snap(**kw), {"root": {"measured": ["internal/a"], "left_out": []}}, {"runs_seen": 1, "reran_runs": 0, "items": []}, ["note"])


def write_bundle(d, **kw):
    out = str(d)
    dd = doc(**kw)
    from quality import abreg
    abreg.dump_json(os.path.join(out, qb.QUALITY_FILE), dd)
    abreg.write_text(os.path.join(out, qb.REPORT_FILE), quality.render_doc(dd)[0])
    abreg.dump_json(os.path.join(out, qb.MANIFEST_FILE), qb.manifest_for(out, 5, SHA, SHA, SHA, "99", 1, dd["verdict"]))
    return out


def test_a_built_document_matches_the_schema():
    assert qb.validate_quality(doc()) == []
    assert qb.validate_quality(doc(lint={"root": {"new": 1, "findings": [{"file": "a.go", "line": 1, "linter": "x", "text": "t"}]}})) == []


@pytest.mark.parametrize("mutate,needle", [
    (lambda d: d.update(schema_version=2), "must be 1"),
    (lambda d: d.pop("verdict"), "missing 'verdict'"),
    (lambda d: d.update(extra=1), "unexpected property 'extra'"),
    (lambda d: d["pr"]["coverage"].update(root={"internal/a": 120.0}), "above 100"),
    (lambda d: d["pr"]["go"]["root"].update(tests=-1), "below 0"),
    (lambda d: d["pr"]["go"]["root"].update(tests="3"), "expected integer"),
    (lambda d: d["pr"]["go"]["root"].update(tests=True), "expected integer"),
    (lambda d: d["pr"]["registry"]["by_expect"].update(x="1"), "expected integer"),
    (lambda d: d["verdict"].update(exit=2), "not one of"),
    (lambda d: d["verdict"].update(gates=["x" * 401]), "longer than 400"),
    (lambda d: d["changed"]["root"].update(measured=["p"] * 51), "more than 50 items"),
    (lambda d: d["pr"].update(parity={"source": "elsewhere"}), "matches none"),
    (lambda d: d["pr"]["timing"].update(root={"internal/a": {"seconds": 1, "status": "weird"}}), "not one of"),
    (lambda d: d["pr"]["registry"]["known_differences"].update({str(i): 1 for i in range(30)}), "more than 20 properties"),
])
def test_the_schema_rejects_malformed_documents(mutate, needle):
    d = copy.deepcopy(doc())
    mutate(d)
    errs = qb.validate_quality(d)
    assert errs and any(needle in e for e in errs), errs


def test_the_manifest_schema():
    good = {"schema_version": 1, "kind": "lh-pr-quality", "pr_number": 5, "head_sha": SHA, "base_sha": SHA, "merge_base_sha": SHA, "run_id": "99", "run_attempt": 1,
            "files": {"quality.json": {"sha256": "b" * 64, "bytes": 10}, "pr-quality.md": {"sha256": "c" * 64, "bytes": 10}}, "verdict": {"failed": False, "gates": 0}}
    assert qb.validate_manifest(good) == []
    for key, bad in (("head_sha", "XYZ"), ("pr_number", 0), ("run_id", "12 ; rm"), ("kind", "other"), ("run_attempt", "1")):
        d = copy.deepcopy(good)
        d[key] = bad
        assert qb.validate_manifest(d), key
    d = copy.deepcopy(good)
    d["files"]["quality.json"]["sha256"] = "g" * 64
    assert qb.validate_manifest(d)


def test_verify_bundle_accepts_a_good_artifact_and_rejects_every_tampering(tmp_path):
    out = write_bundle(tmp_path)
    assert qb.verify_bundle(out) == []
    # edited report
    p = os.path.join(out, qb.REPORT_FILE)
    orig = pathlib.Path(p).read_text()
    pathlib.Path(p).write_text(orig + "<!-- injected -->")
    assert any("does not match its manifest" in e for e in qb.verify_bundle(out))
    pathlib.Path(p).write_text(orig)
    # edited data (valid schema, wrong digest)
    q = os.path.join(out, qb.QUALITY_FILE)
    data = json.loads(pathlib.Path(q).read_text())
    data["notes"] = ["changed"]
    pathlib.Path(q).write_text(json.dumps(data))
    assert any("does not match its manifest" in e for e in qb.verify_bundle(out))
    # schema-invalid data
    data["schema_version"] = 9
    pathlib.Path(q).write_text(json.dumps(data))
    assert any("quality" in e for e in qb.verify_bundle(out))
    # malformed JSON
    pathlib.Path(q).write_text("{not json")
    assert any("unreadable JSON" in e for e in qb.verify_bundle(out))


def test_verify_bundle_rejects_extra_files_symlinks_and_oversize(tmp_path):
    out = write_bundle(tmp_path)
    pathlib.Path(out, "evil.sh").write_text("x")
    assert "expected exactly" in qb.verify_bundle(out)[0]
    os.remove(os.path.join(out, "evil.sh"))
    os.remove(os.path.join(out, qb.REPORT_FILE))
    os.symlink("/etc/passwd", os.path.join(out, qb.REPORT_FILE))
    assert "not a regular file" in qb.verify_bundle(out)[0]
    os.remove(os.path.join(out, qb.REPORT_FILE))
    with open(os.path.join(out, qb.REPORT_FILE), "w") as f:
        f.truncate(qb.MAX_FILE_BYTES + 1)
    assert "larger than" in qb.verify_bundle(out)[0]


def test_verify_bundle_rejects_a_manifest_verdict_that_differs(tmp_path):
    out = write_bundle(tmp_path, lint={"root": {"new": 2, "findings": []}})
    m = os.path.join(out, qb.MANIFEST_FILE)
    data = json.loads(pathlib.Path(m).read_text())
    data["verdict"] = {"failed": False, "gates": 0}
    pathlib.Path(m).write_text(json.dumps(data))
    assert any("verdict differs" in e for e in qb.verify_bundle(out))


def test_md_helpers_neutralise_markup_and_bound_length():
    import re
    entity = re.compile(r"&#\d+;")
    for raw in ("@team hi", "<b>x</b>", "a|b", "#12", "[x](http://e.vil)", "a*b_c~d`e\\f!"):
        out = entity.sub("", qb.md_safe(raw))  # what is left once the entities are removed
        assert not any(ch in out for ch in "@<>|#[]*_~`\\!"), (raw, out)
    assert "://" not in qb.md_safe("https://x.y") and "www." not in qb.md_safe("www.x.y").lower()
    assert qb.md_safe("a\nb\x00c\u202ed") == "a bcd"
    assert len(qb.md_safe("x" * 1000, 50)) == 50
    assert qb.md_code("a`b|c") == "`a'b\\|c`" and qb.md_code("") == "``" and "\n" not in qb.md_code("a\nb")
    assert len(qb.md_code("y" * 1000, 40)) == 42
