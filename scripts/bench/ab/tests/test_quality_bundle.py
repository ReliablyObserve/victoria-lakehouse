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


def manifest(verdict="pass", refs=()):
    return qb.proof_manifest(5, SHA, verdict, list(refs))


def test_the_manifest_matches_the_commenters_contract():
    assert qb.check_manifest(manifest()) == []
    assert manifest("fail", [3, 3, 1])["allowed_issue_refs"] == [1, 3]
    good = manifest()
    for key, bad in (("schema_version", 2), ("pr_number", 0), ("pr_number", True), ("head_sha", "XYZ"), ("head_sha", "A" * 40), ("images", [{"name": "x.png"}]),
                     ("comments", []), ("comments", [{"kind": "perf-smoke", "verdict": "pass"}]), ("comments", [{"kind": "pr-quality", "verdict": "ok"}]),
                     ("comments", [{"kind": "pr-quality", "verdict": "pass", "x": 1}]), ("allowed_issue_refs", [0]), ("allowed_issue_refs", [1, 1]),
                     ("allowed_issue_refs", list(range(1, 22))), ("allowed_issue_refs", "1")):
        d = copy.deepcopy(good)
        d[key] = bad
        assert qb.check_manifest(d), (key, bad)
    d = copy.deepcopy(good)
    d["extra"] = 1
    assert qb.check_manifest(d)
    d = copy.deepcopy(good)
    del d["images"]
    assert qb.check_manifest(d) and qb.check_manifest([]) and qb.check_manifest(None)


def test_the_manifest_equals_the_commenters_schema_when_that_is_in_the_tree():
    path = os.path.join(quality.abreg.ROOT, "scripts", "ci", "proof-comment-manifest.schema.json")
    if not os.path.exists(path):
        pytest.skip("the commenter's schema is not in this tree yet")
    schema = json.loads(pathlib.Path(path).read_text())
    for doc in (manifest(), manifest("skipped", [1, 2])):
        assert qb.validate(doc, schema) == []
    bad = manifest()
    bad["comments"][0]["kind"] = "perf-smoke"
    assert qb.check_manifest(bad)


def test_every_rendered_body_passes_the_body_rules():
    evil = "@everyone [x](http://evil.example) <img src=x onerror=1> www.evil.example #1 | `x"
    pr = snap(lint={"root": {"new": 1, "findings": [{"file": evil, "line": 1, "linter": evil, "text": evil}]}})
    pr["parity"]["known"] = [{"test": evil, "reason": evil, "issues": ["victoria-lakehouse#12", "loki-vl-proxy#7"]}]
    bodies = [quality.render(snap(), snap())[0], quality.render(snap(), pr, {"root": {"measured": [evil], "left_out": [evil]}}, {"runs_seen": 1, "reran_runs": 1, "items": [
        {"workflow": evil, "job": evil, "failed_attempt": 1, "passed_attempt": 2, "tests": [evil]}]}, [evil])[0],
        quality.render(snap(cov={}), snap(cov={"root": {"internal/new": 50.0}}), None, None, None)[0],
        quality.placeholder_body("skipped", evil), quality.placeholder_body("incomplete", "the job failed")]
    for b in bodies:
        assert qb.check_body(b, quality.allowed_refs(pr)) == [], b[:200]
    assert "#12" in bodies[1] and "loki-vl-proxy#" in bodies[1] and quality.allowed_refs(pr) == [12]


@pytest.mark.parametrize("body,needle", [
    ("no marker\n", "marker"),
    ("<!-- lh-pr-quality -->\n<script>x</script>", "tag not allowed"),
    ("<!-- lh-pr-quality -->\n<details open>", "tag not allowed"),
    ("<!-- lh-pr-quality -->\n<img src=x>", "tag not allowed"),
    ("<!-- lh-pr-quality -->\n![a](image:x.png)", "image"),
    ("<!-- lh-pr-quality -->\n[x](https://evil.example/a)", "link not allowed"),
    ("<!-- lh-pr-quality -->\n[x](https://github.com/o/r/blob/main/x)", "link not allowed"),
    ("<!-- lh-pr-quality -->\nsee #99", "not in allowed_issue_refs"),
    ("<!-- lh-pr-quality -->\nping @someone", "mention"),
    ("<!-- lh-pr-quality -->\nhttps://evil.example", "URL"),
    ("<!-- lh-pr-quality -->\n" + "x" * 65000, "above"),
])
def test_check_body_rejects_what_the_commenter_would_neutralise(body, needle):
    assert any(needle in e for e in qb.check_body(body, [])), qb.check_body(body, [])


def test_check_body_accepts_the_allowed_forms():
    ok = ("<!-- lh-pr-quality -->\n### PR quality: GREEN\n\n<details><summary>t</summary>\n\n| a | b |\n|---|---|\n| `@x #9 <b>` | 1 |\n\n</details>\n<sub>s</sub>\n"
          "see #12 and [this run](https://github.com/ReliablyObserve/victoria-lakehouse/actions/runs/123) and [i](https://github.com/o/r/issues/5)\n")
    assert qb.check_body(ok, [12]) == []


def write_artifact(d, **kw):
    pr = snap(**kw)
    text, verdict = quality.render(snap(), pr)
    qb.write_comment_artifact(str(d), qb.proof_manifest(5, SHA, "fail" if verdict["failed"] else "pass", quality.allowed_refs(pr)), text)
    return str(d)


def test_write_and_verify_the_comment_artifact(tmp_path):
    out = write_artifact(tmp_path / "a")
    assert sorted(os.listdir(out)) == ["manifest.json", "pr-quality.md"] and qb.verify_comment_artifact(out) == []
    p = pathlib.Path(out, qb.REPORT_FILE)
    p.write_text(p.read_text() + "<script>")
    assert any("tag not allowed" in e for e in qb.verify_comment_artifact(out))
    p.write_text("<!-- lh-pr-quality -->\nx\n")
    m = pathlib.Path(out, qb.MANIFEST_FILE)
    m.write_text("{not json")
    assert "unreadable" in qb.verify_comment_artifact(out)[0]


def test_write_refuses_a_body_or_manifest_that_breaks_the_contract(tmp_path):
    with pytest.raises(ValueError, match="tag not allowed"):
        qb.write_comment_artifact(str(tmp_path / "x"), manifest(), "<!-- lh-pr-quality -->\n<script>")
    with pytest.raises(ValueError, match="pr_number"):
        qb.write_comment_artifact(str(tmp_path / "y"), qb.proof_manifest(0, SHA, "pass", []), "<!-- lh-pr-quality -->\nok\n")
    assert not os.path.exists(tmp_path / "x") and not os.path.exists(tmp_path / "y")


def test_verify_rejects_extra_files_symlinks_and_oversize(tmp_path):
    out = write_artifact(tmp_path / "a")
    pathlib.Path(out, "quality.json").write_text("{}")
    assert "expected exactly" in qb.verify_comment_artifact(out)[0]
    os.remove(os.path.join(out, "quality.json"))
    os.remove(os.path.join(out, qb.REPORT_FILE))
    os.symlink("/etc/passwd", os.path.join(out, qb.REPORT_FILE))
    assert "not a regular file" in qb.verify_comment_artifact(out)[0]
    os.remove(os.path.join(out, qb.REPORT_FILE))
    with open(os.path.join(out, qb.REPORT_FILE), "w") as f:
        f.truncate(qb.MAX_FILE_BYTES + 1)
    assert "not a regular file" in qb.verify_comment_artifact(out)[0]


def test_md_helpers_neutralise_markup_and_bound_length():
    z = qb.ZWSP
    out = qb.md_safe("@team [x](http://e.vil) <b>x</b> www.e.vil #12 a|b *c* _d_ ~e~ `f` GH-7")
    for bad in ("@team", "[x]", "<b>", "http://", "www.e", "#12", "a|b", "*c*", "_d_", "~e~", "`", "GH-7"):
        assert bad not in out, (bad, out)
    assert out.startswith(f"@{z}team &#91;x&#93;(http:{z}//e.vil) &lt;b>") and "\\|" in out
    assert qb.md_safe("a\nb\x00c\u202ed") == "a bcd" and len(qb.md_safe("x" * 1000, 50)) == 50
    assert qb.md_code("a`b|c") == "`a'b\\|c`" and qb.md_code("") == "``" and "\n" not in qb.md_code("a\nb")
    assert qb.md_code("@x #3") == f"`@{z}x #{z}3`" and len(qb.md_code("y" * 1000, 40)) == 42


def test_md_helpers_are_idempotent_and_stable_under_the_commenters_sanitiser():
    samples = ["@everyone [x](http://evil.example) <img src=x onerror=1> www.evil.example #1 | `x", "&#64;a &amp; &lt; &gt; &#91; &#93; &copy; & #", "GH-12 owner/repo#3 a@b.c", "![i](image:x.png) <details> </sub>",
               "https://github.com/ReliablyObserve/victoria-lakehouse/issues/5 [t](https://github.com/ReliablyObserve/victoria-lakehouse/pull/6)", "\\ * _ ~ \\|"]
    for t in samples:
        assert qb._neutral(qb._neutral(t)) == qb._neutral(t), t
    path = os.path.join(quality.abreg.ROOT, "scripts", "ci", "proof_comment.py")
    if not os.path.exists(path):
        pytest.skip("the commenter is not in this tree yet")
    import importlib.util
    spec = importlib.util.spec_from_file_location("proof_comment", path)
    pc = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pc)
    for t in samples:
        for fn in (qb.md_safe, lambda x: qb.md_code(x)):
            body = fn(t)
            assert pc.sanitize_body(body, repo="ReliablyObserve/victoria-lakehouse", image_names=(), allowed_refs=[])[0] == body, (t, body)
    pr = snap(lint={"root": {"new": 1, "findings": [{"file": samples[0], "line": 1, "linter": samples[0], "text": samples[0]}]}})
    pr["parity"]["known"] = [{"test": samples[0], "reason": samples[2], "issues": ["victoria-lakehouse#12"]}]
    text, _ = quality.render(snap(), pr, {"root": {"measured": [samples[0]], "left_out": [samples[2]]}}, None, [samples[1]])
    body = text.split("\n", 1)[1]
    assert pc.sanitize_body(body, repo="ReliablyObserve/victoria-lakehouse", image_names=(), allowed_refs=[12])[0] == body
