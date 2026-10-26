"""Tests of scripts/ci/proof_comment.py: the base-branch commenter that turns untrusted PR artifacts into comments.

Run: python3 -I -m unittest discover -s scripts/ci/tests -t . -p test_proof_comment.py -v

Every check the commenter makes has a fixture here that must be refused, next to the one that must pass.
Special characters are built with chr() so the file stays plain ASCII.
"""
import hashlib
import io
import json
import os
import pathlib
import random
import re
import stat
import struct
import sys
import tempfile
import unittest
import zipfile
import zlib

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts" / "ci"))
import proof_comment as pc  # noqa: E402

ZW = chr(0x200B)
REPO = "ReliablyObserve/victoria-lakehouse"
SHA = "0123456789abcdef0123456789abcdef01234567"
OTHER_SHA = "f" * 40
PROOF = ".github/workflows/pr-proof.yaml"
QUALITY = ".github/workflows/pr-quality.yaml"


def read_file(path, mode="r"):
    with open(path, mode) as f:
        return f.read()


def write_file(path, data):
    with open(path, "wb" if isinstance(data, bytes) else "w") as f:
        f.write(data)


def chunk(t, d):
    return struct.pack(">I", len(d)) + t + d + struct.pack(">I", zlib.crc32(t + d) & 0xFFFFFFFF)


def raw_rows(w, h, bpp=3, noise=False):
    return b"".join(b"\x00" + (os.urandom(w * bpp) if noise else b"\x10\x20\x30\x40"[:bpp] * w) for _ in range(h))


def png(w=2, h=2, noise=False, ihdr=None, idat_raw=None, extra_chunks=(), trailing=b"", ctype=2, depth=8):
    """A valid truecolor PNG unless a knob says otherwise (ihdr: bytes; idat_raw: the uncompressed pixel stream)."""
    head = ihdr if ihdr is not None else struct.pack(">IIBBBBB", w, h, depth, ctype, 0, 0, 0)
    raw = idat_raw if idat_raw is not None else raw_rows(w, h, noise=noise)
    mid = b"".join(chunk(t, d) for t, d in extra_chunks)
    return pc.PNG_SIG + chunk(b"IHDR", head) + mid + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b"") + trailing


def manifest(**over):
    img = png()
    m = {"schema_version": 1, "pr_number": 7, "head_sha": SHA,
         "comments": [{"kind": "perf-smoke", "verdict": "pass"}, {"kind": "visual-proof", "verdict": "pass"}],
         "images": [{"name": "page.png", "sha256": hashlib.sha256(img).hexdigest(), "bytes": len(img)}],
         "allowed_issue_refs": [500]}
    m.update(over)
    return m


def artifact(m=None, bodies=None, images=None):
    """The files of a valid artifact, as read_zip would return them."""
    m = m or manifest()
    files = {"manifest.json": json.dumps(m).encode()}
    for c in m["comments"]:
        files[f"{c['kind']}.md"] = (bodies or {}).get(c["kind"], b"### ok\n\n![shot](image:page.png)\n")
    for i in m["images"]:
        files[f"images/{i['name']}"] = (images or {}).get(i["name"], png())
    return files


def make_zip(files, extra=None):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for n, d in files.items():
            z.writestr(n, d)
        for info, d in (extra or []):
            z.writestr(info, d)
    return buf.getvalue()


class FakeApi:
    """The slice of the GitHub API the commenter uses, with a call log."""

    def __init__(self, repo=REPO, run_id=99, head_sha=SHA, head_repo=REPO, prs=None, zipbytes=b"", comments=None,
                 run_path=PROOF, artifacts="default", conclusion="success"):
        self.repo, self.run_id, self.head_sha, self.head_repo = repo, run_id, head_sha, head_repo
        self.prs = {7: self.pr(7)} if prs is None else prs
        self.zipbytes, self.comments, self.run_path = zipbytes, comments if comments is not None else [], run_path
        self.artifacts = artifacts
        self.conclusion = conclusion
        self.writes = []
        self.next_id = 1000
        self.fail_patch_404 = False

    def pr(self, n, sha=None, state="open", head_repo=None):
        return {"number": n, "state": state, "head": {"sha": sha or self.head_sha, "repo": {"full_name": head_repo or self.head_repo}},
                "base": {"repo": {"full_name": self.repo}}}

    def get(self, path):
        if path == f"repos/{self.repo}/actions/runs/{self.run_id}":
            return {"event": "pull_request", "head_sha": self.head_sha, "status": "completed", "conclusion": self.conclusion, "path": self.run_path + "@refs/pull/7/merge",
                    "repository": {"full_name": self.repo}, "head_repository": {"full_name": self.head_repo}}
        if path.startswith(f"repos/{self.repo}/actions/runs/{self.run_id}/artifacts"):
            if self.artifacts != "default":
                return {"artifacts": self.artifacts}
            return {"artifacts": [{"id": 5, "name": pc.ARTIFACT_NAME, "expired": False, "size_in_bytes": len(self.zipbytes),
                                   "workflow_run": {"id": self.run_id, "head_sha": self.head_sha}}]}
        m = re.fullmatch(rf"repos/{self.repo}/pulls/(\d+)", path)
        if m:
            return self.prs[int(m.group(1))]
        if path.startswith(f"repos/{self.repo}/pulls?state=open"):
            return list(self.prs.values())
        raise AssertionError(f"unexpected GET {path}")

    def get_paged(self, path):
        if path.startswith(f"repos/{self.repo}/commits/"):
            return list(self.prs.values())
        if path.endswith("/comments") and "/issues/" in path:
            return list(self.comments)
        raise AssertionError(f"unexpected paged GET {path}")

    def get_bytes(self, path):
        assert path == f"repos/{self.repo}/actions/artifacts/5/zip", path
        return self.zipbytes

    def send(self, method, path, payload=None):
        self.writes.append((method, path, payload))
        if method == "PATCH" and self.fail_patch_404:
            raise pc.ApiError("HTTP 404", 404)
        if method == "POST":
            self.next_id += 1
            return {"id": self.next_id}
        return None


def bot_comment(cid, kind, run=1, sha=SHA, login=pc.BOT_LOGIN, utype="Bot", body=None):
    return {"id": cid, "user": {"login": login, "type": utype},
            "body": body if body is not None else f"{pc.MARKERS[kind]}\nold\n<!-- lh-proof-run: {run} {sha} -->\n"}


def env_for(api, **over):
    e = {"GITHUB_REPOSITORY": api.repo, "WR_RUN_ID": str(api.run_id), "WR_HEAD_SHA": api.head_sha,
         "WR_HEAD_REPO": api.head_repo, "WR_EVENT": "pull_request", "WR_PATH": api.run_path + "@refs/pull/7/merge",
         "WR_CONCLUSION": "success", "WR_PRS": "[7]"}
    e.update(over)
    return e


class ContractFiles(unittest.TestCase):
    def test_the_json_schema_and_the_validator_agree(self):
        s = json.loads((ROOT / "scripts/ci/proof-comment-manifest.schema.json").read_text())
        self.assertEqual(set(s["required"]), pc.MANIFEST_KEYS)
        self.assertEqual(set(s["properties"]), pc.MANIFEST_KEYS)
        self.assertEqual(s["properties"]["schema_version"]["const"], pc.SCHEMA_VERSION)
        c = s["properties"]["comments"]
        self.assertEqual(set(c["items"]["properties"]["kind"]["enum"]), set(pc.MARKERS))
        self.assertEqual(tuple(c["items"]["properties"]["verdict"]["enum"]), pc.VERDICTS)
        self.assertEqual(c["maxItems"], len(pc.MARKERS))
        self.assertEqual(s["properties"]["images"]["maxItems"], pc.MAX_IMAGES)
        self.assertEqual(s["properties"]["images"]["items"]["properties"]["bytes"]["maximum"], pc.MAX_IMAGE_BYTES)
        self.assertEqual(s["properties"]["allowed_issue_refs"]["maxItems"], pc.MAX_ISSUE_REFS)
        self.assertEqual(s["properties"]["head_sha"]["pattern"], "^" + pc.SHA_RE.pattern + "$")
        self.assertEqual(s["properties"]["images"]["items"]["properties"]["name"]["pattern"], "^" + pc.IMAGE_NAME_RE.pattern + "$")

    def test_markers_equal_the_ones_scripts_bench_ab_renders(self):
        def const(path, name):
            m = re.search(rf'^{name} = "(<!-- lh-[a-z-]+ -->)"', (ROOT / path).read_text(), re.M)
            self.assertIsNotNone(m, f"{name} not found in {path}")
            return m.group(1)
        self.assertEqual(pc.MARKERS["perf-smoke"], const("scripts/bench/ab/comment.py", "PERF_MARKER"))
        self.assertEqual(pc.MARKERS["api-matrix"], const("scripts/bench/ab/comment.py", "API_MARKER"))
        self.assertEqual(pc.MARKERS["pr-quality"], const("scripts/bench/ab/quality.py", "MARKER"))
        self.assertEqual(pc.MARKERS["visual-proof"], "<!-- lh-visual-proof -->")

    def test_the_fixture_manifest_is_valid(self):
        files = pc.read_dir(str(ROOT / pc.FIXTURE_ROOT / "valid"))
        m = pc.parse_manifest(files["manifest.json"])
        pc.check_files(files, m, PROOF)


class ReadZip(unittest.TestCase):
    def test_a_valid_artifact_reads(self):
        files = artifact()
        self.assertEqual(pc.read_zip(make_zip(files)), files)

    def test_path_traversal_is_refused(self):
        for name in ("../evil.md", "images/../../evil.png", "/etc/passwd", "images\\..\\x.png", "a/../manifest.json", "images/./x.png"):
            with self.subTest(name=name), self.assertRaises(pc.Reject):
                pc.read_zip(make_zip(artifact(), extra=[(name, b"x")]))

    def test_a_symlink_entry_is_refused(self):
        info = zipfile.ZipInfo("images/page2.png")
        info.create_system = 3
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        with self.assertRaisesRegex(pc.Reject, "regular file"):
            pc.read_zip(make_zip(artifact(), extra=[(info, b"/etc/passwd")]))

    def test_a_special_file_entry_is_refused(self):
        info = zipfile.ZipInfo("images/page2.png")
        info.create_system = 3
        info.external_attr = (stat.S_IFCHR | 0o600) << 16
        with self.assertRaises(pc.Reject):
            pc.read_zip(make_zip(artifact(), extra=[(info, b"")]))

    def test_names_outside_the_allowlist_are_refused(self):
        for name in ("script.sh", "images/Page.PNG", "images/a/b.png", "images/.hidden", "notes.md", "images/x.jpg", "manifest.json.bak"):
            with self.subTest(name=name), self.assertRaisesRegex(pc.Reject, "allowlist"):
                pc.read_zip(make_zip(artifact(), extra=[(name, b"x")]))

    def test_a_directory_entry_is_refused_except_images(self):
        with self.assertRaises(pc.Reject):
            pc.read_zip(make_zip(artifact(), extra=[("evil/", b"")]))
        pc.read_zip(make_zip(artifact(), extra=[("images/", b"")]))

    def test_a_duplicate_entry_is_refused(self):
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            data = make_zip(artifact(), extra=[("manifest.json", b"{}")])
        with self.assertRaisesRegex(pc.Reject, "duplicate"):
            pc.read_zip(data)

    def test_an_encrypted_entry_is_refused(self):
        data = bytearray(make_zip(artifact()))
        cd = data.find(b"PK\x01\x02")  # set the "encrypted" bit in the first central directory header
        data[cd + 8] |= 0x1
        with self.assertRaisesRegex(pc.Reject, "encrypted"):
            pc.read_zip(bytes(data))

    def test_too_many_entries_are_refused(self):
        with self.assertRaisesRegex(pc.Reject, "entries"):
            pc.read_zip(make_zip({}, extra=[(f"images/{i}.png", b"") for i in range(pc.MAX_ENTRIES + 1)]))

    def test_an_oversize_body_is_refused(self):
        with self.assertRaisesRegex(pc.Reject, "limit"):
            pc.read_zip(make_zip(artifact(bodies={"perf-smoke": b"x" * (pc.MAX_BODY_BYTES + 1)})))

    def test_a_zip_bomb_is_refused_by_the_total_and_the_per_entry_cap(self):
        bomb = {"perf-smoke.md": b"a" * pc.MAX_BODY_BYTES, "pr-quality.md": b"a" * pc.MAX_BODY_BYTES}
        data = make_zip(bomb)
        self.assertLess(len(data), 5000)  # tiny on the wire
        pc.read_zip(data)  # within every cap: allowed
        many = {f"images/p{i}.png": b"\0" * pc.MAX_IMAGE_BYTES for i in range(40)}
        with self.assertRaisesRegex(pc.Reject, "exceeds"):
            pc.read_zip(make_zip(many))

    def test_a_lying_declared_size_does_not_defeat_the_cap(self):
        data = bytearray(make_zip({"perf-smoke.md": b"a" * (pc.MAX_BODY_BYTES + 10)}))
        # the local header and central directory both carry file_size; make the central one claim a small size
        needle = struct.pack("<I", pc.MAX_BODY_BYTES + 10)
        idx = data.rfind(needle)
        data[idx:idx + 4] = struct.pack("<I", 10)
        with self.assertRaises((pc.Reject, zipfile.BadZipFile)):
            pc.read_zip(bytes(data))

    def test_a_corrupt_entry_is_a_clean_rejection_not_a_traceback(self):
        data = bytearray(make_zip({"perf-smoke.md": b"hello world " * 200}))
        i = data.find(b"perf-smoke.md") + len(b"perf-smoke.md") + 4
        data[i] ^= 0xFF  # damage the compressed stream
        with self.assertRaises(pc.Reject):
            pc.read_zip(bytes(data))

    def test_not_a_zip_is_refused(self):
        with self.assertRaisesRegex(pc.Reject, "not a zip"):
            pc.read_zip(b"PK nope")

    def test_an_oversize_zip_is_refused(self):
        with self.assertRaisesRegex(pc.Reject, "zip is"):
            pc.read_zip(b"0" * (pc.MAX_ZIP_BYTES + 1))


class ReadDir(unittest.TestCase):
    def write(self, root, files):
        for n, d in files.items():
            p = pathlib.Path(root, n)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(d)

    def test_a_directory_reads_like_a_zip(self):
        with tempfile.TemporaryDirectory() as d:
            files = artifact()
            self.write(d, files)
            self.assertEqual(pc.read_dir(d), files)

    def test_a_symlink_is_refused(self):
        with tempfile.TemporaryDirectory() as d:
            self.write(d, artifact())
            os.symlink("/etc/hostname", os.path.join(d, "images", "link.png"))
            with self.assertRaisesRegex(pc.Reject, "regular"):
                pc.read_dir(d)

    def test_an_unlisted_file_is_refused(self):
        with tempfile.TemporaryDirectory() as d:
            self.write(d, {**artifact(), "run.sh": b"x"})
            with self.assertRaisesRegex(pc.Reject, "allowlist"):
                pc.read_dir(d)


class Images(unittest.TestCase):
    def test_a_png_passes(self):
        self.assertTrue(pc.valid_png(png()))

    def test_other_bytes_fail(self):
        jpeg = b"\xff\xd8\xff\xe0" + b"\0" * 100
        for name, data in {"jpeg": jpeg, "html": b"<html><script>alert(1)</script></html>" * 5, "empty": b"",
                           "sig only": pc.PNG_SIG, "no iend": png()[:-12], "text": b"hello" * 30,
                           "huge dims": png(ihdr=struct.pack(">IIBBBBB", 99999, 5, 8, 2, 0, 0, 0)),
                           "zero dims": png(ihdr=struct.pack(">IIBBBBB", 0, 5, 8, 2, 0, 0, 0)),
                           "bad ihdr": png()[:12] + b"IDAT" + png()[16:]}.items():
            with self.subTest(name):
                self.assertFalse(pc.valid_png(data))

    def test_an_oversize_png_fails(self):
        self.assertFalse(pc.valid_png(png(400, 400, noise=True)))

    def test_a_non_image_inside_an_artifact_is_refused(self):
        files = artifact(images={"page.png": b"not an image at all" * 10})
        m = pc.parse_manifest(files["manifest.json"])
        with self.assertRaisesRegex(pc.Reject, "not a valid PNG"):
            pc.check_files(files, m, PROOF)

    def test_an_image_that_disagrees_with_the_manifest_is_refused(self):
        files = artifact()
        files["images/page.png"] = png(3, 3)
        with self.assertRaisesRegex(pc.Reject, "sha256"):
            pc.check_files(files, pc.parse_manifest(files["manifest.json"]), PROOF)

    def test_only_a_visual_proof_manifest_may_carry_images(self):
        m = manifest(comments=[{"kind": "perf-smoke", "verdict": "pass"}, {"kind": "visual-proof", "verdict": "pass"}])
        files = artifact(m)
        pc.check_files(files, pc.parse_manifest(files["manifest.json"]), PROOF)
        for kinds, path in ((["perf-smoke"], PROOF), (["pr-quality"], QUALITY)):
            m = manifest(comments=[{"kind": k, "verdict": "pass"} for k in kinds])
            files = artifact(m)
            with self.assertRaisesRegex(pc.Reject, "visual-proof may carry images"):
                pc.check_files(files, pc.parse_manifest(files["manifest.json"]), path)

    def test_an_unlisted_or_missing_image_is_refused(self):
        files = artifact()
        files["images/extra.png"] = png()
        with self.assertRaisesRegex(pc.Reject, "images must match"):
            pc.check_files(files, pc.parse_manifest(files["manifest.json"]), PROOF)
        del files["images/extra.png"], files["images/page.png"]
        with self.assertRaisesRegex(pc.Reject, "images must match"):
            pc.check_files(files, pc.parse_manifest(files["manifest.json"]), PROOF)


class Manifest(unittest.TestCase):
    def bad(self, raw, rx=None):
        with self.assertRaisesRegex(pc.Reject, rx or ""):
            pc.parse_manifest(raw if isinstance(raw, bytes) else json.dumps(raw).encode())

    def test_valid(self):
        self.assertEqual(pc.parse_manifest(json.dumps(manifest()).encode())["pr_number"], 7)

    def test_each_field_is_checked(self):
        cases = {
            "extra key": {**manifest(), "extra": 1},
            "missing key": {k: v for k, v in manifest().items() if k != "images"},
            "version": manifest(schema_version=2),
            "bool pr": manifest(pr_number=True),
            "string pr": manifest(pr_number="7"),
            "zero pr": manifest(pr_number=0),
            "short sha": manifest(head_sha="abc"),
            "upper sha": manifest(head_sha=SHA.upper()),
            "no comments": manifest(comments=[]),
            "unknown kind": manifest(comments=[{"kind": "evil", "verdict": "pass"}]),
            "unknown verdict": manifest(comments=[{"kind": "perf-smoke", "verdict": "great"}]),
            "dup kind": manifest(comments=[{"kind": "perf-smoke", "verdict": "pass"}] * 2),
            "comment extra": manifest(comments=[{"kind": "perf-smoke", "verdict": "pass", "x": 1}]),
            "image name": manifest(images=[{"name": "../x.png", "sha256": "0" * 64, "bytes": 5}]),
            "image upper": manifest(images=[{"name": "X.png", "sha256": "0" * 64, "bytes": 5}]),
            "image bytes": manifest(images=[{"name": "x.png", "sha256": "0" * 64, "bytes": pc.MAX_IMAGE_BYTES + 1}]),
            "too many refs": manifest(allowed_issue_refs=list(range(1, pc.MAX_ISSUE_REFS + 2))),
            "dup refs": manifest(allowed_issue_refs=[3, 3]),
            "bool ref": manifest(allowed_issue_refs=[True]),
            "negative ref": manifest(allowed_issue_refs=[-1]),
            "list root": [1, 2],
        }
        for name, m in cases.items():
            with self.subTest(name):
                self.bad(m)

    def test_json_traps(self):
        self.bad(b'{"schema_version": 1, "schema_version": 1}', "valid JSON")
        self.bad(b'{"a": NaN}', "valid JSON")
        self.bad(b"\xff\xfe", "valid JSON")
        self.bad(b"[" * 100000, "valid JSON")

    def test_a_producer_may_only_post_its_own_kinds(self):
        files = artifact(manifest(comments=[{"kind": "pr-quality", "verdict": "pass"}], images=[]))
        with self.assertRaisesRegex(pc.Reject, "may not post"):
            pc.check_files(files, pc.parse_manifest(files["manifest.json"]), PROOF)
        pc.check_files(files, pc.parse_manifest(files["manifest.json"]), QUALITY)

    def test_the_md_files_must_match_the_comments(self):
        files = artifact()
        files["api-matrix.md"] = b"x"
        with self.assertRaisesRegex(pc.Reject, "must match"):
            pc.check_files(files, pc.parse_manifest(files["manifest.json"]), PROOF)


class Sanitize(unittest.TestCase):
    def s(self, text, images=(), refs=()):
        return pc.sanitize_body(text, repo=REPO, image_names=images, allowed_refs=refs)[0]

    def test_mentions_are_neutralised(self):
        for t in ("@someone", "ping @org/team now", "mail a@b.com", "(@x)", "x@y"):
            out = self.s(t)
            self.assertIsNone(re.search(r"@[A-Za-z0-9_]", out), out)
        self.assertEqual(self.s("@a"), "@" + ZW + "a")

    def test_cross_references_are_neutralised_unless_allowed(self):
        out = self.s("see #12, GH-9 and other/repo#5 and https://github.com/other/repo/issues/5")
        self.assertIsNone(re.search(r"#\d", out), out)
        self.assertIsNone(re.search(r"GH-\d", out), out)
        self.assertNotIn("://", out)
        self.assertEqual(self.s("known gap #500 and #501", refs=[500]), f"known gap #500 and #{ZW}501")

    def test_html_is_neutralised(self):
        for t in ("<script>alert(1)</script>", "<img src=x onerror=y>", "<!-- lh-api-matrix -->", "<a href='x'>y</a>",
                  "<details open>", "<summary class=x>", "<DETAILS>"):
            out = self.s(t)
            self.assertIsNone(re.search(r"<(?!/?(?:details|summary)>)", out), out)
        self.assertNotIn("<!--", self.s("<!-- lh-perf-smoke -->"))  # a body cannot carry (or spoof) a marker
        self.assertEqual(self.s("<details><summary>x</summary>y</details>"), "<details><summary>x</summary>y</details>")

    def test_entities_cannot_rebuild_a_mention_or_a_tag(self):
        out = self.s("&#64;someone &lt;script&gt; &amp; &#60;img&#62; &#x40;x")
        self.assertIsNone(re.search(r"&#(?!9[13];)", out), out)  # the only numeric entities left are our own brackets
        self.assertIsNone(re.search(r"&#x", out), out)
        self.assertEqual(self.s("&lt;b&gt;"), "&lt;b&gt;")

    def test_images_are_only_the_manifest_ones(self):
        out = self.s("![a](image:page.png) ![t](https://tracker.example/p.gif) ![r](/relative.png) <img src=https://x/y>", images={"page.png"})
        self.assertEqual(out.count("]("), 1)
        self.assertIn("![a](image:page.png)", out)
        self.assertNotIn("://", out)
        self.assertIn("ZZ", self.s("![ZZ](image:other.png)", images={"page.png"}))
        self.assertNotIn("](image:", self.s("![ZZ](image:other.png)", images={"page.png"}))

    def test_reference_style_and_autolinks_are_dead(self):
        out = self.s("[x][1]\n\n[1]: https://evil.example/\n<https://evil.example/> www.evil.example http://evil.example")
        self.assertNotIn("](", out)
        self.assertNotIn("<https", out)
        self.assertNotIn("://", out)
        self.assertNotIn("www.evil", out)

    def test_links_to_this_repository_survive_and_others_die(self):
        good = f"[run](https://github.com/{REPO}/actions/runs/123)"
        self.assertEqual(self.s(good), good)
        self.assertEqual(self.s(f"[i](https://github.com/{REPO}/issues/5)", refs=[5]), f"[i](https://github.com/{REPO}/issues/5)")
        for url in ("https://evil.example/x", "http://github.com/" + REPO + "/issues/1", "https://github.com/other/repo/issues/1",
                    f"https://github.com/{REPO}.evil.example/issues/1", f"https://github.com/{REPO}/issues/1)![x](https://evil.example/t.gif",
                    "javascript:alert(1)", f"https://github.com/{REPO}/actions/runs/1/../../x"):
            with self.subTest(url=url):
                out = self.s(f"[x]({url})")
                for m in re.finditer(r"\]\(([^)]*)\)", out):  # every live link target is an allowlisted one
                    self.assertRegex(m.group(1), rf"^https://github\.com/{REPO}/(actions/runs/\d+|issues/\d+)$", out)
                self.assertNotRegex(out.replace(f"https://github.com/{REPO}", ""), r"https?://")

    def test_a_blob_link_cannot_climb_out_of_the_repository(self):
        sha = "a" * 40
        ok = f"[f](https://github.com/{REPO}/blob/{sha}/.github/workflows/x.yaml)"
        self.assertEqual(self.s(ok), ok)
        for tail in ("../../../../someone/phish", "a/../b", "a/..", ".", "a/./b", "%2e%2e/x"):
            with self.subTest(tail=tail):
                out = self.s(f"[f](https://github.com/{REPO}/blob/{sha}/{tail})")
                self.assertNotIn("](", out)

    def test_a_link_cannot_become_an_image(self):
        out = self.s(f"![x](https://github.com/{REPO}/issues/5)", refs=[5])
        self.assertEqual(out, f"[x](https://github.com/{REPO}/issues/5)")

    def test_a_bare_allowed_url_stays_and_a_bare_foreign_one_is_defanged(self):
        self.assertIn(f"https://github.com/{REPO}/actions/runs/9", self.s(f"run: https://github.com/{REPO}/actions/runs/9."))
        self.assertNotIn("://", self.s("https://tracker.example/pixel.gif"))

    def test_nested_constructs_cannot_rebuild_a_link(self):
        out = self.s("[[a](b)](https://evil.example) ![[x]](https://evil.example)")
        self.assertNotIn("](", out)

    def test_control_and_bidi_characters_are_removed(self):
        t = "a" + chr(0) + chr(7) + chr(0x202E) + chr(0x2066) + chr(0xE000) + "b" + chr(0xE001) + chr(0x2028) + "c\r\nd"
        out = self.s(t)
        self.assertEqual(out, "ab\nc\nd")

    def test_idempotent_and_safe_on_random_input(self):
        rnd = random.Random(7)
        atoms = ["@bob", "#12", "#500", "<b>", "</details>", "<details>", "[x](https://evil.example)", "![i](image:page.png)", "![i](image:no.png)",
                 f"[r](https://github.com/{REPO}/actions/runs/1)", "https://x.example/p", "www.a.b", "&#64;", "&amp;", "&lt;", "[", "]", "(", ")",
                 "!", "\n", " ", "GH-1", "o/r#3", "`", "```", "|", "a@b.c", chr(0x200B), "<!--", "-->", "&", "<", "#", "@", "://"]
        for _ in range(400):
            t = "".join(rnd.choice(atoms) for _ in range(rnd.randint(1, 25)))
            once, _ = pc.sanitize_body(t, repo=REPO, image_names={"page.png"}, allowed_refs={500})
            twice, _ = pc.sanitize_body(once, repo=REPO, image_names={"page.png"}, allowed_refs={500})
            self.assertEqual(once, twice, t)
            self.assertIsNone(re.search(r"<(?!/?(?:details|summary)>)", once), (t, once))
            self.assertIsNone(re.search(r"@[A-Za-z0-9_]", once), (t, once))
            self.assertIsNone(re.search(r"(?<!&)#(?!500\b)\d", once), (t, once))
            stripped = re.sub(r"!\[[^\]\[]*\]\(image:page\.png\)|\[[^\]\[]*\]\(https://github\.com/[^)]*\)", "", once)
            self.assertNotIn("](", stripped, (t, once))
            self.assertNotIn("[", stripped, (t, once))
            self.assertNotRegex(re.sub(rf"https://github\.com/{REPO}[^ \n]*", "", stripped), r"(?i)https?://|(?<![A-Za-z0-9.])www\.", (t, once))


class RealRenderers(unittest.TestCase):
    """Bodies the existing renderers produce must survive the sanitiser unchanged (apart from their own marker line)."""

    def test_the_constructs_scripts_bench_ab_emits_pass_through(self):
        body = (f"### NEUTRAL\n\n| a | b |\n|---|---|\n| `x` | 1 ms |\n\n<details><summary>Why these shapes</summary>\n\nlist\n\n</details>\n\n"
                f"<sub>run metadata, base abc</sub>\n\n[#500](https://github.com/{REPO}/issues/500) and [run](https://github.com/{REPO}/actions/runs/9)\n")
        out, _ = pc.sanitize_body(body, repo=REPO, image_names=(), allowed_refs={500})
        self.assertEqual(out.replace(ZW, ""), body)

    def test_a_leading_own_marker_is_dropped_and_a_foreign_one_is_neutralised(self):
        self.assertEqual(pc.body_text(b"<!-- lh-perf-smoke -->\n### x\n", "perf-smoke"), "### x\n")
        self.assertEqual(pc.body_text(b"<!-- lh-api-matrix -->\n### x\n", "perf-smoke").startswith("<!-- lh-api-matrix -->"), True)
        out, _ = pc.sanitize_body(pc.body_text(b"<!-- lh-api-matrix -->\nx", "perf-smoke"), repo=REPO, image_names=(), allowed_refs=())
        self.assertNotIn("<!--", out)

    def test_the_image_base_form_of_scripts_proof_comment_is_accepted(self):
        out, used = pc.sanitize_body("![a](image:/page.png)", repo=REPO, image_names={"page.png"}, allowed_refs=())
        self.assertEqual((out, used), ("![a](image:page.png)", {"page.png"}))

    def test_sub_is_balanced_when_a_body_is_cut(self):
        out = pc.render_comment("perf-smoke", "<sub>" + "word " * 30000, repo=REPO, pr=7, head_sha=SHA, run_id=1, images_published=True)
        self.assertEqual(out.count("<sub>"), out.count("</sub>"))
        self.assertLessEqual(len(out), pc.COMMENT_LIMIT)


class Render(unittest.TestCase):
    kw = dict(repo=REPO, pr=7, head_sha=SHA, run_id=42)

    def test_images_resolve_to_this_prs_pr_visuals_path(self):
        out = pc.render_comment("visual-proof", "![shot](image:page.png)", images_published=True, **self.kw)
        self.assertIn(f"![shot](https://raw.githubusercontent.com/{REPO}/pr-visuals/pr-7/{SHA[:12]}-page.png)", out)
        self.assertTrue(out.startswith(pc.MARKERS["visual-proof"] + "\n"))
        self.assertIn(f"<!-- lh-proof-run: 42 {SHA} -->", out)

    def test_without_publishing_no_image_is_embedded(self):
        out = pc.render_comment("visual-proof", "![shot](image:page.png)", images_published=False, **self.kw)
        self.assertNotIn("raw.githubusercontent.com", out)
        self.assertNotIn("![", out)

    def test_an_oversize_body_is_truncated_under_the_limit_with_a_note(self):
        body = "<details><summary>big</summary>\n\n```\n" + "line of output\n" * 10000
        out = pc.render_comment("perf-smoke", body, images_published=True, **self.kw)
        self.assertLessEqual(len(out), pc.COMMENT_LIMIT)
        self.assertIn("truncated", out)
        self.assertEqual(out.count("```") % 2, 0)
        self.assertEqual(out.count("<details>"), out.count("</details>"))
        self.assertIn("<!-- lh-proof-run: 42", out)  # the metadata survives truncation
        self.assertTrue(out.startswith(pc.MARKERS["perf-smoke"]))

    def test_the_limit_leaves_room_below_githubs(self):
        self.assertLessEqual(pc.COMMENT_LIMIT, 65_000)

    def test_a_body_exactly_at_the_limit_is_untouched(self):
        overhead = len(pc.render_comment("perf-smoke", "", images_published=True, **self.kw))
        body = "x" * (pc.COMMENT_LIMIT - overhead)
        out = pc.render_comment("perf-smoke", body, images_published=True, **self.kw)
        self.assertEqual(len(out), pc.COMMENT_LIMIT)
        self.assertNotIn("truncated", out)
        self.assertIn("truncated", pc.render_comment("perf-smoke", body + "y", images_published=True, **self.kw))

    def test_one_huge_line_and_multibyte_text_stay_under_the_limit(self):
        for body in ("y" * 200000, ("é" + "z") * 100000, "a\n" * 70000):
            out = pc.render_comment("api-matrix", body, images_published=True, **self.kw)
            self.assertLessEqual(len(out), pc.COMMENT_LIMIT)


class Upsert(unittest.TestCase):
    def up(self, api, run=50, sha=SHA):
        return pc.upsert(api, REPO, 7, "perf-smoke", pc.MARKERS["perf-smoke"] + "\nnew", run, sha)

    def methods(self, api):
        return [(m, p.rsplit("/", 1)[-1] if m != "POST" else "") for m, p, _ in api.writes]

    def test_creates_when_absent(self):
        api = FakeApi()
        self.assertEqual(self.up(api), "created")
        self.assertEqual([m for m, *_ in api.writes], ["POST"])

    def test_edits_in_place_and_removes_duplicates(self):
        api = FakeApi(comments=[bot_comment(30, "perf-smoke"), bot_comment(10, "perf-smoke"), bot_comment(20, "perf-smoke")])
        self.assertIn("updated, removed 2", self.up(api))
        self.assertEqual(self.methods(api), [("PATCH", "10"), ("DELETE", "20"), ("DELETE", "30")])

    def test_only_bot_comments_with_the_marker_at_the_start_count(self):
        api = FakeApi(comments=[
            bot_comment(1, "perf-smoke", login="szibis", utype="User"),
            bot_comment(2, "perf-smoke", login="evil-bot", utype="Bot"),
            bot_comment(3, "perf-smoke", login=pc.BOT_LOGIN, utype="User"),
            bot_comment(4, "api-matrix"),
            bot_comment(5, "perf-smoke", body="quoting " + pc.MARKERS["perf-smoke"]),
        ])
        self.assertEqual(self.up(api), "created")
        self.assertEqual([m for m, *_ in api.writes], ["POST"])  # nothing foreign is edited or deleted

    def test_a_newer_run_is_never_overwritten_by_an_older_one(self):
        api = FakeApi(comments=[bot_comment(10, "perf-smoke", run=60)])
        self.assertIn("newer run", self.up(api, run=50))
        self.assertEqual(api.writes, [])

    def test_a_superseded_head_posts_nothing(self):
        api = FakeApi(prs={7: FakeApi().pr(7, sha=OTHER_SHA)})
        with self.assertRaisesRegex(pc.Skip, "superseded"):
            self.up(api)
        self.assertEqual(api.writes, [])

    def test_a_closed_pr_posts_nothing(self):
        api = FakeApi()
        api.prs = {7: api.pr(7, state="closed")}
        with self.assertRaisesRegex(pc.Skip, "not open"):
            self.up(api)
        self.assertEqual(api.writes, [])

    def test_a_deleted_comment_falls_back_to_a_new_one(self):
        api = FakeApi(comments=[bot_comment(10, "perf-smoke")])
        api.fail_patch_404 = True
        self.assertEqual(self.up(api), "created")
        self.assertEqual([m for m, *_ in api.writes], ["PATCH", "POST"])

    def test_other_api_errors_are_not_swallowed(self):
        api = FakeApi(comments=[bot_comment(10, "perf-smoke")])
        api.send = lambda *a, **k: (_ for _ in ()).throw(pc.ApiError("HTTP 500", 500))
        with self.assertRaises(pc.ApiError):
            self.up(api)


def valid_api(files=None, **kw):
    files = files or artifact()
    return FakeApi(zipbytes=make_zip(files), **kw)


class Validate(unittest.TestCase):
    def run_validate(self, api, env=None, out=None):
        with tempfile.TemporaryDirectory() as d:
            gh_out = os.path.join(d, "out.txt")
            write_file(gh_out, "")
            e = env_for(api, GITHUB_OUTPUT=gh_out, GITHUB_RUN_ID="4242", **(env or {}))
            bundle = os.path.join(d, "bundle")
            rc = pc.main(["validate", "--out", bundle], env=e, api=api)
            outputs = dict(line.split("=", 1) for line in read_file(gh_out).splitlines())
            files = sorted(os.listdir(bundle)) if os.path.isdir(bundle) else []
            meta = json.loads(read_file(os.path.join(bundle, "meta.json"))) if "meta.json" in files else None
            return rc, outputs, files, meta

    def assert_skipped(self, api, env=None):
        rc, out, files, _ = self.run_validate(api, env)
        self.assertEqual((rc, out["proceed"], out["mode"], files), (0, "false", "", []))

    def assert_stale(self, api, reason, env=None):
        rc, out, files, meta = self.run_validate(api, env)
        self.assertEqual((rc, out["proceed"], out["mode"], files), (0, "true", "stale", ["meta.json"]))
        self.assertEqual((meta["reason"], meta["mode"]), (reason, "stale"))
        return meta

    def test_a_valid_artifact_makes_a_bundle(self):
        rc, out, files, meta = self.run_validate(valid_api())
        self.assertEqual((rc, out["proceed"], out["mode"], out["pr"], out["head_sha"], out["has_images"]), (0, "true", "proof", "7", SHA, "true"))
        self.assertEqual(files, ["images", "meta.json", "perf-smoke.md", "visual-proof.md"])
        self.assertEqual(meta["conclusion"], "success")

    def test_a_fork_pr_with_an_empty_payload_list_resolves_by_head_sha(self):
        api = valid_api(head_repo="someone/fork")
        rc, out, _, _ = self.run_validate(api, env={"WR_PRS": "[]"})
        self.assertEqual((rc, out["proceed"], out["mode"]), (0, "true", "proof"))

    def test_the_wrong_pr_number_in_the_manifest_marks_the_head_unproven(self):
        meta = self.assert_stale(valid_api(artifact(manifest(pr_number=8))), "rejected")
        self.assertEqual(meta["prs"], [7])  # the PR comes from the API, never from the manifest

    def test_a_manifest_naming_another_open_pr_marks_the_head_unproven(self):
        api = valid_api(artifact(manifest(pr_number=8)))
        api.prs[8] = api.pr(8, sha=OTHER_SHA)  # another PR, with another head
        self.assertEqual(self.assert_stale(api, "rejected")["prs"], [7])

    def test_two_prs_sharing_a_head_are_told_apart_by_the_manifest(self):
        api = valid_api(artifact(manifest(pr_number=8)), prs={7: FakeApi().pr(7), 8: FakeApi().pr(8)})
        self.assertEqual(self.run_validate(api, env={"WR_PRS": "[]"})[1]["pr"], "8")

    def test_the_wrong_pr_number_is_refused_even_when_the_event_lists_no_prs(self):
        api = valid_api(artifact(manifest(pr_number=8)), head_repo="someone/fork")
        self.assert_stale(api, "rejected", env={"WR_PRS": "[]"})

    def test_the_wrong_sha_in_the_manifest_marks_the_head_unproven(self):
        self.assert_stale(valid_api(artifact(manifest(head_sha=OTHER_SHA))), "rejected")

    def test_a_payload_that_does_not_list_the_manifest_pr_marks_the_head_unproven(self):
        self.assert_stale(valid_api(), "rejected", env={"WR_PRS": "[9]"})

    def test_a_moved_head_or_closed_pr_skips_quietly(self):
        api = valid_api()
        api.prs = {7: api.pr(7, sha=OTHER_SHA)}
        self.assert_skipped(api)
        api = valid_api()
        api.prs = {7: api.pr(7, state="closed")}
        self.assert_skipped(api)

    def test_a_pr_from_another_repository_head_is_not_matched(self):
        api = valid_api()
        api.prs = {7: api.pr(7, head_repo="attacker/fork")}
        self.assert_skipped(api)

    def test_a_missing_artifact_marks_the_head_unproven(self):
        meta = self.assert_stale(FakeApi(artifacts=[]), "no-artifact")
        self.assertEqual((meta["kinds"], meta["prs"], meta["commenter_run"]), (["api-matrix", "perf-smoke", "visual-proof"], [7], 4242))

    def test_a_pr_workflow_that_skips_the_upload_cannot_name_other_producers_kinds(self):
        meta = self.assert_stale(FakeApi(artifacts=[], run_path=QUALITY), "no-artifact", env={"WR_PATH": QUALITY})
        self.assertEqual(meta["kinds"], ["pr-quality"])

    def test_an_artifact_of_another_run_marks_the_head_unproven(self):
        api = FakeApi(zipbytes=make_zip(artifact()), artifacts=[{"id": 5, "name": pc.ARTIFACT_NAME, "size_in_bytes": 10,
                                                                   "workflow_run": {"id": 1, "head_sha": SHA}}])
        self.assert_stale(api, "rejected")

    def test_two_artifacts_with_the_name_mark_the_head_unproven(self):
        a = {"id": 5, "name": pc.ARTIFACT_NAME, "size_in_bytes": 10, "workflow_run": {"id": 99, "head_sha": SHA}}
        self.assert_stale(FakeApi(artifacts=[a, dict(a, id=6)]), "rejected")

    def test_runs_that_are_not_pull_request_runs_are_skipped(self):
        for env in ({"WR_EVENT": "push"}, {"WR_EVENT": "workflow_dispatch"}, {"WR_CONCLUSION": "cancelled"}, {"WR_CONCLUSION": "skipped"},
                    {"WR_PATH": ".github/workflows/evil.yaml"}, {"WR_PATH": ".github/workflows/pr-proof.yaml.evil"}):
            with self.subTest(env=env):
                self.assert_skipped(valid_api(), env=env)

    def test_an_event_that_disagrees_with_the_api_is_rejected(self):
        api = valid_api()
        api.run_path = QUALITY  # the API says pr-quality ran, the event says pr-proof
        self.assertEqual(self.run_validate(api, env={"WR_PATH": PROOF})[0], 1)
        self.assertEqual(self.run_validate(valid_api(), env={"WR_HEAD_REPO": "other/repo"})[0], 1)

    def test_a_run_that_is_not_completed_is_rejected(self):
        api = valid_api()
        orig = api.get
        api.get = lambda path: dict(orig(path), status="in_progress") if path.endswith(f"/runs/{api.run_id}") else orig(path)
        rc, out, files, _ = self.run_validate(api)
        self.assertEqual((rc, out["proceed"], files), (1, "false", []))

    def test_a_pr_edited_workflow_cannot_post_another_producers_kind(self):
        files = artifact(manifest(comments=[{"kind": "pr-quality", "verdict": "pass"}], images=[]))
        self.assert_stale(valid_api(files), "rejected")
        self.assertEqual(self.run_validate(valid_api(files, run_path=QUALITY))[1]["mode"], "proof")

    def test_malformed_event_values_are_rejected_before_any_api_call(self):
        class NoApi:
            def __getattr__(self, name):
                raise AttributeError(f"the API must not be used before the event values are checked: {name}")
        for k, v in (("WR_RUN_ID", "1;rm -rf"), ("WR_HEAD_SHA", "x"), ("WR_HEAD_REPO", "a b"), ("WR_PRS", "{}"), ("WR_PRS", '["7"]')):
            with self.subTest(k=k):
                env = dict(env_for(valid_api()), **{k: v})
                self.assertEqual(pc.main(["validate", "--out", "/nonexistent"], env=env, api=NoApi()), 1)

    def test_traversal_symlink_and_non_image_artifacts_are_rejected_end_to_end(self):
        info = zipfile.ZipInfo("images/link.png")
        info.create_system = 3
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        bad = {
            "traversal": make_zip(artifact(), extra=[("../../x", b"x")]),
            "symlink": make_zip(artifact(), extra=[(info, b"/etc/passwd")]),
            "non-image": make_zip(artifact(images={"page.png": b"MZ\x90\x00" * 40})),
        }
        for name, z in bad.items():
            with self.subTest(name):
                meta = self.assert_stale(FakeApi(zipbytes=z), "rejected")
                self.assertEqual(set(meta), {"mode", "repo", "run_id", "head_sha", "prs", "kinds", "reason", "conclusion", "commenter_run"})

    def test_the_bundle_holds_sanitised_bodies_and_only_referenced_images(self):
        files = artifact(bodies={"perf-smoke": b"hi @someone #999 <b>x</b> ![s](image:page.png)"})
        files["images/unused.png"] = png(3, 3)
        m = json.loads(files["manifest.json"])
        m["images"].append({"name": "unused.png", "sha256": hashlib.sha256(files["images/unused.png"]).hexdigest(), "bytes": len(files["images/unused.png"])})
        files["manifest.json"] = json.dumps(m).encode()
        api = valid_api(files)
        with tempfile.TemporaryDirectory() as d:
            e = env_for(api)
            self.assertEqual(pc.main(["validate", "--out", d], env=e, api=api), 0)
            body = read_file(os.path.join(d, "perf-smoke.md"))
            self.assertNotIn("<b>", body)
            self.assertIsNone(re.search(r"@[a-z]", body))
            self.assertEqual(os.listdir(os.path.join(d, "images")), [f"{SHA[:12]}-page.png"])
            meta = pc.read_bundle(d, REPO)
            self.assertEqual((meta["pr_number"], meta["run_id"]), (7, 99))


class ImageOwnership(unittest.TestCase):
    def test_an_image_shown_by_another_kinds_body_is_not_published_or_embedded(self):
        files = artifact(bodies={"perf-smoke": b"![sneaky](image:page.png)", "visual-proof": b"no image here"})
        api = valid_api(files)
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(pc.main(["validate", "--out", d], env=env_for(api), api=api), 0)
            self.assertEqual(os.listdir(os.path.join(d, "images")), [])
            self.assertNotIn("](", read_file(os.path.join(d, "perf-smoke.md")))


class BundleAndComment(unittest.TestCase):
    def bundle(self, d, api=None):
        api = api or valid_api()
        self.assertEqual(pc.main(["validate", "--out", d], env=env_for(api), api=api), 0)
        return api

    def test_comment_posts_every_kind_and_uses_published_images(self):
        files = artifact(manifest(comments=[{"kind": "perf-smoke", "verdict": "pass"}, {"kind": "visual-proof", "verdict": "pass"}]))
        api = valid_api(files)
        with tempfile.TemporaryDirectory() as d:
            self.bundle(d, api)
            api.writes.clear()
            self.assertEqual(pc.main(["comment", "--bundle", d, "--images-published", "true"], env=env_for(api), api=api), 0)
        self.assertEqual([m for m, *_ in api.writes], ["POST", "POST"])
        bodies = [p["body"] for *_, p in api.writes]
        self.assertTrue(all(len(b) <= pc.COMMENT_LIMIT for b in bodies))
        self.assertTrue(any(f"pr-visuals/pr-7/{SHA[:12]}-page.png" in b for b in bodies))

    def test_comment_without_published_images_embeds_none(self):
        api = valid_api()
        with tempfile.TemporaryDirectory() as d:
            self.bundle(d, api)
            pc.main(["comment", "--bundle", d, "--images-published", "false"], env=env_for(api), api=api)
        self.assertNotIn("raw.githubusercontent.com", api.writes[0][2]["body"])

    def test_comment_stops_when_the_head_moved_after_validation(self):
        api = valid_api()
        with tempfile.TemporaryDirectory() as d:
            self.bundle(d, api)
            api.prs = {7: api.pr(7, sha=OTHER_SHA)}
            self.assertEqual(pc.main(["comment", "--bundle", d, "--images-published", "true"], env=env_for(api), api=api), 0)
        self.assertEqual(api.writes, [])

    def test_check_bundle_reports_whether_the_head_is_current(self):
        api = valid_api()
        with tempfile.TemporaryDirectory() as d:
            self.bundle(os.path.join(d, "b"), api)
            out = os.path.join(d, "o.txt")
            write_file(out, "")
            e = env_for(api, GITHUB_OUTPUT=out)
            d = os.path.join(d, "b")
            self.assertEqual(pc.main(["check-bundle", "--bundle", d], env=e, api=api), 0)
            self.assertIn("current=true", read_file(out))
            api.prs = {7: api.pr(7, sha=OTHER_SHA)}
            write_file(out, "")
            self.assertEqual(pc.main(["check-bundle", "--bundle", d], env=e, api=api), 0)
            got = read_file(out)
            self.assertIn("current=false", got)
            self.assertNotIn("current=true", got)

    def test_a_tampered_bundle_is_rejected_by_the_write_jobs(self):
        api = valid_api()
        with tempfile.TemporaryDirectory() as d:
            self.bundle(d, api)
            pc.read_bundle(d, REPO)
            img = os.path.join(d, "images", f"{SHA[:12]}-page.png")
            good = read_file(img, "rb")
            write_file(img, b"<script>")
            with self.assertRaisesRegex(pc.Reject, "PNG"):
                pc.read_bundle(d, REPO)
            write_file(img, good)
            os.remove(img)
            os.symlink("/etc/hostname", img)
            with self.assertRaisesRegex(pc.Reject, "regular"):
                pc.read_bundle(d, REPO)
            os.remove(img)
            write_file(img, good)
            write_file(os.path.join(d, "stray.sh"), "x")
            with self.assertRaisesRegex(pc.Reject, "unexpected"):
                pc.read_bundle(d, REPO)
            os.remove(os.path.join(d, "stray.sh"))
            write_file(os.path.join(d, "images", "extra.png"), good)
            with self.assertRaisesRegex(pc.Reject, "images"):
                pc.read_bundle(d, REPO)
            with self.assertRaisesRegex(pc.Reject, "meta"):
                pc.read_bundle(d, "other/repo")


class DryRun(unittest.TestCase):
    def run_cli(self, *args):
        import contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = pc.main(["dry-run", *args], env={"GITHUB_REPOSITORY": REPO}, api=None)
        return rc, buf.getvalue()

    def test_the_fixture_renders_and_makes_no_api_call(self):
        rc, out = self.run_cli("--source", str(ROOT / pc.FIXTURE_ROOT / "valid"), "--pr", "12345")
        self.assertEqual(rc, 0)
        self.assertIn("would post (perf-smoke", out)
        self.assertIn("would post (visual-proof", out)
        self.assertNotRegex(out, r"@[a-z]")
        self.assertIn("pr-visuals/pr-12345/", out)

    def test_a_zip_of_the_fixture_renders_too(self):
        with tempfile.TemporaryDirectory(dir=ROOT / pc.FIXTURE_ROOT) as d:
            z = os.path.join(d, "a.zip")
            write_file(z, make_zip(pc.read_dir(str(ROOT / pc.FIXTURE_ROOT / "valid"))))
            self.assertEqual(self.run_cli("--source", z, "--pr", "12345")[0], 0)

    def test_a_source_outside_the_fixture_root_is_refused(self):
        for src in ("/etc", str(ROOT / "scripts"), str(ROOT / pc.FIXTURE_ROOT) + "/../../../.."):
            with self.subTest(src=src):
                self.assertEqual(self.run_cli("--source", src, "--pr", "1")[0], 1)

    def test_a_valid_copy_outside_the_fixture_root_is_refused(self):
        import shutil
        with tempfile.TemporaryDirectory() as d:
            dst = os.path.join(d, "copy")
            shutil.copytree(str(ROOT / pc.FIXTURE_ROOT / "valid"), dst)
            self.assertEqual(self.run_cli("--source", dst, "--pr", "12345")[0], 1)

    def test_a_pr_number_that_is_not_the_fixtures_is_refused(self):
        self.assertEqual(self.run_cli("--source", str(ROOT / pc.FIXTURE_ROOT / "valid"), "--pr", "1")[0], 1)


class StrictPng(unittest.TestCase):
    """The hosted images are plain PNG and nothing else: no payload can ride in them."""

    def test_valid_variants_pass(self):
        self.assertTrue(pc.valid_png(png()))
        self.assertTrue(pc.valid_png(png(40, 30)))
        gray = png(5, 4, ctype=0, idat_raw=raw_rows(5, 4, bpp=1))
        self.assertTrue(pc.valid_png(gray))
        rgba = png(3, 3, ctype=6, idat_raw=raw_rows(3, 3, bpp=4))
        self.assertTrue(pc.valid_png(rgba))
        with_meta = png(extra_chunks=[(b"pHYs", struct.pack(">IIB", 2835, 2835, 1)), (b"sRGB", b"\x00"), (b"gAMA", struct.pack(">I", 45455))])
        self.assertTrue(pc.valid_png(with_meta))
        palette = png(4, 2, ctype=3, depth=8, extra_chunks=[(b"PLTE", b"\x00\x00\x00\xff\xff\xff")], idat_raw=raw_rows(4, 2, bpp=1))
        self.assertTrue(pc.valid_png(palette))

    def test_what_the_montage_tool_writes_passes(self):
        try:
            from PIL import Image
        except ImportError:
            self.skipTest("Pillow is not installed")
        for colors in (2, 16, 256):
            im = Image.frombytes("RGB", (60, 40), os.urandom(60 * 40 * 3))
            buf = io.BytesIO()
            im.quantize(colors=colors, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE).save(buf, "PNG", optimize=True)
            self.assertTrue(pc.valid_png(buf.getvalue()), colors)

    def test_a_text_chunk_is_refused(self):
        for t in (b"tEXt", b"zTXt", b"iTXt", b"eXIf", b"iCCP", b"tIME", b"prVt"):
            with self.subTest(t=t):
                self.assertFalse(pc.valid_png(png(extra_chunks=[(t, b"k\x00<html><script>alert(1)</script>")])))

    def test_html_after_iend_and_an_appended_zip_are_refused(self):
        self.assertFalse(pc.valid_png(png(trailing=b"<html><script>alert(1)</script></html>")))
        exe = io.BytesIO()
        with zipfile.ZipFile(exe, "w") as z:
            z.writestr("payload.exe", b"MZ" + b"\0" * 100)
        self.assertFalse(pc.valid_png(png(trailing=exe.getvalue())))

    def test_a_bad_crc_is_refused(self):
        data = bytearray(png())
        data[16 + 3] ^= 0x01  # inside IHDR
        self.assertFalse(pc.valid_png(bytes(data)))
        data = bytearray(png())
        data[-5] ^= 0xFF  # the CRC of IEND
        self.assertFalse(pc.valid_png(bytes(data)))

    def test_an_idat_stream_that_is_not_exactly_the_image_is_refused(self):
        self.assertFalse(pc.valid_png(png(2, 2, idat_raw=raw_rows(2, 2) + b"<html>hidden</html>")))  # too long
        self.assertFalse(pc.valid_png(png(2, 2, idat_raw=raw_rows(2, 1))))  # too short
        self.assertFalse(pc.valid_png(png(2, 2, idat_raw=b"\x07" + raw_rows(2, 2)[1:])))  # filter byte 7
        rows = bytearray(raw_rows(2, 2))
        self.assertTrue(pc.valid_png(png(2, 2, idat_raw=bytes(rows))))

    def test_data_after_the_zlib_stream_inside_idat_is_refused(self):
        body = zlib.compress(raw_rows(2, 2)) + b"hidden payload"
        data = pc.PNG_SIG + chunk(b"IHDR", struct.pack(">IIBBBBB", 2, 2, 8, 2, 0, 0, 0)) + chunk(b"IDAT", body) + chunk(b"IEND", b"")
        self.assertFalse(pc.valid_png(data))

    def test_a_dimension_bomb_is_refused(self):
        big = struct.pack(">IIBBBBB", 8000, 8000, 8, 2, 0, 0, 0)  # 64 M pixels
        self.assertFalse(pc.valid_png(png(ihdr=big, idat_raw=b"\x00" * (8000 * (1 + 24000)))))
        wide = struct.pack(">IIBBBBB", 10000, 1, 8, 2, 0, 0, 0)
        self.assertFalse(pc.valid_png(png(ihdr=wide, idat_raw=raw_rows(10000, 1))))

    def test_interlace_and_odd_ihdr_fields_are_refused(self):
        self.assertFalse(pc.valid_png(png(ihdr=struct.pack(">IIBBBBB", 2, 2, 8, 2, 0, 0, 1))))
        self.assertFalse(pc.valid_png(png(ihdr=struct.pack(">IIBBBBB", 2, 2, 8, 5, 0, 0, 0))))
        self.assertFalse(pc.valid_png(png(ihdr=struct.pack(">IIBBBBB", 2, 2, 3, 2, 0, 0, 0))))
        self.assertFalse(pc.valid_png(png(ihdr=struct.pack(">IIBBBBB", 2, 2, 8, 2, 1, 0, 0))))

    def test_chunk_order_and_repeats_are_refused(self):
        self.assertFalse(pc.valid_png(png(extra_chunks=[(b"sRGB", b"\x00"), (b"sRGB", b"\x00")])))
        self.assertFalse(pc.valid_png(png(extra_chunks=[(b"sRGB", b"\x00" * 2)])))  # wrong length
        self.assertFalse(pc.valid_png(png(ctype=3, depth=8, idat_raw=raw_rows(2, 2, bpp=1))))  # palette without PLTE
        self.assertFalse(pc.valid_png(png()[:-12]))  # no IEND
        data = png()
        iend = data[-12:]
        self.assertFalse(pc.valid_png(data + iend))  # a second IEND
        self.assertFalse(pc.valid_png(pc.PNG_SIG + chunk(b"IEND", b"")))

    def test_idat_chunks_must_be_consecutive_and_may_be_split(self):
        z = zlib.compress(raw_rows(2, 2))
        head = pc.PNG_SIG + chunk(b"IHDR", struct.pack(">IIBBBBB", 2, 2, 8, 2, 0, 0, 0))
        self.assertTrue(pc.valid_png(head + chunk(b"IDAT", z[:5]) + chunk(b"IDAT", z[5:]) + chunk(b"IEND", b"")))
        self.assertFalse(pc.valid_png(head + chunk(b"IDAT", z[:5]) + chunk(b"sRGB", b"\x00") + chunk(b"IDAT", z[5:]) + chunk(b"IEND", b"")))

    def test_the_non_image_inputs_are_refused_without_a_traceback(self):
        for data in (b"", b"\x89PNG", pc.PNG_SIG + b"\x00" * 100, os.urandom(500), b"GIF89a" * 30):
            self.assertFalse(pc.valid_png(data))


class StaleBanner(unittest.TestCase):
    """A head without valid proof must not leave the previous head's verdict on screen."""

    def comment_run(self, api, env=None):
        with tempfile.TemporaryDirectory() as d:
            e = env_for(api, GITHUB_RUN_ID="4242", **(env or {}))
            self.assertEqual(pc.main(["validate", "--out", d], env=e, api=api), 0)
            self.assertEqual(pc.main(["comment", "--bundle", d, "--images-published", "false"], env=env_for(api), api=api), 0)
        return api.writes

    def old_pass_comments(self):
        return [bot_comment(10 + i, k, run=5, sha=OTHER_SHA, body=f"{pc.MARKERS[k]}\n### PASS\nall good\n<!-- lh-proof-run: 5 {OTHER_SHA} -->\n")
                for i, k in enumerate(("perf-smoke", "api-matrix", "visual-proof"))]

    def test_no_artifact_replaces_the_older_pass_comments_with_a_banner(self):
        api = FakeApi(artifacts=[], comments=self.old_pass_comments())
        writes = self.comment_run(api)
        self.assertEqual(sorted((m, p.rsplit("/", 1)[-1]) for m, p, _ in writes), [("PATCH", "10"), ("PATCH", "11"), ("PATCH", "12")])
        for _, _, payload in writes:
            body = payload["body"]
            self.assertIn(f"No valid proof for head `{SHA[:12]}`", body)
            self.assertIn("uploaded no `lh-proof-comment` artifact", body)
            self.assertIn(f"actions/runs/{api.run_id}", body)
            self.assertIn("runs/4242", body)
            self.assertNotIn("PASS", body)
            self.assertIn(f"<!-- lh-proof-run: {api.run_id} {SHA} -->", body)
            self.assertLessEqual(len(body), pc.COMMENT_LIMIT)

    def test_a_rejected_artifact_does_the_same(self):
        api = FakeApi(zipbytes=make_zip(artifact(), extra=[("../../x", b"x")]), comments=self.old_pass_comments())
        writes = self.comment_run(api)
        self.assertEqual([m for m, *_ in writes], ["PATCH"] * 3)
        self.assertIn("failed the commenter's validation", writes[0][2]["body"])

    def test_a_pr_that_skips_the_upload_on_purpose_gets_the_banner_on_its_own_pr_only(self):
        api = FakeApi(artifacts=[], comments=self.old_pass_comments(), prs={7: FakeApi().pr(7)})
        writes = self.comment_run(api)
        self.assertTrue(all("/issues/comments/" in p for _, p, _ in writes))

    def test_no_existing_comment_means_nothing_is_created(self):
        self.assertEqual(self.comment_run(FakeApi(artifacts=[], comments=[])), [])

    def test_only_the_producers_own_kinds_are_touched(self):
        comments = self.old_pass_comments() + [bot_comment(99, "pr-quality", run=5)]
        writes = self.comment_run(FakeApi(artifacts=[], comments=comments))
        self.assertNotIn("99", [p.rsplit("/", 1)[-1] for _, p, _ in writes])

    def test_a_cancelled_run_or_a_closed_pr_makes_no_bundle_and_changes_nothing(self):
        for env, prs in (({"WR_CONCLUSION": "cancelled"}, None), ({}, "closed")):
            api = FakeApi(artifacts=[], comments=self.old_pass_comments())
            if prs:
                api.prs = {7: api.pr(7, state="closed")}
            with tempfile.TemporaryDirectory() as d:
                out = os.path.join(d, "b")
                self.assertEqual(pc.main(["validate", "--out", out], env=env_for(api, **env), api=api), 0)
                self.assertFalse(os.path.exists(out))
            self.assertEqual(api.writes, [])

    def test_a_newer_runs_comment_is_not_replaced_by_an_older_runs_banner(self):
        comments = [bot_comment(10, "perf-smoke", run=900, sha=OTHER_SHA)]
        self.assertEqual(self.comment_run(FakeApi(artifacts=[], comments=comments)), [])

    def test_the_head_moving_before_the_write_stops_the_banner(self):
        api = FakeApi(artifacts=[], comments=self.old_pass_comments())
        with tempfile.TemporaryDirectory() as d:
            pc.main(["validate", "--out", d], env=env_for(api), api=api)
            api.prs = {7: api.pr(7, sha=OTHER_SHA)}
            pc.main(["comment", "--bundle", d, "--images-published", "false"], env=env_for(api), api=api)
        self.assertEqual(api.writes, [])

    def test_a_tampered_stale_bundle_is_rejected(self):
        api = FakeApi(artifacts=[])
        with tempfile.TemporaryDirectory() as d:
            pc.main(["validate", "--out", d], env=env_for(api), api=api)
            meta = json.loads(read_file(os.path.join(d, "meta.json")))
            for change in ({"reason": "<script>"}, {"kinds": ["evil"]}, {"prs": []}, {"prs": ["7"]}, {"conclusion": "a b"}, {"mode": "x"}):
                write_file(os.path.join(d, "meta.json"), json.dumps(dict(meta, **change)))
                with self.assertRaises(pc.Reject, msg=str(change)):
                    pc.read_bundle(d, REPO)
            write_file(os.path.join(d, "meta.json"), json.dumps(meta))
            write_file(os.path.join(d, "perf-smoke.md"), "x")
            with self.assertRaises(pc.Reject):
                pc.read_bundle(d, REPO)


class Review(unittest.TestCase):
    """Findings of the independent review, each with its failing-first test."""

    def s(self, text, images=(), refs=()):
        return pc.sanitize_body(text, repo=REPO, image_names=images, allowed_refs=refs)[0]

    def test_full_issue_and_pull_urls_obey_the_issue_ref_cap(self):
        urls = [f"https://github.com/{REPO}/{'issues' if i % 2 else 'pull'}/{i + 100}" for i in range(21)]
        body = "\n".join(f"[t{i}]({u}) {u}" for i, u in enumerate(urls))
        out = self.s(body, refs=[100, 101])
        kept = re.findall(rf"https://github\.com/{REPO}/(?:issues|pull)/(\d+)", out)
        self.assertEqual(sorted(set(kept)), ["100", "101"])
        self.assertNotIn("(https://github.com", out.replace(f"(https://github.com/{REPO}/pull/100)", "").replace(f"(https://github.com/{REPO}/issues/101)", ""))
        self.assertEqual(self.s(f"[x]({urls[0]})", refs=[]), "x")

    def test_runs_and_commits_are_not_subject_to_the_issue_cap(self):
        ok = f"[run](https://github.com/{REPO}/actions/runs/5) [c](https://github.com/{REPO}/commit/{'a' * 40})"
        self.assertEqual(self.s(ok), ok)

    def test_double_bang_before_a_link_cannot_become_an_image_and_is_idempotent(self):
        once = self.s(f"!![x](https://github.com/{REPO}/issues/1)", refs=[1])
        self.assertNotRegex(once, r"(?<!&#33;)!\[")
        self.assertEqual(self.s(once, refs=[1]), once)
        img = self.s("!![a](image:page.png)", images={"page.png"})
        self.assertNotRegex(img, r"^!\[|[^;]!\[")
        self.assertEqual(self.s(img, images={"page.png"}), img)

    def test_dollar_math_cannot_hide_a_link(self):
        out = self.s("$\\href{//evil.com}{x}$ and $a$")
        self.assertNotIn("$", out)
        self.assertEqual(self.s(out), out)

    def test_digit_mentions_are_neutralised(self):
        self.assertEqual(self.s("@1name"), "@" + ZW + "1name")

    def test_unclosed_details_sub_summary_and_fence_cannot_hide_the_footer(self):
        for body in ("<details><summary>x", "<sub>text", "```\ncode", "<details><sub>a<summary>b"):
            out = pc.render_comment("perf-smoke", body, repo=REPO, pr=7, head_sha=SHA, run_id=3, images_published=True,
                                    verdict="pass", conclusion="success")
            before_footer = out.split("<sub>Head `", 1)[0]
            self.assertEqual(before_footer.count("<details>"), before_footer.count("</details>"), body)
            self.assertEqual(before_footer.count("<sub>"), before_footer.count("</sub>"), body)
            self.assertEqual(before_footer.count("<summary>"), before_footer.count("</summary>"), body)
            self.assertEqual(before_footer.count("```") % 2, 0, body)

    def test_the_footer_shows_provenance_and_flags_a_pass_from_a_failed_run(self):
        ok = pc.render_comment("perf-smoke", "x", repo=REPO, pr=7, head_sha=SHA, run_id=3, images_published=True, verdict="pass", conclusion="success")
        self.assertIn(f"[run 3](https://github.com/{REPO}/actions/runs/3) concluded `success`", ok)
        self.assertIn("Producer verdict `pass`", ok)
        self.assertNotIn("read this report with care", ok)
        bad = pc.render_comment("perf-smoke", "x", repo=REPO, pr=7, head_sha=SHA, run_id=3, images_published=True, verdict="pass", conclusion="failure")
        self.assertIn("read this report with care", bad)

    def test_the_run_conclusion_from_the_api_reaches_the_comment(self):
        api = valid_api(conclusion="failure")
        with tempfile.TemporaryDirectory() as d:
            pc.main(["validate", "--out", d], env=env_for(api), api=api)
            pc.main(["comment", "--bundle", d, "--images-published", "false"], env=env_for(api), api=api)
        self.assertIn("concluded `failure`", api.writes[0][2]["body"])
        self.assertIn("read this report with care", api.writes[0][2]["body"])

    def test_schema_version_true_and_float_are_refused(self):
        for v in (True, 1.0, "1"):
            with self.assertRaises(pc.Reject, msg=repr(v)):
                pc.parse_manifest(json.dumps(manifest(schema_version=v)).encode())

    def test_an_older_run_among_duplicates_cannot_edit_or_delete_a_newer_runs_comment(self):
        comments = [bot_comment(1, "perf-smoke", run=100), bot_comment(2, "perf-smoke", run=900)]
        api = FakeApi(comments=comments)
        out = pc.upsert(api, REPO, 7, "perf-smoke", pc.MARKERS["perf-smoke"] + "\nnew", 500, SHA)
        self.assertIn("newer run 900", out)
        self.assertEqual(api.writes, [])

    def test_a_rerun_with_the_same_run_id_updates_the_comment(self):
        api = FakeApi(comments=[bot_comment(1, "perf-smoke", run=500)])
        self.assertEqual(pc.upsert(api, REPO, 7, "perf-smoke", pc.MARKERS["perf-smoke"] + "\nnew", 500, SHA), "updated")
        self.assertEqual([m for m, *_ in api.writes], ["PATCH"])

    def test_a_403_from_the_api_is_a_green_warning_and_posts_nothing(self):
        api = valid_api()
        with tempfile.TemporaryDirectory() as d:
            pc.main(["validate", "--out", d], env=env_for(api), api=api)
            api.get_paged = lambda path: (_ for _ in ()).throw(pc.ApiError("HTTP 403: Resource not accessible by integration", 403))
            self.assertEqual(pc.main(["comment", "--bundle", d, "--images-published", "false"], env=env_for(api), api=api), 0)
        self.assertEqual(api.writes, [])
        api2 = valid_api()
        api2.get_paged = lambda path: (_ for _ in ()).throw(pc.ApiError("HTTP 500", 500))
        with tempfile.TemporaryDirectory() as d:
            pc.main(["validate", "--out", d], env=env_for(api2), api=api2)
            self.assertEqual(pc.main(["comment", "--bundle", d, "--images-published", "false"], env=env_for(api2), api=api2), 1)


class WorkflowFile(unittest.TestCase):
    """Static properties of .github/workflows/pr-proof-comment.yaml that the review must not be able to lose."""
    PATH = ROOT / ".github/workflows/pr-proof-comment.yaml"

    @classmethod
    def setUpClass(cls):
        cls.text = cls.PATH.read_text()
        cls.doc = yaml.safe_load(cls.text)

    def jobs(self):
        return self.doc["jobs"]

    def test_top_level_permissions_are_empty(self):
        self.assertEqual(self.doc["permissions"], {})

    def test_triggers_are_workflow_run_of_the_two_producers_and_dispatch(self):
        on = self.doc.get("on", self.doc.get(True))
        self.assertEqual(set(on), {"workflow_run", "workflow_dispatch"})
        self.assertEqual(on["workflow_run"]["workflows"], ["PR Quality", "PR proof"])
        self.assertEqual(on["workflow_run"]["types"], ["completed"])

    def test_no_expression_in_a_run_script(self):
        for jid, job in self.jobs().items():
            for step in job["steps"]:
                self.assertNotIn("${{", step.get("run", ""), f"{jid}: {step.get('name')}")

    def test_no_job_checks_out_pull_request_code(self):
        self.assertNotIn("pull_request_target", self.text)
        self.assertNotIn("head_branch", self.text)
        for jid, job in self.jobs().items():
            for step in job["steps"]:
                if str(step.get("uses", "")).startswith("actions/checkout@"):
                    w = step.get("with", {})
                    self.assertIs(w.get("persist-credentials"), False, jid)
                    self.assertLessEqual(set(w), {"persist-credentials", "sparse-checkout", "sparse-checkout-cone-mode"},
                                         f"{jid}: the checkout must be the default branch (no ref, repository or token)")

    def test_the_jobs_only_run_the_base_scripts_in_isolated_mode(self):
        for jid, job in self.jobs().items():
            for step in job["steps"]:
                run = step.get("run", "")
                if not run:
                    continue
                self.assertNotRegex(run, r"(^|[;&|]\s*)(source|eval|bash|sh)\s", f"{jid}: {run}")
                for line in re.findall(r"python3?\s+(\S+)\s+(\S+)", run):
                    self.assertEqual(line[0], "-I", f"{jid}: python must run isolated")
                    self.assertRegex(line[1], r"^scripts/(ci/proof_comment|proof/visual/publish)\.py$", jid)

    def test_the_pr_number_given_to_publish_comes_from_the_rechecked_bundle(self):
        step = next(s for s in self.jobs()["publish"]["steps"] if s.get("name") == "Publish the images")
        self.assertEqual(step["env"]["PR_NUMBER"], "${{ steps.check.outputs.pr }}")

    def test_every_listed_workflow_name_matches_the_name_of_its_producer_file(self):
        """A rename must not silently stop the comments: workflow_run matches `name:`, not the file name."""
        listed = (self.doc.get("on") or self.doc.get(True))["workflow_run"]["workflows"]
        by_path = {"PR Quality": ".github/workflows/pr-quality.yaml", "PR proof": ".github/workflows/pr-proof.yaml"}
        self.assertEqual(sorted(listed), sorted(by_path))
        self.assertEqual(sorted(by_path.values()), sorted(pc.PRODUCERS))
        for name, path in by_path.items():
            f = ROOT / path
            if not f.exists():  # the producer has not landed yet: the commenter is inert for it until it does
                continue
            self.assertEqual(yaml.safe_load(f.read_text()).get("name"), name, f"{path} must be named {name!r} or its comments stop")

    def test_no_other_workflow_is_named_like_a_listed_producer(self):
        listed = (self.doc.get("on") or self.doc.get(True))["workflow_run"]["workflows"]
        for f in sorted((ROOT / ".github/workflows").glob("*.y*ml")):
            name = yaml.safe_load(f.read_text()).get("name")
            if name in listed:
                self.assertIn(f".github/workflows/{f.name}", pc.PRODUCERS, f"{f.name} is named {name!r} but is not an allowed producer path")

    def test_the_write_token_files_are_gate_files_and_have_owners(self):
        import pr_classify
        owners = (ROOT / ".github/CODEOWNERS").read_text()
        for f in (".github/workflows/pr-proof-comment.yaml", ".github/workflows/pr-visuals-prune.yaml", "scripts/proof/visual/publish.py",
                  "scripts/ci/proof_comment.py"):
            self.assertTrue(pr_classify.GATE_FILES_RE.match(f), f)
        for f in (".github/workflows/pr-proof-comment.yaml", ".github/workflows/pr-visuals-prune.yaml", "scripts/proof/visual/publish.py"):
            self.assertRegex(owners, rf"(?m)^{re.escape(f)}\s+@szibis$")  # scripts/ci/ and .github/ are owned by directory rules

    def test_the_comment_job_takes_published_from_the_publish_step_not_from_the_job_result(self):
        env = next(s for s in self.jobs()["comment"]["steps"] if s.get("name") == "Post the comments")["env"]
        self.assertIn("needs.publish.outputs.published", env["IMAGES_PUBLISHED"])
        self.assertNotIn("result", env["IMAGES_PUBLISHED"])
        self.assertIn("published", self.jobs()["publish"]["outputs"])
        step = next(s for s in self.jobs()["publish"]["steps"] if s.get("id") == "publish")
        self.assertIn('echo "published=true" >> "$GITHUB_OUTPUT"', step["run"])

    def test_publish_and_the_prune_serialise_on_one_group(self):
        prune = yaml.safe_load((ROOT / ".github/workflows/pr-visuals-prune.yaml").read_text())
        self.assertEqual(self.jobs()["publish"]["concurrency"]["group"], prune["jobs"]["prune"]["concurrency"]["group"])
        self.assertIs(self.jobs()["publish"]["concurrency"]["cancel-in-progress"], False)


    def test_job_ifs_gate_on_the_event(self):
        jobs = self.jobs()
        self.assertEqual(jobs["dry-run"]["if"], "github.event_name == 'workflow_dispatch'")
        for j in ("validate", "publish", "comment"):
            self.assertIn("workflow_run", jobs[j]["if"])



class PruneWorkflow(unittest.TestCase):
    PATH = ROOT / ".github/workflows/pr-visuals-prune.yaml"

    @classmethod
    def setUpClass(cls):
        cls.text = cls.PATH.read_text()
        cls.doc = yaml.safe_load(cls.text)

    def test_only_a_schedule_and_a_manual_run_start_it(self):
        self.assertEqual(set(self.doc.get("on") or self.doc.get(True)), {"schedule", "workflow_dispatch"})

    def test_permissions_are_exactly_the_reviewed_ones(self):
        self.assertEqual(self.doc["permissions"], {})
        self.assertEqual(self.doc["jobs"]["prune"]["permissions"], {"contents": "write", "pull-requests": "read"})

    def test_it_runs_no_pull_request_code_and_expands_nothing_in_run(self):
        for step in self.doc["jobs"]["prune"]["steps"]:
            self.assertNotIn("${{", step.get("run", ""))
            if str(step.get("uses", "")).startswith("actions/checkout@"):
                self.assertEqual(set(step["with"]), {"persist-credentials", "sparse-checkout", "sparse-checkout-cone-mode"})
                self.assertIs(step["with"]["persist-credentials"], False)
        run = self.doc["jobs"]["prune"]["steps"][-1]["run"]
        self.assertIn("set -euo pipefail", run)  # a failed listing must not become an empty keep list
        self.assertIn("python3 -I scripts/proof/visual/publish.py prune --keep", run)


if __name__ == "__main__":
    unittest.main()
