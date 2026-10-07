import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import release_skip as r  # noqa: E402

SHA = "a" * 40


def titles(*ts):
    return lambda number=None: list(ts)


def boom(number=None):
    raise OSError("network down")


class Decide(unittest.TestCase):
    def test_marker_in_commit_subject(self):
        d = r.decide("chore: x [skip release]\n\nbody", boom)
        self.assertTrue(d.skip)
        self.assertFalse(d.failed)

    def test_marker_only_in_the_pr_title(self):
        d = r.decide("test(manifest): RangePartitions writer test (#441)", titles("test(manifest): x [skip release]"))
        self.assertTrue(d.skip)
        self.assertIn("[skip release]", d.reason)

    def test_any_associated_pr_with_the_marker_skips(self):
        self.assertTrue(r.decide("fix: x", titles("fix: x", "ci: y [skip release]")).skip)

    def test_no_marker_releases(self):
        self.assertFalse(r.decide("fix(read): x (#429)", titles("fix(read): x")).skip)

    def test_the_pr_number_from_the_subject_is_passed_to_the_lookup(self):
        seen = []
        r.decide("fix: x (#77)", lambda n: seen.append(n) or [])
        r.decide("fix: direct", lambda n: seen.append(n) or [])
        self.assertEqual(seen, ["77", None])

    def test_direct_push_without_a_pr_releases(self):
        d = r.decide("fix: direct", titles())
        self.assertFalse(d.skip)
        self.assertIn("direct push", d.reason)

    def test_api_failure_fails_closed_and_is_marked_failed(self):
        d = r.decide("fix: x (#9)", boom)
        self.assertTrue(d.skip)
        self.assertTrue(d.failed)
        self.assertIn("workflow_dispatch", d.reason)

    def test_a_backport_subject_quoting_another_pr_does_not_matter(self):
        # the lookup is by SHA: the subject's (#12) is never parsed
        self.assertFalse(r.decide("fix: backport (#12)", titles("fix: backport of the real PR")).skip)

    def test_marker_in_the_body_only_does_not_skip(self):
        self.assertFalse(r.decide("fix: x\n\nnot a title [skip release]", titles("fix: x")).skip)

    def test_manual_run_is_never_skipped(self):
        for msg in ("", "chore: x [skip release]", "fix: x (#9)"):
            d = r.decide(msg, boom, event="workflow_dispatch")
            self.assertFalse(d.skip, msg)
            self.assertFalse(d.failed)

    def test_only_main_releases(self):
        for ref in ("refs/heads/feature", "", "refs/tags/v1"):
            d = r.decide("fix: x", titles("fix: x"), event="push", ref=ref)
            self.assertTrue(d.skip, ref)
            self.assertTrue(d.failed, ref)
        d = r.decide("fix: x", titles("fix: x"), event="workflow_dispatch", ref="refs/heads/feature")
        self.assertTrue(d.skip and d.failed, "a manual run on another branch is refused")

    def test_unpacks_as_skip_reason(self):
        skip, reason = r.decide("chore: [skip release]", boom)
        self.assertTrue(skip)
        self.assertTrue(reason)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return io.BytesIO(json.dumps(self.payload).encode())

    def __exit__(self, *a):
        return False


class GithubTitles(unittest.TestCase):
    def fetch(self, pages, number=None, repo="o/r", token="tok", sha=SHA, sleep=lambda s: None, urlopen=None):
        """pages: {url suffix: payload}; returns (titles, [(url, auth, timeout)])."""
        seen = []

        def fake(req, timeout=None):
            seen.append((req.full_url, req.get_header("Authorization"), timeout))
            for suffix, payload in pages.items():
                if req.full_url.endswith(suffix):
                    return FakeResponse(payload)
            raise OSError("404 " + req.full_url)

        with unittest.mock.patch.object(r.urllib.request, "urlopen", urlopen or fake):
            out = r.github_titles(repo, token, sha, sleep)(number)
        return out, seen

    def test_request_shape_and_the_title_key(self):
        out, seen = self.fetch({f"/commits/{SHA}/pulls": [{"title": "T", "body": "B", "merge_commit_sha": SHA}]})
        self.assertEqual(out, ["T"])
        self.assertEqual(seen, [(f"https://api.github.com/repos/o/r/commits/{SHA}/pulls", "Bearer tok", 30)])

    def test_only_the_pr_merged_as_the_pushed_sha_counts(self):
        # Low 3: an unrelated PR associated with the commit must never decide it
        out, _ = self.fetch({f"/commits/{SHA}/pulls": [{"title": "other", "merge_commit_sha": "b" * 40}, {"title": "mine", "merge_commit_sha": SHA}]})
        self.assertEqual(out, ["mine"])
        out, _ = self.fetch({f"/commits/{SHA}/pulls": [{"title": "unrelated open PR [skip release]", "merge_commit_sha": "c" * 40}]})
        self.assertEqual(out, [], "no match is a direct push")

    def test_empty_lookup_with_a_pr_number_reads_that_pr(self):
        # Low 2: the subject says (#5) but the commit is not listed: ask PR 5 itself
        pages = {f"/commits/{SHA}/pulls": [], "/pulls/5": {"title": "fix: x [skip release]", "merge_commit_sha": SHA}}
        out, seen = self.fetch(pages, number="5")
        self.assertEqual(out, ["fix: x [skip release]"])
        self.assertEqual([u.rsplit("/", 2)[-2:] for u, _, _ in seen], [[SHA, "pulls"], ["pulls", "5"]])

    def test_pr_number_merged_as_another_commit_fails_closed(self):
        pages = {f"/commits/{SHA}/pulls": [], "/pulls/5": {"title": "t", "merge_commit_sha": "d" * 40}}
        with self.assertRaises(RuntimeError):
            self.fetch(pages, number="5")
        d = r.decide("fix: x (#5)", lambda n: (_ for _ in ()).throw(RuntimeError("PR #5 was not merged as this commit")))
        self.assertTrue(d.skip and d.failed)

    def test_no_pr_and_no_number_is_a_direct_push(self):
        out, _ = self.fetch({f"/commits/{SHA}/pulls": []})
        self.assertEqual(out, [])

    def test_missing_repo_token_or_sha_fails_without_calling_the_api(self):
        for kw in ({"repo": ""}, {"token": ""}, {"sha": ""}):
            called = []
            with self.assertRaises(RuntimeError):
                self.fetch({}, urlopen=lambda *a, **k: called.append(1), **kw)
            self.assertEqual(called, [], kw)

    def test_retries_with_backoff_then_succeeds(self):
        calls, sleeps = [], []

        def flaky(req, timeout=None):
            calls.append(1)
            if len(calls) < 3:
                raise OSError("503")
            return FakeResponse([{"title": "ok", "merge_commit_sha": SHA}])

        out, _ = self.fetch({}, urlopen=flaky, sleep=sleeps.append)
        self.assertEqual(out, ["ok"])
        self.assertEqual(len(calls), 3)
        self.assertEqual(sleeps, [1, 2])

    def test_gives_up_after_three_attempts(self):
        calls = []

        def down(req, timeout=None):
            calls.append(1)
            raise OSError("down")

        with self.assertRaises(OSError):
            self.fetch({}, urlopen=down)
        self.assertEqual(len(calls), 3)


class Cli(unittest.TestCase):
    ENV_KEYS = ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "COMMIT_MSG", "GITHUB_EVENT_NAME", "GITHUB_REF", "GITHUB_SHA",
                "GITHUB_OUTPUT", "GITHUB_STEP_SUMMARY")

    def run_cli(self, msg, env=None):
        with tempfile.TemporaryDirectory() as d:
            out, summary = os.path.join(d, "out"), os.path.join(d, "summary")
            e = {k: v for k, v in os.environ.items() if k not in self.ENV_KEYS}
            e.update({"COMMIT_MSG": msg, "GITHUB_OUTPUT": out, "GITHUB_STEP_SUMMARY": summary, **(env or {})})
            p = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "..", "release_skip.py")],
                               capture_output=True, text=True, env=e)
            def read(path):
                if not os.path.exists(path):
                    return ""
                with open(path, encoding="utf-8") as fh:
                    return fh.read()

            return p, read(out), read(summary)

    def test_skip_written_and_summarised(self):
        p, out, summary = self.run_cli("chore: x [skip release]")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(out, "skip=true\n")
        self.assertIn("[skip release]", summary)

    def test_fail_closed_is_loud(self):
        p, out, summary = self.run_cli("fix: x (#9)")
        self.assertEqual(p.returncode, 1, "the run must turn red")
        self.assertEqual(out, "skip=true\n")
        self.assertTrue(any(l.startswith("::warning::") for l in p.stdout.splitlines()), p.stdout)
        self.assertIn("workflow_dispatch", summary)

    def test_plain_push_without_a_pr_lookup_failure_releases(self):
        # a token-less environment cannot look the PR up: fail closed, never release blindly
        p, out, _ = self.run_cli("fix: direct push")
        self.assertEqual(out, "skip=true\n")

    def test_manual_run_releases_and_other_ref_is_refused(self):
        p, out, summary = self.run_cli("", {"GITHUB_EVENT_NAME": "workflow_dispatch"})
        self.assertEqual((p.returncode, out), (0, "skip=false\n"))
        self.assertIn("release", summary)
        p, out, _ = self.run_cli("", {"GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/x"})
        self.assertEqual((p.returncode, out), (1, "skip=true\n"))

    def test_main_in_process(self):
        with unittest.mock.patch.dict(os.environ, {"COMMIT_MSG": "chore: [skip release]"}, clear=False), contextlib.redirect_stdout(io.StringIO()):
            os.environ.pop("GITHUB_OUTPUT", None)
            os.environ.pop("GITHUB_STEP_SUMMARY", None)
            os.environ.pop("GITHUB_EVENT_NAME", None)
            os.environ.pop("GITHUB_REF", None)
            self.assertEqual(r.main(), 0)


if __name__ == "__main__":
    unittest.main()
