import json
import os
import subprocess
import sys
import io
import tempfile
import unittest
import unittest.mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import registry_exempt as r  # noqa: E402


def lab(kind, actor, at, i=0, label="registry-exempt"):
    return {"kind": kind, "actor": actor, "at": at, "id": i}


def commit(at, i=0):
    return {"kind": "commit", "actor": "", "at": at, "id": i}


def push(at, i=0):
    return {"kind": "force_push", "actor": "", "at": at, "id": i}


OWNERS = {"szibis"}
T1, T2, T3 = "2026-10-07T10:00:00Z", "2026-10-07T11:00:00Z", "2026-10-07T12:00:00Z"


class Decide(unittest.TestCase):
    def test_owner_applied_after_commits(self):
        ok, _ = r.decide([commit(T1), lab("labeled", "SZIBIS", T2)], OWNERS)
        self.assertTrue(ok)

    def test_non_owner_applied(self):
        ok, msg = r.decide([lab("labeled", "mallory", T1)], OWNERS)
        self.assertFalse(ok)
        self.assertIn("not an approver", msg)

    def test_relabeled_by_non_owner(self):
        ok, msg = r.decide([lab("labeled", "szibis", T1), lab("unlabeled", "szibis", T2), lab("labeled", "mallory", T3)], OWNERS)
        self.assertFalse(ok)
        self.assertIn("mallory", msg)

    def test_owner_relabels_after_non_owner(self):
        ok, _ = r.decide([lab("labeled", "mallory", T1), lab("unlabeled", "mallory", T2), lab("labeled", "szibis", T3)], OWNERS)
        self.assertTrue(ok)

    def test_removed(self):
        ok, _ = r.decide([lab("labeled", "szibis", T1), lab("unlabeled", "szibis", T2)], OWNERS)
        self.assertFalse(ok)

    def test_no_label_event(self):
        self.assertFalse(r.decide([commit(T1)], OWNERS)[0])

    def test_unknown_actor_and_no_approvers(self):
        self.assertFalse(r.decide([lab("labeled", "", T1)], OWNERS)[0])
        self.assertFalse(r.decide([lab("labeled", "szibis", T1)], set())[0])

    def test_commit_after_label_denies(self):
        ok, msg = r.decide([lab("labeled", "szibis", T1), commit(T2)], OWNERS)
        self.assertFalse(ok)
        self.assertIn("later than", msg)

    def test_force_push_after_label_denies(self):
        self.assertFalse(r.decide([lab("labeled", "szibis", T2), push(T3)], OWNERS)[0])

    def test_commit_dated_before_label_allows_and_equal_time_allows(self):
        self.assertTrue(r.decide([commit(T2), lab("labeled", "szibis", T2, 1)], OWNERS)[0])

    def test_sorted_by_time_not_input_order(self):
        # API order is not trusted: the later "labeled" by the owner wins even when listed first.
        ok, _ = r.decide([lab("labeled", "szibis", T3), lab("labeled", "mallory", T1)], OWNERS)
        self.assertTrue(ok)
        ok, _ = r.decide([lab("labeled", "mallory", T3), lab("labeled", "szibis", T1)], OWNERS)
        self.assertFalse(ok)

    def test_same_time_ties_break_by_id(self):
        ok, _ = r.decide([lab("labeled", "szibis", T1, 2), lab("labeled", "mallory", T1, 1)], OWNERS)
        self.assertTrue(ok)
        ok, _ = r.decide([lab("labeled", "szibis", T1, 1), lab("labeled", "mallory", T1, 2)], OWNERS)
        self.assertFalse(ok)

    def test_push_triggered_run_denies(self):
        for action in ("synchronize", "opened", "reopened"):
            self.assertFalse(r.decide([lab("labeled", "szibis", T1)], OWNERS, action)[0], action)
        self.assertTrue(r.decide([lab("labeled", "szibis", T1)], OWNERS, "labeled")[0])
        self.assertTrue(r.decide([lab("labeled", "szibis", T1)], OWNERS, "edited")[0])

    def test_malformed_events_deny(self):
        self.assertFalse(r.decide([{"kind": "labeled"}], OWNERS)[0])
        self.assertFalse(r.decide([lab("labeled", "szibis", "not a date")], OWNERS)[0])

    def test_parse_approvers(self):
        self.assertEqual(r.parse_approvers("# c\nSzibis  # owner\n\n"), {"szibis"})


class Normalise(unittest.TestCase):
    def test_graphql_nodes(self):
        nodes = [
            {"__typename": "LabeledEvent", "createdAt": T2, "actor": {"login": "szibis"}, "label": {"name": "registry-exempt"}},
            {"__typename": "LabeledEvent", "createdAt": T2, "actor": {"login": "x"}, "label": {"name": "bug"}},
            {"__typename": "PullRequestCommit", "commit": {"committedDate": T1, "authoredDate": T3}},
            {"__typename": "HeadRefForcePushedEvent", "createdAt": T3},
            {"__typename": "UnlabeledEvent", "createdAt": T3, "actor": None, "label": {"name": "registry-exempt"}},
        ]
        ev = r.normalise(nodes)
        self.assertEqual([e["kind"] for e in ev], ["labeled", "commit", "force_push", "unlabeled"])
        self.assertEqual(ev[1]["at"], T3)  # the later of authored/committed


class GraphqlPath(unittest.TestCase):
    """fetch_events against fixture GraphQL pages: the query, the headers, pagination and the decision."""

    def page(self, nodes, more, cursor=None):
        return {"data": {"repository": {"pullRequest": {"timelineItems": {
            "pageInfo": {"hasNextPage": more, "endCursor": cursor}, "nodes": nodes}}}}}

    def run_fetch(self, pages):
        seen = []

        class Resp:
            def __init__(self, payload):
                self.payload = payload

            def __enter__(self):
                return io.BytesIO(json.dumps(self.payload).encode())

            def __exit__(self, *a):
                return False

        def fake(req, timeout=None):
            seen.append((req.full_url, req.get_header("Authorization"), json.loads(req.data)))
            return Resp(pages[len(seen) - 1])

        with unittest.mock.patch.object(r.urllib.request, "urlopen", fake):
            return r.fetch_events("o/r", "451", "tok"), seen

    def test_two_pages_normalised_and_decided(self):
        nodes1 = [{"__typename": "PullRequestCommit", "commit": {"committedDate": T1, "authoredDate": T1}}]
        nodes2 = [{"__typename": "LabeledEvent", "createdAt": T2, "actor": {"login": "szibis"}, "label": {"name": "registry-exempt"}}]
        events, seen = self.run_fetch([self.page(nodes1, True, "CUR1"), self.page(nodes2, False)])
        self.assertEqual([e["kind"] for e in events], ["commit", "labeled"])
        self.assertTrue(r.decide(events, {"szibis"}, "labeled")[0])
        self.assertEqual(len(seen), 2)
        url, auth, body = seen[0]
        self.assertEqual((url, auth), ("https://api.github.com/graphql", "Bearer tok"))
        self.assertEqual(body["variables"], {"owner": "o", "name": "r", "pr": 451, "after": None})
        self.assertEqual(seen[1][2]["variables"]["after"], "CUR1")
        for item in ("LABELED_EVENT", "UNLABELED_EVENT", "PULL_REQUEST_COMMIT", "HEAD_REF_FORCE_PUSHED_EVENT"):
            self.assertIn(item, body["query"])

    def test_graphql_errors_fail_closed(self):
        with self.assertRaises(RuntimeError):
            self.run_fetch([{"errors": [{"message": "boom"}]}])


class Cli(unittest.TestCase):
    def run_cli(self, events, approvers="szibis\n", env=None, extra=()):
        with tempfile.TemporaryDirectory() as d:
            ap = os.path.join(d, "approvers")
            with open(ap, "w", encoding="utf-8") as fh:
                fh.write(approvers)
            args = [sys.executable, os.path.join(os.path.dirname(__file__), "..", "registry_exempt.py"), "--approvers", ap, *extra]
            if events is not None:
                ef = os.path.join(d, "e.json")
                with open(ef, "w", encoding="utf-8") as fh:
                    json.dump(events, fh)
                args += ["--events-file", ef]
            e = {k: v for k, v in os.environ.items() if k not in ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "PR_NUMBER", "EVENT_ACTION")}
            e.update(env or {})
            return subprocess.run(args, capture_output=True, text=True, env=e)

    def test_owner_ok(self):
        self.assertEqual(self.run_cli([lab("labeled", "szibis", T1)]).returncode, 0)

    def test_push_action_via_env_denies(self):
        self.assertEqual(self.run_cli([lab("labeled", "szibis", T1)], env={"EVENT_ACTION": "synchronize"}).returncode, 1)

    def test_no_token_fails_closed(self):
        p = self.run_cli(None)
        self.assertEqual(p.returncode, 1)
        self.assertIn("only works in CI", p.stdout)

    def test_api_failure_fails_closed(self):
        p = self.run_cli(None, env={"GITHUB_TOKEN": "x", "GITHUB_REPOSITORY": "invalid/invalid", "PR_NUMBER": "1"})
        self.assertEqual(p.returncode, 1)
        self.assertIn("cannot verify", p.stdout)

    def test_bad_events_file_fails_closed(self):
        self.assertEqual(self.run_cli({"not": "a list"}).returncode, 1)

    def test_missing_approvers_file_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            ef = os.path.join(d, "e.json")
            with open(ef, "w", encoding="utf-8") as fh:
                json.dump([lab("labeled", "szibis", T1)], fh)
            p = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "..", "registry_exempt.py"),
                                "--approvers", os.path.join(d, "nope"), "--events-file", ef], capture_output=True, text=True)
            self.assertEqual(p.returncode, 1)


if __name__ == "__main__":
    unittest.main()
