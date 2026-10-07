"""The Python tie-cut port against the vectors shared with the Go rule (tests/parity/rows_ties.go)."""
import json
import os

import pytest

from scripts.proof.metrics.ties import explained_by_truncated_tie, tie_group_query

GOLDEN = os.path.join(os.path.dirname(__file__), "..", "..", "..", "tests", "parity", "testdata", "tiecut_golden.json")
with open(GOLDEN, encoding="utf-8") as _fh:
    DOC = json.load(_fh)


@pytest.mark.parametrize("c", DOC["query_cases"], ids=lambda c: c["query"])
def test_query_decisions_match_go(c):
    got = tie_group_query(c["query"])
    assert (got is not None) == c["applies"]
    if got:
        assert got == (c["reread_query"], c["sort_fields"])


@pytest.mark.parametrize("c", DOC["tie_cases"], ids=lambda c: c["name"])
def test_tie_decisions_match_go(c):
    fetch = lambda at: (c["ref_status"], c["group_ref"], c["sut_status"], c["group_sut"])  # noqa: E731
    ok, why = explained_by_truncated_tie(fetch, c["ref"], c["sut"], c["sort_fields"], set(c["skip_fields"]))
    assert ok == c["explained"], (c["name"], why)


def test_golden_file_covers_both_decisions():
    assert {c["explained"] for c in DOC["tie_cases"]} == {True, False}
    assert any(c["sort_fields"] for c in DOC["tie_cases"])
