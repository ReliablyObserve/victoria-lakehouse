import pytest

from scripts.proof.metrics.ties import (
    explained_by_truncated_tie, parse_time_ns, row_key, sort_pipe_keys, split_top_level_pipes, tie_group_query,
)


def test_split_pipes_respects_quotes_and_parens():
    assert split_top_level_pipes('a | b') == ["a", "b"]
    assert split_top_level_pipes('"a|b" | c') == ['"a|b"', "c"]
    assert split_top_level_pipes("x:in(a|b) | limit 5") == ["x:in(a|b)", "limit 5"]
    assert split_top_level_pipes("`a|b` | c") == ["`a|b`", "c"]
    assert split_top_level_pipes(r'"a\"|b" | c') == [r'"a\"|b"', "c"]
    assert split_top_level_pipes('"unbalanced') is None
    assert split_top_level_pipes("a) | b") is None
    assert split_top_level_pipes("(a | b") is None


def test_sort_pipe_keys():
    assert sort_pipe_keys("sort by (level, _time desc) desc") == ["level", "_time"]
    assert sort_pipe_keys("sort by (a + 1)") is None
    assert sort_pipe_keys("sort by (_time) limit 3") is None
    assert sort_pipe_keys("limit 3") is None


@pytest.mark.parametrize("q,want", [
    ("*", ("*", ["_time"])),
    ("* | limit 10", ("*", ["_time"])),
    ("error | sort by (_time) desc | limit 5", ("error | sort by (_time) desc", ["_time"])),
    ("* | first 3 by (_time desc)", ("*", ["_time"])),
    ("* | sort by (level, _time) | head 4", ("* | sort by (level, _time)", ["level", "_time"])),
    ("* | stats count()", None),
    ("* | sort by (level)", None),  # no _time key to window the re-read
    ("* | sort by (_time) | sort by (_time)", None),
    ("* | uniq by (a)", None),
    ('"unbalanced', None),
])
def test_tie_group_query(q, want):
    assert tie_group_query(q) == want


def test_parse_time_ns():
    assert parse_time_ns("1970-01-01T00:00:01Z") == 1_000_000_000
    assert parse_time_ns("1970-01-01T00:00:01.5Z") == 1_500_000_000
    assert parse_time_ns("1970-01-01T01:00:00+01:00") == 0
    assert parse_time_ns("1970-01-01T00:00:00.000000001Z") == 1
    assert parse_time_ns("garbage") is None and parse_time_ns(5) is None
    assert parse_time_ns("1970-13-01T00:00:00Z") is None


def rows_at(t, names, **kw):
    return [{"_time": t, "_msg": n, **kw} for n in names]


def test_multi_key_tie_group():
    t = "2026-01-01T00:00:10.000000000Z"
    g = rows_at(t, "ABC", level="warn")
    other = [{"_time": "2026-01-01T00:00:11.000000000Z", "_msg": "z", "level": "info"}]
    ref, sut = other + [g[0], g[1]], other + [g[0], g[2]]
    fetch = lambda at: (200, g, 200, g)  # noqa: E731
    assert explained_by_truncated_tie(fetch, ref, sut, ["level", "_time"]) == (True, "")
    # differing rows with different level values are not one tie group
    sut2 = other + [g[0], {**g[2], "level": "error"}]
    ok, why = explained_by_truncated_tie(fetch, ref, sut2, ["level", "_time"])
    assert not ok and "one value on every sort key" in why


def test_tie_rejections():
    t = "2026-01-01T00:00:10.000000000Z"
    g = rows_at(t, "ABC")
    ref, sut = [g[0]], [g[1]]
    f = lambda at: (200, g, 200, g)  # noqa: E731
    assert explained_by_truncated_tie(None, ref, sut)[0] is False
    assert not explained_by_truncated_tie(f, ref, ref)[0]  # nothing differs
    assert not explained_by_truncated_tie(f, [g[0]], [g[1], g[2]])[0]  # unequal count
    mid = {"_time": "2026-01-01T00:00:12.000000000Z", "_msg": "o"}
    top = {"_time": "2026-01-01T00:00:13.000000000Z", "_msg": "p"}
    tie = rows_at("2026-01-01T00:00:12.500000000Z", "AB")
    assert "edge" in explained_by_truncated_tie(f, [mid, tie[0], top], [mid, tie[1], top])[1]
    bad_time = [{"_time": "x", "_msg": "a"}], [{"_time": "x", "_msg": "b"}]
    assert not explained_by_truncated_tie(f, *bad_time)[0]
    two_times = [rows_at(t, "A")[0]], [rows_at("2026-01-01T00:00:11.000000000Z", "B")[0]]
    assert "share one _time" in explained_by_truncated_tie(f, *two_times)[1]
    # group member check: a kept row that is not in the re-read group
    foreign = [{"_time": t, "_msg": "Q"}]
    assert "not a member" in explained_by_truncated_tie(lambda at: (200, g, 200, g), [g[0]], foreign)[1]


def test_row_key_ignores_key_order_and_skip():
    assert row_key({"a": 1, "b": 2}) == row_key({"b": 2, "a": 1.0})
    assert row_key({"a": 1, "t": 5}, {"t"}) == row_key({"a": 1})
