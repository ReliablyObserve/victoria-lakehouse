"""Tie-group rules for row limits that cut through equal sort keys.

ported-from tests/parity/rows_ties.go@44c6d35f737d (victoria-lakehouse, same logic in Python).

VictoriaLogs' sort pipe does not break ties, so which members of a tie group
survive a limit depends on the storage layout. A difference is accepted only
when all of these hold (see the Go file for the full rationale):

  * rows are ordered by _time alone, or by one plain `sort by (k1, ..., _time)`;
    rows tie only when EVERY sort key is equal;
  * both answers have the same row count, and the differing rows are an equal
    number on each side sharing one tie (the first or last _time for a
    _time-only order, one value on every sort key for a multi-key sort);
  * re-read from both sides without the row-limit pipes, the group is identical
    and larger than what the answers kept;
  * every row each side kept in that group is a member of it.

Anything else stays a mismatch, including a query this module cannot take apart.
"""
from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timezone
from typing import Callable

from .common import dumps

# fetcher(at_ns) -> (ref_status, ref_rows, sut_status, sut_rows): the case's
# own query without its row-limit pipes, over a window around at_ns.
TieFetcher = Callable[[int], tuple[int, list[dict], int, list[dict]]]


def split_top_level_pipes(query: str) -> list[str] | None:
    """Split at the | that separate pipes, skipping quotes and parentheses."""
    depth = 0
    quote = ""
    escaped = False
    last = 0
    segs: list[str] = []
    for i, r in enumerate(query):
        if escaped:
            escaped = False
        elif quote:
            if r == "\\" and quote != "`":
                escaped = True
            elif r == quote:
                quote = ""
        elif r in "\"'`":
            quote = r
        elif r == "(":
            depth += 1
        elif r == ")":
            depth -= 1
            if depth < 0:
                return None
        elif r == "|" and depth == 0:
            segs.append(query[last:i].strip())
            last = i + 1
    if quote or depth != 0:
        return None
    segs.append(query[last:].strip())
    return segs


_LIMIT_PIPE = re.compile(r"^(limit|head|offset|skip)\s+\S+$", re.I)
_TIME_FIRST_LAST = re.compile(
    r"^(first|last)\s+\S+\s+by\s*\(\s*_time(\s+(asc|desc))?\s*\)(\s+(asc|desc))?$", re.I
)
_SORT_BY = re.compile(r"^sort\s+by\s*\(([^()]*)\)(\s+(asc|desc))?$", re.I)
_SORT_KEY = re.compile(r"^([A-Za-z_][A-Za-z0-9_.\-]*)(\s+(asc|desc))?$", re.I)
_ROW_ORDER = re.compile(
    r"^(sort|order|first|last|top|uniq|stats|sample|facets|field_values|field_names|"
    r"block_stats|union|join|limit|head|offset|skip)\b",
    re.I,
)


def sort_pipe_keys(seg: str) -> list[str] | None:
    m = _SORT_BY.match(seg)
    if not m:
        return None
    keys = []
    for item in m.group(1).split(","):
        km = _SORT_KEY.match(item.strip())
        if not km:
            return None
        keys.append(km.group(1))
    return keys or None


def tie_group_query(query: str) -> tuple[str, list[str]] | None:
    """(re-read query, sort fields rows tie on), or None when the rule does not apply."""
    segs = split_top_level_pipes(query)
    if segs is None:
        return None
    kept = segs[:1]
    fields = ["_time"]
    sorts = 0
    for seg in segs[1:]:
        if _LIMIT_PIPE.match(seg) or _TIME_FIRST_LAST.match(seg):
            continue
        if _SORT_BY.match(seg):
            keys = sort_pipe_keys(seg)
            if keys is None or sorts > 0 or "_time" not in keys:
                return None
            sorts += 1
            if len(keys) > 1 or keys[0] != "_time":
                fields = keys
        elif _ROW_ORDER.match(seg):
            return None
        kept.append(seg)
    return " | ".join(kept), fields


def time_only(fields: list[str]) -> bool:
    return not fields or fields == ["_time"]


_TS = re.compile(r"^(\d{4})-(\d\d)-(\d\d)[T ](\d\d):(\d\d):(\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)?$")


def parse_time_ns(s) -> int | None:
    """RFC3339Nano to integer nanoseconds since the epoch; None if unparsable."""
    if not isinstance(s, str):
        return None
    m = _TS.match(s)
    if not m:
        return None
    y, mo, d, h, mi, se, frac, tz = m.groups()
    try:
        dt = datetime(int(y), int(mo), int(d), int(h), int(mi), int(se), tzinfo=timezone.utc)
    except ValueError:
        return None
    ns = int(dt.timestamp()) * 1_000_000_000
    ns += int((frac or "0").ljust(9, "0")[:9])
    if tz and tz != "Z":
        sign = 1 if tz[0] == "+" else -1
        off = (int(tz[1:3]) * 3600 + int(tz[4:6]) * 60) * 1_000_000_000
        ns -= sign * off
    return ns


def row_key(row: dict, skip: frozenset[str] | set[str] = frozenset()) -> str:
    return dumps({k: v for k, v in row.items() if k not in skip})


# The Go rule never looks at the stream fields when it keys rows (extractRowKeys).
_TIE_SKIP = frozenset({"_stream", "_stream_id"})


def row_time(row: dict) -> int | None:
    return parse_time_ns(row.get("_time"))


def _multiset_diff(a: list[str], b: list[str]) -> tuple[list[str], list[str]]:
    ca, cb = Counter(a), Counter(b)
    a_only = sorted((ca - cb).elements())
    b_only = sorted((cb - ca).elements())
    return a_only, b_only


def _is_sub(sub: list[str], sup: list[str]) -> bool:
    return not (Counter(sub) - Counter(sup))


def _times(rows: list[dict]) -> list[int] | None:
    out = []
    for r in rows:
        t = row_time(r)
        if t is None:
            return None
        out.append(t)
    return out


def _sort_tuple(row: dict, fields: list[str]) -> str:
    return "|".join(f"{f}={row.get(f)}" for f in fields)


def explained_by_truncated_tie(
    fetch: TieFetcher | None,
    ref_rows: list[dict],
    sut_rows: list[dict],
    sort_fields: list[str] | None = None,
    skip: frozenset[str] | set[str] = frozenset(),
) -> tuple[bool, str]:
    """Is the row difference fully explained by a limit cutting one tie group?

    Returns (explained, why-not). sort_fields None or ["_time"] means _time alone.
    """
    if fetch is None:
        return False, "the case does not order rows by a re-readable key"
    if len(ref_rows) != len(sut_rows):
        return False, f"row counts differ: ref={len(ref_rows)} sut={len(sut_rows)}"
    fields = sort_fields or ["_time"]
    skip = set(skip) | _TIE_SKIP
    ref_keys = [row_key(r, skip) for r in ref_rows]
    sut_keys = [row_key(r, skip) for r in sut_rows]
    ref_only, sut_only = _multiset_diff(ref_keys, sut_keys)
    if not ref_only or len(ref_only) != len(sut_only):
        return False, "the differing rows are not an equal number on each side"
    by_key = {row_key(r, skip): r for r in ref_rows + sut_rows}
    diff_rows = [by_key[k] for k in ref_only + sut_only]

    if time_only(fields):
        ts = [row_time(r) for r in diff_rows]
        if any(t is None for t in ts) or len(set(ts)) != 1:
            return False, "the differing rows do not share one _time"
        at = ts[0]
        all_t = _times(ref_rows + sut_rows)
        if all_t is None or at not in (min(all_t), max(all_t)):
            return False, "the tie is not at the edge of both answers"
        member = lambda r: row_time(r) == at  # noqa: E731
    else:
        tuples = {_sort_tuple(r, fields) for r in diff_rows}
        if len(tuples) != 1:
            return False, "the differing rows do not share one value on every sort key"
        tup = tuples.pop()
        at = row_time(diff_rows[0])
        if at is None:
            return False, "the differing rows have no parsable _time"
        member = lambda r: _sort_tuple(r, fields) == tup  # noqa: E731

    rs, rrows, ss, srows = fetch(at)
    if rs != 200 or ss != 200:
        return False, f"re-reading the group returned status ref={rs} sut={ss}"
    ref_group = [row_key(r, skip) for r in rrows if member(r)]
    sut_group = [row_key(r, skip) for r in srows if member(r)]
    g_ref, g_sut = _multiset_diff(ref_group, sut_group)
    if g_ref or g_sut:
        return False, f"the full group differs between sides: ref-only {g_ref}, sut-only {g_sut}"
    ref_kept = [row_key(r, skip) for r in ref_rows if member(r)]
    sut_kept = [row_key(r, skip) for r in sut_rows if member(r)]
    if len(ref_group) <= len(ref_kept):
        return False, f"the group has {len(ref_group)} rows and the answers kept {len(ref_kept)}, so no limit cut it"
    if not _is_sub(ref_kept, ref_group) or not _is_sub(sut_kept, sut_group):
        return False, "a kept row of that group is not a member of it"
    return True, ""
