"""Answer validation of the A/B smoke: a port of scripts/bench/run.sh's `extract_result` / `_validate_iter`.

A timing is only a latency when the answer is a correct one: a fast empty, wrong or error answer is a broken
response, never a sample. Every timed request goes through `validate`, and what it extracts is also the
fingerprint that says whether two systems answered the same thing:

  scalar        a count: the integer ("17132")
  groupby       "rows=<groups>;total=<sum>;hash=<sha256 of the sorted (group, count) pairs>"
  values        "rows=<n>;total=<sum of hits>;hash=<sha256 of the sorted (value, hits) pairs>"
  scan          "rows=<n>;hash=<sha256 of the sorted set of row keys>" (key `_msg`, or `trace_id:span_id`)
  spans         "spans=<n>" (trace by id)
  json          "json=<sha256 of the canonical JSON>" (Jaeger, Tempo and TraceQL metrics bodies), volatile members dropped

tests/test_validate.py runs run.sh's own extract_result on the same fixtures and requires the same strings,
so the two cannot drift. Empty answers are invalid except for kinds that may legitimately be empty (MISS_KINDS).
"""
from __future__ import annotations

import hashlib
import json

MISS_KINDS = ("trace_lookup",)  # run.sh MISS_QUERIES: a cross-signal lookup may find nothing
SCAN_LIMIT = 1000  # run.sh SCAN_LIMIT
GROUPBY_COUNT_KEYS = ("n", "count(*)", "count(")
# Members that legitimately differ between two correct answers of the same question.
VOLATILE = frozenset({"startTime", "start_time", "duration_ms", "durationMs", "took", "elapsed", "processed", "exec_time"})


def sha(parts: list[str]) -> str:
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def _lines(body: str):
    """(non-empty lines, parsed objects, parse-error flag): run.sh treats a body with SOME unparseable lines as a parse failure."""
    n, objs = 0, []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        n += 1
        try:
            objs.append(json.loads(line))
        except ValueError:
            continue
    return n, objs, n != len(objs)


def _count_of(d: dict):
    for k in GROUPBY_COUNT_KEYS:
        if k in d:
            return d[k]
    return None


def extract_scalar(body: str) -> str:
    n, objs, bad = _lines(body)
    if n and bad:
        return "invalid:parse-error"
    tot, has = 0, False
    for d in objs:
        v = d.get("n", d.get("count(*)", d.get("count(")))
        if v is not None:
            has = True
            try:
                tot += int(v)
            except (TypeError, ValueError):
                pass
    return str(tot if has else n)


def extract_groupby(body: str) -> str:
    n, objs, bad = _lines(body)
    if n == 0:
        return "invalid:empty-body"
    if bad:
        return "invalid:parse-error"
    total, pairs = 0, []
    for d in objs:
        cnt = _count_of(d)
        key = None
        for k, v in d.items():
            if k not in GROUPBY_COUNT_KEYS:
                key = v
                break
        try:
            total += int(cnt)
        except (TypeError, ValueError):
            pass
        pairs.append((str(key), str(cnt)))
    pairs.sort()
    joined = [k + "\x00" + v for k, v in pairs]
    return f"rows={n};total={total};hash={sha(joined)}"


def extract_values(body: str) -> str:
    raw = body.strip()
    if not raw:
        return "invalid:empty-body"
    try:
        if raw.startswith("{") and '"values"' in raw.split("\n", 1)[0]:
            vals = json.loads(raw)["values"]
        else:
            vals = [json.loads(line) for line in raw.splitlines() if line.strip()]
        pairs = [(str(v["value"]), int(v["hits"])) for v in vals]
    except (ValueError, KeyError, TypeError):
        return "invalid:parse-error"
    if not pairs:
        return "invalid:empty-body"
    pairs.sort()
    joined = [k + "\x00" + str(c) for k, c in pairs]
    return f"rows={len(pairs)};total={sum(c for _, c in pairs)};hash={sha(joined)}"


def scan_keys(body: str):
    """(rows, sorted unique keys) or None on a parse error."""
    n, objs, bad = _lines(body)
    if n and bad:
        return None
    keys = []
    for d in objs:
        if "trace_id" in d or "span_id" in d:
            keys.append("{}:{}".format(d.get("trace_id", ""), d.get("span_id", "")))
        else:
            keys.append(d.get("_msg", ""))
    return n, sorted(set(keys))


def extract_scan(body: str) -> str:
    got = scan_keys(body)
    if got is None:
        return "invalid:parse-error"
    n, uniq = got
    return f"rows={n};hash={sha(uniq)}"


def extract_spans(body: str) -> str:
    n, objs, bad = _lines(body)
    if n and bad:
        return "invalid:parse-error"
    tot = 0
    for d in objs:
        try:
            tot += int(d.get("n"))
        except (TypeError, ValueError):
            pass
    return f"spans={tot}"


def _strip(o):
    if isinstance(o, dict):
        return {k: _strip(v) for k, v in o.items() if k not in VOLATILE}
    if isinstance(o, list):
        return [_strip(v) for v in o]
    return o


def _canon(o):
    """Canonical form: object members sorted, lists of objects sorted by their canonical text (answers differ in order only)."""
    if isinstance(o, dict):
        return {k: _canon(o[k]) for k in sorted(o)}
    if isinstance(o, list):
        items = [_canon(v) for v in o]
        return sorted(items, key=lambda x: json.dumps(x, sort_keys=True)) if all(isinstance(x, (dict, list)) for x in items) else items
    return o


def extract_json(body: str) -> str:
    raw = body.strip()
    if not raw:
        return "invalid:empty-body"
    try:
        obj = json.loads(raw)
    except ValueError:
        return "invalid:parse-error"
    return "json=" + hashlib.sha256(json.dumps(_canon(_strip(obj)), sort_keys=True).encode()).hexdigest()


EXTRACTORS = {"scalar": extract_scalar, "groupby": extract_groupby, "values": extract_values, "scan": extract_scan,
              "spans": extract_spans, "json": extract_json}
# run.sh qkind -> our result kind, for the differential test
RUNSH_KIND = {"count_total": "scalar", "fulltext": "scalar", "level_filter": "scalar", "multi_filter": "scalar", "negation": "scalar",
              "service_filter": "scalar", "slow_spans": "scalar", "span_name": "scalar", "count_by_service": "groupby",
              "high_card": "groupby", "filtered_groupby": "groupby", "fv_level": "values", "fv_service": "values",
              "streams_list": "values", "fv_name": "values", "scan": "scan", "trace_by_id": "spans", "trace_lookup": "spans"}


def extract_result(kind: str, body: str) -> str:
    return EXTRACTORS[kind](body)


def result_is_empty(result: str) -> bool:
    """run.sh result_is_empty."""
    return result in ("0", "rows=0", "spans=0") or result.startswith("rows=0;hash=")


def validate(kind: str, status: int, body: str, first_result: str | None = None, miss: bool = False,
             window_rows: int | None = None, window_keys: set | None = None) -> tuple[bool, str, str]:
    """(valid, reason, result) of one timed answer, the rules of run.sh `_validate_iter`.

    `first_result` is the cell's first valid result: a later iteration that differs is flapping, not a latency.
    A scan with more matching rows than SCAN_LIMIT is truncated at an arbitrary subset, so identity cannot hold;
    it is valid when it returns exactly SCAN_LIMIT rows that are all members of the reference window.
    """
    if not 200 <= status < 300:
        return False, f"http {status}", ""
    if kind == "scan" and window_rows is not None and window_rows > SCAN_LIMIT:
        got = scan_keys(body)
        if got is None:
            return False, "parse-error", ""
        n, keys = got
        res = f"rows={n};hash={sha(keys)}"
        if n != SCAN_LIMIT:
            return False, f"truncated scan returned {n} rows, expected {SCAN_LIMIT}", res
        if window_keys is not None and any(k not in window_keys for k in keys):
            return False, "row not in reference window (membership check failed)", res
        return True, "", res
    res = extract_result(kind, body)
    if res.startswith("invalid:"):
        return False, res[len("invalid:"):], ""
    if not miss and result_is_empty(res):
        return False, "empty result", res
    if first_result and res != first_result:
        return False, "flapping (differs from cell's first valid result)", res
    return True, "", res
