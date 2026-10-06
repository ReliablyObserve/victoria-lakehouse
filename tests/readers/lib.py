"""Shared helpers of the external Parquet reader matrix.

Everything here is plumbing: the S3 client, the Lakehouse API client that produces
the oracle, and the extraction of runnable snippets from docs/open-parquet-format.md.
"""

import json
import os
import pathlib
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
DOC = os.environ.get("READERS_DOC") or os.path.join(REPO, "docs", "open-parquet-format.md")

# Host-side addresses of the lhreaders stack (ports 39400-39499).
S3_HOST = os.environ.get("S3_HOST", "127.0.0.1:39400")
LOGS_URL = os.environ.get("LH_LOGS_URL", "http://127.0.0.1:39401")
TRACES_URL = os.environ.get("LH_TRACES_URL", "http://127.0.0.1:39402")
S3_KEY = os.environ.get("S3_KEY", "minioadmin")
S3_SECRET = os.environ.get("S3_SECRET", "minioadmin")
# The address of the S3 service as seen from the engine containers on the compose network.
S3_NET = os.environ.get("S3_NET", "s3:9000")

BUCKET = {"compacted": "obs-archive", "raw": "obs-raw"}

# The tenants of the fixture. `prefix` is the S3 prefix the layout puts the tenant under.
TENANTS = {
    "numeric": {"account": 4401, "project": 1, "prefix": "4401/1", "headers": {"AccountID": "4401", "ProjectID": "1"}},
    "alias": {"account": 1001, "project": 0, "prefix": "1001/0", "headers": {"X-Scope-OrgID": "acme-corp"}},
    "big": {"account": 3000000000, "project": 0, "prefix": "3000000000/0", "headers": {"AccountID": "3000000000", "ProjectID": "0"}},
    # Hand-written batches (golden.py): `golden` is the edge-case batch (every nanosecond digit, a day
    # boundary, odd map keys, the largest writable AccountID); `bloom` is 60 rows in one hour, the size that gives
    # 96-byte split-block bloom filters (a size ClickHouse cannot read, #341).
    # 4294967295 itself is reserved: Lakehouse rejects writes to it (golden.py proves the rejection), so the largest tenant is 4294967294.
    "golden": {"account": 4294967294, "project": 0, "prefix": "4294967294/0", "headers": {"AccountID": "4294967294", "ProjectID": "0"}},
    "bloom": {"account": 4402, "project": 3, "prefix": "4402/3", "headers": {"AccountID": "4402", "ProjectID": "3"}},
}
SIGNALS = ("logs", "traces")
QUERIES = ("count", "by_service", "field_filter", "time_range", "map_filter",
           "trace_by_id", "dt_filter", "ts_bounds", "utc_check", "tenant")
# `files`: the objects the engine read for the unfiltered scan, for the engines that can say (compared with the inventory).
FILES_QUERY = "files"

# The literals the documentation examples use. CI replaces each one, as plain text, with the
# fixture value before running the snippet; the snippet text is otherwise executed unchanged.
DOC_DEFAULTS = {
    "endpoint": "localhost:9000",
    "bucket": "obs-archive",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "prefix": "4401/1",
    "account": "4401",
    "from_ns": "1767225600000000000",
    "to_ns": "1767229200000000000",
    "trace_id": "0af7651916cd43dd8448eb211c80319c",
    "dt": "2026-01-01",
}


def read_text(path) -> str:
    return pathlib.Path(path).read_text(encoding="utf-8")


def write_text(path, text: str) -> None:
    pathlib.Path(path).write_text(text, encoding="utf-8")


def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def write_json(path, obj, **kwargs) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, **kwargs)


def base_query(name: str):
    """The fixed query a (possibly suffixed) query name is checked against: `tenant_masked`
    is compared with the oracle of `tenant`. None when the name is unknown."""
    if name in QUERIES or name == FILES_QUERY:
        return name
    for q in sorted(QUERIES, key=len, reverse=True):
        if name.startswith(q + "_"):
            return q
    return None


def rfc3339_ns(ns: int) -> str:
    t = datetime.fromtimestamp(ns // 10**9, tz=timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%S") + ".%09dZ" % (ns % 10**9)


def parse_rfc3339_ns(s: str) -> int:
    m = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?Z", s)
    if not m:
        raise ValueError("bad timestamp %r" % s)
    base = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    frac = (m.group(2) or "").ljust(9, "0")[:9]
    return int(base.timestamp()) * 10**9 + int(frac)


def s3fs_client():
    import s3fs
    return s3fs.S3FileSystem(key=S3_KEY, secret=S3_SECRET,
                             client_kwargs={"endpoint_url": "http://" + S3_HOST})


def lh_query(signal: str, tenant: dict, query: str) -> list:
    base = LOGS_URL if signal == "logs" else TRACES_URL
    req = urllib.request.Request(base + "/select/logsql/query",
                                 data=urllib.parse.urlencode({"query": query}).encode(),
                                 headers=tenant["headers"])
    with urllib.request.urlopen(req, timeout=120) as r:
        body = r.read().decode()
    return [json.loads(l) for l in body.splitlines() if l.strip()]


def lh_scalar(signal, tenant, query, field="c") -> int:
    rows = lh_query(signal, tenant, query)
    return int(rows[0][field]) if rows else 0


def jaeger_span_count(tenant: dict, trace_id: str) -> int:
    req = urllib.request.Request(TRACES_URL + "/select/jaeger/api/traces/" + trace_id, headers=tenant["headers"])
    with urllib.request.urlopen(req, timeout=60) as r:
        d = json.load(r)
    return len(d["data"][0]["spans"]) if d.get("data") else 0


# ---------------------------------------------------------------------------------------------
# snippets in the documentation
# ---------------------------------------------------------------------------------------------

MARK = re.compile(r"<!--\s*ci:(?P<attrs>[^>]*?)\s*-->\s*\n```(?P<lang>\w*)\n(?P<body>.*?)\n```", re.S)


def doc_snippets(path: str = DOC) -> list:
    """Every runnable example of the doc: {engine, signal, lang, body, attrs}."""
    text = read_text(path)
    out = []
    for m in MARK.finditer(text):
        attrs = dict(kv.split("=", 1) for kv in m.group("attrs").split())
        out.append({"attrs": attrs, "engine": attrs.get("engine"), "signal": attrs.get("signal"),
                    "lang": m.group("lang"), "body": m.group("body")})
    return out


def find_snippet(snips, engine, signal):
    for s in snips:
        if s["engine"] == engine and s["signal"] == signal and s["attrs"].get("ci", "run") == "run":
            return s
    raise KeyError("no runnable doc snippet for engine=%s signal=%s" % (engine, signal))


def materialise(body: str, params: dict, endpoint: str, bucket: str, prefix: str, account: int) -> str:
    """Replace the doc's literal example values with the fixture's."""
    d = DOC_DEFAULTS
    # Order matters: longer / more specific literals first.
    pairs = [
        (d["endpoint"], endpoint),
        (d["trace_id"], params["trace_id"]),
        (d["from_ns"], str(params["from_ns"])),
        (d["to_ns"], str(params["to_ns"])),
        (d["dt"], params["dt"]),
        ("obs-archive/" + d["prefix"], bucket + "/" + prefix),
        ("'" + d["bucket"] + "'", "'" + bucket + "'"),
        ("s3://" + d["bucket"], "s3://" + bucket),
        ("s3a://" + d["bucket"], "s3a://" + bucket),
        ("s3://obs-archive", "s3://" + bucket),
        ("/" + d["bucket"] + "/", "/" + bucket + "/"),
    ]
    for a, b in pairs:
        body = body.replace(a, b)
    body = body.replace(d["bucket"], bucket)
    body = body.replace(d["prefix"], prefix)
    body = re.sub(r"\b%s\b" % d["account"], str(account), body)
    body = body.replace(d["access_key"], S3_KEY).replace(d["secret_key"], S3_SECRET)
    return body


def split_sql(body: str):
    """Split a SQL snippet into (query_name_or_None, statement) pairs."""
    out, name, cur = [], None, []
    for line in body.splitlines():
        m = re.match(r"\s*--\s*q:\s*(\w+)\s*$", line)
        if m:
            name = m.group(1)
            continue
        if line.strip().startswith("--") or not line.strip():
            continue
        cur.append(line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(cur).rstrip().rstrip(";")
            out.append((name, stmt))
            name, cur = None, []
    return out


def norm_scalar(rows):
    v = rows[0][0]
    return int(v)


def normalise(query: str, rows):
    """Reduce engine rows to the oracle's shape for `query`."""
    rows = [list(r) for r in rows]
    query = base_query(query) or query
    if query in ("count", "time_range", "map_filter", "field_filter", "trace_by_id", "dt_filter", "utc_check"):
        return int(rows[0][0])
    if query == "by_service":
        return {str(r[0]): int(r[1]) for r in rows}
    if query == "ts_bounds":
        return [int(rows[0][0]), int(rows[0][1])]
    if query == "tenant":
        return sorted([[int(r[0]), int(r[1])] for r in rows])
    if query == FILES_QUERY:
        # Whatever the engine prints (s3://bucket/key, bucket/key, a full URL): the object name below <signal>/.
        names = set()
        for r in rows:
            for c in r:
                m = re.search(r"(dt=[^/]+/hour=[^/]+/[^/]+\.parquet)$", str(c))
                if m:
                    names.add(m.group(1))
        return sorted(names)
    raise KeyError(query)
