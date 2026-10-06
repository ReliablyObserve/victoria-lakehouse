"""Runners of the reader matrix: one function per engine.

Each runner receives the runnable doc snippet of an engine plus the cells to run
(tenant, signal, layer), executes the snippet text from the documentation (only the
example values replaced, see lib.materialise) and returns the normalised answers.
"""
import ast
import json
import os
import urllib.parse
import urllib.request

import lib


class Cell:
    def __init__(self, tenant, signal, layer):
        self.tenant, self.signal, self.layer = tenant, signal, layer
        self.t = lib.TENANTS[tenant]
        self.bucket = lib.BUCKET[layer]

    def __repr__(self):
        return "%s/%s/%s" % (self.tenant, self.signal, self.layer)


class QueryError(str):
    """The error text of a query that failed (kept per query so one broken statement
    does not hide the result of the others)."""


def sql_answers(statements, execute):
    """Run (name, stmt) pairs through execute(stmt) -> rows|None; collect the named ones.

    A failing named statement is recorded as a QueryError and the run continues; a failing
    setup statement (no name) aborts the cell."""
    out = {}
    for name, stmt in statements:
        try:
            rows = execute(stmt)
            if name:
                out[name] = lib.normalise(lib.base_query(name), rows)
        except Exception as e:
            if not name:
                raise
            out[name] = QueryError("%s: %s" % (type(e).__name__, str(e)[:300]))
    return out


def run_duckdb(snippet, cell, params):
    import duckdb
    con = duckdb.connect()
    body = lib.materialise(snippet["body"], params, lib.S3_HOST, cell.bucket, cell.t["prefix"], cell.t["account"])

    def ex(stmt):
        cur = con.execute(stmt)
        return cur.fetchall() if cur.description else None

    return sql_answers(lib.split_sql(body), ex)


RUNNERS = {"duckdb": run_duckdb}


def to_rows(v):
    """Turn whatever an engine's Python API returned into a list of rows."""
    if hasattr(v, "to_pylist") and hasattr(v, "num_columns"):  # pyarrow.Table
        cols = [c.to_pylist() for c in v.columns]
        return [list(r) for r in zip(*cols)]
    if hasattr(v, "to_arrow_table"):  # datafusion DataFrame
        return to_rows(v.to_arrow_table())
    if hasattr(v, "iter_rows") and hasattr(v, "columns"):  # polars DataFrame
        return [list(r) for r in v.iter_rows()]
    if hasattr(v, "itertuples") and hasattr(v, "columns"):  # pandas DataFrame
        return [list(r) for r in v.itertuples(index=False, name=None)]
    if hasattr(v, "collect") and hasattr(v, "columns") and not hasattr(v, "iter_rows"):  # spark DataFrame
        return [list(r) for r in v.collect()]
    if hasattr(v, "as_py"):  # pyarrow scalar
        return to_rows(v.as_py())
    if isinstance(v, dict):
        if set(v) == {"min", "max"}:
            return [[v["min"], v["max"]]]
        return [[k, x] for k, x in v.items()]
    if isinstance(v, (list, tuple)):
        if v and isinstance(v[0], (list, tuple)):
            return [list(r) for r in v]
        return [list(v)]
    return [[v]]


def by_service_fix(rows):
    """Group-by results come back as (key, count) or (count, key): order by type."""
    out = []
    for r in rows:
        k = [x for x in r if isinstance(x, str)][0]
        c = [x for x in r if not isinstance(x, str)][0]
        out.append([k, c])
    return out


def run_python(snippet, cell, params, extra_ns=None, endpoint=None):
    body = lib.materialise(snippet["body"], params, endpoint or lib.S3_HOST, cell.bucket, cell.t["prefix"], cell.t["account"])
    return exec_body(body, snippet["engine"], extra_ns)


def exec_body(body, engine, extra_ns=None):
    ns = dict(extra_ns or {})
    out = {}
    setup_error = None
    # Execute the snippet statement by statement: a failing `q_<name> = ...` statement is that
    # query's error; a failing setup statement aborts the cell.
    for node in ast.parse(body).body:
        mod = ast.Module(body=[node], type_ignores=[])
        target = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) \
                and node.targets[0].id.startswith("q_") and node.targets[0].id[2:] in lib.QUERIES:
            target = node.targets[0].id[2:]
        elif isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) \
                and lib.base_query(node.targets[0].id[2:]) and node.targets[0].id.startswith("q_"):
            target = node.targets[0].id[2:]
        try:
            exec(compile(mod, "<doc:%s>" % engine, "exec"), ns)  # noqa: S102 - the snippet is the test subject
            if target:
                v = ns["q_" + target]
                rows = to_rows(v)
                if lib.base_query(target) == "by_service":
                    rows = by_service_fix(rows)
                out[target] = lib.normalise(lib.base_query(target), rows)
        except BaseException as e:  # noqa: BLE001 - engines panic (pyo3.PanicException) outside Exception
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            if not target:
                # A helper statement failed (for example an unfiltered read that hits a bad
                # object). Keep going: the queries that need it fail with their own error and
                # the ones that do not need it still run.
                setup_error = setup_error or QueryError("%s: %s" % (type(e).__name__, str(e)[:300]))
                continue
            out[target] = QueryError("%s: %s%s" % (type(e).__name__, str(e)[:300],
                                                 (" [after setup error: %s]" % setup_error) if setup_error else ""))
    if not out and setup_error:
        raise RuntimeError(setup_error)
    return out


for _e in ("pyarrow", "pandas", "polars", "datafusion"):
    RUNNERS[_e] = run_python


CH_URL = os.environ.get("CH_URL", "http://127.0.0.1:39410")


def run_clickhouse(snippet, cell, params):
    body = lib.materialise(snippet["body"], params, lib.S3_NET, cell.bucket, cell.t["prefix"], cell.t["account"])

    def ex(stmt):
        is_select = stmt.lstrip().upper().startswith("SELECT")
        q = stmt
        req = urllib.request.Request(CH_URL + "/?default_format=JSONCompact", data=q.encode(),
                                     headers={"Authorization": "Basic bGg6bGg="})  # lh:lh
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                data = r.read().decode()
        except urllib.error.HTTPError as e:
            raise RuntimeError(e.read().decode()[:400])
        return json.loads(data)["data"] if is_select else None

    return sql_answers(lib.split_sql(body), ex)


RUNNERS["clickhouse"] = run_clickhouse


def run_trino(snippet, cell, params):
    import trino
    body = lib.materialise(snippet["body"], params, lib.S3_NET, cell.bucket, cell.t["prefix"], cell.t["account"])
    conn = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "127.0.0.1"), port=int(os.environ.get("TRINO_PORT", "39420")),
                               user="lh", catalog="hive", schema="default")

    def ex(stmt):
        cur = conn.cursor()
        cur.execute(stmt)
        rows = cur.fetchall()
        return rows if cur.description else None

    return sql_answers(lib.split_sql(body), ex)


RUNNERS["trino"] = run_trino


SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "apache/spark:4.0.0-scala2.13-java17-python3-ubuntu@sha256:a89782d90529a623fc4471cdddb0f9c32d6eed10a81b256a80207b23c3b1df00")


def batch_spark(items):
    """items: [(cell, params, snippet)] -> {repr(cell): answers}. One container, one JVM."""
    import subprocess
    import tempfile
    job = {"cells": [{"cell": repr(c), "body": lib.materialise(s["body"], p, lib.S3_NET, c.bucket, c.t["prefix"], c.t["account"])}
                     for c, p, s in items]}
    with tempfile.TemporaryDirectory() as d:
        lib.write_json(os.path.join(d, "job.json"), job)
        ivy = os.environ.get("IVY_CACHE", os.path.expanduser("~/.cache/lhreaders-ivy"))
        os.makedirs(ivy, exist_ok=True)
        # Root inside the container: the image's own user has uid 185, and Hadoop's login module
        # fails for any uid without a passwd entry. The ivy cache files stay world-readable, so the
        # CI cache step (which runs as the runner user) can still save them.
        cmd = ["docker", "run", "--rm", "--user", "root",
               "--network", os.environ.get("READERS_NET", "lhreaders_net"),
               "-v", lib.HERE + ":/work:ro", "-v", d + ":/job:ro", "-v", ivy + ":/tmp/.ivy2",
               "-e", "PYTHONPATH=/work", "-e", "PYTHONDONTWRITEBYTECODE=1", "-e", "HOME=/tmp", SPARK_IMAGE,
               "/opt/spark/bin/spark-submit", "--packages", "org.apache.hadoop:hadoop-aws:3.4.1", "--conf", "spark.jars.ivy=/tmp/.ivy2",
               "--conf", "spark.ui.enabled=false", "/work/spark_job.py", "/job/job.json"]
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    marker = "@@RESULT@@"
    line = [l for l in p.stdout.splitlines() if l.startswith(marker)]
    if not line:
        raise RuntimeError("spark produced no result (rc=%d): %s" % (p.returncode, (p.stderr or p.stdout)[-1500:]))
    raw = json.loads(line[0][len(marker):])
    res = {}
    for cell, answers in raw.items():
        if "__error__" in answers:
            res[cell] = RuntimeError(answers["__error__"])
            continue
        res[cell] = {k: (QueryError(v["error"]) if "error" in v else v["value"]) for k, v in answers.items()}
    return res


BATCH = {"spark": batch_spark}


def versions():
    """The engine versions that produced a result (recorded in the result file and the report)."""
    import importlib.metadata as md
    v = {}
    for name, dist in (("duckdb", "duckdb"), ("pyarrow", "pyarrow"), ("pandas", "pandas"), ("polars", "polars"),
                       ("datafusion", "datafusion"), ("parquet-tools", "parquet-tools")):
        try:
            v[name] = md.version(dist)
        except md.PackageNotFoundError:
            pass
    try:
        req = urllib.request.Request(CH_URL + "/", data=b"SELECT version()", headers={"Authorization": "Basic bGg6bGg="})
        with urllib.request.urlopen(req, timeout=10) as r:
            v["clickhouse"] = r.read().decode().strip()
    except Exception:
        pass
    try:
        url = "http://%s:%s/v1/info" % (os.environ.get("TRINO_HOST", "127.0.0.1"), os.environ.get("TRINO_PORT", "39420"))
        with urllib.request.urlopen(url, timeout=10) as r:
            v["trino"] = str(json.load(r)["nodeVersion"]["version"])
    except Exception:
        pass
    v["spark"] = SPARK_IMAGE.split(":", 1)[1].split("-")[0]
    return v
