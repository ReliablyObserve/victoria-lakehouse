#!/usr/bin/env python3
"""The defects mutation-proof.sh injects into an isolated copy of the repository.

  mutants.py <id> <copy-root>

Each mutation is a text patch that fails loudly when its anchor is not found, so a refactor of the
code under test cannot silently turn a mutant into a no-op.
"""
import pathlib
import sys


def read(path):
    return pathlib.Path(path).read_text()


def write(path, text):
    pathlib.Path(path).write_text(text)


def patch(path, old, new, count=1):
    s = read(path)
    assert old in s, "mutation anchor not found in %s: %r" % (path, old[:80])
    s = s.replace(old, new, count)
    write(path, s)


def after_signature(path, signature, body):
    s = read(path)
    i = s.index(signature)
    j = s.index("{\n", i) + 2
    write(path, s[:j] + body + s[j:])


LOGS_WRITER = "internal/storage/parquets3/writer.go"
TRACES_WRITERS = ("internal/storage/parquets3/writer.go", "lakehouse-traces/internal/storage/parquets3/writer.go")
LOGS_SIG = "func writeLogsParquet(rows []schema.LogRow, rowGroupSize int, compressionLevel int) (*flushResult, error)"
TRACES_SIG = "func writeTracesParquet(rows []schema.TraceRow, rowGroupSize int, compressionLevel int) (*flushResult, error)"


def m1(root):
    p = root + "/internal/schema/row.go"
    s = read(p)
    i = s.index("type TraceRow struct")
    # status.code is a nullable column (an absent code is a null, #274): the tag carries ",optional".
    old = 'parquet:"status.code,optional"'
    j = s.index(old, i)
    write(p, s[:j] + 'parquet:"status_code,optional"' + s[j + len(old):])


def m2(root):
    after_signature(root + "/" + LOGS_WRITER, LOGS_SIG, "\tfor i := range rows {\n\t\trows[i].TimestampUnixNano += 3600 * 1000000000\n\t}\n")


def ma(root):
    for w in TRACES_WRITERS:
        after_signature(root + "/" + w, TRACES_SIG, '\tfor i := range rows {\n\t\tdelete(rows[i].SpanAttributes, "rpc.system")\n\t}\n')


def mb(root):
    after_signature(root + "/" + LOGS_WRITER, LOGS_SIG, "\tfor i := range rows {\n\t\trows[i].TimestampUnixNano = rows[i].TimestampUnixNano / 1000 * 1000\n\t}\n")


def mc(root):
    after_signature(root + "/" + LOGS_WRITER, LOGS_SIG, '\tfor i := range rows {\n\t\tdelete(rows[i].LogAttributes, "format")\n\t}\n')


def md2(root):
    """Every 10th row of tenant 1001:0 (the acme-corp alias) is written with tenant columns 4401:1. The insert buffer's
    flusher is the only producer of logs Parquet (the staging engine is gone since the durable-by-default buffer), so
    the rows are rewritten where the flusher reads them back out of the segment."""
    p = root + "/internal/storage/parquets3/buffer_flusher.go"
    patch(p, "\t\tfor _, r := range vlstorage.DataBlockToLogRows(db, tenant) {\n",
          "\t\tfor _, r := range vlstorage.DataBlockToLogRows(db, tenant) {\n"
          "\t\t\tif tenant.AccountID == 1001 && mutantRows.Add(1)%10 == 0 {\n"
          "\t\t\t\tr.AccountID, r.ProjectID = 4401, 1\n"
          "\t\t\t}\n")
    s = read(p)
    s += "\nvar mutantRows atomic.Int64\n"
    if '"sync/atomic"' not in s:
        s = s.replace('import (\n', 'import (\n\t"sync/atomic"\n', 1)
    write(p, s)


def h1a(root):
    p = root + "/tests/readers/run.sh"
    patch(p, '  "$PY" fixture.py snapshot-raw\n', '')
    patch(p, '  "$PY" fixture.py verify-compacted\n', '  "$PY" fixture.py verify-compacted\n  "$PY" fixture.py snapshot-raw   # MUTANT: the raw layer is taken after compaction\n')


def h1b(root):
    patch(root + "/tests/readers/lib.py", 'BUCKET = {"compacted": "obs-archive", "raw": "obs-raw"}', 'BUCKET = {"compacted": "obs-archive", "raw": "obs-archive"}')


def mf(root):
    """Both traces writers store the `_trace_idx` footer value as raw bytes again (#340): Polars and DataFusion refuse
    every traces object, and the fixture's own footer check (truth.py assert-footer) must catch it first."""
    for w in TRACES_WRITERS:
        patch(root + "/" + w, "parquet.KeyValueMetadata(traceIndexMetadataKey, footerkv.Encode(idxData))",
              "parquet.KeyValueMetadata(traceIndexMetadataKey, string(idxData))")
        patch(root + "/" + w, '\t"github.com/ReliablyObserve/victoria-lakehouse/internal/footerkv"\n', '')


def mg(root):
    """Both token_bloom.go store the `_bloom_body_rg_N` footer value as raw bytes again (#340)."""
    for p in ("internal/storage/parquets3/token_bloom.go", "lakehouse-traces/internal/storage/parquets3/token_bloom.go"):
        patch(root + "/" + p, "return key, footerkv.Encode(data)", "return key, string(data)")


MUTANTS = {"M1": m1, "M2": m2, "MA": ma, "MB": mb, "MC": mc, "MD2": md2, "MF": mf, "MG": mg, "H1a": h1a, "H1b": h1b}

if __name__ == "__main__":
    MUTANTS[sys.argv[1]](sys.argv[2])
    print("mutated (%s) in %s" % (sys.argv[1], sys.argv[2]))
