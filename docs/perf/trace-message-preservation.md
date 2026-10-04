# Trace message persistence proof

Baseline: `f714f5f841da544b992eb8118e15c640ee47772c`.

Trace ingestion canonicalizes the native `_msg` field to an empty internal field
name. The old conversion discarded it, while a customer `span_attr:_msg` could
later appear as the native message. New writes store the actual message in an
optional standard Parquet `body` column. Reads, peer bridges, compaction, token
blooms and physical deletion keep native and customer values separate.

## Same-input API comparison

An isolated recorder sent identical request bytes and tenant headers to main,
the corrected binaries and hot VL/VT. Five logs and five OTLP spans cover 10m,
1h, 6h, 24h and 7d query windows. Their positive counts are 1, 2, 3, 5 and 5.
Both references retain 24 hours, so the oldest input is 20 hours old; the 7d
window covers that retained dataset. An earlier fixture outside reference
retention was rejected for this comparison.

After flushing, both Lakehouse builds were restarted on their existing volumes
with `insert.buffer_engine=buffer`. This removes restored native-buffer rows
from the read path and proves the persisted Parquet answer. Six interleaved
repetitions per endpoint/window compare status and unordered complete answers.

| Persisted trace field | Main | Corrected | Hot VT |
|---|---|---|---|
| `_msg` | `customer-1` | `-` | `-` |
| `span_attr:_msg` | absent | `customer-1` | `customer-1` |
| `span_attr:span_attr:_msg` | `literal-1` | `literal-1` | `literal-1` |
| `span_attr:body` | `customer-body-1` | `customer-body-1` | `customer-body-1` |

Across all five windows, corrected trace rows, native-message projections and
`_msg:="-"` filters match hot VT. Count, grouped count, field values, streams
and hits also match. Forty of 45 trace comparisons are exact; the five existing
`field_names` gaps remain. Logs main and corrected answers are identical in all
45 comparisons: 35 match hot VL, while raw rows retain the existing extra
`severity_number:"0"` and `field_names` remains different. These gaps are not
hidden or timed as successful answers.

[Response comparisons](trace-message-response-comparison.json) record statuses,
answer hashes, validity, first-request and warm p50/p95 timings, observed S3 GET
and byte deltas, CPU deltas and RSS. First request means after restart, rather
than guaranteed uncached S3: startup warmup can populate caches. Resource
deltas include background work. This local proof uses the stack's normal S3
latency and makes no latency-injection or throughput improvement claim.

## Deterministic storage cost

The exact field-metadata CI matrix ran twice under both dependency pins: 768
records covering 384 cells. Counters and answer outcomes are stable across
repetitions. Adding the optional trace column changes only read-byte counters
in 348 existing registry rows: +133 to +3204 bytes, at most 0.143%. GET counts,
pages, row groups, paths, answer expectations, comparators and existing latency
budgets are unchanged. The matrix gate passes all 384 cells against those
measured counters. This means existing expected differences remain explicit;
it does not mean every cell has hot/cold answer parity.

Reproduce with the commands used by `field-metadata-perf` in CI:

```sh
GOWORK=off FM_MATRIX_OUT=/tmp/logs.jsonl FM_BUILD=after FM_ITERS=2 FM_WARMUP=0 FM_LATENCIES_MS=0 go test ./internal/storage/parquets3 -run '^TestFieldMetadataMatrix$' -count=1
cd lakehouse-traces
GOWORK=off FM_MATRIX_OUT=/tmp/traces.jsonl FM_BUILD=after FM_ITERS=2 FM_WARMUP=0 FM_LATENCIES_MS=0 go test ./internal/storage/parquets3 -run '^TestFieldMetadataMatrixTraces$' -count=1
```

The validated legacy-row conversion control alternates main and corrected runs,
ten samples per build under each dependency graph. Each iteration checks unique
trace/span IDs, the operation name and absence of a fabricated native message.

| Module | Main ns/op | Corrected ns/op | Bytes/op | Allocs/op |
|---|---:|---:|---:|---:|
| Logs pin, trace-row conversion | 316.3 | 318.8 | 816 → 816 | 4 → 4 |
| Traces pin, trace-row conversion | 331.3 | 299.4 | 1072 → 1072 | 4 → 4 |

Benchstat finds no significant time change (p=0.631 and p=0.052). The small
conversion benchmark has substantial system noise and makes no throughput
claim. An earlier sequential measurement was noisy; interleaving resolves that
comparison without changing the oracle. [Raw samples and benchstat](trace-message-bench.txt).
Run `GOWORK=off go test ./internal/storage/parquets3 -run '^$' -bench
'^BenchmarkTraceMessageLegacyControl$' -benchmem -count=10` in each module.

## Regression coverage

Both modules exercise native/customer message filters after manifest reload,
typed and peer bridges, recursively prefixed customer keys, and a nonconstant
two-row columnar projection. Shared tests cover missing legacy columns, raw-byte
accounting, repeated mixed-file compaction and reopening, and deletion message
provenance. The actual `LogRows.MustAdd` regression proves upstream empty-name
canonicalization and ownership when the pooled arena is overwritten.

| Touched package | Main coverage | Corrected coverage |
|---|---:|---:|
| Shared schema | 96.6% | 96.6% |
| Shared deletion | 96.1% | 96.2% |
| Shared compaction | 95.7% | 95.7% |
| Logs storage | 90.7% | 90.8% |
| Traces storage | 92.1% | 92.2% |
| Traces ingestion | 92.3% | 92.3% |

Full suites, lint, conformance and repeated race runs pass. The concurrent
deletion/compaction fixture now reports a missing object as an error, matching
S3, rather than returning an empty successful object. Its randomized race runs
cover 400 iterations. Twenty-four semantic mutations were caught. Historical
native messages absent from legacy objects cannot be reconstructed.

Existing ingestion conversion and trace-map fuzz targets pass 20 seconds each:
233,063 ingestion executions, 86,151 map executions under the logs pin and
86,237 under the traces pin.

## Safe message bloom pruning

Indexing the preserved native message exposed an unsafe query-text extractor:
`name:="HTTP GET /api/v1/users"` incorrectly required the message token `GET`.
The trace parity job returned zero rows where hot VT returned 467. Both pins
now reuse upstream's parsed filter guarantees. Customer fields contribute no
message tokens, OR branches contribute only common required tokens, negation
contributes none, and prefixes never require an incomplete final word.

The parsed query computes these tokens once before file workers start. Empty
token sets are cached too. Per-file lookups allocate nothing; direct per-file
tooling falls back to native parsing, and malformed queries disable pruning.

A second identical-input fixture uses five quoted operation names and customer
body attributes. Both Lakehouse builds were restarted with native buffers
disabled. [Complete count responses](trace-message-bloom-comparison.json) cover
60 endpoint/window cells and 1,080 responses, with six interleaved repetitions.
Across 10m, 1h, 6h, 24h and 7d, the corrected span-name query returns 1, 2, 3,
5 and 5, matching hot VT; the previous PR build returned zero at every range.
Logs operation-name filtering also now matches hot VL. Trace customer-body
filtering matches hot VT. Unfiltered, OR, NOT and prefix controls match their
references. The existing logs literal `body` schema alias still loses that
customer field in both main and the corrected build; these five cells are
reported as different and receive no valid timings.

[Ten-sample extraction benchmarks](trace-message-bloom-bench.txt) include the
native parser fallback and the cached runtime lookup. The native parser is
slower than the old unsafe text scanner: the logs module takes 0.293–4.390 µs
across the measured fallback shapes versus 0.108–1.252 µs before. Runtime scans
reuse the already parsed query and measure 4.176 ns/file under the logs pin and
3.621 ns/file under the traces pin, with zero bytes and zero allocations. These
are local microbenchmarks, not an end-to-end throughput improvement claim.

Persisted and native row-match regressions cover customer names, message-field
spoofing, prefixes, AND/OR/NOT and a pipe that changes the message before
filtering. Additional property fuzz runs exercise 19,187 cases under the logs
pin and 91,238 under the traces pin; fourteen semantic mutations were caught
across the two pins.
