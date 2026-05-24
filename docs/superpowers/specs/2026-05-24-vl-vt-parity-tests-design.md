# VL/VT Parity Test Suite Design

## Goal

Comprehensive LogsQL API parity testing between Victoria Lakehouse (S3 cold storage) and VictoriaLogs/VictoriaTraces (disk hot tier) as reference implementations. Validates that LH returns identical results to VL/VT for all supported endpoints, query types, and edge cases.

## Architecture

**Dual-write parity testing**: A dedicated lightweight Docker Compose stack dual-writes identical data to both LH and VL (and LH-traces and VT). A Go test binary sends identical LogsQL queries to both systems, parses responses, and diffs results using configurable comparison modes.

### Compose Stack (6 services, ~15s startup)

| Service | Image | Port | Role |
|---------|-------|------|------|
| minio + minio-init | minio/minio | 19000 | S3 backend for LH |
| victorialogs | victoriametrics/victoria-logs:v1.50.0 | 19428 | Reference (logs) |
| lakehouse-logs | local build (Dockerfile.logs) | 29428 | System under test (logs) |
| victoriatraces | victoriametrics/victoria-traces:v0.9.0 | 10428 | Reference (traces) |
| lakehouse-traces | local build (Dockerfile.traces) | 20428 | System under test (traces) |
| datagen-seed | local build (Dockerfile.datagen) | — | 10K logs + 2K traces over 24h |

LH configuration tuned for fast test turnaround:
- `flush-interval=5s` (fast flush so data reaches S3 quickly)
- `manifest.refresh-interval=5s`
- `compaction.interval=15s`
- `cache.memory-mb=256`

### Test Execution

```bash
cd tests/parity
docker compose up -d --build --wait
GOWORK=off go test -tags=parity -v -timeout=300s .
docker compose down -v
```

Build tag: `//go:build parity` — does not run in normal `go test ./...`.

## File Layout

```
tests/parity/
├── docker-compose.yml          # Lightweight 6-service stack
├── parity_test.go              # Test runner + comparison engine + ParityCase type
├── logs_endpoints_test.go      # Family 1: all 14 endpoint smoke tests
├── logs_filters_test.go        # Family 2: filter types (~25 cases)
├── logs_pipes_test.go          # Family 3: pipe operations (~20 cases)
├── logs_stats_test.go          # Family 4: stats_query & stats_query_range (~15 cases)
├── logs_timerange_test.go      # Family 5: time range handling (~10 cases)
├── logs_fields_test.go         # Family 6: field metadata (~10 cases)
├── logs_response_test.go       # Family 7: response format & pagination (~10 cases)
├── logs_edge_test.go           # Family 8: edge cases & errors (~10 cases)
├── traces_parity_test.go       # Traces: Jaeger API + LogsQL on trace data (~15 cases)
└── helpers.go                  # HTTP client, response parsers, diff logic
```

## Core Types

```go
type CompareMode string

const (
    CountEqual     CompareMode = "count_equal"      // Parse count, assert equal
    CountTolerance CompareMode = "count_tolerance"   // Allow ±tolerance difference
    SetEqual       CompareMode = "set_equal"         // Unordered set equality
    SetSuperset    CompareMode = "set_superset"      // LH ⊇ VL expected values
    RowsMatch      CompareMode = "rows_match"        // Same row count + key field values
    StatusEqual    CompareMode = "status_equal"       // HTTP status codes match
    StructureMatch CompareMode = "structure_match"    // JSON structure + numeric equality
    BucketMatch    CompareMode = "bucket_match"       // Histogram buckets within ±1
    NonEmpty       CompareMode = "non_empty"          // Both return >0 results
)

type ParityCase struct {
    Name       string
    Endpoint   string            // "/select/logsql/query"
    Params     map[string]string // query, start, end, limit, field, step, etc.
    Compare    CompareMode
    SkipFields []string          // fields to exclude from row comparison
    Tolerance  float64           // for CountTolerance mode (0.01 = 1%)
}
```

### Comparison Rules

| Mode | When | Logic |
|------|------|-------|
| `count_equal` | Stats count queries | Parse count from Prometheus vector, assert equal |
| `count_tolerance` | Wide-range aggregations | Allow ±tolerance% difference (timing edge cases) |
| `set_equal` | field_names, field_values, services | Parse values, compare as unordered sets |
| `set_superset` | field_names (LH may have extra parquet columns) | LH result ⊇ VL expected fields |
| `rows_match` | query with limit | Same row count, same fields present, key field values match |
| `status_equal` | Error cases, tail endpoint | HTTP status codes match |
| `structure_match` | Prometheus vector/matrix responses | Same JSON structure, numeric values equal |
| `bucket_match` | hits histogram | Same bucket timestamps, counts within ±1 |
| `non_empty` | Smoke tests where exact parity isn't achievable | Both return >0 results |

### Comparison Exclusions

- `_stream` and `_stream_id`: LH generates different stream identifiers than VL — excluded from row comparison.
- Field ordering in JSONL: ignored (VL and LH may serialize column order differently).
- Timestamp precision: compared at second granularity (nanosecond rounding may differ).
- For `rows_match`: compare on `_time` + `_msg` + filter-relevant fields, not all columns.

## Test Matrix

### Family 1: Endpoint Coverage (14 cases)

Every `/select/logsql/*` endpoint tested with a basic `query=*` and valid time range. Verifies HTTP 200 and correct response format.

| # | Endpoint | Query | Compare |
|---|----------|-------|---------|
| 1 | /select/logsql/query | `*` + limit=10 | rows_match |
| 2 | /select/logsql/query_time_range | `*` | structure_match |
| 3 | /select/logsql/facets | `*` | set_equal |
| 4 | /select/logsql/field_names | `*` | set_superset |
| 5 | /select/logsql/field_values | `*` + field=level | set_equal |
| 6 | /select/logsql/stream_field_names | `*` | set_superset |
| 7 | /select/logsql/stream_field_values | `*` + field=service.name | set_equal |
| 8 | /select/logsql/streams | `*` | non_empty |
| 9 | /select/logsql/stream_ids | `*` | non_empty |
| 10 | /select/logsql/hits | `*` + step=3600s | bucket_match |
| 11 | /select/logsql/stats_query | `* \| stats count() rows` | count_equal |
| 12 | /select/logsql/stats_query_range | `* \| stats count() rows` + step=3600s | structure_match |
| 13 | /select/logsql/tail | (any) | status_equal (501) |
| 14 | /select/tenant_ids | (none) | set_equal |

### Family 2: Filter Types (~25 cases)

| # | Name | Query | Compare |
|---|------|-------|---------|
| 1 | wildcard | `*` | count_equal |
| 2 | exact_service | `service.name:="api-gateway"` | count_equal |
| 3 | exact_level | `level:="ERROR"` | count_equal |
| 4 | exact_namespace | `k8s.namespace.name:="production"` | count_equal |
| 5 | substring_msg | `_msg:timeout` | count_equal |
| 6 | substring_case | `_msg:Error` | count_equal |
| 7 | regexp_msg | `_msg:~"timeout\|deadline"` | count_equal |
| 8 | regexp_service | `service.name:~"api-.*"` | count_equal |
| 9 | not_level | `NOT level:="DEBUG"` | count_equal |
| 10 | not_service | `NOT service.name:="api-gateway"` | count_equal |
| 11 | and_filter | `service.name:="api-gateway" AND level:="ERROR"` | count_equal |
| 12 | or_filter | `level:="ERROR" OR level:="WARN"` | count_equal |
| 13 | and_or_combined | `(level:="ERROR" OR level:="WARN") AND service.name:="api-gateway"` | count_equal |
| 14 | field_exists | `trace_id:*` | count_equal |
| 15 | field_not_exists | `NOT nonexistent_field:*` | count_equal |
| 16 | exact_msg | `_msg:="specific log message"` | count_equal |
| 17 | in_filter | `level:in("ERROR", "WARN")` | count_equal |
| 18 | range_numeric | `status:range[400, 599]` | count_equal |
| 19 | seq_filter | `_msg:seq("connection" "refused")` | count_equal |
| 20 | ipv4_filter | `host.ip:ipv4_range("10.0.0.0/8")` | count_equal |
| 21 | len_range | `_msg:len_range(100, 500)` | count_equal |
| 22 | multi_exact | `service.name:="api-gateway" level:="ERROR" k8s.namespace.name:="production"` | count_equal |
| 23 | negated_regexp | `_msg:!~"debug\|trace"` | count_equal |
| 24 | empty_value | `level:=""` | count_equal |
| 25 | stream_filter | `{service.name="api-gateway"}` | count_equal |

### Family 3: Pipe Operations (~20 cases)

All queries use stats_query endpoint with `* \| stats count() rows` base where applicable, or query endpoint with pipes.

| # | Name | Query | Compare |
|---|------|-------|---------|
| 1 | stats_count | `* \| stats count() rows` | count_equal |
| 2 | stats_count_by_level | `* \| stats by(level) count() rows` | structure_match |
| 3 | stats_count_by_service | `* \| stats by(service.name) count() rows` | structure_match |
| 4 | stats_count_uniq | `* \| stats count_uniq(service.name) services` | count_equal |
| 5 | stats_min_max | `* \| stats min(_time) earliest, max(_time) latest` | structure_match |
| 6 | fields_projection | `* \| fields _time, _msg, level` | rows_match |
| 7 | fields_single | `* \| fields _msg` | rows_match |
| 8 | sort_time_asc | `* \| sort by(_time)` | rows_match |
| 9 | sort_time_desc | `* \| sort by(_time) desc` | rows_match |
| 10 | limit_10 | `* \| limit 10` | rows_match |
| 11 | limit_1 | `* \| limit 1` | rows_match |
| 12 | uniq_level | `* \| uniq by(level)` | set_equal |
| 13 | uniq_service | `* \| uniq by(service.name)` | set_equal |
| 14 | top_services | `* \| top 5 by(service.name)` | structure_match |
| 15 | pipe_chain_fields_sort | `* \| fields _time, level \| sort by(_time) \| limit 5` | rows_match |
| 16 | pipe_chain_filter_stats | `level:="ERROR" \| stats by(service.name) count() rows` | structure_match |
| 17 | stats_by_two_fields | `* \| stats by(level, service.name) count() rows` | structure_match |
| 18 | stats_sum | `* \| stats sum(duration) total` | count_tolerance |
| 19 | stats_avg | `* \| stats avg(duration) mean` | count_tolerance |
| 20 | copy_pipe | `* \| copy level AS severity` | rows_match |

### Family 4: Stats Query & Range (~15 cases)

| # | Name | Endpoint | Query | Compare |
|---|------|----------|-------|---------|
| 1 | count_1h | stats_query | `* \| stats count() rows` (1h range) | count_equal |
| 2 | count_6h | stats_query | `* \| stats count() rows` (6h range) | count_equal |
| 3 | count_24h | stats_query | `* \| stats count() rows` (24h range) | count_equal |
| 4 | count_full | stats_query | `* \| stats count() rows` (full range) | count_equal |
| 5 | filtered_count | stats_query | `service.name:="api-gateway" \| stats count() rows` | count_equal |
| 6 | filtered_level | stats_query | `level:="ERROR" \| stats count() rows` | count_equal |
| 7 | group_by_level | stats_query | `* \| stats by(level) count() rows` | structure_match |
| 8 | range_rate_1h | stats_query_range | `* \| stats count() rows` step=300s (1h) | structure_match |
| 9 | range_rate_6h | stats_query_range | `* \| stats count() rows` step=600s (6h) | structure_match |
| 10 | range_rate_24h | stats_query_range | `* \| stats count() rows` step=3600s (24h) | structure_match |
| 11 | range_filtered | stats_query_range | `level:="ERROR" \| stats count() rows` step=3600s | structure_match |
| 12 | range_grouped | stats_query_range | `* \| stats by(level) count() rows` step=3600s | structure_match |
| 13 | multi_stat | stats_query | `* \| stats count() total, count_uniq(level) levels` | structure_match |
| 14 | count_over_subrange | stats_query | `* \| stats count() rows` (1-hour window inside data) | count_equal |
| 15 | empty_range_stats | stats_query | `* \| stats count() rows` (future range) | count_equal |

### Family 5: Time Range Handling (~10 cases)

| # | Name | Params | Compare |
|---|------|--------|---------|
| 1 | ns_epoch | start=ns, end=ns | count_equal |
| 2 | sec_epoch | start=sec, end=sec | count_equal |
| 3 | ms_epoch | start=ms, end=ms (Grafana style) | count_equal |
| 4 | missing_end | start only (end defaults to now) | count_equal |
| 5 | missing_start | end only | count_equal |
| 6 | future_range | start/end in year 3000 | count_equal (both 0) |
| 7 | zero_width | start == end | count_equal (both 0) |
| 8 | narrow_1min | 1-minute window with data | count_equal |
| 9 | full_range | start=0, end=now | count_equal |
| 10 | boundary_ns | exact ns of first/last row | count_tolerance |

### Family 6: Field Metadata (~10 cases)

| # | Name | Endpoint | Params | Compare |
|---|------|----------|--------|---------|
| 1 | field_names_all | field_names | query=* | set_superset |
| 2 | field_names_filtered | field_names | query=service.name:="api-gateway" | set_superset |
| 3 | field_values_level | field_values | field=level | set_equal |
| 4 | field_values_service | field_values | field=service.name | set_equal |
| 5 | field_values_namespace | field_values | field=k8s.namespace.name | set_equal |
| 6 | field_values_limit | field_values | field=service.name&limit=2 | non_empty |
| 7 | stream_field_names | stream_field_names | query=* | set_superset |
| 8 | stream_field_values | stream_field_values | field=service.name | set_equal |
| 9 | streams_list | streams | query=* | non_empty |
| 10 | stream_ids | stream_ids | query=* | non_empty |

### Family 7: Response Format & Pagination (~10 cases)

| # | Name | Test | Compare |
|---|------|------|---------|
| 1 | jsonl_structure | query returns valid JSONL lines | rows_match |
| 2 | limit_respected | limit=5 returns ≤5 rows (both) | rows_match |
| 3 | limit_zero | limit=0 returns all rows | count_equal |
| 4 | hits_bucket_keys | hits step=1800s has correct bucket timestamps | bucket_match |
| 5 | hits_sum_equals_count | sum(hits buckets) == stats count | count_tolerance |
| 6 | stats_vector_format | stats_query returns Prometheus instant vector | structure_match |
| 7 | stats_range_matrix | stats_query_range returns Prometheus matrix | structure_match |
| 8 | field_names_jsonl | field_names returns one field per line | set_superset |
| 9 | field_values_jsonl | field_values returns valid JSON per line | set_equal |
| 10 | large_limit | limit=100000 doesn't crash either system | non_empty |

### Family 8: Edge Cases & Errors (~10 cases)

| # | Name | Test | Compare |
|---|------|------|---------|
| 1 | empty_filter | `nonexistent_service:="xxx"` returns 0 rows | count_equal |
| 2 | tail_501 | /select/logsql/tail returns 501 | status_equal |
| 3 | invalid_query | `query=)))invalid` | status_equal |
| 4 | missing_query | no query param | status_equal |
| 5 | special_chars | `_msg:="hello \"world\""` | count_equal |
| 6 | unicode_msg | `_msg:="日本語"` | count_equal |
| 7 | empty_string_filter | `level:=""` | count_equal |
| 8 | very_long_query | 1KB query string | status_equal |
| 9 | concurrent_queries | 10 parallel identical queries | count_equal |
| 10 | stats_no_pipe | `query=*` on stats_query (no pipe) | status_equal |

### Traces Parity (~15 cases)

Compares LH-traces vs VictoriaTraces.

| # | Name | Endpoint | Compare |
|---|------|----------|---------|
| 1 | jaeger_services | /api/services | set_equal |
| 2 | jaeger_operations | /api/services/{svc}/operations | set_equal |
| 3 | jaeger_search_service | /api/traces?service=api-gateway | non_empty |
| 4 | jaeger_search_limit | /api/traces?service=api-gateway&limit=5 | rows_match |
| 5 | jaeger_trace_detail | /api/traces/{id} | structure_match |
| 6 | jaeger_dependencies | /api/dependencies | non_empty |
| 7 | traces_field_names | /select/logsql/field_names | set_superset |
| 8 | traces_field_values | /select/logsql/field_values?field=service.name | set_equal |
| 9 | traces_query_wildcard | /select/logsql/query?query=* | non_empty |
| 10 | traces_stats_count | /select/logsql/stats_query (count) | count_equal |
| 11 | traces_hits | /select/logsql/hits | bucket_match |
| 12 | traces_filter_service | /select/logsql/query?query=service.name:="api-gateway" | count_equal |
| 13 | traces_trace_id_lookup | /select/logsql/query?query=trace_id:="..." | rows_match |
| 14 | traces_stats_by_service | /select/logsql/stats_query (by service.name) | structure_match |
| 15 | traces_empty_range | /select/logsql/query (future range) | count_equal |

## Known Acceptable Differences

These differences between LH and VL are expected and accounted for:

1. **`_stream` / `_stream_id`**: LH generates different stream identifiers — excluded from row comparison.
2. **Extra field names**: LH may expose additional parquet-internal columns in field_names — use `set_superset` comparison.
3. **`streams` / `stream_ids` values**: Different internal stream representations — use `non_empty` comparison only.
4. **Timestamp sub-second precision**: LH stores timestamps as int64 nanoseconds in parquet; rounding at ns boundary may differ by ±1ns — compare at second granularity.
5. **Field ordering in JSONL**: Column serialization order may differ — compare field sets, not string equality.
6. **`tail` endpoint**: LH returns 501 (not supported on cold storage) — both should return non-200 but exact status may differ.

## Success Criteria

- **All 115 test cases pass** against the lightweight compose stack
- **Zero data correctness failures** in count_equal and set_equal modes
- **Test suite runs in <60s** after compose stack is ready
- **Clean CI integration** possible via `go test -tags=parity`
