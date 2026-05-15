---
title: Storage & Parquet Flow
sidebar_position: 8
---

# Storage & Parquet Flow

End-to-end data flow through Victoria Lakehouse, from VL/VT upstream input through Parquet storage to query output.

## System Overview

```mermaid
graph TD
    subgraph Input
        APP[Applications] -->|"JSON, Loki, ES bulk"| INS[Insert API]
        VL["VL/VT Select"] -->|"internal/select/*"| SEL[Select API]
        GF[Grafana] -->|"select/logsql/*"| SEL
    end

    subgraph Victoria Lakehouse
        INS --> BW[BatchWriter]
        BW --> WAL[WAL]
        BW -->|flush| PQ[Parquet Writer]
        PQ --> S3[(S3)]
        PQ --> MAN[Manifest]

        SEL --> STOR[Storage.RunQuery]
        STOR --> MAN
        STOR --> CACHE[Cache Chain<br/>L1→L2→L3→S3]
        CACHE --> PQR[Parquet Reader]
        PQR --> FILT[Filter Engine]
        FILT --> DB[DataBlock]
        DB --> SEL
    end

    subgraph VL/VT Adapter
        VLAD[vlstorage.SetExternalStorage] --> STOR
    end
```

## Write Path

### Complete Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant H as InsertHandler
    participant BW as BatchWriter
    participant W as WAL
    participant PW as Parquet Writer
    participant S3 as S3
    participant M as Manifest
    participant P as Pusher

    C->>H: POST /insert/jsonline
    H->>H: Parse JSON fields
    H->>H: jsonFieldsToLogRow()
    H->>BW: MustAddLogRows([]LogRow)
    BW->>W: AppendLog(rows)
    BW->>BW: Buffer by partition
    
    Note over BW: Flush trigger:<br/>interval (10s) or<br/>buffer size threshold

    BW->>BW: Snapshot buffers (atomic swap)
    BW->>PW: writeLogsParquet(rows)
    PW->>PW: Sort by timestamp
    PW->>PW: ZSTD compress + bloom filters
    PW-->>BW: flushResult{Data, RawBytes}
    
    BW->>S3: PutObject(partition/batchID.parquet)
    S3-->>BW: OK
    BW->>M: AddFile(partition, FileInfo)
    BW->>W: Truncate()
    BW->>P: Notify(added=[FileInfo])
    P->>P: Broadcast to peers
```

### Insert API

**File:** `internal/insertapi/handler.go`

Three ingestion endpoints, each parsing a different format into the same `LogRow` struct:

| Endpoint | Format | Parser |
|----------|--------|--------|
| `POST /insert/jsonline` | Newline-delimited JSON | `parseJSONLine` |
| `POST /insert/loki/api/v1/push` | Loki push protobuf/JSON | `lokiPushRequest` |
| `POST /insert/elasticsearch/_bulk` | Elasticsearch bulk | ES bulk parser |

**Field Promotion:**

Each parser separates fields into promoted (top-level Parquet columns) and unpromoted (MAP columns):

```mermaid
graph LR
    RAW[Raw JSON Fields] --> PROM{Promoted?}
    PROM -->|Yes| TOP[Top-level columns<br/>service.name, trace_id,<br/>k8s.namespace.name, ...]
    PROM -->|No| MAP[MAP columns<br/>resource.attributes,<br/>log.attributes]
```

Promoted fields (logs): `_time`, `_msg`, `level`, `service.name`, `k8s.namespace.name`, `k8s.pod.name`, `k8s.deployment.name`, `k8s.node.name`, `deployment.environment`, `cloud.region`, `host.name`, `trace_id`, `span_id`, `scope.name`

### BatchWriter

**File:** `internal/storage/parquets3/writer.go`

Buffers rows in memory, partitioned by time:

```mermaid
flowchart TD
    ADD[AddLogRows] --> PART[Partition by timestamp<br/>dt=YYYY-MM-DD/hour=HH]
    PART --> BUF1["logBufs[dt=2026-05-02/hour=10]"]
    PART --> BUF2["logBufs[dt=2026-05-02/hour=11]"]

    TICK[Flush Ticker] -->|every 10s| SNAP[Snapshot & swap buffers]
    SIZE[Size threshold] --> SNAP
    SHUT[Graceful shutdown] --> SNAP

    SNAP --> F1[flushLogPartition hour=10]
    SNAP --> F2[flushLogPartition hour=11]

    F1 --> WRITE[writeLogsParquet]
    F2 --> WRITE
```

**Flush triggers:**
- **Periodic:** `FlushInterval` (default 10s)
- **Size:** when buffer exceeds `TargetFileSize` (default 128 MB)
- **Shutdown:** `FlushAll()` called from graceful shutdown hook

### Parquet Writing

Each flush produces a single Parquet file per partition:

```mermaid
flowchart LR
    ROWS[Sorted LogRows] --> RG[Row Groups<br/>configurable size]
    RG --> COLS[Columnar Layout]
    COLS --> ZSTD[ZSTD Compression]
    ZSTD --> BLOOM[Bloom Filters<br/>service.name, trace_id]
    BLOOM --> BYTES[Parquet Bytes]
    BYTES --> S3[S3 PutObject]
```

**S3 key format:** `{prefix}{partition}/{batchID}.parquet`
- Example: `logs/dt=2026-05-02/hour=10/a1b2c3d4e5f6g7h8.parquet`
- `batchID` is a random 8-byte hex string

**Compression levels:**
- 1-5: ZSTD default speed
- 6-10: ZSTD better compression
- 11+: ZSTD best compression

### WAL (Write-Ahead Log)

**File:** `internal/wal/wal.go`

Optional durability layer. When enabled, rows are appended to the WAL before buffering:

```mermaid
flowchart TD
    ADD[AddLogRows] --> WAL{WAL enabled?}
    WAL -->|Yes| APPEND[WAL.AppendLog<br/>gob-encoded + length header]
    WAL -->|No| BUF[Buffer directly]
    APPEND --> BUF

    FLUSH[Successful flush] --> TRUNC[WAL.Truncate<br/>Atomic file replacement]

    CRASH[Crash recovery] --> REPLAY[WAL.Replay<br/>Read until EOF/corrupt]
    REPLAY --> RESTORE[Restore rows to buffers]
```

**Wire format:** `[4-byte LE length][1-byte mode: 'L'|'T'][gob-encoded row]`

**Recovery:** On startup, replay reads entries until first EOF or corrupt record (crash boundary). This restores any rows that were buffered but not yet flushed to S3.

| Config | Default | Flag |
|--------|---------|------|
| `insert.wal_enabled` | `false` | `--lakehouse.insert.wal-enabled` |
| `insert.wal_dir` | `/data/lakehouse/wal` | `--lakehouse.insert.wal-dir` |
| `insert.wal_max_bytes` | `512MB` | `--lakehouse.insert.wal-max-bytes` |

## Read Path

### Complete Flow

```mermaid
sequenceDiagram
    participant C as Client/VL
    participant A as VL Adapter
    participant S as Storage
    participant M as Manifest
    participant SC as SmartCache
    participant PQ as Parquet Reader
    participant F as Filter Engine

    C->>A: RunQuery(tenantIDs, query, writeBlock)
    A->>S: RunQuery(ctx, tenantIDs, query, writeBlock)
    
    S->>S: Extract time range from query
    S->>M: HasDataForRange(startNs, endNs)
    
    alt No data in range
        M-->>S: false
        S-->>C: return nil (fast path < 1ms)
    else Data exists
        M-->>S: true
        S->>M: GetFilesForRange(startNs, endNs)
        M-->>S: []FileInfo
        
        S->>S: Pin files in SmartCache
        
        par Parallel file workers (8)
            S->>SC: getFileData(key, size)
            SC-->>S: Parquet bytes
            S->>PQ: parquet.OpenFile(bytes)
            PQ-->>S: *parquet.File
            
            loop Each Row Group
                S->>S: rowGroupMatchesTimeRange?
                S->>S: bloomFilterSkip?
                S->>PQ: readRowGroup → rows
                PQ-->>S: []LogRow
                S->>S: typedRowsToDataBlock
                S->>F: filterDataBlock(db, filter)
                F-->>S: filtered DataBlock
                S->>C: writeBlock(DataBlock)
            end
        end
        
        S->>S: Query buffer bridge (unflushed data)
        S->>S: Unpin files
    end
```

### VL/VT Adapter

**File:** `internal/vlstorage/vlstorage.go`

The adapter bridges VictoriaLogs' internal storage dispatch to our Parquet backend:

```mermaid
graph LR
    VL[VL vlstorage dispatch] -->|SetExternalStorage| AD[adapter]
    AD -->|RunQuery| STOR[Storage]
    AD -->|GetFieldNames| STOR
    AD -->|GetFieldValues| STOR
    AD -->|GetStreams| STOR
    AD -->|DeleteRunTask| TS[TombstoneStore]
```

- `vlstorage.SetExternalStorage(&adapter{store, tombstones})` wires our storage into VL's dispatch
- All VL `/select/logsql/*` and `/internal/select/*` endpoints automatically route through our Parquet backend
- No VL/VT code is modified — only the storage dispatch seam is replaced

### Manifest Lookup

```mermaid
flowchart TD
    QUERY["Query: _time: 2026-05-02T10:00 to 2026-05-02T12:00"] 
    QUERY --> FAST{"HasDataForRange?"}
    FAST -->|manifest.minTime > endNs<br/>or maxTime < startNs| EMPTY[Return empty<br/>< 1ms]
    FAST -->|overlap| RANGE[GetFilesForRange]
    RANGE --> MATCH["Match partitions:<br/>dt=2026-05-02/hour=10<br/>dt=2026-05-02/hour=11"]
    MATCH --> FILES["Return all FileInfo<br/>in matching partitions"]
```

### Parallel File Processing

Files are processed in parallel via a worker pool:

```mermaid
graph TD
    FILES["12 matching files"] --> POOL[Worker Pool<br/>8 concurrent]
    POOL --> W1[Worker 1: file_01.parquet]
    POOL --> W2[Worker 2: file_02.parquet]
    POOL --> W3[Worker 3: file_03.parquet]
    POOL --> WN["..."]
    
    W1 --> QF[queryFile]
    W2 --> QF
    W3 --> QF
```

| Config | Default | Flag |
|--------|---------|------|
| `query.file_workers` | `8` | `--lakehouse.query.file-workers` |
| `query.max_concurrent` | (unlimited) | `--lakehouse.query.max-concurrent` |
| `query.timeout` | (none) | `--lakehouse.query.timeout` |
| `query.max_rows` | `0` (unlimited) | `--lakehouse.query.max-rows` |

### Single File Query

**File:** `internal/storage/parquets3/storage_query.go`

Each file goes through a multi-stage filtering pipeline:

```mermaid
flowchart TD
    FILE[FileInfo] --> GET[getFileData<br/>L1→L2→L3→S3]
    GET --> OPEN[parquet.OpenFile]
    OPEN --> LABEL[updateLabelIndex]
    OPEN --> RG[For each Row Group]

    RG --> STATS{Row group stats<br/>match time range?}
    STATS -->|min > endNs or<br/>max < startNs| SKIP1[Skip row group]
    STATS -->|overlap| BLOOM{Bloom filter<br/>check?}

    BLOOM -->|value not in bloom| SKIP2[Skip row group]
    BLOOM -->|pass or no bloom| READ[readRowGroup]

    READ --> BATCH[Read 256-row batches]
    BATCH --> TYPED[typedRowsToDataBlock]
    TYPED --> FILT[filterDataBlock<br/>LogsQL predicate]
    FILT --> TOMB[filterTombstonedRows]
    TOMB --> WRITE[writeBlock callback]
```

#### Row Group Stats Skip

Parquet stores min/max statistics per column per row group. The `rowGroupMatchesTimeRange` function checks `timestamp_unix_nano` column stats:

```
If rowGroup.min_timestamp > query.endNs → skip
If rowGroup.max_timestamp < query.startNs → skip
Otherwise → scan this row group
```

#### Bloom Filter Skip

For columns with bloom filters (service.name, trace_id), exact-match queries check the bloom filter before scanning:

```
buildBloomChecks(queryStr) → [{column: "service.name", value: "api-server"}]
For each check:
    If bloomFilter.Check(value) == false → skip entire row group
```

#### Row Reading

Rows are read in batches of 256 using `parquet.GenericRowGroupReader`. Each batch is converted to a DataBlock via `typedRowsToDataBlock`.

### typedRowsToDataBlock

**File:** `internal/storage/parquets3/storage_query.go`

Converts Parquet-native typed rows into VL's columnar DataBlock format:

```mermaid
flowchart LR
    subgraph Parquet Row
        TS[timestamp_unix_nano: int64]
        BODY[body: string]
        SVC[service.name: string]
        RA["resource.attributes: MAP"]
    end

    subgraph Schema Registry
        FMT[FormatField<br/>TypeTimestampNano → RFC3339Nano<br/>TypeInt32 → decimal<br/>TypeString → passthrough]
    end

    subgraph DataBlock
        COL1["_time: [2026-05-02T10:00:00Z, ...]"]
        COL2["_msg: [log line 1, ...]"]
        COL3["service.name: [api-server, ...]"]
        COL4["custom.field: [value, ...]"]
    end

    TS --> FMT --> COL1
    BODY --> FMT --> COL2
    SVC --> FMT --> COL3
    RA --> FMT --> COL4
```

**Processing steps:**
1. Collect unique column names across all rows
2. For each row, call `toFields(row)` → `[]field{name, value}`
3. Format each field value via `registry.FormatField(name, rawValue)`
4. Accumulate into columnar `map[name][]values`
5. Set columns on DataBlock

### Filter Evaluation

**File:** `internal/storage/parquets3/filter.go`

LogsQL filter predicates are evaluated against DataBlock rows:

```mermaid
flowchart TD
    QUERY["service.name:=api-server AND level:error"] --> PARSE[parseFilterFromQuery]
    PARSE --> LOGSQL[logstorage.ParseFilter]
    LOGSQL --> PRED[Filter predicate]

    DB[DataBlock] --> EVAL[filterDataBlock]
    PRED --> EVAL
    EVAL --> ROW["For each row:<br/>buildRowFields → MatchRow"]
    ROW -->|match| KEEP[Keep row]
    ROW -->|no match| DROP[Drop row]
    KEEP --> RESULT[Filtered DataBlock]
```

Uses VL's native `logstorage.Filter.MatchRow()` for evaluation — full LogsQL compatibility including substring, exact match, regex, NOT, AND, OR.

### Buffer Bridge

**File:** `internal/storage/parquets3/buffer_bridge.go`

For zero-delay reads, select pods query insert pods for unflushed data:

```mermaid
sequenceDiagram
    participant SEL as Select Pod
    participant INS1 as Insert Pod 1
    participant INS2 as Insert Pod 2

    SEL->>SEL: RunQuery: query S3 via manifest
    
    par Buffer query (fan-out)
        SEL->>INS1: GET /internal/buffer/query?start=&end=&mode=logs
        INS1-->>SEL: NDJSON LogRows
        SEL->>INS2: GET /internal/buffer/query?start=&end=&mode=logs
        INS2-->>SEL: NDJSON LogRows
    end
    
    SEL->>SEL: logRowsToDataBlock(buffered rows)
    SEL->>SEL: Merge with S3 results
```

Insert pods are discovered via Kubernetes headless service DNS (`SelectConfig.InsertHeadlessService`).

## Schema Registry

**File:** `internal/schema/registry.go`

Bidirectional mapping between OTLP Parquet column names and VL/VT internal names:

```mermaid
graph LR
    subgraph Parquet Columns
        P1[timestamp_unix_nano]
        P2[body]
        P3[severity_text]
        P4[service.name]
        P5["resource.attributes MAP"]
    end

    subgraph Registry
        R[ResolveToParquet<br/>ResolveFromParquet]
    end

    subgraph VL Internal Names
        V1[_time]
        V2[_msg]
        V3[level]
        V4[service.name]
        V5["resource_attr:key"]
    end

    P1 <-->|TypeTimestampNano| R
    P2 <-->|TypeString| R
    P3 <-->|TypeString| R
    P4 <-->|TypeString + Bloom| R
    P5 <-->|MAP| R
    R <--> V1
    R <--> V2
    R <--> V3
    R <--> V4
    R <--> V5
```

### FieldType System

Each column has a `FieldType` that controls formatting:

| FieldType | Parquet Type | Output Format | Example |
|-----------|-------------|---------------|---------|
| TypeTimestampNano | int64 | RFC3339Nano | `2026-05-02T10:00:00.123456789Z` |
| TypeInt32 | int32 | Decimal | `200` |
| TypeInt64 | int64 | Decimal | `1714694400000000000` |
| TypeFloat64 | float64 | %g format | `3.14` |
| TypeBool | bool | true/false | `true` |
| TypeString | string | Passthrough | `api-server` |

### Profiles

**LogsProfile** — 17 promoted columns:
`timestamp_unix_nano`, `body`, `severity_text`, `severity_number`, `service.name`, `k8s.namespace.name`, `k8s.pod.name`, `k8s.deployment.name`, `k8s.node.name`, `deployment.environment`, `cloud.region`, `host.name`, `trace_id`, `span_id`, `scope.name`, `_stream`, `_stream_id`

MAP columns: `resource.attributes`, `log.attributes`

Bloom filters: `service.name`, `trace_id`

**TracesProfile** — similar with span-specific fields (`span.name`, `span.kind`, `status.code`, `duration_ns`, `parent_span_id`, `start_time_unix_nano`)

MAP columns: `resource.attributes`, `span.attributes`, `scope.attributes`

## Data Row Structs

**File:** `internal/schema/row.go`

### LogRow

```
LogRow {
    TimestampUnixNano  int64              // Primary timestamp
    Body               string             // Log message (_msg)
    SeverityText       string             // level
    SeverityNumber     int32              // OTEL severity number
    ServiceName        string             // Promoted + bloom
    K8sNamespaceName   string             // Promoted
    K8sPodName         string             // Promoted
    K8sDeploymentName  string             // Promoted
    K8sNodeName        string             // Promoted
    DeployEnv          string             // Promoted
    CloudRegion        string             // Promoted
    HostName           string             // Promoted
    TraceID            string             // Promoted + bloom
    SpanID             string             // Promoted
    Stream             string             // _stream label
    StreamID           string             // _stream_id
    ScopeName          string             // Promoted
    ResourceAttributes map[string]string  // MAP column
    LogAttributes      map[string]string  // MAP column
}
```

### TraceRow

```
TraceRow {
    TimestampUnixNano    int64              // Primary timestamp
    StartTimeUnixNano    int64              // Span start
    TraceID              string             // Promoted + bloom
    SpanID               string             // Promoted
    ParentSpanID         string             // Promoted
    SpanName             string             // Promoted
    SpanKind             int32              // Promoted (OTEL enum)
    ServiceName          string             // Promoted + bloom
    StatusCode           int32              // Promoted (OTEL enum)
    StatusMessage        string             // Promoted
    DurationNs           int64              // Promoted
    ScopeName            string             // Promoted
    ... k8s/cloud fields ...
    ResourceAttributes   map[string]string  // MAP column
    SpanAttributes       map[string]string  // MAP column
    ScopeAttributes      map[string]string  // MAP column
}
```

## Tombstone Filtering

Deleted data is suppressed at query time via tombstones:

```mermaid
flowchart TD
    DB[DataBlock from Parquet] --> TOMB{Active tombstones<br/>for time range?}
    TOMB -->|No| PASS[Pass through]
    TOMB -->|Yes| CHECK[For each row:<br/>parse timestamp,<br/>build field map,<br/>MatchesRow?]
    CHECK -->|match| SUPPRESS[Drop row]
    CHECK -->|no match| KEEP[Keep row]
    KEEP --> OUT[Filtered DataBlock]
    SUPPRESS --> METRIC[Increment rows_suppressed_total]
```

## Startup Sequence

```mermaid
flowchart TD
    START[Binary Start] --> CFG[Load Config]
    CFG --> S3POOL[Create S3 Client Pool]
    S3POOL --> MAN[Create Manifest]
    MAN --> CACHE[Create Cache Stack<br/>L1 + L2 + Peer + SmartCache]
    CACHE --> DISC[Start Discovery<br/>Peer + Hot Boundary]

    DISC --> P1[Phase: DiskRecovery<br/>WAL replay]
    P1 --> P2[Phase: S3Refresh<br/>Manifest scan, 5min timeout]
    P2 --> P3[WarmLabelIndex<br/>Sample 10 files]
    P3 --> P4[Phase: Ready<br/>Start serving]

    P4 --> TICK[Periodic:<br/>Manifest refresh,<br/>Cache eviction,<br/>Metadata snapshot]
```

## Insert Configuration

| Config | Default | Flag |
|--------|---------|------|
| `insert.flush_interval` | `10s` | `--lakehouse.insert.flush-interval` |
| `insert.max_buffer_rows` | `50000` | `--lakehouse.insert.max-buffer-rows` |
| `insert.max_buffer_bytes` | `256MB` | `--lakehouse.insert.max-buffer-bytes` |
| `insert.target_file_size` | `128MB` | `--lakehouse.insert.target-file-size` |
| `insert.row_group_size` | `10000` | `--lakehouse.insert.row-group-size` |
| `insert.bloom_columns` | `service.name,trace_id` | `--lakehouse.insert.bloom-columns` |
| `insert.compression_level` | `7` | `--lakehouse.insert.compression-level` |

## Query Configuration

| Config | Default | Flag |
|--------|---------|------|
| `query.file_workers` | `8` | `--lakehouse.query.file-workers` |
| `query.max_concurrent` | (unlimited) | `--lakehouse.query.max-concurrent` |
| `query.timeout` | (none) | `--lakehouse.query.timeout` |
| `query.max_rows` | `0` (unlimited) | `--lakehouse.query.max-rows` |
| `query.slow_threshold` | (none) | `--lakehouse.query.slow-threshold` |
