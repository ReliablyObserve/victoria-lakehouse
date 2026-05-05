# Cost-Aware Deletion Strategy

## Problem

Deleting records from Parquet files on S3 is inherently expensive because Parquet is immutable — you must read the file, filter out deleted rows, and write a new file. On S3 Glacier, this means retrieval fees ($0.03-$0.09/GB) plus rewrite costs. At scale, naive deletion of a single log line from a 2-year-old Glacier file could cost more than storing it for another decade.

Victoria Lakehouse must support VL/VT-compatible delete APIs (`/delete/logsql/*`) with the same query syntax — but implement them intelligently at the storage layer based on the S3 storage class of the underlying data.

## Design: Three-Tier Deletion

### Tier 1: Tombstone (Default, Zero-Cost)

For data on any S3 class, the default deletion mode is **tombstone-based soft delete**:

1. User issues delete query: `POST /delete/logsql/delete?query=service.name:="leaked-credentials"&start=...&end=...`
2. Lakehouse evaluates the query against the manifest to identify affected files
3. Instead of rewriting files, a **tombstone record** is written to the manifest:
   ```json
   {
     "type": "tombstone",
     "query": "service.name:=\"leaked-credentials\"",
     "time_range": {"start": "2025-01-01T00:00:00Z", "end": "2025-06-01T00:00:00Z"},
     "affected_files": ["logs/dt=2025-03-15/hour=14/00001.parquet", ...],
     "created_at": "2026-05-05T10:00:00Z",
     "created_by": "admin@company.com"
   }
   ```
4. On every read query, tombstones are evaluated as post-filters — matching rows are suppressed from results
5. **Cost: $0** — no S3 reads, no rewrites, no retrieval fees

**Tombstones are stored in:**
- In-memory manifest (instant filter application)
- Persisted to disk (survives restarts)
- Synced to S3 as `_tombstones/{id}.json` (survives pod loss)

### Tier 2: Rewrite (S3 Standard Only)

For data still on S3 Standard (typically <90 days old), physical deletion is cheap:

1. Tombstone is created first (immediate visibility suppression)
2. Background job identifies tombstoned files on S3 Standard class
3. Files are read, filtered, rewritten without deleted rows
4. Manifest atomically swaps old file → new file
5. Old file deleted from S3

**Cost:** S3 GET + PUT + storage delta. At $0.0004/1K GETs + $0.005/1K PUTs, rewriting 100 files costs ~$0.05.

**When this runs:**
- Automatically for S3 Standard files (configurable delay, default 1 hour after tombstone)
- Never for S3-IA or Glacier files (cost-prohibited)
- Batch mode: accumulate multiple tombstones before rewriting a file

### Tier 3: Lifecycle Expiry (Glacier/Archive)

For data on S3-IA, Glacier Instant, or Glacier Deep:

1. Tombstone suppresses reads immediately (Tier 1)
2. Physical data remains on S3 until lifecycle policy expires it
3. **No rewrite ever happens** — the data ages out naturally

**User options:**
- Accept tombstone-only (recommended): data invisible but physically present until lifecycle expires
- Force-delete (explicit opt-in, warning about costs): triggers Glacier retrieval + rewrite
- Accelerate lifecycle: move specific prefixes to shorter expiry

## API Surface

### Delete Endpoint (VL-Compatible)

```
POST /delete/logsql/delete
  ?query=<LogsQL filter>
  &start=<timestamp>
  &end=<timestamp>
  &mode=tombstone|rewrite|auto   (default: auto)
```

**Mode behavior:**
- `tombstone`: always soft-delete only (cheapest, instant)
- `rewrite`: force physical deletion (warns if touching Glacier, requires confirmation header)
- `auto` (default): tombstone immediately, schedule rewrite for S3 Standard files only

### Cost Estimation Endpoint

Before executing a delete, users can estimate the cost:

```
POST /delete/logsql/estimate
  ?query=<LogsQL filter>
  &start=<timestamp>
  &end=<timestamp>

Response:
{
  "affected_files": 47,
  "affected_rows_estimate": 12500,
  "storage_classes": {
    "STANDARD": {"files": 12, "bytes": 156000000, "rewrite_cost": "$0.02"},
    "STANDARD_IA": {"files": 20, "bytes": 890000000, "rewrite_cost": "$11.13"},
    "GLACIER_INSTANT": {"files": 10, "bytes": 450000000, "rewrite_cost": "$40.50"},
    "GLACIER_DEEP": {"files": 5, "bytes": 230000000, "rewrite_cost": "$120.75"}
  },
  "recommended_mode": "auto",
  "auto_behavior": "Tombstone all 47 files immediately. Rewrite 12 STANDARD files (cost: $0.02). Leave 35 files on IA/Glacier with tombstone suppression until lifecycle expiry."
}
```

### Tombstone Management

```
GET /delete/logsql/tombstones
  ?active=true                    # Only show active (non-reaped) tombstones

DELETE /delete/logsql/tombstone/{id}
  # Removes a tombstone (un-deletes data if file still exists)

GET /delete/logsql/tombstone/{id}/status
  # Shows rewrite progress for this tombstone
```

## Query-Time Tombstone Evaluation

Tombstones are evaluated during the normal read path:

```
Query arrives
  → Manifest lookup (find files)
  → Check tombstones for matching files
  → For tombstoned files:
      - If entire file is tombstoned: skip file entirely (fast path)
      - If partial tombstone: read file, apply tombstone filter as post-filter
  → Return results with deleted rows suppressed
```

**Performance impact:**
- Full-file tombstones: zero cost (file skipped at manifest level)
- Partial tombstones: one additional filter per affected file (~microseconds)
- Tombstone count in steady state: typically <100 (most get reaped after rewrite)

## Storage Class Detection

Lakehouse detects the S3 storage class of each file via:

1. **HeadObject on first access** — `StorageClass` header in response
2. **Cached in manifest** — storage class stored per-file, refreshed on lifecycle transitions
3. **Lifecycle prediction** — if lifecycle rules are configured, predict class from file age

```yaml
lakehouse:
  delete:
    auto_rewrite_classes: ["STANDARD"]           # Only rewrite these classes
    rewrite_delay: 1h                             # Wait before rewriting (batch tombstones)
    rewrite_batch_size: 50                        # Max files per rewrite job
    glacier_force_header: "X-Force-Glacier-Delete" # Required header for forced Glacier rewrite
    tombstone_persist_path: /data/lakehouse/tombstones
    cost_warning_threshold: "$10"                 # Warn user if estimated cost exceeds this
```

## Cost Comparison: Delete Operations

| Operation | Lakehouse (Tombstone) | Lakehouse (Rewrite, Standard) | Loki | Tempo |
|---|---|---|---|---|
| **Delete single record** | **$0** (instant) | $0.001 (1 file rewrite) | Not supported | Not supported (whole trace only) |
| **Delete by query (1K matches)** | **$0** (instant) | $0.05 (batch rewrite) | Not supported | N/A |
| **Delete from Glacier** | **$0** (tombstone only) | $40-120 (retrieval + rewrite) | N/A | N/A |
| **GDPR "right to erasure"** | **$0** immediate (tombstone satisfies GDPR) | Optional physical delete for compliance | Export + reimport (manual) | Not supported at record level |
| **Retention-based expiry** | S3 Lifecycle (free) | S3 Lifecycle (free) | Compactor retention (CPU cost) | Compactor (CPU cost) |

## GDPR / Compliance Considerations

Tombstone-based deletion **satisfies GDPR right to erasure** requirements because:

1. Data is immediately inaccessible through all query interfaces
2. No API, tool, or user can retrieve tombstoned records
3. Physical deletion occurs automatically when S3 lifecycle expires the file
4. For strict compliance requirements, forced rewrite of S3 Standard files provides immediate physical deletion

**Audit trail:**
- Every tombstone records who deleted what and when
- Tombstone metadata persisted to S3 for compliance auditing
- Optional webhook/SQS notification on delete operations

## Comparison: Why Loki/Tempo Can't Do This

| Capability | Lakehouse | Loki | Tempo |
|---|---|---|---|
| Record-level delete | Tombstone + selective rewrite | Not supported (whole stream only) | Whole trace only |
| Query-based delete | Full LogsQL filter support | Not supported | TraceID only |
| Cost-aware deletion | Per-storage-class strategy | N/A (all S3 Standard) | N/A |
| Glacier-safe delete | Tombstone suppression (no retrieval) | Can't use Glacier (compaction) | Can't use Glacier (compaction) |
| GDPR compliance | Immediate tombstone + audit trail | Manual stream deletion | Manual trace deletion |
| Un-delete | Remove tombstone | Not possible | Not possible |
| Delete cost estimation | Built-in API | N/A | N/A |

## Implementation Notes

### Tombstone Storage Format

```
s3://{bucket}/{tenant}/_tombstones/
  2026-05-05T10-00-00Z_abc123.json   # One file per tombstone
```

Tombstones are small JSON files (<1KB) stored alongside data. They're loaded into memory on startup and synced via manifest broadcasts.

### Rewrite Job Scheduling

```
Background goroutine (every rewrite_delay):
  1. Scan active tombstones
  2. Group by affected file
  3. For each file on STANDARD class:
     a. Read file from S3
     b. Apply all tombstones for this file
     c. Write new file (without deleted rows)
     d. Update manifest (atomic swap)
     e. Delete old file
     f. Mark tombstone as "reaped" for this file
  4. Skip files on IA/Glacier (log advisory message)
```

### Metrics

| Metric | Type | Description |
|---|---|---|
| `lakehouse_delete_tombstones_active` | Gauge | Active tombstones |
| `lakehouse_delete_tombstones_total` | Counter | Total tombstones created |
| `lakehouse_delete_rewrite_total` | Counter | Physical rewrites completed |
| `lakehouse_delete_rewrite_bytes_saved` | Counter | Bytes freed by rewrites |
| `lakehouse_delete_rewrite_skipped_glacier` | Counter | Rewrites skipped (Glacier) |
| `lakehouse_delete_rows_suppressed_total` | Counter | Rows filtered by tombstones at query time |
