package compaction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
)

// CompactorPool abstracts S3 operations needed by the compactor.
type CompactorPool interface {
	Upload(ctx context.Context, key string, data []byte) error
	Download(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

// BloomRebuilder rebuilds bloom entries for a partition after compaction.
type BloomRebuilder interface {
	RebuildPartition(ctx context.Context, partition string, files []manifest.FileInfo) error
}

// CompactorConfig holds all dependencies for the Compactor.
type CompactorConfig struct {
	Pool             CompactorPool
	Manifest         *manifest.Manifest
	Prefix           string
	Mode             config.Mode
	RowGroupSize     int
	CompressionLevel int
	BloomRebuilder   BloomRebuilder

	// CompactionConfig is the full compaction-section config so the
	// compactor can read progressive-compression schedule (and any
	// future per-output-level knobs) without taking another constructor
	// parameter per knob. Passed by value because the struct is small
	// and the compactor doesn't mutate it.
	CompactionConfig config.CompactionConfig

	// TenantCompressionLookup resolves the per-output-level
	// compression schedule for a given tenant prefix (e.g.
	// "1002/0/logs/"). Returns nil to fall through to the global
	// CompactionConfig.CompressionLevelByOutputLevel. Optional —
	// when nil the compactor uses the global schedule for every
	// tenant. Wired by the embedder (main.go) so the compactor
	// stays independent of the tenant policy package.
	TenantCompressionLookup func(tenantPrefix string) []int
}

// CompactResult summarises one compaction run.
type CompactResult struct {
	Partition  string
	InputFiles []string
	// OutputFile holds the first output key for backward-compatible
	// logging. When per-tenant prefix isolation produces multiple
	// outputs (one per tenant whose files were in the input batch),
	// the full list is in OutputFiles.
	OutputFile   string
	OutputFiles  []string
	RowsMerged   int64
	BytesRead    int64
	BytesWritten int64
	OutputLevel  int
	Duration     time.Duration
}

// Compactor merges small Parquet files into larger ones.
type Compactor struct {
	pool             CompactorPool
	manifest         *manifest.Manifest
	prefix           string
	mode             config.Mode
	rowGroupSize     int
	compressionLevel int
	bloomRebuilder   BloomRebuilder
	cfg              config.CompactionConfig
	tenantLookup     func(tenantPrefix string) []int
}

// NewCompactor creates a Compactor from the given config.
func NewCompactor(cfg CompactorConfig) *Compactor {
	return &Compactor{
		pool:             cfg.Pool,
		manifest:         cfg.Manifest,
		prefix:           cfg.Prefix,
		mode:             cfg.Mode,
		rowGroupSize:     cfg.RowGroupSize,
		compressionLevel: cfg.CompressionLevel,
		bloomRebuilder:   cfg.BloomRebuilder,
		cfg:              cfg.CompactionConfig,
		tenantLookup:     cfg.TenantCompressionLookup,
	}
}

// Compact reads the given files from S3, merges all rows sorted by timestamp,
// writes a single compacted Parquet file, uploads it, updates the manifest,
// and removes the source files.
func (c *Compactor) Compact(ctx context.Context, partition string, files []manifest.FileInfo, sourceLevel int) (*CompactResult, error) {
	start := time.Now()

	// Verify all files share the same schema fingerprint.
	if len(files) == 0 {
		return nil, fmt.Errorf("no files to compact")
	}
	fp := files[0].SchemaFingerprint
	for _, f := range files[1:] {
		if f.SchemaFingerprint != fp {
			return nil, fmt.Errorf("schema fingerprint mismatch: %s vs %s", fp, f.SchemaFingerprint)
		}
	}

	result := CompactResult{
		Partition:   partition,
		OutputLevel: sourceLevel + 1,
	}

	// Group inputs by their tenant prefix and bucket so files from
	// different tenants are NOT merged into a single output that
	// would land at one tenant's prefix (or bucket). When prefix
	// isolation is on, the manifest's partition key (dt=…/hour=…)
	// is shared across tenants, so without this split rows from
	// tenant 1002:0 could end up in a file under 0/0/<mode>/…
	// after compaction. Each tenant group becomes its own output.
	groups := groupFilesByTenant(files)

	for _, g := range groups {
		groupResult, err := c.compactGroup(ctx, partition, g, fp, result.OutputLevel)
		if err != nil {
			return nil, err
		}
		result.InputFiles = append(result.InputFiles, groupResult.InputKeys...)
		result.OutputFiles = append(result.OutputFiles, groupResult.OutputKey)
		result.BytesRead += groupResult.BytesRead
		result.BytesWritten += groupResult.BytesWritten
		result.RowsMerged += groupResult.RowsMerged
	}
	if len(result.OutputFiles) > 0 {
		result.OutputFile = result.OutputFiles[0]
	}

	if c.bloomRebuilder != nil {
		currentFiles := c.manifest.FilesForPartition(partition)
		if err := c.bloomRebuilder.RebuildPartition(ctx, partition, currentFiles); err != nil {
			logger.Warnf("bloom rebuild after compaction failed for %s: %v", partition, err)
		}
	}

	result.Duration = time.Since(start)

	logger.Infof("compaction complete; partition=%s, tenant_groups=%d, input_files=%d, rows_merged=%d, bytes_read=%d, bytes_written=%d, output_level=%d, duration=%v",
		partition, len(groups), len(files), result.RowsMerged, result.BytesRead, result.BytesWritten, result.OutputLevel, result.Duration)

	return &result, nil
}

// tenantFileGroup is one tenant's slice of the input batch.
type tenantFileGroup struct {
	TenantPrefix string // e.g. "1002/0/<mode>/" — empty for legacy / non-tenant keys
	Bucket       string // the bucket the source files were in (empty = default)
	Files        []manifest.FileInfo
}

// groupFilesByTenant partitions the input by tenant prefix + bucket.
// Same-tenant files in different buckets are kept separate so output
// inherits the source bucket. Group order is deterministic.
func groupFilesByTenant(files []manifest.FileInfo) []tenantFileGroup {
	type groupKey struct {
		Prefix string
		Bucket string
	}
	byKey := make(map[groupKey]*tenantFileGroup, 1)
	var order []groupKey
	for _, f := range files {
		prefix := tenantPrefixFromKey(f.Key)
		k := groupKey{Prefix: prefix, Bucket: f.Bucket}
		g, ok := byKey[k]
		if !ok {
			g = &tenantFileGroup{TenantPrefix: prefix, Bucket: f.Bucket}
			byKey[k] = g
			order = append(order, k)
		}
		g.Files = append(g.Files, f)
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].Prefix != order[j].Prefix {
			return order[i].Prefix < order[j].Prefix
		}
		return order[i].Bucket < order[j].Bucket
	})
	out := make([]tenantFileGroup, 0, len(order))
	for _, k := range order {
		out = append(out, *byKey[k])
	}
	return out
}

// tenantPrefixFromKey extracts "<acct>/<proj>/<mode>/" from an S3 key
// produced by the writer's per-tenant flush path. Returns the empty
// string for legacy keys (no leading numeric segments) so they share
// a single "legacy" group instead of accidentally fanning out.
func tenantPrefixFromKey(key string) string {
	parts := strings.SplitN(key, "/", 4)
	if len(parts) < 4 {
		return ""
	}
	if _, err := strconv.ParseUint(parts[0], 10, 32); err != nil {
		return ""
	}
	if _, err := strconv.ParseUint(parts[1], 10, 32); err != nil {
		return ""
	}
	return parts[0] + "/" + parts[1] + "/" + parts[2] + "/"
}

type compactGroupResult struct {
	InputKeys    []string
	OutputKey    string
	RowsMerged   int64
	BytesRead    int64
	BytesWritten int64
}

func (c *Compactor) compactGroup(ctx context.Context, partition string, g tenantFileGroup, fp string, outputLevel int) (*compactGroupResult, error) {
	var (
		allData   [][]byte
		inputKeys []string
		bytesRead int64
	)
	for _, f := range g.Files {
		data, err := c.pool.Download(ctx, f.Key)
		if err != nil {
			return nil, fmt.Errorf("download %s: %w", f.Key, err)
		}
		if data == nil {
			return nil, fmt.Errorf("download %s: file not found", f.Key)
		}
		allData = append(allData, data)
		bytesRead += int64(len(data))
		inputKeys = append(inputKeys, f.Key)
	}

	var outputData []byte
	var rowsMerged int64
	var minTime, maxTime int64
	var labelAggregates map[string]map[string]int64

	// Pick the per-output-level compression. Tenant override beats
	// the global progressive schedule, which in turn beats the
	// static c.compressionLevel — that last fallback keeps
	// pre-progressive deployments behaviour-compatible.
	levelForOutput := c.compressionLevel
	if scheduled := c.cfg.CompressionLevelForOutput(outputLevel); scheduled > 0 {
		levelForOutput = scheduled
	}
	if c.tenantLookup != nil {
		if perTenant := c.tenantLookup(g.TenantPrefix); len(perTenant) > 0 {
			idx := outputLevel
			if idx >= len(perTenant) {
				idx = len(perTenant) - 1
			}
			if idx < 0 {
				idx = 0
			}
			if perTenant[idx] > 0 {
				levelForOutput = perTenant[idx]
			}
		}
	}

	// Pick the per-output-level row-group size the same way: the
	// progressive schedule beats the static c.rowGroupSize, and an
	// empty schedule (accessor returns 0) keeps pre-schedule
	// deployments behaviour-compatible. Default schedule doubles the
	// row-group size for L2+ rollups — cold scan-heavy files trade
	// row-group pruning granularity for better compression.
	rowGroupSizeForOutput := c.rowGroupSize
	if scheduled := c.cfg.RowGroupSizeForOutput(outputLevel); scheduled > 0 {
		rowGroupSizeForOutput = scheduled
	}

	switch c.mode {
	case config.ModeLogs:
		merged, err := c.mergeLogFiles(allData)
		if err != nil {
			return nil, err
		}
		rowsMerged = int64(len(merged))
		if rowsMerged > 0 {
			// True min/max scan — NOT merged[0]/merged[len-1]. The merge
			// order is a compression detail (today timestamp, soon
			// (stream_id, timestamp)); positional bounds understate the
			// manifest time range and break range pruning. See
			// schema.LogRowTimeBounds.
			minTime, maxTime = schema.LogRowTimeBounds(merged)
		}
		// Extract the per-(field,value) row counts from the merged ROWS —
		// the same shared implementation (field list + cap) the flush
		// writer uses — NOT by merging the input files' aggregate maps.
		// Input maps are empty for every file written before the #138 fix
		// (the aggregate wipe), so a map merge propagates the emptiness
		// forever; row extraction makes each compaction pass HEAL old
		// files into fully-aggregated outputs.
		labelAggregates = schema.ExtractLogLabelAggregates(merged)
		outputData, err = writeCompactedLogs(merged, rowGroupSizeForOutput, levelForOutput)
		if err != nil {
			return nil, fmt.Errorf("write compacted logs: %w", err)
		}

	case config.ModeTraces:
		merged, err := c.mergeTraceFiles(allData)
		if err != nil {
			return nil, err
		}
		rowsMerged = int64(len(merged))
		if rowsMerged > 0 {
			// True min/max scan — same rationale as the logs branch above.
			minTime, maxTime = schema.TraceRowTimeBounds(merged)
		}
		// Row-extracted aggregates — same healing rationale as the logs
		// branch above (shared schema.Extract* implementation).
		labelAggregates = schema.ExtractTraceLabelAggregates(merged)
		outputData, err = writeCompactedTraces(merged, rowGroupSizeForOutput, levelForOutput)
		if err != nil {
			return nil, fmt.Errorf("write compacted traces: %w", err)
		}

	default:
		return nil, fmt.Errorf("unsupported mode: %s", c.mode)
	}

	// Build output key under the tenant's prefix (preserving the
	// {acct}/{proj}/<mode>/ layout the writer produces) so the
	// compacted file is discoverable to per-tenant readers and
	// stats-attribution paths. Legacy files (no tenant prefix)
	// fall back to the compactor's configured default prefix —
	// preserves single-tenant behavior unchanged.
	outputPrefix := g.TenantPrefix
	if outputPrefix == "" {
		outputPrefix = c.prefix
	}
	short := uuid.New().String()[:8]
	outputKey := fmt.Sprintf("%s%s/compacted-L%d-%s.parquet", outputPrefix, partition, outputLevel, short)

	if err := c.pool.Upload(ctx, outputKey, outputData); err != nil {
		return nil, fmt.Errorf("upload compacted file: %w", err)
	}

	// Sum input RawBytes into the merged file. Compaction is a pure
	// row-union — no dedup, no projection — so the total raw content
	// is conserved. Without this carry-forward, every compaction
	// zeroed the merged file's RawBytes (omitempty default) while
	// Size kept tracking the compressed bytes correctly, producing
	// the impossible total_bytes > raw_bytes ratio on
	// /api/v1/tenants for any tenant whose files have been compacted.
	var inputRawBytes int64
	for _, f := range g.Files {
		inputRawBytes += f.RawBytes
	}

	// Per-output-level compression observability. The ratio is
	// inputRawBytes / len(outputData) — same denominator as the
	// /api/v1/tenants per-tenant ratio so a dashboard reading either
	// can correlate. levelForOutput went through the same tenant >
	// global > static fallback chain above, so the gauge reflects
	// the level that was actually applied (not just the
	// schedule's preferred level).
	outputLevelLabel := strconv.Itoa(outputLevel)
	if inputRawBytes > 0 && len(outputData) > 0 {
		ratio := float64(inputRawBytes) / float64(len(outputData))
		metrics.CompactionCompressionRatio.Observe(outputLevelLabel, ratio)
	}
	metrics.CompactionCompressionLevelUsed.Set(outputLevelLabel, int64(levelForOutput))

	// Union the per-field label sets across input files so the compacted
	// output stays discoverable via the manifest's inverted label index
	// (m.labelIndex). Without this, a field-equality filter like
	// `service.name:="api-gateway"` consults the inverted index, finds
	// only uncompacted files, and silently undercounts by ~80% in any
	// cluster with active compaction. Total counts and stream-shaped
	// filters keep working (they don't use the label-index fast path),
	// which is exactly the asymmetric undercount pattern that exposed
	// the bug. The union is bounded by maxLabelsPerField inside
	// indexFileLabels so a misbehaving input can't blow up the index.
	mergedLabels := mergeFileLabels(g.Files)

	c.manifest.AddFile(partition, manifest.FileInfo{
		Key:               outputKey,
		Bucket:            g.Bucket,
		Size:              int64(len(outputData)),
		RowCount:          rowsMerged,
		MinTimeNs:         minTime,
		MaxTimeNs:         maxTime,
		RawBytes:          inputRawBytes,
		SchemaFingerprint: fp,
		CompactionLevel:   outputLevel,
		Labels:            mergedLabels,
		LabelAggregates:   labelAggregates,
	})

	for _, f := range g.Files {
		c.manifest.RemoveFile(partition, f.Key)
		if err := c.pool.Delete(ctx, f.Key); err != nil {
			logger.Warnf("failed to delete source file; key=%s, error=%s", f.Key, err)
		}
	}

	return &compactGroupResult{
		InputKeys:    inputKeys,
		OutputKey:    outputKey,
		RowsMerged:   rowsMerged,
		BytesRead:    bytesRead,
		BytesWritten: int64(len(outputData)),
	}, nil
}

func (c *Compactor) mergeLogFiles(allData [][]byte) ([]schema.LogRow, error) {
	var merged []schema.LogRow
	for _, data := range allData {
		rows, err := readLogRows(data)
		if err != nil {
			return nil, fmt.Errorf("read log rows: %w", err)
		}
		merged = append(merged, rows...)
	}

	// Compaction-time trace-shape filter. Mirrors the ingest gate in
	// internal/vlstorage/insert.go: rows whose _stream marks them as
	// VT span / service-graph data get dropped on the way to the
	// merged output. Without this, historical files that already
	// hold trace-shape rows (written before the ingest gate landed)
	// would keep round-tripping through compaction, keeping the
	// manifest RowCount inflated forever. The drop here is the
	// natural off-ramp — each compaction pass shrinks the bad data
	// monotonically. Mass-rewrite tools (`/internal/compact`) can
	// trigger this immediately if an operator wants the cleanup to
	// finish on their schedule.
	if len(merged) > 0 {
		kept := merged[:0]
		dropped := 0
		for i := range merged {
			if storage.IsTraceShapedStream(merged[i].Stream) {
				dropped++
				continue
			}
			kept = append(kept, merged[i])
		}
		if dropped > 0 {
			metrics.LogsTraceShapedRowsDroppedAtCompaction.Add(dropped)
		}
		merged = kept
	}

	// Compaction-time SeverityText backfill. Rows written before the
	// insert-time fallback existed have empty SeverityText even when
	// the stream tag carries `level="X"` or severity_number is set.
	// Each compaction pass rewrites the rows on disk, so it's the
	// natural healing point — historical parquets get healed as they
	// roll up through compaction levels, no separate rewrite tool
	// needed.
	//
	// Reuses the same schema.DeriveSeverityText helper the insert
	// path uses, so the precedence chain (explicit text → derived
	// from severity_number → stream-tag `level`) is identical
	// across the two write paths.
	if len(merged) > 0 {
		backfilled := 0
		for i := range merged {
			if merged[i].SeverityText != "" {
				continue
			}
			var st *logstorage.StreamTags
			if merged[i].Stream != "" {
				st = logstorage.GetStreamTags()
				if err := st.UnmarshalString(merged[i].Stream); err != nil {
					logstorage.PutStreamTags(st)
					st = nil
				}
			}
			if derived := schema.DeriveSeverityText("", merged[i].SeverityNumber, st); derived != "" {
				merged[i].SeverityText = derived
				backfilled++
			}
			if st != nil {
				logstorage.PutStreamTags(st)
			}
		}
		if backfilled > 0 {
			metrics.LogsSeverityTextBackfilledAtCompaction.Add(backfilled)
		}
	}

	sort.Slice(merged, func(i, j int) bool {
		if merged[i].TimestampUnixNano != merged[j].TimestampUnixNano {
			return merged[i].TimestampUnixNano < merged[j].TimestampUnixNano
		}
		return merged[i].ServiceName < merged[j].ServiceName
	})
	return merged, nil
}

func (c *Compactor) mergeTraceFiles(allData [][]byte) ([]schema.TraceRow, error) {
	var merged []schema.TraceRow
	for _, data := range allData {
		rows, err := readTraceRows(data)
		if err != nil {
			return nil, fmt.Errorf("read trace rows: %w", err)
		}
		merged = append(merged, rows...)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].TimestampUnixNano != merged[j].TimestampUnixNano {
			return merged[i].TimestampUnixNano < merged[j].TimestampUnixNano
		}
		if merged[i].ServiceName != merged[j].ServiceName {
			return merged[i].ServiceName < merged[j].ServiceName
		}
		return merged[i].TraceID < merged[j].TraceID
	})
	return merged, nil
}

func readLogRows(data []byte) ([]schema.LogRow, error) {
	reader := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()

	n := int(reader.NumRows())
	rows := make([]schema.LogRow, n)
	total, err := reader.Read(rows)
	// parquet-go returns io.EOF when the reader is exhausted — including the
	// read that consumes the final rows AND the immediate read of a valid
	// 0-row file (NumRows()==0 → Read returns total=0, err=io.EOF). io.EOF
	// is NOT an error here: an empty parquet file is valid (a flush can
	// legitimately produce one). Treating it as fatal made compaction abort
	// the whole partition on a single empty input file, and because the
	// scheduler re-picks the oldest partition first, that one empty file
	// starved compaction of every newer partition indefinitely — growing
	// the cold-tier L0 backlog (and the recently-flushed trace_id-query
	// reachability lag) without bound.
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read parquet: %w", err)
	}
	return rows[:total], nil
}

func readTraceRows(data []byte) ([]schema.TraceRow, error) {
	reader := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()

	n := int(reader.NumRows())
	rows := make([]schema.TraceRow, n)
	total, err := reader.Read(rows)
	// See readLogRows: io.EOF (incl. the valid 0-row file case) is a clean
	// end-of-data, not a failure. Only a genuine decode error aborts.
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read parquet: %w", err)
	}
	return rows[:total], nil
}

// activeSlotResolver carries the Tier-2 custom-attribute slot binding into
// compaction so merged files keep their slot blooms + self-describing footer-KV
// mapping. Set at startup via SetSlotResolver; nil = no custom slots.
var activeSlotResolver *schema.SlotResolver

// SetSlotResolver installs the Tier-2 slot resolver for the compactor.
func SetSlotResolver(r *schema.SlotResolver) { activeSlotResolver = r }

func writeCompactedLogs(rows []schema.LogRow, rowGroupSize int, compressionLevel int) ([]byte, error) {
	var buf bytes.Buffer
	codec := &zstd.Codec{Level: zstdLevel(compressionLevel)}
	opts := []parquet.WriterOption{
		parquet.Compression(codec),
		parquet.MaxRowsPerRowGroup(int64(rowGroupSize)),
		parquet.BloomFilters(bloomFilters(schema.LogBloomColumns(activeSlotResolver.BloomSlots()...))...),
	}
	if kv := schema.MarshalSlotMapping(activeSlotResolver.Mapping()); kv != nil {
		opts = append(opts, parquet.KeyValueMetadata(schema.DedicatedSlotsMetaKey, string(kv)))
	}
	writer := parquet.NewGenericWriter[schema.LogRow](&buf, opts...)
	if _, err := writer.Write(rows); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCompactedTraces(rows []schema.TraceRow, rowGroupSize int, compressionLevel int) ([]byte, error) {
	var buf bytes.Buffer
	codec := &zstd.Codec{Level: zstdLevel(compressionLevel)}
	opts := []parquet.WriterOption{
		parquet.Compression(codec),
		parquet.MaxRowsPerRowGroup(int64(rowGroupSize)),
		parquet.BloomFilters(bloomFilters(schema.TraceBloomColumns(activeSlotResolver.BloomSlots()...))...),
	}

	// Tier-2: re-stamp the slot→name mapping so the compacted file stays
	// self-describing (queries read its slots by their configured name).
	if kv := schema.MarshalSlotMapping(activeSlotResolver.Mapping()); kv != nil {
		opts = append(opts, parquet.KeyValueMetadata(schema.DedicatedSlotsMetaKey, string(kv)))
	}

	// Preserve the per-file `_trace_idx` footer index across compaction.
	// Without this, every merged file loses the embedded trace-ID time
	// bounds and the cold-tier trace-by-ID fast path collapses to a
	// full span scan for compacted data. Index lives in standard Parquet
	// KV metadata (see internal/traceindex.MetadataKey) — same slot as
	// the original writer in lakehouse-traces/.../parquets3/writer.go,
	// so a query can't tell whether the file was freshly flushed or
	// later compacted.
	if idxData := traceindex.Marshal(traceindex.Compute(rows)); len(idxData) > 0 {
		opts = append(opts, parquet.KeyValueMetadata(traceindex.MetadataKey, string(idxData)))
	}

	writer := parquet.NewGenericWriter[schema.TraceRow](&buf, opts...)
	if _, err := writer.Write(rows); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func zstdLevel(level int) zstd.Level {
	switch {
	case level <= 1:
		return zstd.SpeedFastest
	case level <= 5:
		return zstd.SpeedDefault
	case level <= 10:
		return zstd.SpeedBetterCompression
	default:
		return zstd.SpeedBestCompression
	}
}

// mergeFileLabels unions the per-field label value sets from a slice of
// input files. Used by the compactor so the merged output file stays
// discoverable in the manifest's inverted label index — every value that
// any input file had on a given field gets registered against the output
// key. Returns nil when no input carries labels (the manifest's
// rebuildIndex / indexFileLabels both treat nil identically to "this
// file isn't indexed yet" — readers fall back to the per-row filter).
func mergeFileLabels(files []manifest.FileInfo) map[string][]string {
	merged := make(map[string]map[string]struct{})
	for _, f := range files {
		for field, values := range f.Labels {
			set, ok := merged[field]
			if !ok {
				set = make(map[string]struct{})
				merged[field] = set
			}
			for _, v := range values {
				set[v] = struct{}{}
			}
		}
	}
	if len(merged) == 0 {
		return nil
	}
	out := make(map[string][]string, len(merged))
	for field, set := range merged {
		vals := make([]string, 0, len(set))
		for v := range set {
			vals = append(vals, v)
		}
		out[field] = vals
	}
	return out
}

// bloomFilters builds SplitBlockFilter columns (10 bits/value ≈ 1% FPP) from the
// strict per-signal bloom set in internal/schema (cardinality-aligned: high-card
// equality-queried columns only).
func bloomFilters(cols []string) []parquet.BloomFilterColumn {
	bf := make([]parquet.BloomFilterColumn, 0, len(cols))
	for _, c := range cols {
		bf = append(bf, parquet.SplitBlockFilter(10, c))
	}
	return bf
}
