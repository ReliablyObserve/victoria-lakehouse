package vlstorage

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// LogWriter is the subset of parquets3.Storage needed for the insert path.
type LogWriter interface {
	MustAddLogRows(rows []schema.LogRow)
	CanWriteData() error
}

// TenantCardinalityGate decides whether a (tenant, stream) row may be
// admitted. Implemented by *tenant.CardinalityLimiter; declared here as
// an interface so this package stays import-light.
type TenantCardinalityGate interface {
	AllowStream(accountID, projectID uint32, stream string) bool
}

var globalCardinalityGate TenantCardinalityGate

// SetCardinalityGate installs the per-tenant cardinality limiter the
// insert path consults before admitting a row. nil disables the check.
// Safe to call at any time; reads are mediated through a package
// variable rather than a per-adapter field to keep the existing
// SetInsertStorage signature stable.
func SetCardinalityGate(g TenantCardinalityGate) {
	globalCardinalityGate = g
}

// FlushRowKeeper returns the gate-at-flush predicate for the WAL cutover's
// BufferFlusher: it keeps exactly the rows the legacy authoritative path would
// have written to Parquet — i.e. streams within the per-tenant cardinality
// limit. (Logs have no VT-internal trace_id_idx rows; that drop is traces-only.)
// The gate is read live so it tracks SetCardinalityGate.
func FlushRowKeeper() func(accountID, projectID uint32, stream string) bool {
	return func(accountID, projectID uint32, stream string) bool {
		if globalCardinalityGate != nil && stream != "" &&
			!globalCardinalityGate.AllowStream(accountID, projectID, stream) {
			return false
		}
		return true
	}
}

type insertAdapter struct {
	writer LogWriter
}

// SetInsertStorage configures VL's vlinsert handlers to route all ingested
// rows through the given LogWriter. This is the insert-path counterpart of
// SetStorage (which handles the select path).
func SetInsertStorage(w LogWriter) {
	insertutil.SetLogRowsStorage(&insertAdapter{writer: w})
}

// BufferStore is the narrow write surface of the Option B logstorage-native
// buffer (membuffer.Store). nil unless BufferEngine=="logstore".
type BufferStore interface {
	MustAddRows(lr *logstorage.LogRows)
}

var bufferStore BufferStore

// bufferAuthoritative is the WAL-cutover FLIP. When true the logstore buffer is
// the authoritative Parquet producer (via the BufferFlusher), so MustAddRows
// feeds ONLY the buffer and SKIPS the legacy LogRow staging path — no double
// Parquet, no LH WAL. When false (default) the legacy path is authoritative and
// the buffer is a read-side shadow (dual-write). Reversible: flip the flag.
var bufferAuthoritative bool

// SetBufferStore enables Option B dual-write: every ingested LogRows batch is
// ALSO added to the logstorage-native buffer, alongside the legacy LogRow
// staging path. Call once at startup before serving. nil disables.
func SetBufferStore(bs BufferStore) {
	bufferStore = bs
}

// SetBufferAuthoritative performs the cutover flip (true) or reverts it (false).
// Must be called before serving; when true, a BufferFlusher must be running to
// produce Parquet from the buffer, else recent data never reaches S3.
func SetBufferAuthoritative(v bool) {
	bufferAuthoritative = v
}

func (a *insertAdapter) MustAddRows(lr *logstorage.LogRows) {
	// Legacy staging path — SKIPPED once the buffer is authoritative (the flip),
	// so the BufferFlusher is the sole Parquet producer (no double-write, no WAL).
	if !bufferAuthoritative {
		rows := logRowsToSchemaRows(lr)
		if len(rows) > 0 {
			a.writer.MustAddLogRows(rows)
		}
	}
	// Feed the logstorage-native buffer (dual-write shadow when legacy is
	// authoritative; the sole sink once flipped), while lr is still valid —
	// vlinsert resets the arena-backed LogRows immediately after this returns;
	// logstorage copies the rows into its own parts, so this is safe.
	if bufferStore != nil {
		addRowsToBufferSafely(lr)
	}
}

// addRowsToBufferSafely isolates the Option B dual-write so a buffer failure
// (panic in logstorage.MustAddRows) can NEVER break ingestion. The legacy
// staging path above already accepted the rows and stays authoritative; on
// failure we only count it and drop this batch from the buffer.
func addRowsToBufferSafely(lr *logstorage.LogRows) {
	defer func() {
		if r := recover(); r != nil {
			metrics.BufferStoreDualWriteFailures.Inc()
		}
	}()
	bufferStore.MustAddRows(lr)
}

func (a *insertAdapter) CanWriteData() error {
	return a.writer.CanWriteData()
}

// logRowsToSchemaRows converts VL's LogRows into our Parquet schema rows.
// VL has already parsed all protocols, extracted timestamps, built stream
// tags, and normalized field names — we only map fields to columns.
//
// IMPORTANT: All string values are cloned via strings.Clone because VL uses
// arena-allocated unsafe strings that become invalid after ResetKeepSettings()
// is called (immediately after MustAddRows returns). Since our writer buffers
// rows asynchronously, we must own the string memory.
func logRowsToSchemaRows(lr *logstorage.LogRows) []schema.LogRow {
	n := lr.RowsCount()
	if n == 0 {
		return nil
	}

	rows := make([]schema.LogRow, 0, n)

	lr.ForEachRow(func(_ uint64, r *logstorage.InsertRow) {
		row := schema.LogRow{
			AccountID:         r.TenantID.AccountID,
			ProjectID:         r.TenantID.ProjectID,
			TimestampUnixNano: r.Timestamp,
		}

		// stPooled is held across the field-mapping loop so the post-
		// loop severity derivation can read the parsed stream tags
		// without re-unmarshaling. Released to VL's pool right before
		// the row is appended; nil for rows that have no stream tag.
		var stPooled *logstorage.StreamTags

		if r.StreamTagsCanonical != "" {
			stPooled = logstorage.GetStreamTags()
			if err := unmarshalStreamTags(stPooled, r.StreamTagsCanonical); err == nil {
				row.Stream = strings.Clone(stPooled.String())
			} else {
				logstorage.PutStreamTags(stPooled)
				stPooled = nil
			}

			// Compute _stream_id deterministically from (TenantID,
			// StreamTagsCanonical) using VL's own hash algorithm.
			// VL's hot path computes this internally; LH's cold path
			// must produce the same value so /select/logsql/stream_ids
			// returns identical results.
			row.StreamID = computeStreamID(r.TenantID, r.StreamTagsCanonical)
		}

		// Ingest-side trace-shape filter: drop rows whose stream
		// would mark them as VT span / service-graph data rather
		// than logs. This is the write-side counterpart of the
		// read-side preFilter in storage_query.go — without it,
		// trace-shaped rows still land in parquet files, inflate
		// the manifest RowCount (which the manifestFastPath uses
		// to answer `* | stats count()` queries), and the
		// resulting count is bloated by ~50% in clusters where
		// some upstream pipeline misroutes span data to the logs
		// ingest path. The read-side filter still runs on every
		// query for historical files that were written before
		// this gate was in place. New rows after this commit
		// won't be persisted and the count drift stops growing.
		if storage.IsTraceShapedStream(row.Stream) {
			metrics.LogsTraceShapedRowsDroppedAtIngest.Inc()
			return
		}

		// Per-tenant cardinality gate. Stream uniqueness is keyed by
		// the canonical stream tags (what VL itself hashes for the
		// stream ID). Rows beyond a tenant's MaxStreams cap drop here
		// and the limiter increments its rejected counter.
		if globalCardinalityGate != nil && r.StreamTagsCanonical != "" {
			if !globalCardinalityGate.AllowStream(r.TenantID.AccountID, r.TenantID.ProjectID, r.StreamTagsCanonical) {
				return
			}
		}

		for _, f := range r.Fields {
			mapFieldToRow(&row, f.Name, f.Value)
		}

		// Single derivation step shared with the compactor's
		// backfill path. Walks the precedence chain (explicit
		// SeverityText → derived from severity_number → lifted
		// from stream-tag `level`) using VL upstream helpers. Empty
		// when no source has a level — leaves SeverityText as the
		// canonical "no severity" signal rather than substituting
		// a fake "Unspecified".
		row.SeverityText = schema.DeriveSeverityText(row.SeverityText, row.SeverityNumber, stPooled)

		if stPooled != nil {
			logstorage.PutStreamTags(stPooled)
		}

		rows = append(rows, row)
	})

	return rows
}

// unmarshalStreamTags unmarshals canonical stream tags into dst.
func unmarshalStreamTags(dst *logstorage.StreamTags, canonical string) error {
	src := []byte(canonical)
	tail, err := dst.UnmarshalCanonicalInplace(src)
	if err != nil {
		return err
	}
	if len(tail) > 0 {
		return fmt.Errorf("unexpected trailing data in stream tags: %d bytes", len(tail))
	}
	return nil
}

// mapFieldToRow maps a single VL field to the appropriate schema.LogRow column.
// Empty field name is VL's canonical form for _msg.
// All stored values are cloned to detach from VL's arena memory.
func mapFieldToRow(row *schema.LogRow, name, value string) {
	switch name {
	case "":
		row.Body = strings.Clone(value)
	case "level", "severity_text":
		// VL upstream's OTLP handler emits the field as
		// `severity_text` (see deps/VictoriaLogs/app/vlinsert/
		// opentelemetry/pb.go:340 `fs.Add("severity_text", ...)`).
		// The non-OTLP path emits `level`. Both name the same
		// concept; map to SeverityText so OTLP-ingested rows
		// don't fall into the "unknown level" bucket in Grafana.
		row.SeverityText = strings.Clone(value)
	case "severity_number":
		if v, err := strconv.ParseInt(value, 10, 32); err == nil {
			row.SeverityNumber = int32(v)
		}
	case "service.name":
		row.ServiceName = strings.Clone(value)
	case "trace_id":
		row.TraceID = strings.Clone(value)
	case "span_id":
		row.SpanID = strings.Clone(value)
	case "k8s.namespace.name":
		row.K8sNamespaceName = strings.Clone(value)
	case "k8s.pod.name":
		row.K8sPodName = strings.Clone(value)
	case "k8s.deployment.name":
		row.K8sDeploymentName = strings.Clone(value)
	case "k8s.node.name":
		row.K8sNodeName = strings.Clone(value)
	case "deployment.environment":
		row.DeployEnv = strings.Clone(value)
	case "cloud.region":
		row.CloudRegion = strings.Clone(value)
	case "host.name":
		row.HostName = strings.Clone(value)
	case "scope.name":
		row.ScopeName = strings.Clone(value)
	default:
		if row.LogAttributes == nil {
			row.LogAttributes = make(map[string]string)
		}
		row.LogAttributes[strings.Clone(name)] = strings.Clone(value)
	}
}
