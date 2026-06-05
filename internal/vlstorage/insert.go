package vlstorage

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	otelpb "github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/opentelemetry"
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

type insertAdapter struct {
	writer LogWriter
}

// SetInsertStorage configures VL's vlinsert handlers to route all ingested
// rows through the given LogWriter. This is the insert-path counterpart of
// SetStorage (which handles the select path).
func SetInsertStorage(w LogWriter) {
	insertutil.SetLogRowsStorage(&insertAdapter{writer: w})
}

func (a *insertAdapter) MustAddRows(lr *logstorage.LogRows) {
	rows := logRowsToSchemaRows(lr)
	if len(rows) > 0 {
		a.writer.MustAddLogRows(rows)
	}
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

		if r.StreamTagsCanonical != "" {
			st := logstorage.GetStreamTags()
			if err := unmarshalStreamTags(st, r.StreamTagsCanonical); err == nil {
				row.Stream = strings.Clone(st.String())
			}
			logstorage.PutStreamTags(st)

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

		// Fall back to deriving SeverityText from severity_number when
		// the source row has the OTel numeric severity but no text
		// level (common with raw OTLP ingestion, stack traces, and the
		// datagen mix). VL hot exposes a derived `level` in this case;
		// without this fallback, LH cold queries return `level=""` for
		// the same rows and Grafana's log-volume chart shows them as
		// an "unknown" bucket.
		if row.SeverityText == "" && row.SeverityNumber > 0 {
			row.SeverityText = severityTextFromNumber(row.SeverityNumber)
		}

		// Final fallback: if the row's level field is still empty but
		// the stream tag carries `level=...`, lift the value to the
		// row-level SeverityText. VL hot promotes stream-label level
		// to a row field at query time; LH cold materializes rows
		// from parquet columns, so we must persist the lifted value
		// at write time instead. Catches the ingest paths (Loki API,
		// jsonline with stream-only level) that put level in the
		// stream tag but never as a row field.
		if row.SeverityText == "" && row.Stream != "" {
			if lvl := extractStreamTagLevel(row.Stream); lvl != "" {
				row.SeverityText = lvl
			}
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

// severityTextFromNumber maps an OTel severity_number to the canonical
// text label by delegating to VL upstream's exported FormatSeverity
// (added by patches/vl-logs/vl-export-severity.patch and
// patches/vl-traces/vl-export-severity.patch). The wrapper exists
// solely to filter the n=0 "Unspecified" case so a missing severity
// stays empty rather than being labeled — VL hot emits "Unspecified"
// at the proto layer because OTel requires a slot, but LH's row
// schema treats empty SeverityText as the canonical "no severity"
// signal that operators can grep for.
func severityTextFromNumber(n int32) string {
	if n < 1 || n > 24 {
		return ""
	}
	return otelpb.FormatSeverity(n)
}

// extractStreamTagLevel parses the canonical stream tag string for a
// `level="X"` tag and returns the value. Empty when the stream tag
// doesn't carry one. The canonical form is the VL StreamTags.String()
// output — `{key1="val1",key2="val2",...}`. A simple substring scan
// is enough because VL escapes embedded quotes on MarshalCanonical,
// so the first `"` after `level="` is the value terminator.
//
// Mirrors the level-from-stream-tag promotion VL hot does at query
// time. LH cold needs it at write time because the row's level
// column is what queries read, and the Loki API / jsonline ingest
// paths put `level` in the stream tag without also emitting it as
// a row field. Without this, 28% of cold rows (per measured ingest
// in the e2e stack) land with empty SeverityText and surface as
// Grafana's "unknown" log-volume bucket.
func extractStreamTagLevel(stream string) string {
	// Search for either `{level="` (first key) or `,level="` (later
	// key) to avoid false-positives on suffix-only matches like
	// `{not_level="X"}` or `{my.level="Y"}`. The canonical stream
	// tag format is fixed (VL StreamTags.MarshalCanonical) so the
	// preceding char is always one of these two.
	for _, prefix := range []string{`{level="`, `,level="`} {
		if i := strings.Index(stream, prefix); i >= 0 {
			rest := stream[i+len(prefix):]
			end := strings.IndexByte(rest, '"')
			if end < 0 {
				return ""
			}
			return strings.Clone(rest[:end])
		}
	}
	return ""
}
