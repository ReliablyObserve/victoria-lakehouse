package vlstorage

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	vtinsertutil "github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/insertutil"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// VT-internal row kinds reported by vtInternalRowKind. Used as the "kind"
// label of metrics.VTInternalRowsDropped so an operator can tell which of
// VT's internal streams is being discarded by the cold-tier insert path.
const (
	vtInternalKindTraceIDIdx   = "trace_id_idx"
	vtInternalKindServiceGraph = "service_graph"
)

// TraceWriter is the subset of parquets3.Storage needed for the insert path.
type TraceWriter interface {
	MustAddTraceRows(rows []schema.TraceRow)
	CanWriteData() error
}

// TenantCardinalityGate gates rows by per-tenant cardinality limits.
// Implemented by *tenant.CardinalityLimiter; declared here as an
// interface to keep this package's imports narrow.
type TenantCardinalityGate interface {
	AllowStream(accountID, projectID uint32, stream string) bool
}

var globalCardinalityGate TenantCardinalityGate

// SetCardinalityGate installs the per-tenant cardinality limiter the
// insert path consults before admitting a trace row. nil disables.
func SetCardinalityGate(g TenantCardinalityGate) {
	globalCardinalityGate = g
}

// vtInsertAdapter satisfies VT's insertutil.LogRowsStorage interface
// (MustAddRows + CanWriteData + IsLocalStorage).
type vtInsertAdapter struct {
	writer TraceWriter
}

// SetInsertStorage configures VT's vtinsert handler to route all ingested
// trace spans through the given TraceWriter.
func SetInsertStorage(w TraceWriter) {
	vtinsertutil.SetLogRowsStorage(&vtInsertAdapter{writer: w})
}

// BufferStore is the narrow write surface of the Option B logstorage-native
// buffer (membuffer.Store). Declared here as an interface to keep this
// package's imports narrow. nil unless BufferEngine=="logstore".
type BufferStore interface {
	MustAddRows(lr *logstorage.LogRows)
}

var bufferStore BufferStore

// SetBufferStore enables Option B dual-write: every ingested LogRows batch is
// ALSO added to the logstorage-native buffer, in parallel with the legacy
// TraceRow staging path. Call once at startup before serving. nil disables.
func SetBufferStore(bs BufferStore) {
	bufferStore = bs
}

func (a *vtInsertAdapter) MustAddRows(lr *logstorage.LogRows) {
	// Legacy path first so it stays byte-identical to today.
	rows := logRowsToTraceRows(lr)
	if len(rows) > 0 {
		a.writer.MustAddTraceRows(rows)
	}
	// Option B (P1 dual-write): feed the same canonical LogRows to the
	// logstorage-native buffer, while lr is still valid — vtinsert resets the
	// arena-backed LogRows immediately after this returns; logstorage copies
	// the rows into its own parts, so this is safe.
	if bufferStore != nil {
		addRowsToBufferSafely(lr)
	}
}

// addRowsToBufferSafely isolates the Option B dual-write so a buffer failure
// (panic in logstorage.MustAddRows, e.g. unexpected internal state) can NEVER
// break ingestion. The legacy staging path above already accepted the rows and
// stays authoritative; on failure we only count it and drop this batch from the
// buffer (the buffer may under-return for those rows until the next flush).
func addRowsToBufferSafely(lr *logstorage.LogRows) {
	defer func() {
		if r := recover(); r != nil {
			metrics.BufferStoreDualWriteFailures.Inc()
		}
	}()
	bufferStore.MustAddRows(lr)
}

func (a *vtInsertAdapter) CanWriteData() error {
	return a.writer.CanWriteData()
}

func (a *vtInsertAdapter) IsLocalStorage() bool {
	return true
}

// logRowsToTraceRows converts VL's LogRows into trace schema rows.
// VL handles all protocol parsing — we map fields to TraceRow columns.
//
// IMPORTANT: All string values are cloned via strings.Clone because VL uses
// arena-allocated unsafe strings that become invalid after ResetKeepSettings()
// is called (immediately after MustAddRows returns). Since our writer buffers
// rows asynchronously, we must own the string memory.
func logRowsToTraceRows(lr *logstorage.LogRows) []schema.TraceRow {
	n := lr.RowsCount()
	if n == 0 {
		return nil
	}

	rows := make([]schema.TraceRow, 0, n)

	lr.ForEachRow(func(_ uint64, r *logstorage.InsertRow) {
		// Detect VT-internal stream rows. trace_id_idx drops (we
		// have a smaller cold-tier index in `_trace_idx` footer KV);
		// service_graph rows pass through to the writer so the
		// upstream `/select/jaeger/api/dependencies` reader works
		// unchanged. The metric counter still ticks for both kinds
		// so the parity check's expected_drift accounts for what
		// the writer dropped.
		if kind, drop := vtInternalRowKind(r); drop {
			metrics.VTInternalRowsDropped.Inc(kind)
			return
		}

		row := schema.TraceRow{
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

			// Mirror VL/VT's stream-ID computation so /select/jaeger and
			// /select/logsql/stream_ids return the same value VT would for
			// the equivalent insert. Required by the 100% VL/VT API
			// compatibility rule.
			row.StreamID = computeStreamID(r.TenantID, r.StreamTagsCanonical)
		}

		if globalCardinalityGate != nil && r.StreamTagsCanonical != "" {
			if !globalCardinalityGate.AllowStream(r.TenantID.AccountID, r.TenantID.ProjectID, r.StreamTagsCanonical) {
				return
			}
		}

		for _, f := range r.Fields {
			mapFieldToTraceRow(&row, f.Name, f.Value)
		}

		rows = append(rows, row)
	})

	return rows
}

// vtInternalRowKind classifies VT-internal index entries (trace_id_idx,
// service-graph) that the writer treats specially. Returns the metric
// "kind" label for the detected stream, or "" for normal spans.
//
// Drop policy (per kind):
//
//   - trace_id_idx: DROP. VT's hot-tier trace-by-ID index is high
//     cardinality (one row per trace_id per partition bucket) and
//     we replace it with our `_trace_idx` Parquet footer KV — much
//     smaller, single-file lookup. Persisting the upstream index
//     rows would 10–100× our cold-tier row count for no read win.
//
//   - service_graph: KEEP. These are LOW-cardinality aggregate rows
//     emitted by VT's `servicegraph` background task (bounded by
//     services² × time bucket, not per-trace), and the
//     `/select/jaeger/api/dependencies` reader expects to find them
//     in storage via {trace_service_graph_stream="-"} | stats by
//     (parent,child) sum(callCount). Dropping them silently breaks
//     Grafana's Service Graph view; persisting them lets the
//     upstream task + reader work unchanged.
//
// Caller still receives a non-empty kind for service_graph rows so
// metrics.VTInternalRowsDropped's "kind" label can record activity
// without us actually dropping anything; the writer checks the
// boolean returned to decide whether to skip the row.
func vtInternalRowKind(r *logstorage.InsertRow) (kind string, drop bool) {
	for _, f := range r.Fields {
		switch f.Name {
		case otelpb.TraceIDIndexFieldName, otelpb.TraceIDIndexStreamName:
			return vtInternalKindTraceIDIdx, true
		case otelpb.ServiceGraphStreamName:
			return vtInternalKindServiceGraph, false
		}
	}
	return "", false
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

// mapFieldToTraceRow maps a VL/VT field to the appropriate TraceRow column.
// Handles both VT's prefixed naming (resource_attr:, span_attr:) and VL's
// flat naming from jsonline ingestion.
// All stored string values are cloned to detach from VL's arena memory.
//
//nolint:gocyclo // field-routing switch is inherently branchy but readable
func mapFieldToTraceRow(row *schema.TraceRow, name, value string) {
	// VT OTLP trace fields (from vtinsert/opentelemetry)
	switch name {
	case otelpb.TraceIDField:
		row.TraceID = strings.Clone(value)
		return
	case otelpb.SpanIDField:
		row.SpanID = strings.Clone(value)
		return
	case otelpb.ParentSpanIDField:
		row.ParentSpanID = strings.Clone(value)
		return
	case otelpb.NameField:
		row.SpanName = strings.Clone(value)
		return
	case otelpb.KindField:
		if v, err := strconv.ParseInt(value, 10, 32); err == nil {
			row.SpanKind = int32(v)
		}
		return
	case otelpb.DurationField:
		if v, err := strconv.ParseInt(value, 10, 64); err == nil {
			row.DurationNs = v
		}
		return
	case otelpb.StartTimeUnixNanoField:
		if v, err := strconv.ParseInt(value, 10, 64); err == nil {
			row.StartTimeUnixNano = v
		}
		storeSpanAttr(row, strings.Clone(name), strings.Clone(value))
		return
	case otelpb.StatusCodeField:
		if v, err := strconv.ParseInt(value, 10, 32); err == nil {
			row.StatusCode = int32(v)
		}
		return
	case otelpb.StatusMessageField:
		row.StatusMessage = strings.Clone(value)
		return
	case otelpb.InstrumentationScopeName:
		row.ScopeName = strings.Clone(value)
		return

	// OTLP metadata fields: store in span attributes for VT field parity.
	// VT stores these as regular LogRow fields; LH preserves them in the map
	// so field_names/field_values/query responses match VT.
	case otelpb.EndTimeUnixNanoField,
		otelpb.TraceStateField, otelpb.FlagsField,
		otelpb.DroppedAttributesCountField, otelpb.DroppedEventsCountField, otelpb.DroppedLinksCountField,
		otelpb.InstrumentationScopeVersion:
		storeSpanAttr(row, strings.Clone(name), strings.Clone(value))
		return

	// VT-internal trace-ID index fields: replaced by our `_trace_idx`
	// footer KV (see internal/traceindex), so skip entirely here.
	case otelpb.TraceIDIndexFieldName, otelpb.TraceIDIndexStreamName,
		otelpb.TraceIDIndexStartTimeFieldName, otelpb.TraceIDIndexEndTimeFieldName:
		return

	// Service-graph stream tag: marker only, the row carries no data here.
	case otelpb.ServiceGraphStreamName:
		return

	// Service-graph edge payload: route to dedicated TraceRow columns so
	// the upstream Jaeger Dependencies reader's
	// `{trace_service_graph_stream="-"} | fields parent, child,
	// callCount | stats by (parent, child) sum(callCount)` query can
	// project them as top-level fields. Storing them only in
	// SpanAttributes would not surface them as top-level fields and
	// the reader would return zero edges.
	case otelpb.ServiceGraphParentFieldName:
		row.ServiceGraphParent = strings.Clone(value)
		return
	case otelpb.ServiceGraphChildFieldName:
		row.ServiceGraphChild = strings.Clone(value)
		return
	case otelpb.ServiceGraphCallCountFieldName:
		row.ServiceGraphCallCount = strings.Clone(value)
		return
	}

	// VT resource attributes (resource_attr:key)
	if strings.HasPrefix(name, otelpb.ResourceAttrPrefix) {
		key := strings.TrimPrefix(name, otelpb.ResourceAttrPrefix)
		mapResourceAttr(row, key, value)
		return
	}

	// VT span attributes (span_attr:key)
	if strings.HasPrefix(name, otelpb.SpanAttrPrefixField) {
		key := strings.TrimPrefix(name, otelpb.SpanAttrPrefixField)
		mapSpanAttr(row, key, value)
		return
	}

	// VT scope attributes, events, links — ignored
	if strings.HasPrefix(name, otelpb.InstrumentationScopeAttrPrefix) ||
		strings.HasPrefix(name, otelpb.EventPrefix) ||
		strings.HasPrefix(name, otelpb.LinkPrefix) {
		return
	}

	// Legacy flat field names (from jsonline insert path)
	switch name {
	case "":
		return
	case "_msg":
		storeSpanAttr(row, "_msg", strings.Clone(value))
		return
	case "trace_id":
		row.TraceID = strings.Clone(value)
	case "span_id":
		row.SpanID = strings.Clone(value)
	case "parent_span_id":
		row.ParentSpanID = strings.Clone(value)
	case "span.name":
		row.SpanName = strings.Clone(value)
	case "service.name":
		row.ServiceName = strings.Clone(value)
	case "duration_ns":
		if v, err := strconv.ParseInt(value, 10, 64); err == nil {
			row.DurationNs = v
		}
	case "start_time_unix_nano":
		if v, err := strconv.ParseInt(value, 10, 64); err == nil {
			row.StartTimeUnixNano = v
		}
	case "status.code":
		if v, err := strconv.ParseInt(value, 10, 32); err == nil {
			row.StatusCode = int32(v)
		}
	case "status.message":
		row.StatusMessage = strings.Clone(value)
	case "span.kind":
		if v, err := strconv.ParseInt(value, 10, 32); err == nil {
			row.SpanKind = int32(v)
		}
	case "scope.name":
		row.ScopeName = strings.Clone(value)
	case "http.method":
		row.HTTPMethod = strings.Clone(value)
	case "http.status_code":
		row.HTTPStatusCode = strings.Clone(value)
	case "http.url":
		row.HTTPUrl = strings.Clone(value)
	case "db.system":
		row.DBSystem = strings.Clone(value)
	case "db.statement":
		row.DBStatement = strings.Clone(value)
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
	default:
		if row.SpanAttributes == nil {
			row.SpanAttributes = make(map[string]string)
		}
		row.SpanAttributes[strings.Clone(name)] = strings.Clone(value)
	}
}

func storeSpanAttr(row *schema.TraceRow, key, value string) {
	if row.SpanAttributes == nil {
		row.SpanAttributes = make(map[string]string)
	}
	row.SpanAttributes[key] = value
}

func mapResourceAttr(row *schema.TraceRow, key, value string) {
	switch key {
	case "service.name":
		row.ServiceName = strings.Clone(value)
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
	default:
		if row.ResourceAttributes == nil {
			row.ResourceAttributes = make(map[string]string)
		}
		row.ResourceAttributes[strings.Clone(key)] = strings.Clone(value)
	}
}

func mapSpanAttr(row *schema.TraceRow, key, value string) {
	switch key {
	case "http.method":
		row.HTTPMethod = strings.Clone(value)
	case "http.status_code":
		row.HTTPStatusCode = strings.Clone(value)
	case "http.url":
		row.HTTPUrl = strings.Clone(value)
	case "db.system":
		row.DBSystem = strings.Clone(value)
	case "db.statement":
		row.DBStatement = strings.Clone(value)
	default:
		if row.SpanAttributes == nil {
			row.SpanAttributes = make(map[string]string)
		}
		row.SpanAttributes[strings.Clone(key)] = strings.Clone(value)
	}
}
