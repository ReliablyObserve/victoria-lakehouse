package vlstorage

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
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

// keepStream is the admission rule of the traces binary, applied when a row is
// added to the insert buffer and again when it is flushed: a stream over its
// tenant's cardinality limit is dropped.
func keepStream(accountID, projectID uint32, stream string) bool {
	return globalCardinalityGate == nil || stream == "" ||
		globalCardinalityGate.AllowStream(accountID, projectID, stream)
}

// FlushRowKeeper returns the flusher's keep filter: the admission rule the
// insert path applies (keepStream), read live so it tracks SetCardinalityGate,
// and the drop of VictoriaTraces' trace_id_idx rows. Those rows stay in the
// insert buffer, where hot VictoriaTraces also returns them for unflushed
// data; they are never written to Parquet (only the _trace_idx footer is used
// in reads, never a Parquet row). service_graph rows are kept.
func FlushRowKeeper() func(accountID, projectID uint32, stream string) bool {
	return func(accountID, projectID uint32, stream string) bool {
		if strings.Contains(stream, otelpb.TraceIDIndexStreamName) {
			return false
		}
		return keepStream(accountID, projectID, stream)
	}
}

// BufferStore is the insert buffer: upstream logstorage, cut into segments by
// ingest time (membuffer.Segments). Every acknowledged row is added to it, and
// it is the only place the row lives until the flusher writes it to Parquet.
type BufferStore interface {
	MustAddRows(lr *logstorage.LogRows)
	IsReadOnly() bool
}

// vtInsertAdapter satisfies VT's insertutil.LogRowsStorage interface
// (MustAddRows + CanWriteData + IsLocalStorage).
type vtInsertAdapter struct {
	buf BufferStore
	dir string // the buffer directory, named in the read-only error
}

// SetInsertStorage routes every span VT's insert handlers parse into buf.
// dir is the buffer directory (insert.buffer_dir).
func SetInsertStorage(buf BufferStore, dir string) {
	vtinsertutil.SetLogRowsStorage(&vtInsertAdapter{buf: buf, dir: dir})
}

// MustAddRows adds the admitted rows of lr to the buffer while lr is valid
// (vtinsert reuses its memory after this returns; upstream copies the rows).
// A failure inside the buffer is not recovered here: as in upstream, it fails
// the request, so the client retries instead of getting an ack for rows that
// were stored nowhere.
func (a *vtInsertAdapter) MustAddRows(lr *logstorage.LogRows) {
	blr, owned := admittedRows(lr)
	if blr != nil {
		metrics.InsertRowsTotal.Add(blr.RowsCount())
		a.buf.MustAddRows(blr)
	}
	if owned {
		logstorage.PutLogRows(blr)
	}
}

// admittedRows returns the rows of lr that keepStream admits: lr itself when it
// admits them all (the common case), nil when it admits none, otherwise a copy
// (owned is then true and the caller releases it). The decision is made once
// per stream of the batch.
func admittedRows(lr *logstorage.LogRows) (out *logstorage.LogRows, owned bool) {
	if globalCardinalityGate == nil {
		return lr, false
	}
	decided := map[uint64]bool{}
	var st logstorage.StreamTags
	dropped, total := 0, 0
	lr.ForEachRow(func(streamHash uint64, r *logstorage.InsertRow) {
		total++
		keep, ok := decided[streamHash]
		if !ok {
			stream := ""
			if r.StreamTagsCanonical != "" {
				st.Reset()
				if err := unmarshalStreamTags(&st, r.StreamTagsCanonical); err == nil {
					stream = st.String()
				}
			}
			keep = keepStream(r.TenantID.AccountID, r.TenantID.ProjectID, stream)
			decided[streamHash] = keep
		}
		if !keep {
			dropped++
		}
	})
	switch dropped {
	case 0:
		return lr, false
	case total:
		return nil, false
	}
	cp := logstorage.GetLogRows(nil, nil, nil, nil, "")
	lr.ForEachRow(func(streamHash uint64, r *logstorage.InsertRow) {
		if decided[streamHash] {
			cp.MustAddInsertRow(r)
		}
	})
	return cp, true
}

// CanWriteData is upstream's rule (VictoriaLogs app/vlstorage
// Storage.CanWriteData): 429 Too Many Requests while the buffer's volume is
// below its free-space floor, and nothing else. An unreachable object store
// does not refuse writes: the rows wait on the local disk, as they would in
// VictoriaTraces' own storage, and the disk floor turns into 429 if it lasts.
func (a *vtInsertAdapter) CanWriteData() error {
	if a.buf.IsReadOnly() {
		metrics.InsertRejected.Inc("read_only")
		return &httpserver.ErrorWithStatusCode{
			Err: fmt.Errorf("cannot add rows into storage in read-only mode; the storage can be in read-only mode "+
				"because of lack of free disk space at insert.buffer_dir=%s", a.dir),
			StatusCode: http.StatusTooManyRequests,
		}
	}
	return nil
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

var activeSlotResolver *schema.SlotResolver

// SetSlotResolver installs the Tier-2 custom-attribute slot resolver.
func SetSlotResolver(r *schema.SlotResolver) { activeSlotResolver = r }

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
	// Dedicated columns — Tier 1 (resource-scope OTel). Routed to typed
	// columns; emitted on read under the same bare name (VL/VT-compatible).
	case "container.id":
		row.ContainerID = strings.Clone(value)
	case "service.instance.id":
		row.ServiceInstanceID = strings.Clone(value)
	case "k8s.cluster.name":
		row.K8sClusterName = strings.Clone(value)
	case "telemetry.sdk.name":
		row.TelemetrySDKName = strings.Clone(value)
	case "cloud.account.id":
		row.CloudAccountID = strings.Clone(value)
	default:
		if slot, ok := activeSlotResolver.SlotForName(key); ok {
			schema.SetTraceSlot(row, slot, strings.Clone(value))
			return
		}
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
	// Dedicated columns — Tier 1 (span-scope OTel). Routed to typed columns;
	// emitted on read under the same bare name (VL/VT-compatible).
	case "url.full":
		row.URLFull = strings.Clone(value)
	case "client.address":
		row.ClientAddress = strings.Clone(value)
	case "server.address":
		row.ServerAddress = strings.Clone(value)
	case "network.peer.address":
		row.NetworkPeerAddress = strings.Clone(value)
	case "db.collection.name":
		row.DBCollectionName = strings.Clone(value)
	case "db.operation.name":
		row.DBOperationName = strings.Clone(value)
	case "db.query.text":
		row.DBQueryText = strings.Clone(value)
	case "rpc.method":
		row.RPCMethod = strings.Clone(value)
	case "messaging.destination.name":
		row.MessagingDestination = strings.Clone(value)
	case "code.function.name":
		row.CodeFunctionName = strings.Clone(value)
	case "exception.type":
		row.ExceptionType = strings.Clone(value)
	default:
		if slot, ok := activeSlotResolver.SlotForName(key); ok {
			schema.SetTraceSlot(row, slot, strings.Clone(value))
			return
		}
		if row.SpanAttributes == nil {
			row.SpanAttributes = make(map[string]string)
		}
		row.SpanAttributes[strings.Clone(key)] = strings.Clone(value)
	}
}

// RepromoteTraceRow is RepromoteLogRow for trace rows — the compaction-time healing
// of v1 files written before a key was promoted. Re-derives dedicated columns
// (Tier-1 OTel) and Tier-2 custom slots from BOTH attribute maps (resource + span)
// by re-routing each entry through the SAME ingest mappers used at write time, so
// semantics are identical. Promoted/slotted keys land in their typed column and leave
// the map; non-promoted keys rebuild the map. Idempotent for v2 rows; the empty key
// is preserved in its map rather than routed.
func RepromoteTraceRow(r *schema.TraceRow) {
	if len(r.ResourceAttributes) > 0 {
		attrs := r.ResourceAttributes
		r.ResourceAttributes = nil
		for k, v := range attrs {
			if k == "" {
				if r.ResourceAttributes == nil {
					r.ResourceAttributes = make(map[string]string)
				}
				r.ResourceAttributes[k] = v
				continue
			}
			mapResourceAttr(r, k, v)
		}
	}
	if len(r.SpanAttributes) > 0 {
		attrs := r.SpanAttributes
		r.SpanAttributes = nil
		for k, v := range attrs {
			if k == "" {
				if r.SpanAttributes == nil {
					r.SpanAttributes = make(map[string]string)
				}
				r.SpanAttributes[k] = v
				continue
			}
			mapSpanAttr(r, k, v)
		}
	}
}
