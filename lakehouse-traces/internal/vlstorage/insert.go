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
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// vtInternalKindTraceIDIdx is the "kind" label of metrics.VTInternalRowsDropped
// for VictoriaTraces' trace-ID index rows, which the flush drops.
const vtInternalKindTraceIDIdx = "trace_id_idx"

// The kinds of metrics.VTInternalRowsDropped for trace_id_idx rows the insert
// path refuses when they are added: one whose _time (the span's start time) is
// before the Unix epoch, and one of a stream over the tenant's cardinality
// limit. They are not spans, so lakehouse_insert_rows_dropped_total does not
// count them.
const (
	vtInternalKindTraceIDIdxBeforeEpoch = "trace_id_idx_before_epoch"
	vtInternalKindTraceIDIdxCardinality = "trace_id_idx_cardinality_limit"
)

func init() {
	// Exported at 0 from process start, so increase() sees the first drop.
	metrics.VTInternalRowsDropped.Init(vtInternalKindTraceIDIdx, vtInternalKindTraceIDIdxBeforeEpoch, vtInternalKindTraceIDIdxCardinality)
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

// keepStream is the admission rule of the traces binary for a stream, applied
// once, when a row is added to the insert buffer: a stream over its tenant's
// cardinality limit is dropped. The limiter is per process and starts empty, so
// the decision cannot be repeated later: the flusher uses FlushRowKeeper, which
// holds only the part that does not depend on state.
func keepStream(accountID, projectID uint32, stream string) bool {
	return globalCardinalityGate == nil || stream == "" ||
		globalCardinalityGate.AllowStream(accountID, projectID, stream)
}

// FlushRowKeeper returns the flusher's keep filter. A row in the insert buffer
// was admitted when it was added and acknowledged, so the flusher never judges
// it by a limit again: the tenant cardinality gate is process-local and empty
// after a restart, and applying it here dropped acknowledged rows (#389). The
// one drop that remains does not depend on state: VictoriaTraces' trace_id_idx
// rows stay in the insert buffer, where hot VictoriaTraces also returns them for
// unflushed data; they are never written to Parquet (only the _trace_idx footer
// is used in reads, never a Parquet row); each one dropped is counted in
// lakehouse_vt_internal_rows_dropped_total{kind="trace_id_idx"}, which the
// parity check uses to explain the difference from hot VictoriaTraces.
// service_graph rows are kept.
func FlushRowKeeper() func(accountID, projectID uint32, stream string) bool {
	return func(_, _ uint32, stream string) bool {
		if isTraceIDIndexRow(stream) {
			metrics.VTInternalRowsDropped.Inc(vtInternalKindTraceIDIdx)
			return false
		}
		return true
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
		metrics.InsertRowsTotal.Add(spanRows(blr))
		a.buf.MustAddRows(blr)
	}
	if owned {
		logstorage.PutLogRows(blr)
	}
}

// spanRows counts the rows of lr that are spans: VictoriaTraces' trace-ID index
// rows (its trace_id_idx_stream) are kept in the buffer like upstream keeps
// them, but they are not written to Parquet, and lakehouse_insert_rows_total
// counts what the Lakehouse stores.
func spanRows(lr *logstorage.LogRows) int {
	n := 0
	lr.ForEachRow(func(_ uint64, r *logstorage.InsertRow) {
		if !isTraceIDIndexRow(r.StreamTagsCanonical) {
			n++
		}
	})
	return n
}

// streamKey is what an admission decision is made for: a stream of one tenant.
// Upstream's stream hash is computed from the stream tags alone, without the
// tenant, and a batch can carry several tenants (/insert/multitenant/native), so
// the hash alone would let one tenant's verdict (and its slot of the cardinality
// limit) stand for another's.
type streamKey struct {
	tenant logstorage.TenantID
	hash   uint64
}

// admittedRows returns the rows of lr that the admission rule admits: lr itself
// when it admits them all (the common case), nil when it admits none, otherwise
// a copy (owned is then true and the caller releases it). The rule has two
// parts, and both are decided here, when the row is added, never again when it
// is flushed: a row before the Unix epoch is refused (storage.BeforeEpoch), and
// keepStream decides the stream, once per stream of the batch. A refused row
// does not take a slot of its tenant's cardinality limit.
//
// VictoriaTraces stamps a span with its END time and the trace's trace_id_idx
// row with the trace's START time. OTLP times are unsigned and VictoriaTraces
// drops, before this adapter, a span whose start or end is above 2^63-1 ns (it
// cannot index it), so over OTLP no row arrives with a negative _time; through
// /insert/native the sender sets the timestamp and one can. A span can then be
// admitted while its index row (stamped with a different time) is refused, or the
// reverse. Refused spans are counted in lakehouse_insert_rows_dropped_total;
// refused index rows, which are not spans, in
// lakehouse_vt_internal_rows_dropped_total.
func admittedRows(lr *logstorage.LogRows) (out *logstorage.LogRows, owned bool) {
	// refusal collects what a stream refused for the limit needs for its counter
	// and its log line; the map exists only when a stream is refused.
	type refusal struct {
		tenant logstorage.TenantID
		stream string
		idx    bool // the stream of VictoriaTraces' trace_id_idx rows, not of spans
		rows   int
	}
	var decided map[streamKey]bool // allocated when a gate is installed and a stream is first seen
	var refused map[streamKey]*refusal
	var st logstorage.StreamTags
	dropped, total := 0, 0
	lr.ForEachRow(func(streamHash uint64, r *logstorage.InsertRow) {
		total++
		if storage.BeforeEpoch(r.Timestamp) {
			dropped++
			if isTraceIDIndexRow(r.StreamTagsCanonical) {
				// An index row carries the trace's start time. It is internal, not a
				// span: counted apart, so the span counters stay span counts.
				metrics.VTInternalRowsDropped.Inc(vtInternalKindTraceIDIdxBeforeEpoch)
			} else {
				storage.NoteBeforeEpoch(r)
			}
			return
		}
		if globalCardinalityGate == nil {
			return
		}
		key := streamKey{r.TenantID, streamHash}
		keep, ok := decided[key]
		if !ok {
			stream := ""
			if r.StreamTagsCanonical != "" {
				st.Reset()
				if err := unmarshalStreamTags(&st, r.StreamTagsCanonical); err == nil {
					stream = st.String()
				}
			}
			keep = keepStream(r.TenantID.AccountID, r.TenantID.ProjectID, stream)
			if decided == nil {
				decided = map[streamKey]bool{}
			}
			decided[key] = keep
			if !keep {
				if refused == nil {
					refused = map[streamKey]*refusal{}
				}
				refused[key] = &refusal{tenant: r.TenantID, stream: stream, idx: isTraceIDIndexRow(r.StreamTagsCanonical)}
			}
		}
		if !keep {
			dropped++
			refused[key].rows++
		}
	})
	// Every row refused for the limit is counted, once per batch.
	for _, x := range refused {
		if x.idx {
			metrics.VTInternalRowsDropped.Add(vtInternalKindTraceIDIdxCardinality, x.rows)
		} else {
			noteCardinalityRefused(x.tenant.AccountID, x.tenant.ProjectID, x.stream, x.rows)
		}
	}
	switch dropped {
	case 0:
		return lr, false
	case total:
		return nil, false
	}
	cp := logstorage.GetLogRows(nil, nil, nil, nil, "")
	lr.ForEachRow(func(streamHash uint64, r *logstorage.InsertRow) {
		if storage.BeforeEpoch(r.Timestamp) {
			return
		}
		if keep, ok := decided[streamKey{r.TenantID, streamHash}]; !ok || keep {
			cp.MustAddInsertRow(r)
		}
	})
	return cp, true
}

// isTraceIDIndexRow reports whether a row's canonical stream tags name the
// stream of VictoriaTraces' trace_id_idx rows.
func isTraceIDIndexRow(streamTagsCanonical string) bool {
	return strings.Contains(streamTagsCanonical, otelpb.TraceIDIndexStreamName)
}

// noteCardinalityRefused counts and logs the rows of a stream refused for the
// tenant's cardinality limit; a variable so the tests can see which tenant and
// stream each refusal is attributed to.
var noteCardinalityRefused = storage.NoteCardinalityRefused

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
			row.SpanKind = schema.Int32Ptr(int32(v))
		}
		return
	case otelpb.DurationField:
		if v, err := strconv.ParseInt(value, 10, 64); err == nil {
			row.DurationNs = schema.Int64Ptr(v)
		}
		return
	case otelpb.StartTimeUnixNanoField:
		if v, err := strconv.ParseInt(value, 10, 64); err == nil {
			row.StartTimeUnixNano = schema.Int64Ptr(v)
		}
		storeSpanAttr(row, strings.Clone(name), strings.Clone(value))
		return
	case otelpb.StatusCodeField:
		if v, err := strconv.ParseInt(value, 10, 32); err == nil {
			row.StatusCode = schema.Int32Ptr(int32(v))
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

	// VT instrumentation-scope attributes: the scope.attributes map column,
	// which the cold read path already emits back as `scope_attr:<key>`.
	if key, ok := strings.CutPrefix(name, otelpb.InstrumentationScopeAttrPrefix); ok {
		if key == "" {
			return
		}
		if row.ScopeAttributes == nil {
			row.ScopeAttributes = make(map[string]string)
		}
		row.ScopeAttributes[strings.Clone(key)] = strings.Clone(value)
		return
	}

	// VT span events and links carry one field per sub-field AND per index
	// (`event:event_name:0`, ...). A field alone cannot be placed, so they are
	// not mapped here: DataBlockToTraceRows hands them to a per-row
	// schema.SpanSubFieldCollector, which writes the span.events_json and
	// span.links_json columns. A caller that maps single fields drops them.
	if strings.HasPrefix(name, otelpb.EventPrefix) || strings.HasPrefix(name, otelpb.LinkPrefix) {
		return
	}

	// Legacy flat field names (from jsonline insert path)
	switch name {
	case "", "_msg":
		// LogRows canonicalizes the native _msg name to an empty string.
		row.Body = strings.Clone(value)
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
			row.DurationNs = schema.Int64Ptr(v)
		}
	case "start_time_unix_nano":
		if v, err := strconv.ParseInt(value, 10, 64); err == nil {
			row.StartTimeUnixNano = schema.Int64Ptr(v)
		}
	case "status.code":
		if v, err := strconv.ParseInt(value, 10, 32); err == nil {
			row.StatusCode = schema.Int32Ptr(int32(v))
		}
	case "status.message":
		row.StatusMessage = strings.Clone(value)
	case "span.kind":
		if v, err := strconv.ParseInt(value, 10, 32); err == nil {
			row.SpanKind = schema.Int32Ptr(int32(v))
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
