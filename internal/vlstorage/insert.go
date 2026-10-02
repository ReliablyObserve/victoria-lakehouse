package vlstorage

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// TenantCardinalityGate decides whether a (tenant, stream) row may be
// admitted. Implemented by *tenant.CardinalityLimiter; declared here as
// an interface so this package stays import-light.
type TenantCardinalityGate interface {
	AllowStream(accountID, projectID uint32, stream string) bool
}

var globalCardinalityGate TenantCardinalityGate

// SetCardinalityGate installs the per-tenant cardinality limiter the
// insert path consults before admitting a row. nil disables the check.
func SetCardinalityGate(g TenantCardinalityGate) {
	globalCardinalityGate = g
}

// keepStream is the admission rule of the logs binary, applied when a row is
// added to the insert buffer and again when it is flushed: a stream that
// identifies trace spans, not logs, is dropped (see storage.IsTraceShapedStream),
// and so is a stream over its tenant's cardinality limit.
func keepStream(accountID, projectID uint32, stream string) (keep bool, traceShaped bool) {
	if storage.IsTraceShapedStream(stream) {
		return false, true
	}
	if globalCardinalityGate != nil && stream != "" &&
		!globalCardinalityGate.AllowStream(accountID, projectID, stream) {
		return false, false
	}
	return true, false
}

// FlushRowKeeper returns the flusher's keep filter: the same admission rule
// the insert path applies (keepStream), read live so it tracks
// SetCardinalityGate.
func FlushRowKeeper() func(accountID, projectID uint32, stream string) bool {
	return func(accountID, projectID uint32, stream string) bool {
		keep, _ := keepStream(accountID, projectID, stream)
		return keep
	}
}

// BufferStore is the insert buffer: upstream logstorage, cut into segments by
// ingest time (membuffer.Segments). Every acknowledged row is added to it, and
// it is the only place the row lives until the flusher writes it to Parquet.
type BufferStore interface {
	MustAddRows(lr *logstorage.LogRows)
	IsReadOnly() bool
}

type insertAdapter struct {
	buf BufferStore
	dir string // the buffer directory, named in the read-only error
}

// SetInsertStorage routes every row VL's insert handlers parse into buf.
// dir is the buffer directory (insert.buffer_dir).
func SetInsertStorage(buf BufferStore, dir string) {
	insertutil.SetLogRowsStorage(&insertAdapter{buf: buf, dir: dir})
}

// MustAddRows adds the admitted rows of lr to the buffer while lr is valid
// (vlinsert reuses its memory after this returns; upstream copies the rows).
// A failure inside the buffer is not recovered here: as in upstream, it fails
// the request, so the client retries instead of getting an ack for rows that
// were stored nowhere.
func (a *insertAdapter) MustAddRows(lr *logstorage.LogRows) {
	blr, owned := admittedRows(lr)
	if blr != nil {
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
	type verdict struct{ keep, traceShaped bool }
	decided := map[uint64]verdict{}
	var st logstorage.StreamTags
	dropped, total := 0, 0
	lr.ForEachRow(func(streamHash uint64, r *logstorage.InsertRow) {
		total++
		v, ok := decided[streamHash]
		if !ok {
			stream := ""
			if r.StreamTagsCanonical != "" {
				st.Reset()
				if err := unmarshalStreamTags(&st, r.StreamTagsCanonical); err == nil {
					stream = st.String()
				}
			}
			v.keep, v.traceShaped = keepStream(r.TenantID.AccountID, r.TenantID.ProjectID, stream)
			decided[streamHash] = v
		}
		if !v.keep {
			dropped++
			if v.traceShaped {
				metrics.LogsTraceShapedRowsDroppedAtIngest.Inc()
			}
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
		if decided[streamHash].keep {
			cp.MustAddInsertRow(r)
		}
	})
	return cp, true
}

// CanWriteData is upstream's rule (VictoriaLogs app/vlstorage
// Storage.CanWriteData): 429 Too Many Requests while the buffer's volume is
// below its free-space floor, and nothing else. An unreachable object store
// does not refuse writes: the rows wait on the local disk, as they would in
// VictoriaLogs' own storage, and the disk floor turns into 429 if it lasts.
func (a *insertAdapter) CanWriteData() error {
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
	// Dedicated columns — Tier 1 (strict OTel). Routing a key here lifts it
	// out of the attribute map into its typed column; the read path emits it
	// under the same bare field name, so VL/VT query semantics are unchanged.
	case "container.id":
		row.ContainerID = strings.Clone(value)
	case "service.instance.id":
		row.ServiceInstanceID = strings.Clone(value)
	case "service.version":
		row.ServiceVersion = strings.Clone(value)
	case "exception.type":
		row.ExceptionType = strings.Clone(value)
	case "exception.message":
		row.ExceptionMessage = strings.Clone(value)
	case "k8s.cluster.name":
		row.K8sClusterName = strings.Clone(value)
	case "telemetry.sdk.name":
		row.TelemetrySDKName = strings.Clone(value)
	case "telemetry.sdk.language":
		row.TelemetrySDKLang = strings.Clone(value)
	case "telemetry.sdk.version":
		row.TelemetrySDKVer = strings.Clone(value)
	case "cloud.account.id":
		row.CloudAccountID = strings.Clone(value)
	case "cloud.provider":
		row.CloudProvider = strings.Clone(value)
	case "os.type":
		row.OSType = strings.Clone(value)
	case "host.arch":
		row.HostArch = strings.Clone(value)
	case "process.runtime.name":
		row.ProcessRuntimeName = strings.Clone(value)
	case "process.runtime.version":
		row.ProcessRuntimeVer = strings.Clone(value)
	default:
		// Tier-2: an operator-configured custom attribute routes to a spare
		// slot column (and out of the map). Falls through to the map when no
		// resolver is set or the key isn't configured.
		if slot, ok := activeSlotResolver.SlotForName(name); ok {
			schema.SetLogSlot(row, slot, strings.Clone(value))
			return
		}
		if row.LogAttributes == nil {
			row.LogAttributes = make(map[string]string)
		}
		row.LogAttributes[strings.Clone(name)] = strings.Clone(value)
	}
}

// activeSlotResolver holds the process-wide Tier-2 custom-attribute slot
// binding, set once at startup from config via SetSlotResolver. nil = no custom
// promotions (all SlotResolver methods are nil-safe).
var activeSlotResolver *schema.SlotResolver

// SetSlotResolver installs the Tier-2 slot resolver (built from
// config.ActivePromotedAttributes at startup). Safe to call with nil.
func SetSlotResolver(r *schema.SlotResolver) { activeSlotResolver = r }

// RepromoteLogRow re-derives dedicated columns (Tier-1 OTel) and Tier-2 custom
// slots from a row's attribute MAP — the compaction-time healing path for v1 files
// written before a key was promoted (promotion runs only at ingest; the writer and
// compactor never re-applied it, so old files keep promoted attrs in the map →
// they miss the dedicated-column compression AND their dedicated-column cardinality
// reads 0). Re-routes every map entry through the SAME ingest mapper used at write
// time, so semantics are identical: a promoted/slotted key lands in its typed column
// and leaves the map; a non-promoted key rebuilds the map. Idempotent — a v2 row
// whose map holds no promotable key is unchanged. The empty key (VL's _msg form) is
// preserved in the map rather than routed to Body (it isn't an attribute here).
func RepromoteLogRow(r *schema.LogRow) {
	if len(r.LogAttributes) == 0 {
		return
	}
	attrs := r.LogAttributes
	r.LogAttributes = nil
	for k, v := range attrs {
		if k == "" {
			if r.LogAttributes == nil {
				r.LogAttributes = make(map[string]string)
			}
			r.LogAttributes[k] = v
			continue
		}
		mapFieldToRow(r, k, v)
	}
}
