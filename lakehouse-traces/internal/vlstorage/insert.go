package vlstorage

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	vtinsertutil "github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/insertutil"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TraceWriter is the subset of parquets3.Storage needed for the insert path.
type TraceWriter interface {
	MustAddTraceRows(rows []schema.TraceRow)
	CanWriteData() error
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

func (a *vtInsertAdapter) MustAddRows(lr *logstorage.LogRows) {
	rows := logRowsToTraceRows(lr)
	if len(rows) > 0 {
		a.writer.MustAddTraceRows(rows)
	}
}

func (a *vtInsertAdapter) CanWriteData() error {
	return a.writer.CanWriteData()
}

func (a *vtInsertAdapter) IsLocalStorage() bool {
	return true
}

// logRowsToTraceRows converts VL's LogRows into trace schema rows.
// VL handles all protocol parsing — we map fields to TraceRow columns.
func logRowsToTraceRows(lr *logstorage.LogRows) []schema.TraceRow {
	n := lr.RowsCount()
	if n == 0 {
		return nil
	}

	rows := make([]schema.TraceRow, 0, n)

	lr.ForEachRow(func(_ uint64, r *logstorage.InsertRow) {
		row := schema.TraceRow{
			TimestampUnixNano: r.Timestamp,
		}

		if r.StreamTagsCanonical != "" {
			st := logstorage.GetStreamTags()
			if err := unmarshalStreamTags(st, r.StreamTagsCanonical); err == nil {
				row.Stream = st.String()
			}
			logstorage.PutStreamTags(st)
		}

		for _, f := range r.Fields {
			mapFieldToTraceRow(&row, f.Name, f.Value)
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

// mapFieldToTraceRow maps a VL/VT field to the appropriate TraceRow column.
// Handles both VT's prefixed naming (resource_attr:, span_attr:) and VL's
// flat naming from jsonline ingestion.
func mapFieldToTraceRow(row *schema.TraceRow, name, value string) {
	if value == "-" && name == "_msg" {
		return
	}

	// VT OTLP trace fields (from vtinsert/opentelemetry)
	switch name {
	case otelpb.TraceIDField:
		row.TraceID = value
		return
	case otelpb.SpanIDField:
		row.SpanID = value
		return
	case otelpb.ParentSpanIDField:
		row.ParentSpanID = value
		return
	case otelpb.NameField:
		row.SpanName = value
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
		return
	case otelpb.EndTimeUnixNanoField:
		return
	case otelpb.StatusCodeField:
		if v, err := strconv.ParseInt(value, 10, 32); err == nil {
			row.StatusCode = int32(v)
		}
		return
	case otelpb.StatusMessageField:
		row.StatusMessage = value
		return
	case otelpb.InstrumentationScopeName:
		row.ScopeName = value
		return
	case otelpb.InstrumentationScopeVersion:
		return
	case otelpb.TraceStateField, otelpb.FlagsField,
		otelpb.DroppedAttributesCountField, otelpb.DroppedEventsCountField, otelpb.DroppedLinksCountField:
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

	// VT scope attributes, events, links — store in span attributes
	if strings.HasPrefix(name, otelpb.InstrumentationScopeAttrPrefix) ||
		strings.HasPrefix(name, otelpb.EventPrefix) ||
		strings.HasPrefix(name, otelpb.LinkPrefix) {
		return
	}

	// Legacy flat field names (from jsonline insert path)
	switch name {
	case "", "_msg":
		return
	case "trace_id":
		row.TraceID = value
	case "span_id":
		row.SpanID = value
	case "parent_span_id":
		row.ParentSpanID = value
	case "span.name":
		row.SpanName = value
	case "service.name":
		row.ServiceName = value
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
		row.StatusMessage = value
	case "span.kind":
		if v, err := strconv.ParseInt(value, 10, 32); err == nil {
			row.SpanKind = int32(v)
		}
	case "scope.name":
		row.ScopeName = value
	case "http.method":
		row.HTTPMethod = value
	case "http.status_code":
		row.HTTPStatusCode = value
	case "http.url":
		row.HTTPUrl = value
	case "db.system":
		row.DBSystem = value
	case "db.statement":
		row.DBStatement = value
	case "k8s.namespace.name":
		row.K8sNamespaceName = value
	case "k8s.pod.name":
		row.K8sPodName = value
	case "k8s.deployment.name":
		row.K8sDeploymentName = value
	case "k8s.node.name":
		row.K8sNodeName = value
	case "deployment.environment":
		row.DeployEnv = value
	case "cloud.region":
		row.CloudRegion = value
	case "host.name":
		row.HostName = value
	default:
		if row.SpanAttributes == nil {
			row.SpanAttributes = make(map[string]string)
		}
		row.SpanAttributes[name] = value
	}
}

func mapResourceAttr(row *schema.TraceRow, key, value string) {
	switch key {
	case "service.name":
		row.ServiceName = value
	case "k8s.namespace.name":
		row.K8sNamespaceName = value
	case "k8s.pod.name":
		row.K8sPodName = value
	case "k8s.deployment.name":
		row.K8sDeploymentName = value
	case "k8s.node.name":
		row.K8sNodeName = value
	case "deployment.environment":
		row.DeployEnv = value
	case "cloud.region":
		row.CloudRegion = value
	case "host.name":
		row.HostName = value
	default:
		if row.ResourceAttributes == nil {
			row.ResourceAttributes = make(map[string]string)
		}
		row.ResourceAttributes[key] = value
	}
}

func mapSpanAttr(row *schema.TraceRow, key, value string) {
	switch key {
	case "http.method":
		row.HTTPMethod = value
	case "http.status_code":
		row.HTTPStatusCode = value
	case "http.url":
		row.HTTPUrl = value
	case "db.system":
		row.DBSystem = value
	case "db.statement":
		row.DBStatement = value
	default:
		if row.SpanAttributes == nil {
			row.SpanAttributes = make(map[string]string)
		}
		row.SpanAttributes[key] = value
	}
}
