package vlstorage

import (
	"fmt"
	"strconv"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TraceWriter is the subset of parquets3.Storage needed for the insert path.
type TraceWriter interface {
	MustAddTraceRows(rows []schema.TraceRow)
	CanWriteData() error
}

type insertAdapter struct {
	writer TraceWriter
}

// SetInsertStorage configures VL's vlinsert handlers to route all ingested
// rows through the given TraceWriter. This is the insert-path counterpart of
// SetStorage (which handles the select path).
func SetInsertStorage(w TraceWriter) {
	insertutil.SetLogRowsStorage(&insertAdapter{writer: w})
}

func (a *insertAdapter) MustAddRows(lr *logstorage.LogRows) {
	rows := logRowsToTraceRows(lr)
	if len(rows) > 0 {
		a.writer.MustAddTraceRows(rows)
	}
}

func (a *insertAdapter) CanWriteData() error {
	return a.writer.CanWriteData()
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

// mapFieldToTraceRow maps a single VL field to the appropriate TraceRow column.
func mapFieldToTraceRow(row *schema.TraceRow, name, value string) {
	switch name {
	case "":
		row.SpanName = value
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
	case "scope.name":
		row.ScopeName = value
	default:
		if row.SpanAttributes == nil {
			row.SpanAttributes = make(map[string]string)
		}
		row.SpanAttributes[name] = value
	}
}
