package vlstorage

import (
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// LogWriter is the subset of parquets3.Storage needed for the insert path.
type LogWriter interface {
	MustAddLogRows(rows []schema.LogRow)
	CanWriteData() error
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
func logRowsToSchemaRows(lr *logstorage.LogRows) []schema.LogRow {
	n := lr.RowsCount()
	if n == 0 {
		return nil
	}

	rows := make([]schema.LogRow, 0, n)

	lr.ForEachRow(func(_ uint64, r *logstorage.InsertRow) {
		row := schema.LogRow{
			TimestampUnixNano: r.Timestamp,
		}

		// Recover _stream string from canonical binary representation.
		if r.StreamTagsCanonical != "" {
			st := logstorage.GetStreamTags()
			if err := unmarshalStreamTags(st, r.StreamTagsCanonical); err == nil {
				row.Stream = st.String()
			}
			logstorage.PutStreamTags(st)
		}

		for _, f := range r.Fields {
			mapFieldToRow(&row, f.Name, f.Value)
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
	_ = tail
	return nil
}

// mapFieldToRow maps a single VL field to the appropriate schema.LogRow column.
// Empty field name is VL's canonical form for _msg.
func mapFieldToRow(row *schema.LogRow, name, value string) {
	switch name {
	case "":
		row.Body = value
	case "_level":
		row.SeverityText = value
	case "service.name":
		row.ServiceName = value
	case "trace_id":
		row.TraceID = value
	case "span_id":
		row.SpanID = value
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
		if row.LogAttributes == nil {
			row.LogAttributes = make(map[string]string)
		}
		row.LogAttributes[name] = value
	}
}
