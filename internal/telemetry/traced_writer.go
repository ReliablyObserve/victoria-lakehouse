package telemetry

import (
	"context"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// LogWriter is the subset of parquets3.Storage needed for the insert path.
type LogWriter interface {
	MustAddLogRows(rows []schema.LogRow)
	CanWriteData() error
}

// TracedWriter wraps a LogWriter and adds OTEL tracing spans
// to the MustAddLogRows method.
type TracedWriter struct {
	inner LogWriter
}

// NewTracedWriter returns a TracedWriter decorator around w.
func NewTracedWriter(w LogWriter) *TracedWriter {
	return &TracedWriter{inner: w}
}

func (t *TracedWriter) MustAddLogRows(rows []schema.LogRow) {
	_, span := otel.Tracer("lakehouse").Start(context.Background(), "storage.add_rows",
		trace.WithAttributes(attribute.Int("row_count", len(rows))),
	)
	defer span.End()
	t.inner.MustAddLogRows(rows)
}

func (t *TracedWriter) CanWriteData() error {
	return t.inner.CanWriteData()
}
