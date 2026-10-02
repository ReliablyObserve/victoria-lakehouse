package telemetry

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Buffer is the insert buffer as the insert path uses it.
type Buffer interface {
	MustAddRows(lr *logstorage.LogRows)
	IsReadOnly() bool
}

// TracedBuffer wraps the insert buffer and adds an OTEL span to every add.
type TracedBuffer struct {
	inner Buffer
}

// NewTracedBuffer returns a TracedBuffer decorator around b.
func NewTracedBuffer(b Buffer) *TracedBuffer {
	return &TracedBuffer{inner: b}
}

// MustAddRows adds lr to the buffer inside a "storage.add_rows" span.
func (t *TracedBuffer) MustAddRows(lr *logstorage.LogRows) {
	_, span := otel.Tracer("lakehouse").Start(context.Background(), "storage.add_rows",
		trace.WithAttributes(attribute.Int("row_count", lr.RowsCount())),
	)
	defer span.End()
	t.inner.MustAddRows(lr)
}

// IsReadOnly reports whether the buffer refuses writes.
func (t *TracedBuffer) IsReadOnly() bool {
	return t.inner.IsReadOnly()
}
