package schema

import (
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
)

// traceDeltaColumns are the TraceRow columns that are written DELTA_BINARY_PACKED
// although their Go field is a pointer (an optional Parquet column).
//
// parquet-go v0.32.0 rejects the `delta` struct tag on a pointer field
// ("invalid parquet struct tag"), so the encoding cannot be declared on the
// struct. Before the span numerics became nullable, start_time_unix_nano carried
// `parquet:"...,delta"`; without the encoding the column is plain 8 bytes per
// value and an object grows 6.6 % to 7.8 % (start time equals the record
// timestamp for almost every span, which delta packs to a few bits). The
// encoding is therefore attached to the schema node the writer uses instead.
//
// Only columns that had an explicit encoding before they became optional belong
// here; duration_ns, status.code and span.kind were plain and stay plain.
var traceDeltaColumns = map[string]struct{}{
	"start_time_unix_nano": {},
}

// TraceWriterDeltaColumns returns, sorted, the optional TraceRow columns that
// NewTraceWriter writes DELTA_BINARY_PACKED although no struct tag says so. A
// verifier that derives the expected delta columns from the struct tags (the
// parquet readback gate) adds these.
func TraceWriterDeltaColumns() []string {
	cols := make([]string, 0, len(traceDeltaColumns))
	for c := range traceDeltaColumns {
		cols = append(cols, c)
	}
	slices.Sort(cols)
	return cols
}

// encodedField is a schema field whose encoding is overridden. Everything else
// (name, optionality, type, how to read the Go value) is the struct-derived field.
type encodedField struct {
	parquet.Field
	enc encoding.Encoding
}

func (f encodedField) Encoding() encoding.Encoding { return f.enc }

// orderedGroup is the struct-derived root group with some leaf fields swapped.
// parquet.Group would reorder the columns alphabetically; the physical column
// order is part of the file layout, so the struct order is kept.
type orderedGroup struct {
	*parquet.Schema
	fields []parquet.Field
}

func (g *orderedGroup) Fields() []parquet.Field { return g.fields }

var (
	traceWriterSchemaOnce sync.Once
	traceWriterSchemaVal  *parquet.Schema
)

// TraceWriterSchema is the Parquet schema every Lakehouse writer uses for
// TraceRow: the schema parquet-go derives from the struct, with the
// DELTA_BINARY_PACKED encoding restored on the optional columns listed in
// traceDeltaColumns. It is equal to the struct schema under parquet.EqualNodes
// (the encoding is not part of node equality), so parquet-go keeps its direct
// Go-value-to-column write path and reads files back into TraceRow unchanged.
//
// It is a parquet.WriterOption; NewTraceWriter is the way to use it.
func TraceWriterSchema() *parquet.Schema {
	traceWriterSchemaOnce.Do(func() { traceWriterSchemaVal = buildTraceWriterSchema() })
	return traceWriterSchemaVal
}

func buildTraceWriterSchema() *parquet.Schema {
	base := parquet.SchemaOf(new(TraceRow))
	src := base.Fields()
	fields := make([]parquet.Field, len(src))
	seen := 0
	for i, f := range src {
		fields[i] = f
		if _, ok := traceDeltaColumns[f.Name()]; ok {
			fields[i] = encodedField{Field: f, enc: &parquet.DeltaBinaryPacked}
			seen++
		}
	}
	if seen != len(traceDeltaColumns) {
		panic(fmt.Sprintf("schema: TraceRow lacks one of the delta columns %v", traceDeltaColumns))
	}
	return parquet.NewSchema(base.Name(), &orderedGroup{Schema: base, fields: fields})
}

// NewTraceWriter is the only way a Lakehouse code path writes a TraceRow
// Parquet object: flush, compaction and the delete rewriter in both binaries
// all call it, so the encodings of an object do not depend on which of them
// wrote it.
func NewTraceWriter(w io.Writer, opts ...parquet.WriterOption) *parquet.GenericWriter[TraceRow] {
	all := make([]parquet.WriterOption, 0, len(opts)+1)
	all = append(all, opts...)
	all = append(all, TraceWriterSchema())
	return parquet.NewGenericWriter[TraceRow](w, all...)
}
