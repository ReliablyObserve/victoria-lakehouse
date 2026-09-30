package main

import (
	"flag"
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/insertutil"
)

// VictoriaTraces' ingest code (vtinsert/insertutil) and VictoriaLogs' share
// two command-line flags in this binary, -insert.maxFieldsPerLine and
// -defaultMsgValue. vtinsert-flag-dedup.patch makes VictoriaTraces use
// VictoriaLogs' flag itself; it used to copy the value when the package
// initialised, before flag.Parse, so an operator's value never reached span
// ingest (issue #259: spans with more than 1000 fields were always dropped).
// These tests set the flag after init, as flag.Parse does, and ingest through
// VictoriaTraces' own processor.

type rowsRecorder struct{ rows []map[string]string }

func (r *rowsRecorder) MustAddRows(lr *logstorage.LogRows) {
	lr.ForEachRow(func(_ uint64, ir *logstorage.InsertRow) {
		m := map[string]string{}
		for _, f := range ir.Fields {
			m[f.Name] = f.Value
		}
		r.rows = append(r.rows, m)
	})
}
func (r *rowsRecorder) CanWriteData() error  { return nil }
func (r *rowsRecorder) IsLocalStorage() bool { return true }

func ingest(t *testing.T, fields []logstorage.Field) []map[string]string {
	t.Helper()
	rec := &rowsRecorder{}
	insertutil.SetLogRowsStorage(rec)
	cp := &insertutil.CommonParams{TimeFields: []string{"_time"}}
	lmp := cp.NewLogMessageProcessor("flag-test", false)
	lmp.AddRow(1790000000000000000, fields, 0)
	lmp.MustClose()
	return rec.rows
}

func setFlag(t *testing.T, name, value string) {
	t.Helper()
	f := flag.Lookup(name)
	if f == nil {
		t.Fatalf("-%s is not registered", name)
	}
	old := f.Value.String()
	if err := flag.Set(name, value); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set(name, old) })
}

func manyFields(n int) []logstorage.Field {
	fields := make([]logstorage.Field, 0, n)
	fields = append(fields, logstorage.Field{Name: "_msg", Value: "span"})
	for i := 1; i < n; i++ {
		fields = append(fields, logstorage.Field{Name: fmt.Sprintf("span_attr:k%d", i), Value: "v"})
	}
	return fields
}

func TestVTIngestHonoursMaxFieldsPerLine(t *testing.T) {
	if rows := ingest(t, manyFields(1001)); len(rows) != 0 {
		t.Fatalf("a span with 1001 fields was accepted at the default limit of 1000")
	}
	if rows := ingest(t, manyFields(1000)); len(rows) != 1 {
		t.Fatalf("a span with 1000 fields was dropped at the default limit of 1000")
	}

	// The operator raises the limit after startup, as flag.Parse does.
	setFlag(t, "insert.maxFieldsPerLine", "1500")
	if rows := ingest(t, manyFields(1001)); len(rows) != 1 {
		t.Errorf("-insert.maxFieldsPerLine=1500 did not reach VictoriaTraces' ingest: the 1001-field span was dropped")
	}
	// ...and lowers it.
	setFlag(t, "insert.maxFieldsPerLine", "5")
	if rows := ingest(t, manyFields(6)); len(rows) != 0 {
		t.Errorf("-insert.maxFieldsPerLine=5 did not reach VictoriaTraces' ingest: a 6-field span was accepted")
	}
}

func TestVTIngestHonoursDefaultMsgValue(t *testing.T) {
	noMsg := []logstorage.Field{{Name: "trace_id", Value: "abc"}}
	setFlag(t, "defaultMsgValue", "no-message")
	rows := ingest(t, noMsg)
	// The message field is stored under the empty name in a LogRows row.
	if len(rows) != 1 || rows[0][""] != "no-message" {
		t.Errorf("-defaultMsgValue did not reach VictoriaTraces' ingest: rows = %v", rows)
	}
}

// The alias must be the same pointer, not a copy: VictoriaTraces registers no
// flag of its own for these two names.
func TestVTIngestFlagsAreVictoriaLogsFlags(t *testing.T) {
	for name, ptr := range map[string]any{
		"insert.maxFieldsPerLine": insertutil.MaxFieldsPerLine,
		"defaultMsgValue":         insertutil.DefaultMsgValue,
	} {
		f := flag.Lookup(name)
		if f == nil {
			t.Fatalf("-%s is not registered", name)
		}
		switch p := ptr.(type) {
		case *int:
			old := *p
			if err := flag.Set(name, "4321"); err != nil {
				t.Fatal(err)
			}
			if *p != 4321 {
				t.Errorf("-%s: insertutil holds %d after flag.Set(4321): it is a copy", name, *p)
			}
			_ = flag.Set(name, fmt.Sprint(old))
		case *string:
			old := *p
			if err := flag.Set(name, "probe"); err != nil {
				t.Fatal(err)
			}
			if *p != "probe" {
				t.Errorf("-%s: insertutil holds %q after flag.Set(probe): it is a copy", name, *p)
			}
			_ = flag.Set(name, old)
		}
	}
}
