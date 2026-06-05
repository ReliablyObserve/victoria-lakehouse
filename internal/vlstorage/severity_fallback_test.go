package vlstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// TestSeverityText_FallsBackFromSeverityNumber pins the regression
// fix for the cold-tier "unknown level" bucket. OTLP ingest routinely
// produces rows that carry severity_number but no level/severity_text
// field; before the fallback those rows landed in cold parquet with
// SeverityText="", and Grafana's log-volume chart bucketed them as
// level="" (the "unknown" bar). VL hot derives the text from the
// number internally; this test guarantees LH cold does the same.
func TestSeverityText_FallsBackFromSeverityNumber(t *testing.T) {
	// The expected labels mirror VL upstream's logSeverities table
	// exactly (see deps/VictoriaLogs/app/vlinsert/opentelemetry/pb.go).
	// LH cold delegates to FormatSeverity through patches/vl-{logs,traces}/
	// vl-export-severity.patch so a sev_number=9 row queries with the
	// same "Info" label whether it lands in hot or cold storage.
	cases := []struct {
		name        string
		sevNumber   string
		wantLevel   string
		wantNumeric int32
	}{
		{"trace 1", "1", "Trace", 1},
		{"trace 4", "4", "Trace4", 4},
		{"debug 5", "5", "Debug", 5},
		{"debug 8", "8", "Debug4", 8},
		{"info 9", "9", "Info", 9},
		{"info 12", "12", "Info4", 12},
		{"warn 13", "13", "Warn", 13},
		{"warn 16", "16", "Warn4", 16},
		{"error 17", "17", "Error", 17},
		{"error 20", "20", "Error4", 20},
		{"fatal 21", "21", "Fatal", 21},
		{"fatal 24", "24", "Fatal4", 24},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lr := makeLogRows(t,
				logstorage.Field{Name: "", Value: "body"},
				logstorage.Field{Name: "severity_number", Value: tc.sevNumber},
			)
			rows := logRowsToSchemaRows(lr)
			logstorage.PutLogRows(lr)
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			if rows[0].SeverityText != tc.wantLevel {
				t.Errorf("SeverityText = %q, want %q", rows[0].SeverityText, tc.wantLevel)
			}
			if rows[0].SeverityNumber != tc.wantNumeric {
				t.Errorf("SeverityNumber = %d, want %d", rows[0].SeverityNumber, tc.wantNumeric)
			}
		})
	}
}

// TestSeverityText_ExplicitLevelWinsOverDerived pins the precedence
// rule: when the source row carries BOTH level and severity_number,
// the explicit level text is preserved verbatim — derived fallback
// only fires when SeverityText is still empty. Otherwise an upstream
// "EMERG" or custom level would silently turn into the derived
// canonical value, surprising downstream queries.
func TestSeverityText_ExplicitLevelWinsOverDerived(t *testing.T) {
	lr := makeLogRows(t,
		logstorage.Field{Name: "", Value: "body"},
		logstorage.Field{Name: "level", Value: "CRITICAL"},
		logstorage.Field{Name: "severity_number", Value: "13"}, // would derive WARN
	)
	rows := logRowsToSchemaRows(lr)
	logstorage.PutLogRows(lr)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].SeverityText != "CRITICAL" {
		t.Errorf("SeverityText = %q, want %q (explicit level must win)",
			rows[0].SeverityText, "CRITICAL")
	}
}

// TestSeverityText_NoNumberNoLevel verifies the no-op path: rows
// without level AND without severity_number leave SeverityText empty.
// The derived fallback cannot invent information; legitimate
// no-severity entries (raw stdout lines, syslog body-only) keep
// their original empty state and surface as their own bucket.
func TestSeverityText_NoNumberNoLevel(t *testing.T) {
	lr := makeLogRows(t,
		logstorage.Field{Name: "", Value: "naked log line"},
	)
	rows := logRowsToSchemaRows(lr)
	logstorage.PutLogRows(lr)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].SeverityText != "" {
		t.Errorf("SeverityText = %q, want empty (no source severity)",
			rows[0].SeverityText)
	}
}

// TestSeverityText_OutOfRangeNumberStaysEmpty guards the OTel
// boundary check. A severity_number outside the documented 1-24
// range (older non-OTel pipelines, broken instrumentation) should
// produce empty SeverityText rather than a misleading derived
// value — operators must be able to spot bad input rather than
// see it labeled with a confidence we don't have.
func TestSeverityText_OutOfRangeNumberStaysEmpty(t *testing.T) {
	for _, n := range []string{"0", "25", "99", "-1"} {
		t.Run("n="+n, func(t *testing.T) {
			lr := makeLogRows(t,
				logstorage.Field{Name: "", Value: "body"},
				logstorage.Field{Name: "severity_number", Value: n},
			)
			rows := logRowsToSchemaRows(lr)
			logstorage.PutLogRows(lr)
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			if rows[0].SeverityText != "" {
				t.Errorf("severity_number=%s: SeverityText = %q, want empty (out-of-range)",
					n, rows[0].SeverityText)
			}
		})
	}
}
