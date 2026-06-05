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

// TestSeverityText_AcceptsBothLevelAndSeverityTextFieldNames pins the
// dual-alias contract. VL's OTLP handler emits the field as
// `severity_text` (deps/VictoriaLogs/app/vlinsert/opentelemetry/pb.go:340
// `fs.Add("severity_text", ...)`), while VL's non-OTLP path emits it as
// `level`. Cold ingest now accepts both, so OTLP-sourced rows don't
// silently drop their severity and land in the Grafana "unknown" bucket.
func TestSeverityText_AcceptsBothLevelAndSeverityTextFieldNames(t *testing.T) {
	cases := []struct {
		name      string
		fieldName string
	}{
		{"non-OTLP path: level=Info", "level"},
		{"OTLP path: severity_text=Info", "severity_text"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lr := makeLogRows(t,
				logstorage.Field{Name: "", Value: "body"},
				logstorage.Field{Name: tc.fieldName, Value: "Info"},
			)
			rows := logRowsToSchemaRows(lr)
			logstorage.PutLogRows(lr)
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			if rows[0].SeverityText != "Info" {
				t.Errorf("field=%q: SeverityText = %q, want %q",
					tc.fieldName, rows[0].SeverityText, "Info")
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

// TestSeverityText_FallsBackFromStreamTag pins the last-resort fix:
// when a row has neither severity_text nor severity_number but the
// stream tag carries `level="WARN"` (Loki API / jsonline with stream-
// only level is the common source), we lift the value from the tag
// to row.SeverityText. Without this the row queries as level=""
// even though the stream-label level is visible elsewhere — which
// was the residual 28% "unknown" bucket measured in the e2e stack.
func TestSeverityText_FallsBackFromStreamTag(t *testing.T) {
	cases := []struct {
		name      string
		streamTag string
		want      string
	}{
		{
			name:      "level after service.name in canonical form",
			streamTag: `{cloud.region="us-east-1",level="WARN",service.name="api-gateway"}`,
			want:      "WARN",
		},
		{
			name:      "level first in canonical form",
			streamTag: `{level="ERROR",service.name="payment-service"}`,
			want:      "ERROR",
		},
		{
			name:      "no level in stream tag",
			streamTag: `{service.name="foo",cloud.region="bar"}`,
			want:      "",
		},
		{
			name:      "empty stream tag",
			streamTag: ``,
			want:      "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractStreamTagLevel(tc.streamTag)
			if got != tc.want {
				t.Errorf("extractStreamTagLevel(%q) = %q, want %q",
					tc.streamTag, got, tc.want)
			}
		})
	}
}

// TestSeverityText_NoStreamLevelTagDoesNotMisparse guards the parser
// against false-positives. Field values that contain `"level=` as
// content — e.g. a k8s label like `app.label="level=prod"` — must
// not be mistaken for the canonical stream `level="X"` tag. The
// canonical form always has the key followed by `="`; values are
// escaped so they can't carry an unescaped `"`. The parser hunts
// for the exact six-char prefix `level="` which doesn't appear as
// a literal sequence inside any other quoted value.
func TestSeverityText_NoStreamLevelTagDoesNotMisparse(t *testing.T) {
	// A legitimate VL canonical stream tag would never put `level="`
	// inside a quoted value because VL escapes the `"`. But test the
	// edge case anyway — confirm the parser only matches a top-level
	// key=value pair.
	streamTag := `{app="my-app",msg="contains level=prod text"}`
	if got := extractStreamTagLevel(streamTag); got != "" {
		t.Errorf("extractStreamTagLevel(%q) = %q, want empty (level= inside another value should not match)",
			streamTag, got)
	}

	// Suffix-only matches must also not trigger: `not_level="X"` and
	// `my.level="Y"` end with `level="...` but are distinct tag names.
	// The parser anchors on `{level="` or `,level="` so neither matches.
	for _, st := range []string{
		`{not_level="X",service="foo"}`,
		`{service="foo",not_level="X"}`,
		`{my.level="X",service="foo"}`,
	} {
		t.Run(st, func(t *testing.T) {
			if got := extractStreamTagLevel(st); got != "" {
				t.Errorf("extractStreamTagLevel(%q) = %q, want empty (suffix tag should not match)", st, got)
			}
		})
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
