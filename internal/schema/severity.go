package schema

import (
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/opentelemetry"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// DeriveSeverityText returns a non-empty severity text label given
// the row's existing fields, or "" when no source has a level. The
// derivation order:
//
//  1. existing severityText — used as-is when present
//  2. severityNumber via VL upstream's FormatSeverity (1..24 range)
//  3. the `level` tag in the parsed stream tags, if any
//
// All three steps reuse VL upstream code via the
// patches/vl-{logs,traces}/vl-export-*.patch family — the only LH
// contribution is the gate logic and the orchestration. Callers
// pass an already-parsed *logstorage.StreamTags so this function
// stays cheap on the insert hot path (where the tags are unmarshaled
// once for other purposes); the compactor passes its own parsed
// tags after running StreamTags.UnmarshalString on the row's
// human-readable Stream column.
//
// Returns "" when the row truly has no severity information
// (legitimate raw-stdout / syslog-only lines). The caller treats
// this as "leave SeverityText empty" rather than substituting a
// fake "Unspecified".
func DeriveSeverityText(severityText string, severityNumber int32, st *logstorage.StreamTags) string {
	if severityText != "" {
		return severityText
	}
	if severityNumber >= 1 && severityNumber <= 24 {
		return opentelemetry.FormatSeverity(severityNumber)
	}
	if st != nil {
		if lvl, ok := st.Get("level"); ok && lvl != "" {
			return lvl
		}
	}
	return ""
}

// Int32Ptr returns a pointer to v. It builds the presence-carrying value of an
// optional numeric column, such as LogRow.SeverityNumber.
func Int32Ptr(v int32) *int32 { return &v }

// Int32Value returns *p, or 0 when p is nil (the field was absent).
func Int32Value(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

// Int64Ptr returns a pointer to v (the presence-carrying value of an optional
// int64 column, such as TraceRow.DurationNs).
func Int64Ptr(v int64) *int64 { return &v }

// Int64Value returns *p, or 0 when p is nil (the field was absent).
func Int64Value(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}
