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
