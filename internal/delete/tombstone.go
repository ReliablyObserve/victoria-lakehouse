package delete

import (
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Tombstone is a delete: the rows of its tenants, with a _time in
// [StartNs, EndNs], that match its LogsQL filter are hidden from every read for
// as long as the tombstone is in force. Its fields are exactly the definition
// that was written to the bucket once; nothing about its progress is recorded
// anywhere, because there is nothing to record (see keys.go).
type Tombstone struct {
	ID      string
	Query   string
	StartNs int64
	EndNs   int64
	// CreatedAt is the time of the definition's key: the task's own timestamp for an
	// upstream delete task, otherwise the creating pod's clock.
	CreatedAt time.Time
	// CreatedBy is the audit trail of a Lakehouse API delete. An upstream task
	// has none.
	CreatedBy string
	// Mode is hide, permanent or auto. Physical removal is not enabled, so every
	// mode hides the rows at query time.
	Mode string

	// FilterAt is the time (unix nanoseconds) the query's relative time filters
	// (`_time:5m`) are evaluated at: when the delete was issued, as upstream
	// evaluates a delete task at its start time. Zero evaluates them at parse
	// time.
	FilterAt int64 `json:",omitempty"`

	// Tenants limits the tombstone to the rows of these tenants: query-time
	// suppression, field listings and the delete API all act only on objects and
	// rows of a tenant listed here. At least one is required (Validate).
	Tenants []TenantRef `json:",omitempty"`
}

// AffectsFile returns true if the tombstone's time range overlaps with the
// file's time range [fileMinNs, fileMaxNs].
func (t *Tombstone) AffectsFile(fileMinNs, fileMaxNs int64) bool {
	if t.StartNs > t.EndNs {
		return false
	}
	return t.StartNs <= fileMaxNs && t.EndNs >= fileMinNs
}

// MatchesRow returns true if the given row matches this tombstone's query
// and the timestamp falls within the tombstone's time range.
// Uses VL's ParseFilter + Filter.MatchRow for full LogsQL evaluation.
func (t *Tombstone) MatchesRow(row map[string]string, timestampNs int64) bool {
	if timestampNs < t.StartNs || timestampNs > t.EndNs {
		return false
	}
	if t.Query == "" || t.Query == "*" {
		return true
	}
	f := parseFilterCached(t.Query, t.FilterAt)
	if f == nil {
		return false
	}
	fields := make([]logstorage.Field, 0, len(row))
	for k, v := range row {
		fields = append(fields, logstorage.Field{Name: k, Value: v})
	}
	return f.MatchRow(fields)
}

// MatchesFields is MatchesRow for callers that already hold the row as
// []logstorage.Field — the shape VL's filters evaluate against. It skips the
// map round-trip MatchesRow needs, which matters on the field-enumeration paths
// where every scanned row is checked.
func (t *Tombstone) MatchesFields(fields []logstorage.Field, timestampNs int64) bool {
	if timestampNs < t.StartNs || timestampNs > t.EndNs {
		return false
	}
	if t.Query == "" || t.Query == "*" {
		return true
	}
	f := parseFilterCached(t.Query, t.FilterAt)
	if f == nil {
		return false
	}
	return f.MatchRow(fields)
}

// Filter returns the parsed LogsQL filter for this tombstone's query, or nil
// when the query is match-all or does not parse. Read paths use it to learn
// which columns the tombstone predicate needs so a column-projected scan can
// include them — evaluating a tombstone against a projection that omits its
// own fields silently under-suppresses.
func (t *Tombstone) Filter() *logstorage.Filter {
	if t.Query == "" || t.Query == "*" {
		return nil
	}
	return parseFilterCached(t.Query, t.FilterAt)
}

// Validate rejects a tombstone that cannot be stored and enforced faithfully.
//
// Two of these checks exist because of what happens if they do not:
//
//   - A query that does not parse as LogsQL matches nothing. The delete then
//     appears to succeed and silently hides no rows, which the user experiences
//     as "the delete did not work" with no error anywhere. Rejecting at the API
//     turns a silent no-op into a 400.
//   - A string carrying invalid UTF-8 does not survive the JSON encoding of the
//     definition: the bytes come back replaced with U+FFFD, so the tombstone a
//     pod reads is not the tombstone that was accepted.
//
// An inverted time range is rejected for the same reason as the first: it can
// never match a row.
func (t *Tombstone) Validate() error {
	if t.ID == "" {
		return fmt.Errorf("tombstone has no id")
	}
	for _, f := range []struct{ name, value string }{
		{"id", t.ID},
		{"query", t.Query},
		{"created_by", t.CreatedBy},
		{"mode", t.Mode},
	} {
		if !utf8.ValidString(f.value) {
			return fmt.Errorf("%s is not valid UTF-8; it would not survive being stored", f.name)
		}
	}
	if len(escapeID(t.ID)) > maxEscapedIDLen {
		return fmt.Errorf("id is too long (%d bytes escaped, at most %d)", len(escapeID(t.ID)), maxEscapedIDLen)
	}
	if t.StartNs > t.EndNs {
		return fmt.Errorf("time range is inverted: start %d is after end %d", t.StartNs, t.EndNs)
	}
	if len(t.Tenants) == 0 {
		return ErrNoTenantScope
	}
	if t.Query != "" && t.Query != "*" {
		if _, err := parseFilterAt(t.Query, t.FilterAt); err != nil {
			return fmt.Errorf("query does not parse as LogsQL: %w", err)
		}
	}
	switch t.Mode {
	case "", "hide", "permanent", "auto":
	default:
		return fmt.Errorf("unknown mode %q", t.Mode)
	}
	return nil
}
