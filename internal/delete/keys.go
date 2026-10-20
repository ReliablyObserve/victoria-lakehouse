package delete

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Tombstone keys.
//
// A delete is a small set of write-once objects under "<prefix>_tombstones/".
// Nothing under that prefix is ever overwritten, so there is no shared mutable
// record for pods to clobber, no version to reconcile and nothing a pod's disk
// can contradict:
//
//	t<16 hex>-<id>.json   the definition (what is deleted), written once
//	t<16 hex>-<16 hex>-<id>.undo
//	                          the event that tells pods about an un-delete (hide mode
//	                          only); the second number is the time of the definition
//	                          key it undoes, so an undo binds to one definition and
//	                          never to whatever else carries the same id
//
// The hex number is a nanosecond timestamp, fixed width, so the lexical order of
// the keys is their time order and a pod can list only what is new
// (ListObjectsV2 StartAfter). The definition's time is the delete task's own
// timestamp when its id is one (upstream's ids are the creating node's clock in
// nanoseconds), so the same task always lands on the same key; otherwise it is
// the creating pod's clock. An undo key carries the time of the un-delete, so a
// pod that polls "keys after T" learns of it. Every time is a pod's own clock:
// there is no shared clock; a creator whose clock is more than the poll window
// off is caught by the hourly full listing and counted (see protocol.go).

const (
	tombstonePrefixSegment = "_tombstones/"

	kindDefinition = ".json"
	kindUndo       = ".undo"

	// maxEscapedIDLen keeps every key far below the 1024-byte S3 limit, whatever
	// the prefix.
	maxEscapedIDLen = 400
)

// tombKey is a parsed tombstone key.
type tombKey struct {
	kind   string // one of the kind* suffixes
	timeNs int64
	id     string // unescaped
	// defNs is, for an undo key, the time of the definition key it undoes (zero
	// for a definition).
	defNs int64
}

// TombstonePrefix returns the S3 key prefix the tombstone objects of the signal
// prefix live under, always ending in "/". Exported so the operator-facing docs,
// the orphan sweep's protected list and the tests all name the same thing.
//
// TrimRight rather than TrimSuffix: a prefix ending in more than one slash (an
// empty tenant prefix concatenated with a signal suffix) would otherwise keep one
// of them and produce a key nothing lists.
func TombstonePrefix(prefix string) string {
	prefix = strings.TrimRight(prefix, "/")
	if prefix == "" {
		return tombstonePrefixSegment
	}
	return prefix + "/" + tombstonePrefixSegment
}

// escapeID makes a task or tombstone id safe inside a key and unambiguous to
// parse: every byte outside [A-Za-z0-9_-] becomes %XX (upper-case hex). Upstream
// accepts any task id, so none is refused for its characters.
func escapeID(id string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(id))
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&15])
		}
	}
	return b.String()
}

// unescapeID inverts escapeID. It accepts only what escapeID produces, so a key
// has exactly one spelling.
func unescapeID(s string) (string, bool) {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
			b.WriteByte(c)
		case c == '%':
			if i+2 >= len(s) {
				return "", false
			}
			hi, ok1 := upperHexVal(s[i+1])
			lo, ok2 := upperHexVal(s[i+2])
			if !ok1 || !ok2 {
				return "", false
			}
			v := hi<<4 | lo
			// A byte escapeID would have left bare must not be escaped.
			if v >= 'a' && v <= 'z' || v >= 'A' && v <= 'Z' || v >= '0' && v <= '9' || v == '_' || v == '-' {
				return "", false
			}
			b.WriteByte(v)
			i += 2
		default:
			return "", false
		}
	}
	return b.String(), true
}

func upperHexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// hexTime renders a key time: 16 lower-case hex digits, so the lexical order of
// keys is the numeric order of their times.
func hexTime(ns int64) string {
	const digits = "0123456789abcdef"
	var buf [16]byte
	v := uint64(ns)
	for i := 15; i >= 0; i-- {
		buf[i] = digits[v&15]
		v >>= 4
	}
	return string(buf[:])
}

// formatKey is the S3 key of k under prefix (a TombstonePrefix value).
func formatKey(prefix string, k tombKey) string {
	if k.kind == kindUndo {
		return prefix + "t" + hexTime(k.timeNs) + "-" + hexTime(k.defNs) + "-" + escapeID(k.id) + k.kind
	}
	return prefix + "t" + hexTime(k.timeNs) + "-" + escapeID(k.id) + k.kind
}

// definitionKey is the key of the definition an undo key undoes.
func (k tombKey) definitionKey(prefix string) string {
	return formatKey(prefix, tombKey{kind: kindDefinition, timeNs: k.defNs, id: k.id})
}

// startAfterKey is the key a StartAfter listing resumes from so that every key
// with a time of at least ns is returned.
func startAfterKey(prefix string, ns int64) string {
	if ns <= 0 {
		return ""
	}
	return prefix + "t" + hexTime(ns)
}

// parseKey parses a key under prefix. It accepts exactly the keys formatKey
// produces (so formatKey(parseKey(k)) == k), and nothing else: a key of any
// other shape is not a tombstone object.
func parseKey(prefix, key string) (tombKey, bool) {
	if !strings.HasPrefix(key, prefix) {
		return tombKey{}, false
	}
	rest := key[len(prefix):]
	var kind string
	for _, k := range []string{kindDefinition, kindUndo} {
		if strings.HasSuffix(rest, k) {
			kind = k
			rest = rest[:len(rest)-len(k)]
			break
		}
	}
	if kind == "" || len(rest) < 1+16+1+1 || rest[0] != 't' || rest[17] != '-' {
		return tombKey{}, false
	}
	ns, okTime := parseHexTime(rest[1:17])
	if !okTime {
		return tombKey{}, false
	}
	if ns == 0 || ns > 1<<63-1 {
		return tombKey{}, false
	}
	escaped := rest[18:]
	var defNs uint64
	if kind == kindUndo {
		// t<16 hex>-<16 hex>-<id>: the second number is the definition's key time.
		if len(escaped) < 16+1+1 || escaped[16] != '-' {
			return tombKey{}, false
		}
		var ok bool
		if defNs, ok = parseHexTime(escaped[:16]); !ok || defNs == 0 || defNs > 1<<63-1 {
			return tombKey{}, false
		}
		escaped = escaped[17:]
	}
	if len(escaped) > maxEscapedIDLen {
		return tombKey{}, false
	}
	id, ok := unescapeID(escaped)
	if !ok || id == "" || !utf8.ValidString(id) {
		return tombKey{}, false
	}
	return tombKey{kind: kind, timeNs: int64(ns), id: id, defNs: int64(defNs)}, true
}

// definition is the content of a definition key. Its encoding is canonical
// (fixed field order, sorted tenants, no receive time and no pod field), so two
// pods creating the same upstream delete task write identical bytes and the
// second sees a harmless duplicate.
type definition struct {
	V        int         `json:"v"`
	ID       string      `json:"id"`
	Tenants  []defTenant `json:"tenants"`
	Filter   string      `json:"filter"`
	FilterAt int64       `json:"filter_at"`
	StartNs  int64       `json:"start_ns"`
	EndNs    int64       `json:"end_ns"`
	Mode     string      `json:"mode"`
	// By is the audit trail of a Lakehouse API delete (the creating tenant). An
	// upstream task carries none, which keeps its bytes identical on every node.
	By string `json:"by,omitempty"`
}

type defTenant struct {
	A uint32 `json:"a"`
	P uint32 `json:"p"`
}

const definitionVersion = 1

// encodeDefinition is the canonical bytes of ts's definition.
func encodeDefinition(ts Tombstone) ([]byte, error) {
	d := definition{
		V:        definitionVersion,
		ID:       ts.ID,
		Filter:   ts.Query,
		FilterAt: ts.FilterAt,
		StartNs:  ts.StartNs,
		EndNs:    ts.EndNs,
		Mode:     ts.Mode,
		By:       ts.CreatedBy,
	}
	for _, t := range NormalizeTenants(ts.Tenants) {
		d.Tenants = append(d.Tenants, defTenant{A: t.AccountID, P: t.ProjectID})
	}
	return json.Marshal(d)
}

// decodeDefinition reads a definition key's content. Any other shape (a record
// of the previous mutable protocol, an unknown version) is rejected, never read.
func decodeDefinition(data []byte) (Tombstone, error) {
	var d definition
	if err := json.Unmarshal(data, &d); err != nil {
		return Tombstone{}, fmt.Errorf("not a tombstone definition: %w", err)
	}
	if d.V != definitionVersion {
		return Tombstone{}, fmt.Errorf("unsupported tombstone definition version %d", d.V)
	}
	ts := Tombstone{
		ID:        d.ID,
		Query:     d.Filter,
		StartNs:   d.StartNs,
		EndNs:     d.EndNs,
		FilterAt:  d.FilterAt,
		Mode:      d.Mode,
		CreatedBy: d.By,
	}
	for _, t := range d.Tenants {
		ts.Tenants = append(ts.Tenants, TenantRef{AccountID: t.A, ProjectID: t.P})
	}
	ts.Tenants = NormalizeTenants(ts.Tenants)
	if err := ts.Validate(); err != nil {
		return Tombstone{}, err
	}
	return ts, nil
}

// idTime is the key time of a task id that is a nanosecond timestamp (what
// upstream VictoriaLogs/VictoriaTraces use as a task id), so that the same task
// always maps to the same key. ok is false for any other id, which then takes the
// creating pod's clock. The range keeps small numeric ids (tests, hand-made ids)
// off the time axis: 1e18 ns is 2001-09, 4e18 ns is 2096.
func idTime(id string) (int64, bool) {
	v, err := strconv.ParseInt(id, 10, 64)
	if err != nil || v < 1_000_000_000_000_000_000 || v > 4_000_000_000_000_000_000 {
		return 0, false
	}
	if strconv.FormatInt(v, 10) != id {
		return 0, false // one spelling per id
	}
	return v, true
}

// namedTenants reads only the "tenants" of a definition body that failed to
// decode, so that an unreadable definition can close the read gate for the tenants
// it names and no others. ok is false when even that cannot be read.
func namedTenants(body []byte) ([]TenantRef, bool) {
	var d struct {
		Tenants []defTenant `json:"tenants"`
	}
	if err := json.Unmarshal(body, &d); err != nil || len(d.Tenants) == 0 {
		return nil, false
	}
	out := make([]TenantRef, 0, len(d.Tenants))
	for _, t := range d.Tenants {
		out = append(out, TenantRef{AccountID: t.A, ProjectID: t.P})
	}
	return NormalizeTenants(out), true
}

// parseHexTime reads a 16-digit lower-case hex key time.
func parseHexTime(s string) (uint64, bool) {
	if len(s) != 16 {
		return 0, false
	}
	var ns uint64
	for i := 0; i < 16; i++ {
		c := s[i]
		var v byte
		switch {
		case c >= '0' && c <= '9':
			v = c - '0'
		case c >= 'a' && c <= 'f':
			v = c - 'a' + 10
		default:
			return 0, false
		}
		ns = ns<<4 | uint64(v)
	}
	return ns, true
}
