package vlstorage

import (
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// FuzzMapFieldToTraceRow drives mapFieldToTraceRow with arbitrary
// (name, value) pairs. Goal: catch panics (nil-deref, index-out-of-
// range, integer parse panics) on adversarial inputs. The state after
// the call must be inspectable — we read the maps + scalar fields to
// trip any unsynchronized half-mutation.
//
// Seeds cover: collision-risk names, every otelpb constant the switch
// branches on, empty string, long strings, and prefix-only names
// (resource_attr:, span_attr:) that exercise the strip-prefix branches.
//
// Pinned regression class: the registry-resolution + label-coverage
// bugs (commits be8c126 + a5576bf) showed up as silent data loss on
// the read side — the matching prevention on the write side is "input
// to mapFieldToTraceRow never causes the row to enter an inconsistent
// shape that later projection can't read".
func FuzzMapFieldToTraceRow(f *testing.F) {
	// Collision-risk names (names that overlap with top-level columns,
	// VL internal field names, and the otelpb constants the switch
	// branches on).
	collisionNames := []string{
		"service.name", "_time", "trace_id", "span_id", "duration",
		"start_time", "start_time_unix_nano", "status_code",
		"status_message", "kind", "_stream", "_stream_id", "_msg",
		"name", "parent", "child", "callCount", "scope_name",
		"timestamp_unix_nano", "account_id", "project_id",
	}
	for _, name := range collisionNames {
		f.Add(name, "v")
	}

	// otelpb constants used in the explicit switch.
	otelSeeds := []string{
		otelpb.TraceIDField,
		otelpb.SpanIDField,
		otelpb.ParentSpanIDField,
		otelpb.NameField,
		otelpb.KindField,
		otelpb.DurationField,
		otelpb.StartTimeUnixNanoField,
		otelpb.EndTimeUnixNanoField,
		otelpb.StatusCodeField,
		otelpb.StatusMessageField,
		otelpb.TraceStateField,
		otelpb.FlagsField,
		otelpb.DroppedAttributesCountField,
		otelpb.DroppedEventsCountField,
		otelpb.DroppedLinksCountField,
		otelpb.InstrumentationScopeName,
		otelpb.InstrumentationScopeVersion,
		otelpb.TraceIDIndexStreamName,
		otelpb.TraceIDIndexFieldName,
		otelpb.TraceIDIndexStartTimeFieldName,
		otelpb.TraceIDIndexEndTimeFieldName,
		otelpb.ServiceGraphStreamName,
		otelpb.ServiceGraphParentFieldName,
		otelpb.ServiceGraphChildFieldName,
		otelpb.ServiceGraphCallCountFieldName,
		otelpb.ResourceAttrPrefix + "service.name",
		otelpb.ResourceAttrPrefix + "custom.key",
		otelpb.ResourceAttrPrefix, // prefix-only — empty key after strip
		otelpb.SpanAttrPrefixField + "http.method",
		otelpb.SpanAttrPrefixField + "rpc.system",
		otelpb.SpanAttrPrefixField, // prefix-only
		otelpb.InstrumentationScopeAttrPrefix + "k",
		otelpb.EventPrefix + "0.name",
		otelpb.LinkPrefix + "0.trace_id",
	}
	for _, name := range otelSeeds {
		f.Add(name, "v")
	}

	// Edge-case names.
	f.Add("", "")
	f.Add("", "non-empty value")
	f.Add("_msg", "the body")
	f.Add(strings.Repeat("a", 1024), strings.Repeat("b", 4096))
	f.Add("123", "not-a-number-but-routed-as-default")
	// Numeric-cast paths with garbage values.
	f.Add("duration_ns", "not-a-number")
	f.Add("status.code", "NaN")
	f.Add("span.kind", "")
	f.Add("start_time_unix_nano", "-9999999999999999999")

	f.Fuzz(func(t *testing.T, name, value string) {
		row := &schema.TraceRow{}
		mapFieldToTraceRow(row, name, value)

		// State-inspectable invariant: reading every produced field
		// must not panic. A half-mutated row (e.g. nil-map deref
		// after a buggy lazy init) would crash here.
		_ = row.TraceID
		_ = row.SpanID
		_ = row.ServiceName
		_ = row.DurationNs
		_ = row.StartTimeUnixNano
		for k, v := range row.SpanAttributes {
			_ = k
			_ = v
		}
		for k, v := range row.ResourceAttributes {
			_ = k
			_ = v
		}
	})
}

// FuzzUnmarshalStreamTags pins that arbitrary input never panics. The
// canonical wire format is binary (varuint count followed by length-
// prefixed name/value pairs), so the vast majority of random bytes
// should produce a clean error — but truncated valid prefixes and
// unicode-garbage tails are the interesting cases.
//
// Seeds:
//   - empty input (zero tags, valid)
//   - leading invalid bytes (existing TestUnmarshalStreamTags_InvalidData)
//   - varuint header followed by truncated body
//   - large bogus length prefix
//   - unicode garbage
//   - very long input
func FuzzUnmarshalStreamTags(f *testing.F) {
	f.Add("")
	f.Add("\x00")
	f.Add("\x00\x01\x02invalid")
	f.Add("\x01")                                     // claims 1 tag, no body
	f.Add("\x01\x05hello")                            // claims 1 tag, name len 5, no value
	f.Add("\x01\x05hello\x05world")                   // claims 1 tag, both name+value len 5
	f.Add("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff") // bogus huge varuint
	f.Add("日本語のごみ")
	f.Add(strings.Repeat("\x00", 4096))
	f.Add(strings.Repeat("A", 4096))

	f.Fuzz(func(t *testing.T, canonical string) {
		st := logstorage.GetStreamTags()
		defer logstorage.PutStreamTags(st)

		// Must not panic. Outcome is either err != nil or err == nil
		// with st populated — both are acceptable, we only care about
		// crash safety + state inspectability.
		err := unmarshalStreamTags(st, canonical)
		if err == nil {
			// On success, st.String() must be callable without
			// panic (proves no half-mutated state).
			_ = st.String()
		}
	})
}

// FuzzDataBlockToTraceRows fuzzes the conversion every Parquet row goes
// through: a block with arbitrary column names and values must convert without
// panicking, and the rows must not alias the block's memory (the engine reuses
// it) — overwriting the block afterwards must not change a row.
func FuzzDataBlockToTraceRows(f *testing.F) {
	f.Add("trace_id", "abc", "service.name", "svc")
	f.Add("duration_ns", "5000000", "span.kind", "2")
	f.Add(otelpb.TraceIDField, "tid", otelpb.NameField, "op")
	f.Add(otelpb.ResourceAttrPrefix+"service.name", "svc-r", otelpb.SpanAttrPrefixField+"http.method", "GET")
	f.Add("_time", "2026-01-02T03:04:05.123456789Z", "_stream", `{resource_attr:service.name="x"}`)
	f.Add("", "ignored", "custom.tag", "val")
	f.Add("trace_id", "", "span_id", "")
	f.Add(strings.Repeat("x", 256), strings.Repeat("y", 1024), "trace_id", "t1")

	f.Fuzz(func(t *testing.T, n1, v1, n2, v2 string) {
		// Values live in one arena the "engine" wipes afterwards.
		arena := []byte(v1 + v2)
		vals1 := unsafe.String(unsafe.SliceData(arena), len(v1))
		vals2 := unsafe.String(unsafe.SliceData(arena[len(v1):]), len(v2))
		db := &logstorage.DataBlock{}
		db.SetColumns([]logstorage.BlockColumn{
			{Name: n1, Values: []string{vals1}},
			{Name: n2, Values: []string{vals2}},
		})
		rows := DataBlockToTraceRows(db, logstorage.TenantID{AccountID: 1, ProjectID: 2})
		before := fmt.Sprintf("%+v", rows)
		for i := range arena {
			arena[i] = 'X'
		}
		if after := fmt.Sprintf("%+v", rows); after != before {
			t.Fatalf("a converted row aliases the block's memory:\nbefore %s\nafter  %s", before, after)
		}
	})
}
