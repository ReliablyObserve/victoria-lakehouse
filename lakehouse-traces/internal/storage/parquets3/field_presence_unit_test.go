package parquets3

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/fieldpresence"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func namesOfBuilder(t *testing.T, b *fieldpresence.Builder) []string {
	t.Helper()
	return append([]string(nil), b.Facet().Names...)
}

// The drift guard's golden: the field names a fixed span produces. When the
// conversion changes what it names, this fails; update the list and bump
// fieldpresence.NamingVersion in the same change, so a reader stops trusting the
// facets written under the old naming.
func TestFieldPresence_NamingGolden(t *testing.T) {
	const goldenNaming = 1
	if fieldpresence.NamingVersion != goldenNaming {
		t.Fatalf("NamingVersion = %d, golden is for %d: update the golden list with it", fieldpresence.NamingVersion, goldenNaming)
	}
	spans := []schema.TraceRow{{
		TimestampUnixNano: 1_700_000_000_000_000_000, StartTimeUnixNano: 1, TraceID: "t", SpanID: "s", ParentSpanID: "p", SpanName: "op", ServiceName: "svc",
		DurationNs: 5, StatusCode: 1, SpanKind: 2, HTTPMethod: "GET", Stream: `{a="b"}`, StreamID: strings.Repeat("ab", 24), URLFull: "http://x", ContainerID: "c",
		Body: "m", ResourceAttributes: map[string]string{"r.k": "v"}, SpanAttributes: map[string]string{"s.k": "v", "empty": ""}, ScopeAttributes: map[string]string{"sc.k": "v"},
		EventsJSON: `[{"event_name":"exc","event_attr:exception.type":"IOError"}]`, LinksJSON: `[{"link_span_id":"abc"}]`,
	}}
	want := "_msg,_stream,_stream_id,_time,duration,event:event_attr:exception.type:0,event:event_name:0,kind,link:link_span_id:0,name,parent_span_id,resource_attr:container.id,resource_attr:r.k,resource_attr:service.name,scope_attr:sc.k,span_attr:http.method,span_attr:s.k,span_attr:url.full,span_id,start_time_unix_nano,status_code,trace_id"
	if got := strings.Join(namesOfBuilder(t, presenceOfTraceRows(spans, nil)), ","); got != want {
		t.Errorf("span names changed:\n got %s\nwant %s", got, want)
	}
}

func TestFieldPresence_DayOfAnyTimestamp(t *testing.T) {
	for _, tc := range []struct {
		ns   int64
		want int32
	}{{0, 0}, {86_400_000_000_000, 1}, {86_399_999_999_999, 0}, {-1, -1}, {-86_400_000_000_000, -1}, {-86_400_000_000_001, -2}} {
		if got := presenceDay(presenceTracesRegistry, tc.ns); got != tc.want {
			t.Errorf("presenceDay(%d) = %d, want %d", tc.ns, got, tc.want)
		}
	}
}

// More distinct patterns of non-empty columns than the cache keeps: the answer
// lists every column that any row had.
func TestFieldPresence_PatternCacheOverflowIsExact(t *testing.T) {
	cols := []func(r *schema.TraceRow){
		func(r *schema.TraceRow) { r.ParentSpanID = "x" }, func(r *schema.TraceRow) { r.StatusMessage = "x" }, func(r *schema.TraceRow) { r.HTTPMethod = "x" },
		func(r *schema.TraceRow) { r.HTTPUrl = "x" }, func(r *schema.TraceRow) { r.DBSystem = "x" }, func(r *schema.TraceRow) { r.URLFull = "x" },
		func(r *schema.TraceRow) { r.ClientAddress = "x" }, func(r *schema.TraceRow) { r.ServerAddress = "x" }, func(r *schema.TraceRow) { r.RPCMethod = "x" },
		func(r *schema.TraceRow) { r.ExceptionType = "x" }, func(r *schema.TraceRow) { r.ContainerID = "x" }, func(r *schema.TraceRow) { r.K8sClusterName = "x" },
		func(r *schema.TraceRow) { r.CodeFunctionName = "x" }, func(r *schema.TraceRow) { r.Body = "x" },
	}
	n := 1 << len(cols)
	if n <= maxPresencePatterns {
		t.Fatal("the test must exceed the cache")
	}
	rows := make([]schema.TraceRow, n)
	for i := range rows {
		rows[i] = schema.TraceRow{TimestampUnixNano: int64(i), TraceID: "t", Stream: fmt.Sprintf(`{s="%d"}`, i%3)}
		for bit, set := range cols {
			if i>>bit&1 == 1 {
				set(&rows[i])
			}
		}
	}
	f := presenceOfTraceRows(rows, nil).Facet()
	var total uint64
	union := map[string]bool{}
	for _, e := range f.Entries {
		total += e.Rows
		for _, nm := range f.NamesOf(e) {
			union[nm] = true
		}
	}
	if total != uint64(n) {
		t.Fatalf("rows = %d, want %d", total, n)
	}
	// One name per column set above; each must be there.
	for _, want := range []string{"parent_span_id", "status_message", "span_attr:http.method", "span_attr:http.url", "span_attr:db.system", "span_attr:url.full",
		"span_attr:client.address", "span_attr:server.address", "span_attr:rpc.method", "span_attr:exception.type", "resource_attr:container.id",
		"resource_attr:k8s.cluster.name", "span_attr:code.function.name", "_msg"} {
		if !union[want] {
			t.Errorf("%s is missing from the facet", want)
		}
	}
}
