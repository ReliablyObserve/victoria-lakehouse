package parquets3

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/fieldpresence"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// namesOfBuilder is the sorted union of the names the builder holds.
func namesOfBuilder(t *testing.T, b *fieldpresence.Builder) []string {
	t.Helper()
	f := b.Facet()
	return append([]string(nil), f.Names...)
}

// The drift guard's golden: the field names a fixed corpus of rows produces. When
// the conversion changes what it names, this fails; update the list and bump
// fieldpresence.NamingVersion in the same change, so a reader stops trusting the
// facets written under the old naming.
func TestFieldPresence_NamingGolden(t *testing.T) {
	const goldenNaming = 1
	if fieldpresence.NamingVersion != goldenNaming {
		t.Fatalf("NamingVersion = %d, golden is for %d: update the golden list with it", fieldpresence.NamingVersion, goldenNaming)
	}
	logs := []schema.LogRow{{
		TimestampUnixNano: 1_700_000_000_000_000_000, Body: "m", SeverityText: "INFO", SeverityNumber: 9, ServiceName: "svc", TraceID: "t", SpanID: "s",
		K8sPodName: "pod", Stream: `{a="b"}`, StreamID: strings.Repeat("ab", 24), ScopeName: "scope", ContainerID: "c", ExceptionType: "E",
		ResourceAttributes: map[string]string{"res.k": "v"}, LogAttributes: map[string]string{"log.k": "v", "empty": ""}, ScopeAttributes: map[string]string{"scope.k": "v"},
	}}
	want := "_msg,_stream,_stream_id,_time,container.id,exception.type,k8s.pod.name,level,log.k,res.k,scope.k,scope.name,service.name,severity_number,span_id,trace_id"
	if got := strings.Join(namesOfBuilder(t, presenceOfLogRows(logs, nil)), ","); got != want {
		t.Errorf("logs names changed:\n got %s\nwant %s", got, want)
	}
}

// presenceDay follows the counter's day for any timestamp, also the ones the
// division shortcut does not take.
func TestFieldPresence_DayOfAnyTimestamp(t *testing.T) {
	for _, tc := range []struct {
		ns   int64
		want int32
	}{{0, 0}, {86_400_000_000_000, 1}, {86_399_999_999_999, 0}, {-1, -1}, {-86_400_000_000_000, -1}, {-86_400_000_000_001, -2}} {
		if got := presenceDay(presenceLogsRegistry, tc.ns); got != tc.want {
			t.Errorf("presenceDay(%d) = %d, want %d", tc.ns, got, tc.want)
		}
	}
}

// More distinct patterns of non-empty columns than the cache keeps: the answer
// is the same as with room for all of them.
func TestFieldPresence_PatternCacheOverflowIsExact(t *testing.T) {
	cols := []func(r *schema.LogRow){
		func(r *schema.LogRow) { r.TraceID = "x" }, func(r *schema.LogRow) { r.SpanID = "x" }, func(r *schema.LogRow) { r.K8sPodName = "x" },
		func(r *schema.LogRow) { r.K8sNodeName = "x" }, func(r *schema.LogRow) { r.HostName = "x" }, func(r *schema.LogRow) { r.CloudRegion = "x" },
		func(r *schema.LogRow) { r.ContainerID = "x" }, func(r *schema.LogRow) { r.ServiceVersion = "x" }, func(r *schema.LogRow) { r.ExceptionType = "x" },
		func(r *schema.LogRow) { r.OSType = "x" }, func(r *schema.LogRow) { r.HostArch = "x" }, func(r *schema.LogRow) { r.CloudProvider = "x" },
		func(r *schema.LogRow) { r.ProcessRuntimeName = "x" }, func(r *schema.LogRow) { r.K8sClusterName = "x" },
	}
	n := 1 << len(cols) // 16384 patterns > maxPresencePatterns
	if n <= maxPresencePatterns {
		t.Fatal("the test must exceed the cache")
	}
	rows := make([]schema.LogRow, n)
	for i := range rows {
		rows[i] = schema.LogRow{TimestampUnixNano: int64(i), Stream: fmt.Sprintf(`{s="%d"}`, i%3)}
		for bit, set := range cols {
			if i>>bit&1 == 1 {
				set(&rows[i])
			}
		}
	}
	f := presenceOfLogRows(rows, nil).Facet()
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
	var got []string
	for nm := range union {
		got = append(got, nm)
	}
	sort.Strings(got)
	for _, want := range []string{"trace_id", "span_id", "k8s.pod.name", "k8s.node.name", "host.name", "cloud.region", "container.id", "service.version", "exception.type", "os.type", "host.arch", "cloud.provider", "process.runtime.name", "k8s.cluster.name"} {
		if !union[want] {
			t.Errorf("%s is missing from the facet (have %v)", want, got)
		}
	}
}

// Spans: the span event and link fields come from the JSON columns of the span.
func TestFieldPresence_SpanRowsListTheirFields(t *testing.T) {
	spans := []schema.TraceRow{{
		TimestampUnixNano: 1_700_000_000_000_000_000, StartTimeUnixNano: 1, TraceID: "t", SpanID: "s", SpanName: "op", ServiceName: "svc", StreamID: strings.Repeat("cd", 24),
		SpanAttributes: map[string]string{"http.route": "/x"}, ResourceAttributes: map[string]string{"r.k": "v"},
	}}
	names := namesOfBuilder(t, presenceOfTraceRows(spans, nil))
	got := strings.Join(names, ",")
	for _, want := range []string{"_time", "trace_id", "span_id", "name"} {
		if !strings.Contains(","+got+",", ","+want+",") {
			t.Errorf("%s not listed: %s", want, got)
		}
	}
}
