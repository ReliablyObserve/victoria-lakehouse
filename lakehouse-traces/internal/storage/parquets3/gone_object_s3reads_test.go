package parquets3

// The S3 reads of one Jaeger trace-by-ID when a listed object is gone (#772): the
// footer lookup (LookupTraceIndex) lists the tenant's objects over all time, so a
// gone object anywhere in that listing leaves an index entry unknown (unless the
// in-RAM pmeta bloom facet proves the object never held the trace) and the lookup
// reports a miss, and the span scan of the query window answers (and fails for an
// object inside the window). The numbers are logged for the PR record
// (go test -v -run TestTraceByID_GoneObject_S3Reads); the assertions are the
// answers: never a short 200, never a 404.

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
)

// installBloomFacet fills the in-RAM pmeta bloom facet of the fixture's partition
// with the trace ids each object holds (the far object, if any, holds object 0's).
// With mayContain set, that object's bloom also lists tid, so the facet cannot
// exclude it.
func installBloomFacet(fx *rcFixture, extraKeys []string, mayContain, tid string) {
	fx.s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "traces/")
	add := func(key string, file int) {
		var ids []string
		for j := 0; j < rcRowsPerFile; j++ {
			ids = append(ids, rcTraceID(file, j))
		}
		if key == mayContain {
			ids = append(ids, tid)
		}
		fx.s.catalog.OnFileFlush(pmeta.FileContribution{Partition: manifest.ExtractTenantPartition(key), FileKey: key,
			BloomValues: map[string][]string{"trace_id": ids}})
	}
	for i, k := range fx.keys {
		add(k, i)
	}
	for _, k := range extraKeys {
		add(k, 0)
	}
	// Production objects attest lowercase-hex trace ids in their footer, which is
	// what lets the span scan (VT's phrase form trace_id:"X") prune by the bloom.
	for _, fi := range fx.s.manifest.GetFilesForRange(0, 1<<62) {
		fx.s.manifest.RemoveFile(fx.partitionOf(fi.Key), fi.Key)
		fi.TraceIDHex = true
		fx.s.manifest.AddFile(fx.partitionOf(fi.Key), fi)
	}
}

func TestTraceByID_GoneObject_S3Reads(t *testing.T) {
	tid := rcTraceID(1, 0) // the trace is in object 1
	params := func() url.Values {
		return url.Values{
			"start": {fmt.Sprint(time.Now().Add(-3 * time.Hour).UnixMicro())},
			"end":   {fmt.Sprint(time.Now().Add(time.Hour).UnixMicro())},
		}
	}
	path := "/select/jaeger/api/traces/" + tid
	type result struct {
		code   int
		body   string
		gets   int64
		spans  int
		hits   uint64 // lakehouse_trace_index_lookups_total{result="hit"} during the request
		misses uint64
	}
	run := func(t *testing.T, gone string, warm bool, facet string) result {
		fx := newRCFixture(t, 4, rcKeyFmt)
		rcServe(t, fx)
		var key string
		switch gone {
		case "inside_window":
			key = fx.keys[2] // an object of the window the trace is not in
		case "outside_window":
			// An object 30 days before the window: in the all-time listing of the
			// footer lookup, never read by the scan of the window.
			far := fx.base.Add(-30 * 24 * time.Hour)
			key = fmt.Sprintf("traces/dt=%s/hour=%02d/rc-far.parquet", far.Format("2006-01-02"), far.Hour())
			registerFileInMockS3(t, fx.s, fx.fs.mock, key, fx.datas[0], far)
		}
		if facet != "off" {
			// "excludes": the facet proves the gone object never held the trace.
			// "may_contain": its bloom lists the trace, so the facet cannot exclude it.
			var extra []string
			if gone == "outside_window" {
				extra = []string{key}
			}
			mayContain := ""
			if facet == "may_contain" {
				mayContain = key
			}
			installBloomFacet(fx, extra, mayContain, tid)
		}
		if warm {
			if code, body := rcGet(fx, path, params()); code != http.StatusOK || strings.Contains(body, "not found") {
				t.Fatalf("warm-up: %d %.100q", code, body)
			}
		}
		if key != "" {
			fx.removeObject(key)
		}
		before, hitsBefore, missBefore := fx.fs.gets.Load(), metrics.TraceIndexLookups.Get("hit"), metrics.TraceIndexLookups.Get("miss")+metrics.TraceIndexLookups.Get("error")
		code, body := rcGet(fx, path, params())
		return result{code: code, body: body, gets: fx.fs.gets.Load() - before, spans: strings.Count(body, `"spanID"`),
			hits: metrics.TraceIndexLookups.Get("hit") - hitsBefore, misses: metrics.TraceIndexLookups.Get("miss") + metrics.TraceIndexLookups.Get("error") - missBefore}
	}
	// The reads of the healthy trace-by-ID with the same facet: what the fast path
	// costs when nothing is gone.
	baseline := run(t, "none", false, "excludes").gets
	for _, c := range []struct {
		warm  bool
		gone  string
		facet string
	}{
		{false, "none", "off"}, {false, "inside_window", "off"}, {false, "outside_window", "off"},
		{true, "none", "off"}, {true, "inside_window", "off"}, {true, "outside_window", "off"},
		{false, "none", "excludes"}, {false, "inside_window", "excludes"}, {false, "outside_window", "excludes"},
		{false, "inside_window", "may_contain"}, {false, "outside_window", "may_contain"},
	} {
		warm, gone, facet := c.warm, c.gone, c.facet
		t.Run(fmt.Sprintf("gone=%s/warm_footers=%v/bloom_facet=%s", gone, warm, facet), func(t *testing.T) {
			r := run(t, gone, warm, facet)
			t.Logf("S3READS gone=%-14s warm_footers=%-5v bloom_facet=%-11s -> %d, %d GETs, %d spans", gone, warm, facet, r.code, r.gets, r.spans)
			if strings.Contains(r.body, "not found") {
				t.Fatalf("%d %.200q: a trace that exists reported as not found", r.code, r.body)
			}
			if facet == "excludes" && !warm {
				// The facet proves the gone object never held the trace: the lookup
				// keeps its fast path. Inside the window that costs nothing over a
				// healthy read; outside it only the footer GET of the listed object.
				limit := baseline
				if gone == "outside_window" {
					limit++
				}
				if r.gets > limit {
					t.Fatalf("%d GETs, want at most %d (the fast path of a healthy trace-by-ID plus the gone object's own footer GET)", r.gets, limit)
				}
			}
			if gone == "inside_window" && !warm && (facet == "excludes" || facet == "off" || facet == "may_contain") {
				// The stage-1 lookup is a hit only when the facet proves the gone
				// object irrelevant: without the gate this end-to-end test fails.
				if facet == "excludes" && r.hits == 0 {
					t.Fatalf("lakehouse_trace_index_lookups_total{result=hit} did not rise: the bloom facet did not keep the stage-1 hit (misses %d)", r.misses)
				}
				if facet != "excludes" && r.hits != 0 {
					t.Fatalf("stage-1 lookup was a hit (%d) with a gone object the facet cannot exclude", r.hits)
				}
			}
			complete := gone == "none" || gone == "outside_window" || warm || facet == "excludes"
			if complete {
				if r.code != http.StatusOK || r.spans != 7 {
					t.Fatalf("%d with %d spans %.200q, want 200 with the trace's 7 spans", r.code, r.spans, r.body)
				}
				return
			}
			// A gone object inside the window that nothing proves empty of the trace:
			// the read fails (a warm cache serves the object and is complete above).
			if r.code == http.StatusOK {
				t.Fatalf("200 %.200q for a read that listed a gone object", r.body)
			}
		})
	}
}
