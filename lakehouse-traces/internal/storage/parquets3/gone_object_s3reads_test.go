package parquets3

// The S3 reads of one Jaeger trace-by-ID when a listed object is gone (#772): the
// footer lookup (LookupTraceIndex) lists the tenant's objects over all time, so a
// gone object anywhere in that listing leaves an index entry unknown and the lookup
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
)

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
		code  int
		body  string
		gets  int64
		spans int
	}
	run := func(t *testing.T, gone string, warm bool) result {
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
		if warm {
			if code, body := rcGet(fx, path, params()); code != http.StatusOK || strings.Contains(body, "not found") {
				t.Fatalf("warm-up: %d %.100q", code, body)
			}
		}
		if key != "" {
			fx.removeObject(key)
		}
		before := fx.fs.gets.Load()
		code, body := rcGet(fx, path, params())
		return result{code: code, body: body, gets: fx.fs.gets.Load() - before, spans: strings.Count(body, `"spanID"`)}
	}
	for _, warm := range []bool{false, true} {
		for _, gone := range []string{"none", "inside_window", "outside_window"} {
			t.Run(fmt.Sprintf("gone=%s/warm_footers=%v", gone, warm), func(t *testing.T) {
				r := run(t, gone, warm)
				t.Logf("S3READS gone=%-14s warm_footers=%-5v -> %d, %d GETs, %d spans", gone, warm, r.code, r.gets, r.spans)
				if strings.Contains(r.body, "not found") {
					t.Fatalf("%d %.200q: a trace that exists reported as not found", r.code, r.body)
				}
				switch gone {
				case "none", "outside_window":
					if r.code != http.StatusOK || r.spans != 7 {
						t.Fatalf("%d with %d spans %.200q, want 200 with the trace's 7 spans", r.code, r.spans, r.body)
					}
				case "inside_window":
					if warm {
						// The warm-up left the object's body in the cache: the read is
						// served from it, complete and exact, with no S3 read.
						if r.code != http.StatusOK || r.spans != 7 {
							t.Fatalf("%d with %d spans %.200q, want the cached trace", r.code, r.spans, r.body)
						}
						return
					}
					if r.code == http.StatusOK {
						t.Fatalf("200 %.200q for a read that listed a gone object", r.body)
					}
				}
			})
		}
	}
}
