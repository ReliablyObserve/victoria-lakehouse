package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestCounter(t *testing.T) {
	c := NewCounter("test_counter_basic")
	c.Inc()
	c.Inc()
	c.Add(3)

	if got := c.Get(); got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
}

func TestGauge(t *testing.T) {
	g := NewGauge("test_gauge_basic")
	g.Set(42)
	if got := g.Get(); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}

	g.Inc()
	g.Inc()
	g.Dec()
	if got := g.Get(); got != 43 {
		t.Fatalf("expected 43, got %d", got)
	}
}

func TestFloatGauge(t *testing.T) {
	g := NewFloatGauge("test_float_gauge_basic")
	g.Set(3.14)
	if got := g.Get(); got != 3.14 {
		t.Fatalf("expected 3.14, got %f", got)
	}
}

func TestHistogram(t *testing.T) {
	h := NewHistogram("test_hist_basic", DefBuckets)
	h.Observe(0.05)
	h.Observe(0.3)
	h.Observe(0.8)
	h.Observe(5.0)

	var buf bytes.Buffer
	WritePrometheus(&buf, false)
	out := buf.String()

	if !strings.Contains(out, "test_hist_basic") {
		t.Fatalf("histogram not in output:\n%s", out)
	}
}

func TestCounterVec(t *testing.T) {
	cv := NewCounterVec("test_vec_basic", "method")
	cv.Inc("GET")
	cv.Inc("GET")
	cv.Inc("POST")

	if got := cv.Get("GET"); got != 2 {
		t.Fatalf("expected GET=2, got %d", got)
	}
	if got := cv.Get("POST"); got != 1 {
		t.Fatalf("expected POST=1, got %d", got)
	}
	if got := cv.Get("DELETE"); got != 0 {
		t.Fatalf("expected DELETE=0, got %d", got)
	}
}

func TestCounterVecPrometheusOutput(t *testing.T) {
	cv := NewCounterVec("test_vec_prom", "op")
	cv.Inc("read")

	var buf bytes.Buffer
	WritePrometheus(&buf, false)
	out := buf.String()

	if !strings.Contains(out, `test_vec_prom{op="read"}`) {
		t.Fatalf("missing counter vec in output:\n%s", out)
	}
}

func TestInfoGauge(t *testing.T) {
	NewInfoGauge("test_info_basic", map[string]string{
		"version": "0.7.0",
		"mode":    "logs",
	})

	var buf bytes.Buffer
	WritePrometheus(&buf, false)
	out := buf.String()

	if !strings.Contains(out, `test_info_basic{mode="logs",version="0.7.0"}`) {
		t.Fatalf("info gauge not in output:\n%s", out)
	}
}

func TestGaugeFunc(t *testing.T) {
	val := 99.0
	NewGaugeFunc("test_gaugefunc_basic", func() float64 { return val })

	var buf bytes.Buffer
	WritePrometheus(&buf, false)
	if !strings.Contains(buf.String(), "test_gaugefunc_basic 99") {
		t.Fatalf("gauge func not in output:\n%s", buf.String())
	}
}

func TestWritePrometheusContainsLakehouseMetrics(t *testing.T) {
	var buf bytes.Buffer
	WritePrometheus(&buf, false)
	out := buf.String()
	if !strings.Contains(out, "lakehouse_") {
		t.Fatal("WritePrometheus should include lakehouse metrics from init")
	}
}

func BenchmarkCounterInc(b *testing.B) {
	c := NewCounter("bench_counter_inc")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}

func BenchmarkCounterVecInc(b *testing.B) {
	cv := NewCounterVec("bench_vec_inc", "op")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cv.Inc("GET")
		}
	})
}

func BenchmarkHistogramObserve(b *testing.B) {
	h := NewHistogram("bench_hist_observe", DefBuckets)
	b.RunParallel(func(pb *testing.PB) {
		v := 0.01
		for pb.Next() {
			h.Observe(v)
			v += 0.001
			if v > 10 {
				v = 0.01
			}
		}
	})
}

func BenchmarkWritePrometheus(b *testing.B) {
	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WritePrometheus(&buf, false)
	}
}
