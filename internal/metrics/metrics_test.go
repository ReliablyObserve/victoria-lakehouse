package metrics

import (
	"bytes"
	"strings"
	"sync/atomic"
	"testing"
)

func newCounterVecFor(r *Registry, name, label string) *CounterVec {
	cv := &CounterVec{
		counters: make(map[string]*atomic.Uint64),
		name:     name,
		label:    label,
	}
	r.register(name, cv)
	return cv
}

func TestCounter(t *testing.T) {
	r := NewRegistry()
	c := &Counter{}
	r.register("test_counter", c)

	c.Inc()
	c.Inc()
	c.Add(3)

	if got := c.Get(); got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}

	var buf bytes.Buffer
	r.WritePrometheus(&buf)
	if !strings.Contains(buf.String(), "test_counter 5\n") {
		t.Fatalf("unexpected output: %s", buf.String())
	}
}

func TestGauge(t *testing.T) {
	r := NewRegistry()
	g := &Gauge{}
	r.register("test_gauge", g)

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

	var buf bytes.Buffer
	r.WritePrometheus(&buf)
	if !strings.Contains(buf.String(), "test_gauge 43\n") {
		t.Fatalf("unexpected output: %s", buf.String())
	}
}

func TestFloatGauge(t *testing.T) {
	r := NewRegistry()
	g := &FloatGauge{}
	r.register("test_float_gauge", g)

	g.Set(3.14)
	if got := g.Get(); got != 3.14 {
		t.Fatalf("expected 3.14, got %f", got)
	}

	var buf bytes.Buffer
	r.WritePrometheus(&buf)
	if !strings.Contains(buf.String(), "test_float_gauge 3.14\n") {
		t.Fatalf("unexpected output: %s", buf.String())
	}
}

func TestHistogram(t *testing.T) {
	r := NewRegistry()
	h := &Histogram{
		bounds: []float64{0.1, 0.5, 1.0},
		counts: make([]uint64, 4),
	}
	r.register("test_hist", h)

	h.Observe(0.05)
	h.Observe(0.3)
	h.Observe(0.8)
	h.Observe(5.0)

	var buf bytes.Buffer
	r.WritePrometheus(&buf)
	out := buf.String()

	expectations := []string{
		`test_hist_bucket{le="0.1"} 1`,
		`test_hist_bucket{le="0.5"} 2`,
		`test_hist_bucket{le="1"} 3`,
		`test_hist_bucket{le="+Inf"} 4`,
		`test_hist_sum 6.15`,
		`test_hist_count 4`,
	}
	for _, exp := range expectations {
		if !strings.Contains(out, exp) {
			t.Errorf("missing %q in output:\n%s", exp, out)
		}
	}
}

func TestCounterVec(t *testing.T) {
	r := NewRegistry()
	cv := newCounterVecFor(r, "test_vec", "method")

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

	var buf bytes.Buffer
	r.WritePrometheus(&buf)
	out := buf.String()

	if !strings.Contains(out, `test_vec{method="GET"} 2`) {
		t.Errorf("missing GET in output:\n%s", out)
	}
	if !strings.Contains(out, `test_vec{method="POST"} 1`) {
		t.Errorf("missing POST in output:\n%s", out)
	}
}

func TestInfoGauge(t *testing.T) {
	r := NewRegistry()
	ig := &InfoGauge{labels: `mode="logs",version="0.7.0"`}
	r.register("test_info", ig)

	var buf bytes.Buffer
	r.WritePrometheus(&buf)
	out := buf.String()

	if !strings.Contains(out, `test_info{mode="logs",version="0.7.0"} 1`) {
		t.Fatalf("unexpected output: %s", out)
	}
}

func TestGaugeFunc(t *testing.T) {
	r := NewRegistry()
	val := 99.0
	gf := &GaugeFunc{fn: func() float64 { return val }}
	r.register("test_gaugefunc", gf)

	var buf bytes.Buffer
	r.WritePrometheus(&buf)
	if !strings.Contains(buf.String(), "test_gaugefunc 99\n") {
		t.Fatalf("unexpected output: %s", buf.String())
	}
}

func TestRegistryOrder(t *testing.T) {
	r := NewRegistry()
	r.register("z_metric", &Counter{})
	r.register("a_metric", &Counter{})
	r.register("m_metric", &Counter{})

	var buf bytes.Buffer
	r.WritePrometheus(&buf)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")

	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d: %v", len(lines), lines)
	}
	if !strings.HasPrefix(lines[0], "z_metric") {
		t.Errorf("expected z_metric first (registration order), got %s", lines[0])
	}
	if !strings.HasPrefix(lines[1], "a_metric") {
		t.Errorf("expected a_metric second, got %s", lines[1])
	}
}

func TestDefaultRegistry(t *testing.T) {
	reg := Default()
	if reg == nil {
		t.Fatal("default registry is nil")
	}
	var buf bytes.Buffer
	reg.WritePrometheus(&buf)
	if buf.Len() == 0 {
		t.Fatal("default registry should have lakehouse metrics registered")
	}
}

func BenchmarkCounterInc(b *testing.B) {
	c := &Counter{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}

func BenchmarkCounterVecInc(b *testing.B) {
	cv := &CounterVec{
		counters: make(map[string]*atomic.Uint64),
		name:     "bench_vec",
		label:    "op",
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cv.Inc("GET")
		}
	})
}

func BenchmarkHistogramObserve(b *testing.B) {
	h := &Histogram{
		bounds: DefBuckets,
		counts: make([]uint64, len(DefBuckets)+1),
	}
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
	r := Default()
	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		buf.Reset()
		r.WritePrometheus(&buf)
	}
}
