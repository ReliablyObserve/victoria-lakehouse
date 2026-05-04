package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var defaultRegistry = NewRegistry()

func Default() *Registry { return defaultRegistry }

type Registry struct {
	mu      sync.RWMutex
	metrics map[string]metric
	order   []string
}

type metric interface {
	writePrometheus(w io.Writer, name string)
}

func NewRegistry() *Registry {
	return &Registry{metrics: make(map[string]metric)}
}

func (r *Registry) register(name string, m metric) {
	r.mu.Lock()
	if _, ok := r.metrics[name]; !ok {
		r.order = append(r.order, name)
	}
	r.metrics[name] = m
	r.mu.Unlock()
}

func (r *Registry) WritePrometheus(w io.Writer) {
	r.mu.RLock()
	names := make([]string, len(r.order))
	copy(names, r.order)
	r.mu.RUnlock()

	for _, name := range names {
		r.mu.RLock()
		m := r.metrics[name]
		r.mu.RUnlock()
		m.writePrometheus(w, name)
	}
}

// Counter is a monotonically increasing counter.
type Counter struct {
	v atomic.Uint64
}

func NewCounter(name string) *Counter {
	c := &Counter{}
	defaultRegistry.register(name, c)
	return c
}

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(delta int) { c.v.Add(uint64(delta)) }
func (c *Counter) Get() uint64   { return c.v.Load() }

func (c *Counter) writePrometheus(w io.Writer, name string) {
	_, _ = fmt.Fprintf(w, "%s %d\n", name, c.v.Load())
}

// Gauge is a value that can go up and down.
type Gauge struct {
	v atomic.Int64
}

func NewGauge(name string) *Gauge {
	g := &Gauge{}
	defaultRegistry.register(name, g)
	return g
}

func (g *Gauge) Set(v int64) { g.v.Store(v) }
func (g *Gauge) Inc()        { g.v.Add(1) }
func (g *Gauge) Dec()        { g.v.Add(-1) }
func (g *Gauge) Add(v int64) { g.v.Add(v) }
func (g *Gauge) Get() int64  { return g.v.Load() }

func (g *Gauge) writePrometheus(w io.Writer, name string) {
	_, _ = fmt.Fprintf(w, "%s %d\n", name, g.v.Load())
}

// FloatGauge stores a float64 value.
type FloatGauge struct {
	v atomic.Uint64
}

func NewFloatGauge(name string) *FloatGauge {
	g := &FloatGauge{}
	defaultRegistry.register(name, g)
	return g
}

func (g *FloatGauge) Set(v float64) { g.v.Store(math.Float64bits(v)) }
func (g *FloatGauge) Get() float64  { return math.Float64frombits(g.v.Load()) }

func (g *FloatGauge) writePrometheus(w io.Writer, name string) {
	_, _ = fmt.Fprintf(w, "%s %g\n", name, g.Get())
}

// Histogram tracks value distributions in predefined buckets.
type Histogram struct {
	mu      sync.Mutex
	bounds  []float64
	counts  []uint64
	sum     float64
	count   uint64
}

func NewHistogram(name string, buckets []float64) *Histogram {
	sorted := make([]float64, len(buckets))
	copy(sorted, buckets)
	sort.Float64s(sorted)
	h := &Histogram{
		bounds: sorted,
		counts: make([]uint64, len(sorted)+1),
	}
	defaultRegistry.register(name, h)
	return h
}

func (h *Histogram) Observe(v float64) {
	idx := sort.SearchFloat64s(h.bounds, v)
	h.mu.Lock()
	h.counts[idx]++
	h.sum += v
	h.count++
	h.mu.Unlock()
}

func (h *Histogram) writePrometheus(w io.Writer, name string) {
	h.mu.Lock()
	counts := make([]uint64, len(h.counts))
	copy(counts, h.counts)
	sum := h.sum
	count := h.count
	h.mu.Unlock()

	var cumulative uint64
	for i, bound := range h.bounds {
		cumulative += counts[i]
		_, _ = fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, bound, cumulative)
	}
	cumulative += counts[len(h.bounds)]
	_, _ = fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, cumulative)
	_, _ = fmt.Fprintf(w, "%s_sum %g\n", name, sum)
	_, _ = fmt.Fprintf(w, "%s_count %d\n", name, count)
}

// DefBuckets are default histogram buckets for latency in seconds.
var DefBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// CounterVec is a set of counters indexed by label values.
type CounterVec struct {
	mu       sync.RWMutex
	counters map[string]*atomic.Uint64
	name     string
	label    string
}

func NewCounterVec(name, label string) *CounterVec {
	cv := &CounterVec{
		counters: make(map[string]*atomic.Uint64),
		name:     name,
		label:    label,
	}
	defaultRegistry.register(name, cv)
	return cv
}

func (cv *CounterVec) Inc(labelValue string) {
	cv.get(labelValue).Add(1)
}

func (cv *CounterVec) Add(labelValue string, delta int) {
	cv.get(labelValue).Add(uint64(delta))
}

func (cv *CounterVec) Get(labelValue string) uint64 {
	cv.mu.RLock()
	c, ok := cv.counters[labelValue]
	cv.mu.RUnlock()
	if !ok {
		return 0
	}
	return c.Load()
}

func (cv *CounterVec) get(labelValue string) *atomic.Uint64 {
	cv.mu.RLock()
	c, ok := cv.counters[labelValue]
	cv.mu.RUnlock()
	if ok {
		return c
	}
	cv.mu.Lock()
	c, ok = cv.counters[labelValue]
	if !ok {
		c = &atomic.Uint64{}
		cv.counters[labelValue] = c
	}
	cv.mu.Unlock()
	return c
}

func (cv *CounterVec) writePrometheus(w io.Writer, name string) {
	cv.mu.RLock()
	keys := make([]string, 0, len(cv.counters))
	for k := range cv.counters {
		keys = append(keys, k)
	}
	cv.mu.RUnlock()
	sort.Strings(keys)

	for _, k := range keys {
		cv.mu.RLock()
		c := cv.counters[k]
		cv.mu.RUnlock()
		_, _ = fmt.Fprintf(w, "%s{%s=%q} %d\n", name, cv.label, k, c.Load())
	}
}

// GaugeFunc is a gauge backed by a callback function.
type GaugeFunc struct {
	fn func() float64
}

func NewGaugeFunc(name string, fn func() float64) *GaugeFunc {
	gf := &GaugeFunc{fn: fn}
	defaultRegistry.register(name, gf)
	return gf
}

func (gf *GaugeFunc) writePrometheus(w io.Writer, name string) {
	_, _ = fmt.Fprintf(w, "%s %g\n", name, gf.fn())
}

// InfoGauge emits a gauge with value 1 and static labels for metadata.
type InfoGauge struct {
	labels string
}

func NewInfoGauge(name string, labels map[string]string) *InfoGauge {
	parts := make([]string, 0, len(labels))
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, labels[k]))
	}
	ig := &InfoGauge{labels: strings.Join(parts, ",")}
	defaultRegistry.register(name, ig)
	return ig
}

func (ig *InfoGauge) writePrometheus(w io.Writer, name string) {
	_, _ = fmt.Fprintf(w, "%s{%s} 1\n", name, ig.labels)
}
