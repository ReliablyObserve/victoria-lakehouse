package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"

	vmmetrics "github.com/VictoriaMetrics/metrics"
)

// WritePrometheus writes all registered metrics in Prometheus text format.
func WritePrometheus(w io.Writer, exposeProcessMetrics bool) {
	vmmetrics.WritePrometheus(w, exposeProcessMetrics)
}

// Counter is a monotonically increasing counter backed by VictoriaMetrics/metrics.
type Counter struct {
	c *vmmetrics.Counter
}

func NewCounter(name string) *Counter {
	return &Counter{c: vmmetrics.NewCounter(name)}
}

func (c *Counter) Inc()          { c.c.Inc() }
func (c *Counter) Add(delta int) { c.c.Add(delta) }
func (c *Counter) Get() uint64   { return c.c.Get() }

// Gauge is a value that can go up and down, backed by VictoriaMetrics/metrics.
type Gauge struct {
	g *vmmetrics.Gauge
}

func NewGauge(name string) *Gauge {
	return &Gauge{g: vmmetrics.NewGauge(name, nil)}
}

func (g *Gauge) Set(v int64) { g.g.Set(float64(v)) }
func (g *Gauge) Inc()        { g.g.Inc() }
func (g *Gauge) Dec()        { g.g.Dec() }
func (g *Gauge) Add(v int64) { g.g.Add(float64(v)) }
func (g *Gauge) Get() int64  { return int64(g.g.Get()) }

// FloatGauge stores a float64 value, backed by VictoriaMetrics/metrics.
type FloatGauge struct {
	g *vmmetrics.Gauge
}

func NewFloatGauge(name string) *FloatGauge {
	return &FloatGauge{g: vmmetrics.NewGauge(name, nil)}
}

func (g *FloatGauge) Set(v float64) { g.g.Set(v) }
func (g *FloatGauge) Get() float64  { return g.g.Get() }

// Histogram tracks value distributions, backed by VictoriaMetrics/metrics.
// The buckets parameter is accepted for API compatibility but ignored;
// VM histograms use automatic bucket ranges.
type Histogram struct {
	h *vmmetrics.Histogram
}

func NewHistogram(name string, _ []float64) *Histogram {
	return &Histogram{h: vmmetrics.NewHistogram(name)}
}

func (h *Histogram) Observe(v float64) { h.h.Update(v) }

// DefBuckets kept for API compatibility (ignored by VM histograms).
var DefBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// CounterVec is a set of counters indexed by a single label value,
// backed by VictoriaMetrics/metrics GetOrCreateCounter.
type CounterVec struct {
	name  string
	label string
}

func NewCounterVec(name, label string) *CounterVec {
	return &CounterVec{name: name, label: label}
}

func (cv *CounterVec) Inc(labelValue string) {
	vmmetrics.GetOrCreateCounter(fmt.Sprintf(`%s{%s=%q}`, cv.name, cv.label, labelValue)).Inc()
}

func (cv *CounterVec) Add(labelValue string, delta int) {
	vmmetrics.GetOrCreateCounter(fmt.Sprintf(`%s{%s=%q}`, cv.name, cv.label, labelValue)).Add(delta)
}

func (cv *CounterVec) Get(labelValue string) uint64 {
	return vmmetrics.GetOrCreateCounter(fmt.Sprintf(`%s{%s=%q}`, cv.name, cv.label, labelValue)).Get()
}

// GaugeVec is a set of gauges indexed by a single label value.
type GaugeVec struct {
	name  string
	label string
}

func NewGaugeVec(name, label string) *GaugeVec {
	return &GaugeVec{name: name, label: label}
}

func (gv *GaugeVec) Set(labelValue string, v int64) {
	vmmetrics.GetOrCreateGauge(fmt.Sprintf(`%s{%s=%q}`, gv.name, gv.label, labelValue), nil).Set(float64(v))
}

func (gv *GaugeVec) Get(labelValue string) int64 {
	return int64(vmmetrics.GetOrCreateGauge(fmt.Sprintf(`%s{%s=%q}`, gv.name, gv.label, labelValue), nil).Get())
}

// FloatGaugeVec is a set of float gauges indexed by a single label value.
type FloatGaugeVec struct {
	name  string
	label string
}

func NewFloatGaugeVec(name, label string) *FloatGaugeVec {
	return &FloatGaugeVec{name: name, label: label}
}

func (gv *FloatGaugeVec) Set(labelValue string, v float64) {
	vmmetrics.GetOrCreateGauge(fmt.Sprintf(`%s{%s=%q}`, gv.name, gv.label, labelValue), nil).Set(v)
}

func (gv *FloatGaugeVec) Get(labelValue string) float64 {
	return vmmetrics.GetOrCreateGauge(fmt.Sprintf(`%s{%s=%q}`, gv.name, gv.label, labelValue), nil).Get()
}

// NewGaugeFunc creates a gauge backed by a callback function.
func NewGaugeFunc(name string, fn func() float64) {
	vmmetrics.NewGauge(name, fn)
}

// NewInfoGauge emits a counter with value 1 and static labels for metadata.
func NewInfoGauge(name string, labels map[string]string) {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, labels[k]))
	}
	vmmetrics.GetOrCreateCounter(fmt.Sprintf(`%s{%s}`, name, strings.Join(parts, ","))).Set(1)
}
