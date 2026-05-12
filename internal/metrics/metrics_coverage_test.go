package metrics

import (
	"testing"
)

func TestGauge_Add(t *testing.T) {
	g := NewGauge("test_gauge_add_cov")
	g.Set(10)
	g.Add(5)
	if got := g.Get(); got != 15 {
		t.Errorf("Gauge.Add: expected 15, got %d", got)
	}
	g.Add(-3)
	if got := g.Get(); got != 12 {
		t.Errorf("Gauge.Add negative: expected 12, got %d", got)
	}
}

func TestCounterVec_Add(t *testing.T) {
	cv := NewCounterVec("test_vec_add_cov", "op")
	cv.Add("read", 5)
	cv.Add("read", 3)
	if got := cv.Get("read"); got != 8 {
		t.Errorf("CounterVec.Add: expected 8, got %d", got)
	}
	cv.Add("write", 1)
	if got := cv.Get("write"); got != 1 {
		t.Errorf("CounterVec.Add write: expected 1, got %d", got)
	}
}
