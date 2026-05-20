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

// TestGaugeVec_SetGet exercises GaugeVec.Set and GaugeVec.Get (previously 0%).
func TestGaugeVec_SetGet(t *testing.T) {
	gv := NewGaugeVec("test_gaugevec_setget_cov", "tenant")

	gv.Set("prod", 42)
	if got := gv.Get("prod"); got != 42 {
		t.Errorf("GaugeVec.Get(prod) = %d, want 42", got)
	}

	gv.Set("staging", 7)
	if got := gv.Get("staging"); got != 7 {
		t.Errorf("GaugeVec.Get(staging) = %d, want 7", got)
	}

	// Update the same label.
	gv.Set("prod", 100)
	if got := gv.Get("prod"); got != 100 {
		t.Errorf("GaugeVec.Get(prod) after update = %d, want 100", got)
	}
}

// TestFloatGaugeVec_SetGet exercises FloatGaugeVec.Set and FloatGaugeVec.Get (previously 0%).
func TestFloatGaugeVec_SetGet(t *testing.T) {
	fgv := NewFloatGaugeVec("test_floatgaugevec_setget_cov", "region")

	fgv.Set("us-east-1", 0.75)
	if got := fgv.Get("us-east-1"); got != 0.75 {
		t.Errorf("FloatGaugeVec.Get(us-east-1) = %f, want 0.75", got)
	}

	fgv.Set("eu-west-1", 0.5)
	if got := fgv.Get("eu-west-1"); got != 0.5 {
		t.Errorf("FloatGaugeVec.Get(eu-west-1) = %f, want 0.5", got)
	}

	// Update the same label.
	fgv.Set("us-east-1", 0.99)
	if got := fgv.Get("us-east-1"); got != 0.99 {
		t.Errorf("FloatGaugeVec.Get(us-east-1) after update = %f, want 0.99", got)
	}
}
