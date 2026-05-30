package resourcebounds

// PrometheusSink wires a Bound's Metrics interface to four
// VictoriaMetrics-style sinks (acquired_total counter, rejected_total
// counter, outstanding_bytes gauge, outstanding_count gauge).
//
// Surface owners construct this with the four metric handles already
// registered against the surface's name prefix
// (e.g. "lakehouse_resourcebound_s3_concurrent_downloads_*"), then
// pass the sink into NewBound. This keeps the resourcebounds package
// agnostic of the global metric registry while preserving the
// per-surface label shape required by the spec.
//
// All hooks are nil-safe — operators can wire only the metrics they
// care about during development; production deployments wire all
// four for full operator visibility.
type PrometheusSink struct {
	Acquired         func(n int64)
	Rejected         func(n int64)
	OutstandingBytes func(v int64)
	OutstandingCount func(v int64)
}

// AcquiredAdd forwards to the configured hook if non-nil.
func (s *PrometheusSink) AcquiredAdd(n int64) {
	if s == nil || s.Acquired == nil {
		return
	}
	s.Acquired(n)
}

// RejectedAdd forwards to the configured hook if non-nil.
func (s *PrometheusSink) RejectedAdd(n int64) {
	if s == nil || s.Rejected == nil {
		return
	}
	s.Rejected(n)
}

// OutstandingBytesSet forwards to the configured hook if non-nil.
func (s *PrometheusSink) OutstandingBytesSet(v int64) {
	if s == nil || s.OutstandingBytes == nil {
		return
	}
	s.OutstandingBytes(v)
}

// OutstandingCountSet forwards to the configured hook if non-nil.
func (s *PrometheusSink) OutstandingCountSet(v int64) {
	if s == nil || s.OutstandingCount == nil {
		return
	}
	s.OutstandingCount(v)
}
