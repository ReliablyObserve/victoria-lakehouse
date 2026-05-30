package resourcebounds

import "testing"

// All four PrometheusSink setters share the same nil-safety contract:
// nil receiver and nil hook are both no-ops; non-nil hook is invoked
// exactly once with the supplied value. The Bound calls these on every
// Acquire / Release / try-block edge, so silently mis-routing or
// double-counting would corrupt operator dashboards.
//
// Each setter is covered by three rows: nil-receiver, nil-hook,
// hook-set. Hook-set asserts both invocation and the forwarded value
// to lock in "no off-by-one and no value mutation".

func TestPrometheusSink_AcquiredAdd(t *testing.T) {
	t.Run("nil_receiver", func(t *testing.T) {
		var s *PrometheusSink
		s.AcquiredAdd(7)
	})
	t.Run("nil_hook", func(t *testing.T) {
		s := &PrometheusSink{}
		s.AcquiredAdd(7)
	})
	t.Run("hook_set", func(t *testing.T) {
		var got int64
		s := &PrometheusSink{Acquired: func(n int64) { got += n }}
		s.AcquiredAdd(3)
		s.AcquiredAdd(4)
		if got != 7 {
			t.Fatalf("Acquired hook: want 7 want, got %d", got)
		}
	})
}

func TestPrometheusSink_RejectedAdd(t *testing.T) {
	t.Run("nil_receiver", func(t *testing.T) {
		var s *PrometheusSink
		s.RejectedAdd(11)
	})
	t.Run("nil_hook", func(t *testing.T) {
		s := &PrometheusSink{}
		s.RejectedAdd(11)
	})
	t.Run("hook_set", func(t *testing.T) {
		var got int64
		s := &PrometheusSink{Rejected: func(n int64) { got += n }}
		s.RejectedAdd(5)
		s.RejectedAdd(6)
		if got != 11 {
			t.Fatalf("Rejected hook: want 11, got %d", got)
		}
	})
}

func TestPrometheusSink_OutstandingBytesSet(t *testing.T) {
	t.Run("nil_receiver", func(t *testing.T) {
		var s *PrometheusSink
		s.OutstandingBytesSet(1024)
	})
	t.Run("nil_hook", func(t *testing.T) {
		s := &PrometheusSink{}
		s.OutstandingBytesSet(1024)
	})
	t.Run("hook_set", func(t *testing.T) {
		var got int64
		s := &PrometheusSink{OutstandingBytes: func(v int64) { got = v }}
		s.OutstandingBytesSet(1024)
		s.OutstandingBytesSet(2048)
		if got != 2048 {
			t.Fatalf("OutstandingBytes hook: want 2048 (last value, gauge semantics), got %d", got)
		}
	})
}

func TestPrometheusSink_OutstandingCountSet(t *testing.T) {
	t.Run("nil_receiver", func(t *testing.T) {
		var s *PrometheusSink
		s.OutstandingCountSet(4)
	})
	t.Run("nil_hook", func(t *testing.T) {
		s := &PrometheusSink{}
		s.OutstandingCountSet(4)
	})
	t.Run("hook_set", func(t *testing.T) {
		var got int64
		s := &PrometheusSink{OutstandingCount: func(v int64) { got = v }}
		s.OutstandingCountSet(4)
		s.OutstandingCountSet(8)
		if got != 8 {
			t.Fatalf("OutstandingCount hook: want 8 (last value, gauge semantics), got %d", got)
		}
	})
}

// All four hooks set simultaneously is the production wiring shape;
// this end-to-end sanity asserts none of the setters cross-talk.
func TestPrometheusSink_AllHooks_NoCrosstalk(t *testing.T) {
	var acq, rej, ob, oc int64
	s := &PrometheusSink{
		Acquired:         func(n int64) { acq += n },
		Rejected:         func(n int64) { rej += n },
		OutstandingBytes: func(v int64) { ob = v },
		OutstandingCount: func(v int64) { oc = v },
	}
	s.AcquiredAdd(1)
	s.RejectedAdd(2)
	s.OutstandingBytesSet(3)
	s.OutstandingCountSet(4)
	if acq != 1 || rej != 2 || ob != 3 || oc != 4 {
		t.Fatalf("hook crosstalk: acq=%d rej=%d ob=%d oc=%d", acq, rej, ob, oc)
	}
}
