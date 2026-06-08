package vlstorage

import "testing"

// fakeGate denies exactly the streams in deny; allows everything else.
type fakeGate struct{ deny map[string]bool }

func (g fakeGate) AllowStream(_, _ uint32, stream string) bool { return !g.deny[stream] }

func TestSetBufferAuthoritative_Flip(t *testing.T) {
	orig := bufferAuthoritative
	t.Cleanup(func() { bufferAuthoritative = orig })

	SetBufferAuthoritative(true)
	if !bufferAuthoritative {
		t.Fatal("SetBufferAuthoritative(true) did not set the flip")
	}
	SetBufferAuthoritative(false)
	if bufferAuthoritative {
		t.Fatal("SetBufferAuthoritative(false) did not revert the flip")
	}
}

func TestFlushRowKeeper_GatePredicate(t *testing.T) {
	orig := globalCardinalityGate
	t.Cleanup(func() { globalCardinalityGate = orig })

	// No gate → keep everything.
	SetCardinalityGate(nil)
	keep := FlushRowKeeper()
	if !keep(1, 2, `{service.name="api-gateway"}`) {
		t.Fatal("nil gate must keep all rows")
	}
	if !keep(1, 2, "") {
		t.Fatal("empty stream must be kept")
	}

	// With a gate → drop only denied streams.
	SetCardinalityGate(fakeGate{deny: map[string]bool{`{service.name="blocked"}`: true}})
	keep = FlushRowKeeper()
	if keep(1, 2, `{service.name="blocked"}`) {
		t.Fatal("cardinality-denied stream must be dropped")
	}
	if !keep(1, 2, `{service.name="api-gateway"}`) {
		t.Fatal("allowed stream must be kept")
	}
	// Empty stream short-circuits before the gate (never denied).
	if !keep(1, 2, "") {
		t.Fatal("empty stream must be kept even with a gate")
	}
}
