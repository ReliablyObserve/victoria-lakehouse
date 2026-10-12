package vlstorage

import "testing"

// fakeGate denies exactly the streams in deny; allows everything else.
type fakeGate struct{ deny map[string]bool }

func (g fakeGate) AllowStream(_, _ uint32, stream string) bool { return !g.deny[stream] }

// The flush filter does not consult the tenant cardinality gate: a row in the
// insert buffer was admitted when it was added, and the gate (process-local,
// empty after a restart) must not judge it again (#389). The insert path still
// applies the gate; only the stateless drop of trace-shaped streams remains at
// the flush.
func TestFlushRowKeeper_IgnoresTheCardinalityGate(t *testing.T) {
	orig := globalCardinalityGate
	t.Cleanup(func() { globalCardinalityGate = orig })

	denied := `{service.name="blocked"}`
	SetCardinalityGate(fakeGate{deny: map[string]bool{denied: true}})
	keep := FlushRowKeeper()
	if !keep(1, 2, denied) {
		t.Fatal("the flush filter dropped a stream the cardinality gate denies; the gate is applied when a row is added, never at the flush")
	}
	if admitted, _ := keepStream(1, 2, denied); admitted {
		t.Fatal("the insert path admitted a stream the cardinality gate denies")
	}
	if !keep(1, 2, `{service.name="api-gateway"}`) || !keep(1, 2, "") {
		t.Fatal("an ordinary stream, and the empty stream, must be kept")
	}
	// Stateless: the trace-shaped drop does not depend on the gate.
	SetCardinalityGate(nil)
	if keep(1, 2, `{resource_attr:service.name="api"}`) {
		t.Fatal("a trace-shaped stream must not be written as logs")
	}
}
