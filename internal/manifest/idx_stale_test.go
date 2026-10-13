package manifest

import "testing"

// The trace_id_idx objects have a layout of their own, not the span schema the
// fingerprint describes: a stats pass that called them stale would list every hour
// that holds one as a re-promotion candidate.
func TestIdxObjectsAreNeverStale(t *testing.T) {
	m := New("bucket", "")
	for _, k := range []string{"1001/0/traces_idx/dt=2026-10-13/hour=10/a.parquet", "1001/0/traces_idx/dt=2026-10-13/hour=10/compacted-L1-b.parquet"} {
		m.AddFile("dt=2026-10-13/hour=10", FileInfo{Key: k, Size: 1000, RawBytes: 4000})
	}
	st := m.ComputeCompactionStats("current", nil)
	if st.StaleSchemaFiles != 0 || len(st.Candidates) != 0 {
		t.Errorf("idx objects counted stale: files=%d candidates=%+v", st.StaleSchemaFiles, st.Candidates)
	}

	// the same two files under the span signal are stale: the control
	m2 := New("bucket", "")
	for _, k := range []string{"1001/0/traces/dt=2026-10-13/hour=10/a.parquet", "1001/0/traces/dt=2026-10-13/hour=10/b.parquet"} {
		m2.AddFile("dt=2026-10-13/hour=10", FileInfo{Key: k, Size: 1000, RawBytes: 4000})
	}
	if st := m2.ComputeCompactionStats("current", nil); st.StaleSchemaFiles != 2 || len(st.Candidates) != 1 {
		t.Errorf("control: span objects must be stale: files=%d candidates=%d", st.StaleSchemaFiles, len(st.Candidates))
	}

	f := FileInfo{Key: "0/0/traces_idx/dt=x/hour=00/a.parquet"}
	if f.StaleAgainst("current") || f.StaleAgainst("") {
		t.Error("an idx object is never stale")
	}
	g := FileInfo{Key: "0/0/traces/dt=x/hour=00/a.parquet", SchemaFingerprint: "old"}
	if !g.StaleAgainst("current") || g.StaleAgainst("") || g.StaleAgainst("old") {
		t.Error("a span object is stale only against a different non-empty fingerprint")
	}
}
