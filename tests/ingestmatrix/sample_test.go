package ingestmatrix

import "testing"

func TestStrictSampleRejectsHandoffDip(t *testing.T) {
	hot := []string{"row-one", "row-two"}
	for _, lh := range [][]string{nil, {"row-one"}, {"row-one", "row-two", "row-two"}, {"row-one", "wrong-field"}} {
		if err := CheckSample(hot, lh, 2); err == nil {
			t.Fatalf("invalid handoff sample accepted: %v", lh)
		}
	}
	if err := CheckSample(hot, hot, 2); err != nil {
		t.Fatal(err)
	}
}
