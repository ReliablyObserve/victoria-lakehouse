package ingestmatrix

import "fmt"

// CheckSample checks one observation without retrying missing or wrong rows.
// Arrival polling is a separate concern; a sampled handoff dip is a failure.
func CheckSample(hot, lh []string, want int) error {
	if len(hot) != want || len(lh) != want {
		return fmt.Errorf("sample count hot=%d lakehouse=%d want=%d", len(hot), len(lh), want)
	}
	for i := range hot {
		if hot[i] != lh[i] {
			return fmt.Errorf("sample row %d hot=%s lakehouse=%s", i, hot[i], lh[i])
		}
	}
	return nil
}
