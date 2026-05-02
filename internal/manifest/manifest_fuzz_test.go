package manifest

import (
	"testing"
)

func FuzzExtractPartition(f *testing.F) {
	f.Add("logs/dt=2026-05-02/hour=10/00000-abc.parquet")
	f.Add("traces/dt=2026-04-01/hour=00/file.parquet")
	f.Add("dt=2026-01-01/hour=23/data.parquet")
	f.Add("prefix/nested/dt=2026-12-31/hour=01/x.parquet")
	f.Add("no-partition.parquet")
	f.Add("")
	f.Add("/")
	f.Add("dt=/hour=")
	f.Add("dt=bad-date/hour=99/f.parquet")
	f.Add("dt=2026-01-01/f.parquet")
	f.Add("some/path/hour=05/f.parquet")
	f.Add("dt=2026-01-01/hour=00/hour=01/f.parquet")

	f.Fuzz(func(t *testing.T, key string) {
		result := extractPartition(key)
		if result != "" {
			if len(result) < 3 {
				t.Errorf("extractPartition(%q) = %q — too short to be valid", key, result)
			}
		}
	})
}

func FuzzParsePartitionTime(f *testing.F) {
	f.Add("dt=2026-05-02/hour=10")
	f.Add("dt=2026-01-01/hour=00")
	f.Add("dt=2026-12-31/hour=23")
	f.Add("dt=2026-05-02")
	f.Add("dt=bad-date")
	f.Add("hour=10")
	f.Add("")
	f.Add("dt=2026-02-29/hour=00")
	f.Add("dt=2026-13-01/hour=00")
	f.Add("dt=2026-01-32/hour=00")
	f.Add("dt=2026-01-01/hour=24")
	f.Add("dt=2026-01-01/hour=-1")
	f.Add("dt=2026-01-01/hour=abc")
	f.Add("dt=0000-00-00/hour=00")

	f.Fuzz(func(t *testing.T, partition string) {
		result, err := parsePartitionTime(partition)
		if err != nil {
			return
		}
		if result.IsZero() {
			t.Errorf("parsePartitionTime(%q) returned zero time without error", partition)
		}
	})
}
