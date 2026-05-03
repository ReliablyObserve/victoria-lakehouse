package manifest

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkHasDataForRange(b *testing.B) {
	l := testLogger()
	m := New("bucket", "logs/", l)

	for d := 0; d < 365; d++ {
		for h := 0; h < 24; h++ {
			date := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d)
			partition := fmt.Sprintf("dt=%s/hour=%02d", date.Format("2006-01-02"), h)
			m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/file.parquet", partition), Size: 1024})
		}
	}

	startNs := time.Date(2026, 6, 15, 10, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC).UnixNano()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.HasDataForRange(startNs, endNs)
	}
}

func BenchmarkGetFilesForRange_1Hour(b *testing.B) {
	l := testLogger()
	m := New("bucket", "logs/", l)

	for d := 0; d < 30; d++ {
		for h := 0; h < 24; h++ {
			date := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d)
			partition := fmt.Sprintf("dt=%s/hour=%02d", date.Format("2006-01-02"), h)
			m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/file.parquet", partition), Size: 1024})
		}
	}

	startNs := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 15, 11, 0, 0, 0, time.UTC).UnixNano()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.GetFilesForRange(startNs, endNs)
	}
}

func BenchmarkGetFilesForRange_24Hours(b *testing.B) {
	l := testLogger()
	m := New("bucket", "logs/", l)

	for d := 0; d < 30; d++ {
		for h := 0; h < 24; h++ {
			date := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d)
			partition := fmt.Sprintf("dt=%s/hour=%02d", date.Format("2006-01-02"), h)
			m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/file.parquet", partition), Size: 1024})
		}
	}

	startNs := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC).UnixNano()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.GetFilesForRange(startNs, endNs)
	}
}

func BenchmarkAddFile(b *testing.B) {
	l := testLogger()
	m := New("bucket", "logs/", l)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i%28)+1, i%24)
		m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/file-%d.parquet", partition, i), Size: 1024})
	}
}

func BenchmarkExtractPartition(b *testing.B) {
	key := "logs/dt=2026-05-02/hour=10/00000-abc.parquet"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extractPartition(key)
	}
}

func BenchmarkParsePartitionTime(b *testing.B) {
	partition := "dt=2026-05-02/hour=10"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parsePartitionTime(partition)
	}
}
