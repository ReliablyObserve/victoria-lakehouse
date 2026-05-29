package parquets3

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestExtractDistinctFromStats_NoTruncation regression-guards the fix
// for service-name truncation in /select/jaeger/api/services. The
// original implementation read distinct values from parquet column-index
// min/max stats, which parquet-go truncates at 16 bytes per the
// Apache Parquet PageIndex spec. That produced values like
// "notification-ser" and "notification-ses" alongside the full
// "notification-service" in the LabelIndex, surfacing as duplicate
// truncated service names in every GetFieldValues consumer.
//
// This test writes a parquet file with a long string column value
// ("notification-service-with-long-name") and asserts the extracted
// distinct set contains the FULL value with no truncated prefixes.
func TestExtractDistinctFromStats_NoTruncation(t *testing.T) {
	type row struct {
		ServiceName string `parquet:"service.name"`
	}
	rows := []row{
		{ServiceName: "notification-service-with-long-name"},
		{ServiceName: "user-service"},
		{ServiceName: "api-gateway"},
		{ServiceName: "notification-service-with-long-name"},
		{ServiceName: "billing-service-also-with-long-name"},
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	// service.name is the only column → colIdx 0.
	got := extractDistinctFromStats(f, 0)

	// Verify all full values present.
	gotSet := make(map[string]bool, len(got))
	for _, v := range got {
		gotSet[v] = true
	}
	for _, want := range []string{
		"notification-service-with-long-name",
		"billing-service-also-with-long-name",
		"user-service",
		"api-gateway",
	} {
		if !gotSet[want] {
			t.Errorf("missing full value %q in extracted distinct set: %v", want, got)
		}
	}

	// Verify no truncated prefix is present. A truncated prefix from
	// the 16-byte column-index stat would be e.g. "notification-ser"
	// or "billing-service-" (16 chars). The fixed implementation reads
	// only data pages — which return untruncated values — so neither
	// should appear.
	for _, badPrefix := range []string{
		"notification-ser",
		"notification-ses",
		"billing-service-",
		"billing-service-a",
	} {
		if gotSet[badPrefix] {
			t.Errorf("found truncated prefix %q (length %d); should be filtered out", badPrefix, len(badPrefix))
		}
	}
}
