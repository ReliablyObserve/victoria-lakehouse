package traceindex

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// benchEntries builds an index of n traces in one of the shapes the generators
// of this repository produce: "random128" (cmd/datagen: random 128-bit hex IDs,
// start times spread over an hour), "sha256" (tests/ingestmatrix: the first 16
// bytes of a SHA-256, tight window) and "short" (the field-metadata matrix
// fixture: "t<n>").
func benchEntries(shape string, n int) []Entry {
	rng := rand.New(rand.NewSource(int64(n)))
	now := int64(1_760_000_000_000_000_000)
	entries := make([]Entry, n)
	for i := range entries {
		var tid string
		var start int64
		switch shape {
		case "random128":
			var b [16]byte
			rng.Read(b[:])
			tid = hex.EncodeToString(b[:])
			start = now - int64(rng.Intn(3600))*1_000_000_000 - int64(rng.Intn(1_000_000_000))
		case "sha256":
			sum := sha256.Sum256([]byte(fmt.Sprintf("run-%d", i)))
			tid = hex.EncodeToString(sum[:16])
			start = now - int64(rng.Intn(3_000))*1_000_000
		default:
			tid = fmt.Sprintf("t%d", i)
			start = now - int64(i)*1_000_000
		}
		entries[i] = Entry{TraceID: tid, Partition: Partition(tid), StartNs: start, EndNs: start + 1_000_000 + int64(rng.Intn(2_000_000_000))}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].TraceID < entries[j].TraceID })
	return entries
}

var benchShapes = []string{"random128", "sha256", "short"}
var benchSizes = []int{1000, 8000, 9500, 10000, 12000}

// BenchmarkMetadataValue is the write side: the flush and compaction cost of
// building the `_trace_idx` footer value from the entries.
func BenchmarkMetadataValue(b *testing.B) {
	for _, shape := range benchShapes {
		for _, n := range benchSizes {
			entries := benchEntries(shape, n)
			raw := Marshal(entries)
			b.Run(fmt.Sprintf("%s/%d", shape, n), func(b *testing.B) {
				var v string
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					v = MetadataValue(entries)
				}
				b.ReportMetric(float64(len(raw)), "raw-bytes")
				b.ReportMetric(float64(len(v)), "value-bytes")
			})
		}
	}
}

// BenchmarkFromMetadata is the read side: what every trace-by-ID file visit
// pays to turn the footer value into entries. BenchmarkUnmarshalRaw is the
// former cost (the value was the raw Marshal bytes), for comparison.
func BenchmarkFromMetadata(b *testing.B) {
	for _, shape := range benchShapes {
		for _, n := range benchSizes {
			meta := map[string]string{MetadataKey: MetadataValue(benchEntries(shape, n))}
			b.Run(fmt.Sprintf("%s/%d", shape, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, ok := FromMetadata(meta); !ok {
						b.Fatal("decode failed")
					}
				}
			})
		}
	}
}

func BenchmarkUnmarshalRaw(b *testing.B) {
	for _, shape := range benchShapes {
		for _, n := range benchSizes {
			raw := Marshal(benchEntries(shape, n))
			b.Run(fmt.Sprintf("%s/%d", shape, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := Unmarshal(raw); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkLookup is what one file visit of a trace-by-ID query costs now: decode
// the footer value and ask whether it lists the ID (ContainsAny) or fetch its
// entry (Find). BenchmarkLookupFormerRaw is the same question answered the way
// the code did before the value was compressed: Unmarshal the raw bytes into
// entries and loop over them.
func BenchmarkLookup(b *testing.B) {
	for _, shape := range benchShapes {
		for _, n := range benchSizes {
			entries := benchEntries(shape, n)
			val := MetadataValue(entries)
			ids := map[string]bool{"absent-id": true}
			b.Run(fmt.Sprintf("ContainsAny/%s/%d", shape, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, ok := ContainsAny(val, ids); !ok {
						b.Fatal()
					}
				}
			})
			b.Run(fmt.Sprintf("Find/%s/%d", shape, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, _, ok := Find(val, entries[n/2].TraceID); !ok {
						b.Fatal()
					}
				}
			})
		}
	}
}

func BenchmarkLookupFormerRaw(b *testing.B) {
	for _, shape := range benchShapes {
		for _, n := range benchSizes {
			raw := Marshal(benchEntries(shape, n))
			b.Run(fmt.Sprintf("ContainsAny/%s/%d", shape, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					got, err := Unmarshal(raw)
					if err != nil {
						b.Fatal(err)
					}
					for _, e := range got {
						if e.TraceID == "absent-id" {
							b.Fatal()
						}
					}
				}
			})
		}
	}
}
