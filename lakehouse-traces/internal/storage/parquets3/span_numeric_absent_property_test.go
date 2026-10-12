package parquets3

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Property: for any mix of absent, explicit-0 and real values in the four
// numeric span columns, any row-group size and either read path, a span comes
// back with each numeric field exactly when it was written with it.
func TestSpanNumericAbsent_Property(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	startNs, endNs := now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()
	pick64 := func(rng *rand.Rand, mode int) *int64 {
		switch mode {
		case 1:
			return nil
		case 3:
			return schema.Int64Ptr(0)
		}
		switch rng.Intn(3) {
		case 0:
			return nil
		case 1:
			return schema.Int64Ptr(0)
		}
		return schema.Int64Ptr(int64(1 + rng.Intn(1_000_000)))
	}
	pick32 := func(rng *rand.Rand, mode int) *int32 {
		if v := pick64(rng, mode); v != nil {
			return schema.Int32Ptr(int32(*v % 6))
		}
		return nil
	}
	str := func(p any) string {
		switch v := p.(type) {
		case *int64:
			if v != nil {
				return strconv.FormatInt(*v, 10)
			}
		case *int32:
			if v != nil {
				return strconv.Itoa(int(*v))
			}
		}
		return ""
	}
	for seed := int64(1); seed <= 60; seed++ {
		rng := rand.New(rand.NewSource(seed))
		n := 1 + rng.Intn(40)
		mode := rng.Intn(4)
		rows := make([]schema.TraceRow, n)
		want := map[string]map[string]string{}
		for i := range rows {
			id := fmt.Sprintf("sp%d-%d", seed, i)
			r := schema.TraceRow{TimestampUnixNano: now.Add(time.Duration(i) * time.Second).UnixNano(), TraceID: "t", SpanID: id,
				SpanName: "op", ServiceName: "svc", Stream: `{svc="a"}`, StreamID: "s1",
				StartTimeUnixNano: pick64(rng, mode), DurationNs: pick64(rng, mode), StatusCode: pick32(rng, mode), SpanKind: pick32(rng, mode)}
			rows[i] = r
			want[id] = map[string]string{"start_time_unix_nano": str(r.StartTimeUnixNano), "duration": str(r.DurationNs),
				"status_code": str(r.StatusCode), "kind": str(r.SpanKind)}
		}
		rg := 1 + rng.Intn(n)
		res, err := writeTracesParquet(rows, rg, 3)
		if err != nil {
			t.Fatal(err)
		}
		f := openParquetBytes(t, res.Data)
		s := tracesTestStorage()
		for name, got := range map[string][]map[string]string{
			"scan":  scanRowGroups(t, s, f, startNs, endNs, nil),
			"typed": scanRowGroupsTyped(t, s, f, startNs, endNs),
		} {
			if len(got) != n {
				t.Fatalf("seed %d %s: %d rows, want %d", seed, name, len(got), n)
			}
			for _, r := range got {
				for k, w := range want[r["span_id"]] {
					if r[k] != w {
						t.Errorf("seed %d (mode %d, row group %d) %s path: span %s %s=%q, want %q", seed, mode, rg, name, r["span_id"], k, r[k], w)
					}
				}
			}
		}
	}
}
