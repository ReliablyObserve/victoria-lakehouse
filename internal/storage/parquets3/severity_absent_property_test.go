package parquets3

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Property: for any mix of absent, explicit-0 and real severity_number values,
// any row-group size and either read path, a row comes back with severity_number
// exactly when it was written with one, and with the value it was written with.
// An optimisation of the column readers that turns NULL into 0 or into a
// neighbour's value (the constant-column shortcut did) fails here.
func TestSeverityAbsent_Property(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	startNs, endNs := now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()
	for seed := int64(1); seed <= 60; seed++ {
		rng := rand.New(rand.NewSource(seed))
		n := 1 + rng.Intn(40)
		rows := make([]schema.LogRow, n)
		want := make(map[string]string, n)
		mode := rng.Intn(4) // 0 mixed, 1 all absent, 2 one value everywhere + some absent, 3 all zero
		for i := range rows {
			body := fmt.Sprintf("p%d-%d", seed, i)
			rows[i] = schema.LogRow{TimestampUnixNano: now.Add(time.Duration(i) * time.Second).UnixNano(), Body: body,
				ServiceName: "svc", Stream: `{service.name="svc"}`, StreamID: "s1"}
			var v *int32
			switch mode {
			case 0:
				switch rng.Intn(3) {
				case 1:
					v = schema.Int32Ptr(0)
				case 2:
					v = schema.Int32Ptr(int32(1 + rng.Intn(24)))
				}
			case 2:
				if rng.Intn(3) != 0 {
					v = schema.Int32Ptr(9)
				}
			case 3:
				v = schema.Int32Ptr(0)
			}
			rows[i].SeverityNumber = v
			if v != nil {
				want[body] = strconv.Itoa(int(*v))
			} else {
				want[body] = ""
			}
		}
		rg := 1 + rng.Intn(n)
		res, err := writeLogsParquet(rows, rg, 3)
		if err != nil {
			t.Fatal(err)
		}
		f := openParquetBytes(t, res.Data)
		s := testStorage()
		for name, got := range map[string][]map[string]string{
			"scan":  scanRowGroups(t, s, f, startNs, endNs, nil),
			"typed": scanRowGroupsTyped(t, s, f, startNs, endNs),
		} {
			if len(got) != n {
				t.Fatalf("seed %d %s: %d rows, want %d", seed, name, len(got), n)
			}
			for _, r := range got {
				if r["severity_number"] != want[r["_msg"]] {
					t.Errorf("seed %d (mode %d, row group %d) %s path: row %q severity_number=%q, want %q",
						seed, mode, rg, name, r["_msg"], r["severity_number"], want[r["_msg"]])
				}
			}
		}
	}
}
