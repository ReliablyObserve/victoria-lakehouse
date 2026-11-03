package parquets3

import (
	"crypto/sha256"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// flushBytesGolden pins the exact bytes the flush writer produces for fixed rows:
// the SHA-256 of each object. It is the byte-identity proof of the shared Parquet
// writer (internal/parquetwriter): moving the encoding there must not move a
// single byte of what a flush writes. The fixtures vary what the footer depends
// on: several row groups, rows with an empty body, a Tier-2 slot resolver with a
// bloomed slot and none, the lh.trace_id_hex attestation true and false, and span
// bodies.
//
// A hash that moves is a format change: it needs a reviewer's decision, not a
// regenerated constant. Set LH_PRINT_FLUSH_GOLDEN=1 to print the current values.
var flushBytesGolden = map[string]string{
	"logs-hex/nil-slots":      "485abf4d33cd70101ebed1cf024615ac83582f077ecb4e146cef15b5b66fc819",
	"logs-hex/slots":          "739c48d8f3cba49ca2d0f60e755a9b25df5404d8271a21f2868a47ca056dae6a",
	"logs-nonhex/nil-slots":   "db0c2abe69981d452471b4ce98d7a72a144d5c648ca0836f20d5e1593f4631da",
	"logs-nonhex/slots":       "4c769334fd6be872de520c029daffa6afd0a3d92c43b130004b04be4dd453f87",
	"traces-hex/nil-slots":    "13ae2d60c773680b5ecf02816605df6847223b1fda3c8a2f86c52de890a92c98",
	"traces-hex/slots":        "eca63196183d970ec532adfad47852c98075222ccb50395372022e267bbea7f0",
	"traces-nonhex/nil-slots": "f93ee9a93e2da3daec42b3d5cfd00e86303019fc844103e23336bffe049d88c7",
	"traces-nonhex/slots":     "c2b97ed6c0177fbe19dd61f31252d6fe563f42888b8fb2ac6748c6994acf2c59",
}

func printFlushGolden() bool { return os.Getenv("LH_PRINT_FLUSH_GOLDEN") == "1" }

func flushGoldenSlots() *schema.SlotResolver {
	return schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant.tier", Bloom: true}, {Name: "build.id"}})
}

// flushGoldenCases returns name -> object bytes for every fixture.
func flushGoldenCases(t *testing.T) map[string][]byte {
	t.Helper()
	saved := activeSlotResolver
	t.Cleanup(func() { activeSlotResolver = saved })

	const rg = 1000
	logRows := func(n int) []schema.LogRow {
		rows := goldenLogRows(n)
		for i := range rows {
			if i%7 == 0 {
				rows[i].Body = "" // a row without a body adds no token to its row group
			}
		}
		return rows
	}
	nonHexLogs := func(n int) []schema.LogRow {
		rows := logRows(n)
		rows[n/2].TraceID = "not-a-hex-trace-id"
		return rows
	}
	spanRows := func(n int) []schema.TraceRow {
		rows := goldenTraceRows(n)
		for i := range rows {
			if i%3 == 0 {
				rows[i].Body = fmt.Sprintf("span event %d fired", i)
			}
		}
		return rows
	}
	nonHexSpans := func(n int) []schema.TraceRow {
		rows := spanRows(n)
		rows[n/2].TraceID = "not-a-hex-trace-id"
		return rows
	}

	out := map[string][]byte{}
	for _, withSlots := range []bool{false, true} {
		suffix := "/nil-slots"
		activeSlotResolver = nil
		if withSlots {
			suffix = "/slots"
			activeSlotResolver = flushGoldenSlots()
		}
		for name, rows := range map[string][]schema.LogRow{
			"logs-hex":    logRows(2*rg + 321),
			"logs-nonhex": nonHexLogs(rg + 5),
		} {
			res, err := writeLogsParquet(rows, rg, 3)
			if err != nil {
				t.Fatalf("writeLogsParquet(%s%s): %v", name, suffix, err)
			}
			out[name+suffix] = res.Data
		}
		for name, rows := range map[string][]schema.TraceRow{
			"traces-hex":    spanRows(2*rg + 321),
			"traces-nonhex": nonHexSpans(rg + 5),
		} {
			res, err := writeTracesParquet(rows, rg, 3)
			if err != nil {
				t.Fatalf("writeTracesParquet(%s%s): %v", name, suffix, err)
			}
			out[name+suffix] = res.Data
		}
	}
	return out
}

func TestFlushWriterBytesGolden(t *testing.T) {
	got := flushGoldenCases(t)
	names := make([]string, 0, len(got))
	for n := range got {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		sum := fmt.Sprintf("%x", sha256.Sum256(got[n]))
		if printFlushGolden() {
			fmt.Printf("FLUSHGOLDEN %q: %q,\n", n, sum)
			continue
		}
		want, ok := flushBytesGolden[n]
		if !ok {
			t.Errorf("%s: no golden hash recorded (got %s)", n, sum)
			continue
		}
		if sum != want {
			t.Errorf("%s: the flush writer's bytes changed: want sha256 %s, got %s (%d bytes)", n, want, sum, len(got[n]))
		}
	}
	if len(flushBytesGolden) != len(got) && !printFlushGolden() {
		t.Errorf("golden has %d entries, fixtures produce %d", len(flushBytesGolden), len(got))
	}
}
