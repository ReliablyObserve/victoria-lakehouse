package metrics

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// seriesHelperEnv makes TestMustStayZeroSeries_ExportedFromStart print /metrics.
const seriesHelperEnv = "LH_METRICS_SERIES_HELPER"

// The series that must stay 0 are on /metrics before anything has happened, so
// increase() sees the first event. Read in a fresh process: in this one an
// earlier test may have created a series by using it.
func TestMustStayZeroSeries_ExportedFromStart(t *testing.T) {
	if os.Getenv(seriesHelperEnv) != "" {
		// The child process: print /metrics as the package's init functions left
		// it, before any other test has created a series by using it.
		var buf bytes.Buffer
		WritePrometheus(&buf, false)
		fmt.Print("SERIES-BEGIN\n", buf.String(), "SERIES-END\n")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMustStayZeroSeries_ExportedFromStart$", "-test.count=1")
	cmd.Env = append(os.Environ(), seriesHelperEnv+"=1")
	raw, err := cmd.Output()
	if err != nil {
		t.Fatalf("child process: %v", err)
	}
	out := string(raw)
	if !strings.Contains(out, "SERIES-BEGIN") {
		t.Fatalf("the child process printed no series: %q", out)
	}
	for _, stage := range BufferFlushErrorStages {
		if series := fmt.Sprintf(`lakehouse_buffer_flush_errors_total{stage=%q}`, stage); !strings.Contains(out, series) {
			t.Errorf("%s is not exported at start", series)
		}
	}
	for _, reason := range InsertRowsDroppedReasons {
		if series := fmt.Sprintf(`lakehouse_insert_rows_dropped_total{reason=%q}`, reason); !strings.Contains(out, series) {
			t.Errorf("%s is not exported at start", series)
		}
	}
	if !strings.Contains(out, `lakehouse_buffer_stalled_segments `) {
		t.Error("lakehouse_buffer_stalled_segments is not exported at start")
	}
}

// TestBufferFlushErrorStages_MatchCallSites keeps BufferFlushErrorStages equal to
// the stages the flushers of both binaries increment the counter with.
func TestBufferFlushErrorStages_MatchCallSites(t *testing.T) {
	dirs := []string{
		filepath.Join("..", "storage", "parquets3"),
		filepath.Join("..", "..", "lakehouse-traces", "internal", "storage", "parquets3"),
	}
	callRe := regexp.MustCompile(`BufferFlushErrors\.(?:Inc|Add)\("([^"]+)"`)
	for _, dir := range dirs {
		used := map[string]bool{}
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil || len(files) == 0 {
			t.Fatalf("no Go sources under %s (%v) — the drift guard would pass vacuously", dir, err)
		}
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			src, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range callRe.FindAllStringSubmatch(string(src), -1) {
				used[m[1]] = true
			}
		}
		listed := map[string]bool{}
		for _, r := range BufferFlushErrorStages {
			listed[r] = true
		}
		var missing, unused []string
		for r := range used {
			if !listed[r] {
				missing = append(missing, r)
			}
		}
		for r := range listed {
			if !used[r] {
				unused = append(unused, r)
			}
		}
		sort.Strings(missing)
		sort.Strings(unused)
		if len(missing) > 0 || len(unused) > 0 {
			t.Errorf("%s: BufferFlushErrorStages drifted from the call sites: not listed %v, listed but never incremented %v", dir, missing, unused)
		}
	}
}
