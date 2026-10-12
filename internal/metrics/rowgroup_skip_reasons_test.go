package metrics

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestRowGroupSkipReasons_ExportedFromStart: every reason series of
// lakehouse_parquet_row_groups_skipped_total is on /metrics before any query
// has skipped anything.
func TestRowGroupSkipReasons_ExportedFromStart(t *testing.T) {
	var buf bytes.Buffer
	WritePrometheus(&buf, false)
	out := buf.String()
	for _, reason := range RowGroupSkipReasons {
		series := fmt.Sprintf(`lakehouse_parquet_row_groups_skipped_total{reason=%q}`, reason)
		if !strings.Contains(out, series) {
			t.Errorf("%s is not exported", series)
		}
	}
}

// TestRowGroupSkipReasons_MatchCallSites keeps RowGroupSkipReasons equal to the
// reasons the storage packages of both binaries actually increment the counter
// with: a new skip stage must be listed (or its series stays missing until its
// first skip), and a listed reason nobody uses is a dead series.
func TestRowGroupSkipReasons_MatchCallSites(t *testing.T) {
	dirs := []string{
		filepath.Join("..", "storage", "parquets3"),
		filepath.Join("..", "..", "lakehouse-traces", "internal", "storage", "parquets3"),
	}
	callRe := regexp.MustCompile(`ParquetRowGroupsSkipped\.(?:Inc|Add)\("([^"]+)"`)
	used := map[string]bool{}
	for _, dir := range dirs {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		if len(files) == 0 {
			t.Fatalf("no Go sources under %s — the drift guard would pass vacuously", dir)
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
	}
	listed := map[string]bool{}
	for _, r := range RowGroupSkipReasons {
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
		t.Fatalf("RowGroupSkipReasons drifted from the call sites: not listed %v, listed but never incremented %v", missing, unused)
	}
}
