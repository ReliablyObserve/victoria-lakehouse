package registry

import (
	"strings"
	"testing"
)

func TestLoadDir_Valid(t *testing.T) {
	reg, err := LoadDir("testdata/valid")
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if len(reg.Rows) != 2 || reg.ByID["lh.stats.overview.schema"] == nil {
		t.Fatalf("unexpected rows: %+v", reg.Rows)
	}
	if n := reg.Native(); len(n) != 1 || n[0].ID != "vl.select.query.wildcard" {
		t.Fatalf("Native(): %+v", n)
	}
	keys := reg.UpstreamKeys()
	if ids := keys["route:/select/logsql/query"]; len(ids) != 1 {
		t.Fatalf("UpstreamKeys: %v", keys)
	}
}

func TestLoadDir_DuplicateID(t *testing.T) {
	_, err := LoadDir("testdata/invalid")
	if err == nil || !strings.Contains(err.Error(), "duplicate id") {
		t.Fatalf("want duplicate id error, got %v", err)
	}
}

func TestLoadDir_Missing(t *testing.T) {
	if _, err := LoadDir("testdata/nope"); err == nil {
		t.Fatal("want error for missing dir")
	}
}
