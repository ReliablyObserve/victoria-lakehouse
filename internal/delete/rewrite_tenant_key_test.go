package delete

import (
	"context"
	"path"
	"strings"
	"testing"
	"time"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestRewrite_ReplacementStaysInTheSourceDirectory pins that a rewrite writes
// its replacement next to the object it supersedes.
//
// The replacement key used to be built from the rewriter's configured prefix
// plus the partition. That is only right for a single-tenant layout: under the
// {AccountID}/{ProjectID}/<signal>/ layout the configured prefix is the DEFAULT
// tenant's, so rewriting tenant 1002/0's file moved its kept rows into tenant
// 0/0's key space — served to the wrong tenant, attributed to the wrong tenant's
// storage totals, and outside the owning tenant's scoped listing.
func TestRewrite_ReplacementStaysInTheSourceDirectory(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		key    string
	}{
		{"tenant file under the default tenant's prefix", "0/0/logs/", "1002/0/logs/dt=2026-03-01/hour=07/src-0001.parquet"},
		{"tenant file under the bare signal prefix", "logs/", "7/3/logs/dt=2026-03-01/hour=07/src-0001.parquet"},
		{"compacted tenant file", "0/0/logs/", "1002/0/logs/dt=2026-03-01/hour=07/compacted-L1-1234abcd.parquet"},
		{"single-tenant layout", "logs/", "logs/dt=2026-03-01/hour=07/src-0001.parquet"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := []schema.LogRow{
				{TimestampUnixNano: 1000, Body: "keep", SeverityText: "info", ServiceName: "web"},
				{TimestampUnixNano: 2000, Body: "drop", SeverityText: "error", ServiceName: "web"},
			}
			pool := newMockRewriterPool()
			pool.Put(tc.key, buildTestParquet(t, rows))

			rw := NewRewriter(pool, tc.prefix, 1000, "logs")
			res, err := rw.RewriteFile(context.Background(), tc.key, []Tombstone{{
				Tenants: []TenantRef{{}},
				ID:      "t", Query: `severity_text:="error"`, StartNs: 0, EndNs: 10000, Mode: "permanent",
			}})
			if err != nil {
				t.Fatalf("rewrite: %v", err)
			}
			if res.NewKey == "" {
				t.Fatal("expected a replacement object")
			}
			if got, want := path.Dir(res.NewKey), path.Dir(tc.key); got != want {
				t.Fatalf("replacement written under %q, want the source directory %q (new key %s)", got, want, res.NewKey)
			}
			if !strings.HasSuffix(res.NewKey, ".parquet") || res.NewKey == tc.key {
				t.Fatalf("replacement key %q must be a distinct .parquet object", res.NewKey)
			}
			if !pool.Has(res.NewKey) {
				t.Fatalf("replacement %s was not uploaded", res.NewKey)
			}
		})
	}
}

// TestRewrite_TenantFileStaysWithItsTenantInTheManifest runs the same bug
// through the scheduler and the tenant-scoped manifest lookups the query path
// uses: after the rewrite, the kept rows must be served to the tenant that owns
// them and to no other.
func TestRewrite_TenantFileStaysWithItsTenantInTheManifest(t *testing.T) {
	const key = "1002/0/logs/dt=2026-03-01/hour=07/src-0001.parquet"
	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "keep-a", SeverityText: "info", ServiceName: "web"},
		{TimestampUnixNano: 2000, Body: "drop", SeverityText: "error", ServiceName: "web"},
		{TimestampUnixNano: 3000, Body: "keep-b", SeverityText: "info", ServiceName: "api"},
	}
	pool := newMockRewriterPool()
	pool.Put(key, buildTestParquet(t, rows))

	m := lhmanifest.New("test-bucket", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	m.AddFile(extractPartition(key), lhmanifest.FileInfo{
		Key: key, Size: 1024, RowCount: int64(len(rows)), MinTimeNs: 1, MaxTimeNs: 1 << 40,
	})

	store := NewTombstoneStore()
	store.Add(Tombstone{
		Tenants: []TenantRef{{AccountID: 1002}},
		ID:      "ts-tenant", Query: `severity_text:="error"`, StartNs: 0, EndNs: 10000,
		AffectedKeys: []string{key}, CreatedAt: time.Now().Add(-2 * time.Hour),
		Mode: "permanent", Reaped: map[string]bool{},
	})
	sched := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          store,
		Rewriter:       NewRewriter(pool, "0/0/logs/", 1000, "logs"),
		Detector:       NewStorageClassDetector(nil),
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
		Manifest:       m,
	})

	results := sched.RunOnce(context.Background())
	if len(results) != 1 || results[0].NewKey == "" {
		t.Fatalf("expected one rewrite with a replacement, got %+v", results)
	}

	owner := m.GetFilesForRangeTenant(0, 1<<62, "1002", "0")
	if len(owner) != 1 || owner[0].Key != results[0].NewKey {
		t.Fatalf("tenant 1002/0 must see exactly the replacement %s, got %v", results[0].NewKey, fileKeys(owner))
	}
	if owner[0].RowCount != 2 {
		t.Fatalf("replacement row count = %d, want 2", owner[0].RowCount)
	}
	if other := m.GetFilesForRangeTenant(0, 1<<62, "0", "0"); len(other) != 0 {
		t.Fatalf("the default tenant must not see tenant 1002/0's rows, got %v", fileKeys(other))
	}
	for _, k := range pool.Keys() {
		if !strings.HasPrefix(k, "1002/0/logs/") {
			t.Fatalf("object %s escaped the owning tenant's prefix", k)
		}
	}
}

// TestReplacementKey_NeverReusesTheSourceName covers the id collision: a source
// that is itself a rewrite output is named <id>.parquet, and a replacement
// drawing the same id would overwrite the only copy of the kept rows before the
// publish — and the commit would then delete it.
func TestReplacementKey_NeverReusesTheSourceName(t *testing.T) {
	for _, src := range []string{
		"1002/0/logs/dt=2026-03-01/hour=07/0123abcd.parquet",
		"0123abcd.parquet",
	} {
		got := replacementKey(src, "0123abcd")
		if got == src {
			t.Fatalf("replacementKey(%q) reused the source key", src)
		}
		if path.Dir(got) != path.Dir(src) {
			t.Fatalf("replacementKey(%q) = %q left the source directory", src, got)
		}
	}
}

func fileKeys(files []lhmanifest.FileInfo) []string {
	out := make([]string, 0, len(files))
	for _, fi := range files {
		out = append(out, fi.Key)
	}
	return out
}
