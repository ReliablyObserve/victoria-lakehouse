package retention

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// The trace_id_idx table is under retention like every object: it expires with its
// partition, and it is never kept for less than the longest period any rule keeps
// spans (its objects carry no labels, so no rule matches them).
func TestRunOnce_IdxObjectsExpireAndOutliveRules(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	file := func(key string, age time.Duration, labels map[string][]string) manifest.FileInfo {
		return manifest.FileInfo{Key: key, Size: 1, MaxTimeNs: now.Add(-age).UnixNano(), Labels: labels}
	}
	day := 24 * time.Hour
	mf := newMockManifest(map[string][]manifest.FileInfo{
		"dt=a/hour=10": {
			file("1/0/traces/dt=a/hour=10/x.parquet", 100*day, map[string][]string{"service.name": {"keep-long"}}),
			file("1/0/traces_idx/dt=a/hour=10/x.parquet", 100*day, nil),
		},
		"dt=b/hour=10": {
			file("1/0/traces_idx/dt=b/hour=10/y.parquet", 400*day, nil),
			file("1/0/traces/dt=b/hour=10/y.parquet", 400*day, map[string][]string{"service.name": {"keep-long"}}),
		},
	})
	deleter := &mockDeleter{}
	cfg := Config{Enabled: true, Default: "30d", CheckInterval: "1h", Rules: []Rule{{Match: map[string]string{"service.name": "keep-long"}, Keep: "365d"}}}
	mgr, err := New(cfg, mf, deleter, "b", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	mgr.nowFunc = func() time.Time { return now }
	if _, err := mgr.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, k := range deleter.getDeleted() {
		got[k] = true
	}
	if got["b/1/0/traces_idx/dt=a/hour=10/x.parquet"] {
		t.Error("the index object of a trace whose spans a rule keeps for 365 days was deleted at 100 days")
	}
	if !got["b/1/0/traces_idx/dt=b/hour=10/y.parquet"] || !got["b/1/0/traces/dt=b/hour=10/y.parquet"] {
		t.Errorf("objects past every period were not deleted: %v", got)
	}
}
