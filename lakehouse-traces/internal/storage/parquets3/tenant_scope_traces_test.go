package parquets3

import (
	"context"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// TestTenantScope_TraceIndexLookup pins the trace-by-id fast path to the
// request tenant, in both layouts. It used to walk every tenant's footers: a
// tenant could learn whether another tenant holds a trace ID (and its time
// bounds), and in a bucket-per-tenant deployment the walk read other tenants'
// buckets.
func TestTenantScope_TraceIndexLookup(t *testing.T) {
	for _, layout := range tsLayouts() {
		t.Run(string(layout), func(t *testing.T) {
			f := newEmptyTenantScopeFixture(t, layout, "{AccountID}/{ProjectID}/")
			now := tsNow()

			objects := []struct {
				tenant  logstorage.TenantID
				key     string
				traceID string
			}{
				{logstorage.TenantID{}, "0/0/traces/" + tsPartition + "/a.parquet", "trace-of-tenant-0"},
				{logstorage.TenantID{AccountID: 1001}, "1001/0/traces/" + tsPartition + "/b.parquet", "trace-of-tenant-1001"},
			}
			for _, o := range objects {
				data := writeParquetWithTraceIndex(t, []TraceIndexEntry{{TraceID: o.traceID, StartNs: now.UnixNano(), EndNs: now.UnixNano() + 10}}, 4)
				bucket := tsBucketOf(layout, o.key)
				f.mock.put(bucket, o.key, data)
				f.s.manifest.AddFile(tsPartition, manifest.FileInfo{
					Key: o.key, Size: int64(len(data)), RowCount: 5,
					MinTimeNs: now.UnixNano(), MaxTimeNs: now.UnixNano() + 10,
				})
				f.tenants = append(f.tenants, tsTenant{tenant: o.tenant, key: o.key, bucket: bucket})
			}

			cases := []struct {
				name      string
				ctx       context.Context
				ids       []logstorage.TenantID
				traceID   string
				wantFound bool
			}{
				{"own trace is found", context.Background(), []logstorage.TenantID{{AccountID: 1001}}, "trace-of-tenant-1001", true},
				{"another tenant's trace is not found", context.Background(), []logstorage.TenantID{{AccountID: 1001}}, "trace-of-tenant-0", false},
				{"no headers cannot find tenant 1001's trace", context.Background(), nil, "trace-of-tenant-1001", false},
				{"unknown tenant finds nothing", context.Background(), []logstorage.TenantID{{AccountID: 7}}, "trace-of-tenant-0", false},
				{"global read finds any tenant's trace", storage.WithGlobalRead(context.Background()), []logstorage.TenantID{{}}, "trace-of-tenant-1001", true},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					f.s.footerCache = NewFooterCache(1000)
					f.mock.reset()
					_, _, found, err := f.s.LookupTraceIndex(tc.ctx, tc.ids, tc.traceID)
					if err != nil {
						t.Fatalf("LookupTraceIndex: %v", err)
					}
					if found != tc.wantFound {
						t.Errorf("found = %v, want %v", found, tc.wantFound)
					}
					assertBucketsWithin(t, "trace index lookup", f.mock.bucketsTouched(),
						f.expectedBuckets(tc.ids, storage.IsGlobalRead(tc.ctx)))
				})
			}
		})
	}
}
