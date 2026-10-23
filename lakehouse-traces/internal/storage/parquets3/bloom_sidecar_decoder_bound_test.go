package parquets3

import (
	"context"
	"runtime"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// bloomSidecarAllocDelta reports the bytes allocated while f runs.
func bloomSidecarAllocDelta(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestCheckFileBloom_HostileSidecarBounded pins issue #763 on the real read
// path: a per-file `.bloom` object is untrusted bucket data, and a 5-byte value
// declaring 1,000,000 entries used to make bloomindex.Unmarshal preallocate
// ~56 MB before failing. The file must be kept (a corrupt sidecar never prunes)
// and the decode must not allocate in proportion to the declared count.
// Mirrored in the traces module; the logs and traces binaries share the
// decoder (internal/bloomindex), so one fix covers both.
func TestCheckFileBloom_HostileSidecarBounded(t *testing.T) {
	const ceiling = 4 << 20 // assumed generous: TotalAlloc is process-wide and includes the HTTP round trip; the crashers allocated ~56 MB
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	ctx := context.Background()
	const query = `service.name:="nonexistent-service-xyz-42"`

	// Non-vacuous: the same path does prune on a valid sidecar that lacks the value.
	good := bloomindex.NewFileBloomIndex(map[string][]string{"service.name": {"api-gw"}}, 0.01)
	mock.putFile("traces/good.parquet.bloom", good.Marshal())
	if !s.checkFileBloom(ctx, manifest.FileInfo{Key: "traces/good.parquet", Size: 1000}, query) {
		t.Fatal("valid sidecar without the value must prune the file")
	}

	hostile := map[string][]byte{
		"v1 count 1M": {1, 0x40, 0x42, 0x0F, 0x00},
		"v2 count 1M": {2, 0x40, 0x42, 0x0F, 0x00},
	}
	for name, data := range hostile {
		key := "traces/hostile-" + name[:2] + ".parquet"
		mock.putFile(key+".bloom", data)
		var skipped bool
		a := bloomSidecarAllocDelta(func() {
			skipped = s.checkFileBloom(ctx, manifest.FileInfo{Key: key, Size: 1000}, query)
		})
		if skipped {
			t.Errorf("%s: a corrupt sidecar must not prune the file", name)
		}
		if a > ceiling {
			t.Errorf("%s: read path allocated %d bytes for a %d-byte sidecar (ceiling %d)", name, a, len(data), ceiling)
		}
	}
}

// TestBloomS3Loader_HostileBundleBounded: the partition `_bloom.bin` loader
// (bloomS3Loader, used by the bloom cache) decodes the same untrusted bytes
// with bloomindex.Unmarshal. A corrupt bundle is skipped (nil index, no error)
// without allocating in proportion to the declared count (#763).
func TestBloomS3Loader_HostileBundleBounded(t *testing.T) {
	const ceiling = 4 << 20 // assumed generous: TotalAlloc is process-wide; the crashers allocated ~56 MB
	mock := newMockS3Server()
	defer mock.close()
	pool := testPool(t, mock.url())
	loader := bloomS3Loader(pool, "traces/")

	good := bloomindex.NewFileBloomIndex(map[string][]string{"service.name": {"api-gw"}}, 0.01)
	mock.putFile("traces/dt=2026-05-10/hour=01/_bloom.bin", good.Marshal())
	if idx, err := loader(context.Background(), "dt=2026-05-10/hour=01"); err != nil || idx == nil {
		t.Fatalf("valid bundle must load: idx=%v err=%v", idx, err)
	}

	for i, data := range [][]byte{
		{1, 0x40, 0x42, 0x0F, 0x00},
		{2, 0x40, 0x42, 0x0F, 0x00},
	} {
		part := "dt=2026-05-10/hour=0" + string(rune('2'+i))
		mock.putFile("traces/"+part+"/_bloom.bin", data)
		var (
			idx *bloomindex.Index
			err error
		)
		a := bloomSidecarAllocDelta(func() { idx, err = loader(context.Background(), part) })
		if err != nil || idx != nil {
			t.Errorf("v%d: a corrupt bundle must be skipped (nil, nil), got idx=%v err=%v", i+1, idx, err)
		}
		if a > ceiling {
			t.Errorf("v%d: loader allocated %d bytes for a %d-byte bundle (ceiling %d)", i+1, a, len(data), ceiling)
		}
	}
}
