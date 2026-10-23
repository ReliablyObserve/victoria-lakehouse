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
	mock.putFile("logs/good.parquet.bloom", good.Marshal())
	if !s.checkFileBloom(ctx, manifest.FileInfo{Key: "logs/good.parquet", Size: 1000}, query) {
		t.Fatal("valid sidecar without the value must prune the file")
	}

	hostile := map[string][]byte{
		"v1 count 1M": {1, 0x40, 0x42, 0x0F, 0x00},
		"v2 count 1M": {2, 0x40, 0x42, 0x0F, 0x00},
	}
	for name, data := range hostile {
		key := "logs/hostile-" + name[:2] + ".parquet"
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
