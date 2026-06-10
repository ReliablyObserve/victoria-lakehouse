package parquets3

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// snapshotPath returns a fresh snapshot file path in a per-test temp dir.
func snapshotPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "footer_cache.snapshot")
}

// TestFooterCacheSnapshot_RoundTrip is the core contract: Save writes
// the cache's keys MRU-first and Load returns them in the same order,
// so the next process prefetches the hottest footers first.
func TestFooterCacheSnapshot_RoundTrip(t *testing.T) {
	fc := NewFooterCache(10)
	fc.Put("logs/dt=2026-06-01/hour=01/a.parquet", &CachedFooter{FileSize: 1})
	fc.Put("logs/dt=2026-06-01/hour=02/b.parquet", &CachedFooter{FileSize: 2})
	fc.Put("logs/dt=2026-06-01/hour=03/c.parquet", &CachedFooter{FileSize: 3})
	// Touch "a" so it becomes most-recently-used.
	if _, ok := fc.Get("logs/dt=2026-06-01/hour=01/a.parquet"); !ok {
		t.Fatal("expected a.parquet in cache")
	}

	wantOrder := []string{
		"logs/dt=2026-06-01/hour=01/a.parquet",
		"logs/dt=2026-06-01/hour=03/c.parquet",
		"logs/dt=2026-06-01/hour=02/b.parquet",
	}
	if got := fc.Keys(); !reflect.DeepEqual(got, wantOrder) {
		t.Fatalf("Keys() MRU order = %v, want %v", got, wantOrder)
	}

	path := snapshotPath(t)
	if err := SaveFooterCacheKeys(fc, path); err != nil {
		t.Fatalf("SaveFooterCacheKeys: %v", err)
	}

	keys, err := LoadFooterCacheKeys(path)
	if err != nil {
		t.Fatalf("LoadFooterCacheKeys: %v", err)
	}
	if !reflect.DeepEqual(keys, wantOrder) {
		t.Errorf("loaded keys = %v, want %v (MRU-first preserved)", keys, wantOrder)
	}

	// The temp file must be gone (atomic rename, no debris).
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("temp snapshot file left behind: stat err=%v", err)
	}
}

func TestFooterCacheSnapshot_EmptyCache(t *testing.T) {
	fc := NewFooterCache(4)
	path := snapshotPath(t)
	if err := SaveFooterCacheKeys(fc, path); err != nil {
		t.Fatalf("SaveFooterCacheKeys(empty): %v", err)
	}
	keys, err := LoadFooterCacheKeys(path)
	if err != nil {
		t.Fatalf("LoadFooterCacheKeys(empty): %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("expected no keys, got %v", keys)
	}
}

func TestSaveFooterCacheKeys_NilCache(t *testing.T) {
	if err := SaveFooterCacheKeys(nil, snapshotPath(t)); err == nil {
		t.Fatal("expected error for nil cache")
	}
}

func TestSaveFooterCacheKeys_UnwritablePath(t *testing.T) {
	fc := NewFooterCache(4)
	fc.Put("k", &CachedFooter{})
	// Parent directory does not exist — OpenFile must fail and the
	// error must surface (shutdown sequence logs it).
	path := filepath.Join(t.TempDir(), "no", "such", "dir", "snap")
	if err := SaveFooterCacheKeys(fc, path); err == nil {
		t.Fatal("expected error for unwritable path")
	}
}

// TestLoadFooterCacheKeys_Missing: first boot / fresh deploy — absence
// is "no prior cache", NOT an error.
func TestLoadFooterCacheKeys_Missing(t *testing.T) {
	keys, err := LoadFooterCacheKeys(filepath.Join(t.TempDir(), "nope"))
	if err != nil {
		t.Fatalf("missing snapshot must not error, got %v", err)
	}
	if keys != nil {
		t.Errorf("missing snapshot must yield nil keys, got %v", keys)
	}
}

// TestLoadFooterCacheKeys_Corrupt walks every adversarial-input gate
// in the loader: each corruption must produce an error, never a panic
// or a silently-wrong key list.
func TestLoadFooterCacheKeys_Corrupt(t *testing.T) {
	write := func(t *testing.T, data []byte) string {
		t.Helper()
		path := snapshotPath(t)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	t.Run("bad magic", func(t *testing.T) {
		path := write(t, []byte("XXXXXXXXXXXXXXXXXXXX"))
		if _, err := LoadFooterCacheKeys(path); err == nil {
			t.Fatal("expected magic-mismatch error")
		}
	})

	t.Run("truncated before magic", func(t *testing.T) {
		path := write(t, []byte{'L', 'H'})
		if _, err := LoadFooterCacheKeys(path); err == nil {
			t.Fatal("expected read-magic error")
		}
	})

	t.Run("wrong version byte", func(t *testing.T) {
		bad := append([]byte{'L', 'H', 'F', 'C', 99}, make([]byte, 8)...)
		path := write(t, bad)
		if _, err := LoadFooterCacheKeys(path); err == nil {
			t.Fatal("expected version-mismatch error (magic includes version)")
		}
	})

	t.Run("truncated count", func(t *testing.T) {
		path := write(t, append([]byte(nil), footerCacheSnapshotMagic...))
		if _, err := LoadFooterCacheKeys(path); err == nil {
			t.Fatal("expected read-count error")
		}
	})

	t.Run("implausible count", func(t *testing.T) {
		// Claims 2^63 entries in a 13-byte file: the loader must refuse
		// the allocation instead of OOMing.
		data := append([]byte(nil), footerCacheSnapshotMagic...)
		var countBuf [8]byte
		binary.LittleEndian.PutUint64(countBuf[:], 1<<63)
		data = append(data, countBuf[:]...)
		path := write(t, data)
		if _, err := LoadFooterCacheKeys(path); err == nil {
			t.Fatal("expected implausible-count error")
		}
	})

	t.Run("truncated key data", func(t *testing.T) {
		// Header promises a 100-byte key but the file ends after 3 bytes.
		data := append([]byte(nil), footerCacheSnapshotMagic...)
		var countBuf [8]byte
		binary.LittleEndian.PutUint64(countBuf[:], 1)
		data = append(data, countBuf[:]...)
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], 100)
		data = append(data, hdr[:]...)
		data = append(data, 'a', 'b', 'c')
		path := write(t, data)
		if _, err := LoadFooterCacheKeys(path); err == nil {
			t.Fatal("expected truncated-key error")
		}
	})

	t.Run("missing key length header", func(t *testing.T) {
		// Count says 2 entries but only 1 is present.
		fc := NewFooterCache(4)
		fc.Put("only-key", &CachedFooter{})
		good := snapshotPath(t)
		if err := SaveFooterCacheKeys(fc, good); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(good)
		if err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint64(data[len(footerCacheSnapshotMagic):], 2)
		path := write(t, data)
		if _, err := LoadFooterCacheKeys(path); err == nil {
			t.Fatal("expected error when count exceeds stored entries")
		}
	})
}

func TestBytesEqual(t *testing.T) {
	cases := []struct {
		a, b []byte
		want bool
	}{
		{nil, nil, true},
		{[]byte{}, nil, true},
		{[]byte{1, 2}, []byte{1, 2}, true},
		{[]byte{1, 2}, []byte{1, 3}, false},
		{[]byte{1, 2}, []byte{1, 2, 3}, false},
	}
	for _, c := range cases {
		if got := bytesEqual(c.a, c.b); got != c.want {
			t.Errorf("bytesEqual(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}
