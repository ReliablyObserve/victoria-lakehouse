package cache

import (
	"os"
	"path/filepath"
	"testing"
)

func FuzzLRUPutGet(f *testing.F) {
	f.Add("key1", []byte("value1"))
	f.Add("", []byte("empty-key"))
	f.Add("key", []byte{})
	f.Add("key", []byte("\x00\x01\x02"))
	f.Add("very-long-key-aaaaaaaaaaaaa", []byte("short"))
	f.Add("key/with/slashes", []byte("data"))
	f.Add("key with spaces", []byte("data"))

	f.Fuzz(func(t *testing.T, key string, val []byte) {
		c := NewLRU(1024 * 1024)
		c.Put(key, val)

		got, ok := c.Get(key)
		if !ok {
			t.Errorf("Get(%q) returned not found after Put", key)
			return
		}
		if len(got) != len(val) {
			t.Errorf("Get(%q) length = %d, want %d", key, len(got), len(val))
			return
		}
		for i := range got {
			if got[i] != val[i] {
				t.Errorf("Get(%q)[%d] = %d, want %d", key, i, got[i], val[i])
				return
			}
		}
	})
}

func FuzzDiskCacheKeyToPath(f *testing.F) {
	f.Add("simple")
	f.Add("dt=2026-05-02/hour=10/file.parquet")
	f.Add("key:with:colons")
	f.Add("key=with=equals")
	f.Add("")
	f.Add("../../../etc/passwd")
	f.Add("\x00\x01\x02")
	f.Add("a/b/c/d/e")

	f.Fuzz(func(t *testing.T, key string) {
		dir := t.TempDir()
		dc, err := NewDiskCache(dir, 1024*1024, 0.8)
		if err != nil {
			t.Fatal(err)
		}

		path := dc.keyToPath(key)

		if !filepath.IsAbs(path) && path != filepath.Join(dir, "") {
			absPath, err := filepath.Abs(path)
			if err != nil {
				return
			}
			absDir, err := filepath.Abs(dir)
			if err != nil {
				return
			}
			if len(absPath) > 0 && !hasPrefix(absPath, absDir) {
				t.Errorf("keyToPath(%q) = %q escapes cache dir %q", key, path, dir)
			}
		}
	})
}

func hasPrefix(path, prefix string) bool {
	return len(path) >= len(prefix) && path[:len(prefix)] == prefix
}

func FuzzLabelIndexAddGet(f *testing.F) {
	f.Add("service.name", "api-gw")
	f.Add("", "")
	f.Add("k8s.pod.name", "pod-abc-123")
	f.Add("field\x00null", "value\x00null")
	f.Add("a.b.c.d.e", "value")

	f.Fuzz(func(t *testing.T, name, value string) {
		idx := NewLabelIndex()
		idx.Add(name, []string{value})

		names := idx.GetFieldNames()
		found := false
		for _, n := range names {
			if n == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("GetFieldNames() missing %q after Add", name)
		}

		vals := idx.GetFieldValues(name, 0)
		if len(vals) == 0 {
			t.Errorf("GetFieldValues(%q) returned empty after Add", name)
		}
	})
}

func FuzzPersisterRoundTrip(f *testing.F) {
	f.Add("field1", "val1", "val2")
	f.Add("", "", "")
	f.Add("service.name", "api-gw", "web-frontend")

	f.Fuzz(func(t *testing.T, name, v1, v2 string) {
		dir := t.TempDir()
		p, err := NewPersister(dir)
		if err != nil {
			t.Fatal(err)
		}

		idx := NewLabelIndex()
		idx.Add(name, []string{v1, v2})

		if err := p.SaveLabelIndex(idx); err != nil {
			t.Fatal(err)
		}

		loaded, err := p.LoadLabelIndex()
		if err != nil {
			t.Fatal(err)
		}

		if loaded.Len() != idx.Len() {
			t.Errorf("loaded index len = %d, want %d", loaded.Len(), idx.Len())
		}
	})
}

func FuzzDiskCachePutFromPath_Traversal(f *testing.F) {
	f.Add("normal-key")
	f.Add("../escape")
	f.Add("../../etc/passwd")
	f.Add("..%2f..%2fetc%2fpasswd")
	f.Add("key/../../../tmp/evil")
	f.Add("/absolute/path")
	f.Add("a/b/../../../outside")

	f.Fuzz(func(t *testing.T, key string) {
		dir := t.TempDir()
		dc, err := NewDiskCache(dir, 1024*1024, 0.8)
		if err != nil {
			t.Fatal(err)
		}

		srcFile := filepath.Join(dir, "src.dat")
		if err := os.WriteFile(srcFile, []byte("test"), 0o600); err != nil {
			t.Fatal(err)
		}

		_ = dc.PutFromPath(key, srcFile)
	})
}
