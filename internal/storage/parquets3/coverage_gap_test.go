package parquets3

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// ---------------------------------------------------------------------------
// 1. isFileNotFoundError
// ---------------------------------------------------------------------------

func TestCoverage_isFileNotFoundError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"NoSuchKey", errors.New("NoSuchKey: bucket/key"), true},
		{"NotFound", errors.New("NotFound"), true},
		{"HTTP 404", errors.New("HTTP 404"), true},
		{"does not exist", errors.New("does not exist"), true},
		{"file not found", errors.New("file not found"), true},
		{"connection timeout", errors.New("connection timeout"), false},
		{"access denied", errors.New("access denied"), false},
		{"empty error", errors.New(""), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isFileNotFoundError(tc.err)
			if got != tc.want {
				t.Fatalf("isFileNotFoundError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 2. fileLabelsMatch
// ---------------------------------------------------------------------------

func TestCoverage_fileLabelsMatch(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		check  PushDownCheck
		want   bool
	}{
		// PushDownExact
		{"exact match present", []string{"alpha", "beta"}, PushDownCheck{Op: PushDownExact, Value: "beta"}, true},
		{"exact match absent", []string{"alpha", "beta"}, PushDownCheck{Op: PushDownExact, Value: "gamma"}, false},
		{"exact empty values", []string{}, PushDownCheck{Op: PushDownExact, Value: "any"}, false},

		// PushDownPrefix
		{"prefix match", []string{"foobar", "baz"}, PushDownCheck{Op: PushDownPrefix, Value: "foo"}, true},
		{"prefix no match", []string{"bar", "baz"}, PushDownCheck{Op: PushDownPrefix, Value: "foo"}, false},

		// PushDownGreaterThan
		{"gt value above", []string{"b", "d"}, PushDownCheck{Op: PushDownGreaterThan, Value: "c"}, true},
		{"gt all below", []string{"a", "b"}, PushDownCheck{Op: PushDownGreaterThan, Value: "c"}, false},

		// PushDownLessThan
		{"lt value below", []string{"b", "d"}, PushDownCheck{Op: PushDownLessThan, Value: "c"}, true},
		{"lt all above", []string{"d", "e"}, PushDownCheck{Op: PushDownLessThan, Value: "c"}, false},

		// Unknown op → conservative true
		{"unknown op", []string{"anything"}, PushDownCheck{Op: PushDownOp(99), Value: "x"}, true},

		// Unknown op with empty values → false (loop doesn't execute)
		{"unknown op empty", []string{}, PushDownCheck{Op: PushDownOp(99), Value: "x"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := fileLabelsMatch(tc.values, tc.check)
			if got != tc.want {
				t.Fatalf("fileLabelsMatch(%v, %+v) = %v, want %v", tc.values, tc.check, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 3. parquetValueToString
// ---------------------------------------------------------------------------

func TestCoverage_parquetValueToString(t *testing.T) {
	tests := []struct {
		name string
		val  parquet.Value
		want string
	}{
		{"null value", parquet.NullValue(), ""},
		{"byte array string", parquet.ValueOf("hello").Level(0, 0, 0), "hello"},
		{"int64", parquet.ValueOf(int64(42)), "42"},
		{"bool true", parquet.ValueOf(true), "true"},
		{"bool false", parquet.ValueOf(false), "false"},
		{"int32", parquet.ValueOf(int32(7)), "7"},
		{"float64", parquet.ValueOf(float64(3.14)), "3.14"},
		{"empty string", parquet.ValueOf("").Level(0, 0, 0), ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parquetValueToString(tc.val)
			if got != tc.want {
				t.Fatalf("parquetValueToString(%v) = %q, want %q", tc.val, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 4. valueMatchesCheck
// ---------------------------------------------------------------------------

func TestCoverage_valueMatchesCheck(t *testing.T) {
	mkVal := func(s string) parquet.Value {
		return parquet.ValueOf(s).Level(0, 0, 0)
	}

	tests := []struct {
		name  string
		val   parquet.Value
		check PushDownCheck
		want  bool
	}{
		// Exact
		{"exact match", mkVal("abc"), PushDownCheck{Op: PushDownExact, Value: "abc"}, true},
		{"exact miss", mkVal("abc"), PushDownCheck{Op: PushDownExact, Value: "xyz"}, false},

		// Prefix
		{"prefix present", mkVal("foobar"), PushDownCheck{Op: PushDownPrefix, Value: "foo"}, true},
		{"prefix absent", mkVal("bar"), PushDownCheck{Op: PushDownPrefix, Value: "foo"}, false},

		// GreaterThan
		{"gt above", mkVal("d"), PushDownCheck{Op: PushDownGreaterThan, Value: "c"}, true},
		{"gt equal", mkVal("c"), PushDownCheck{Op: PushDownGreaterThan, Value: "c"}, false},
		{"gt below", mkVal("b"), PushDownCheck{Op: PushDownGreaterThan, Value: "c"}, false},

		// LessThan
		{"lt below", mkVal("b"), PushDownCheck{Op: PushDownLessThan, Value: "c"}, true},
		{"lt equal", mkVal("c"), PushDownCheck{Op: PushDownLessThan, Value: "c"}, false},
		{"lt above", mkVal("d"), PushDownCheck{Op: PushDownLessThan, Value: "c"}, false},

		// Unknown op → true
		{"unknown op", mkVal("anything"), PushDownCheck{Op: PushDownOp(99), Value: "x"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := valueMatchesCheck(tc.val, tc.check)
			if got != tc.want {
				t.Fatalf("valueMatchesCheck(%v, %+v) = %v, want %v", tc.val, tc.check, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 5. footerReaderAt.ReadAt
// ---------------------------------------------------------------------------

func TestCoverage_footerReaderAt_ReadAt(t *testing.T) {
	footer := []byte("FOOTERDATA")      // 10 bytes
	var fileSize int64 = 4 + 20 + 10     // magic(4) + gap(20) + footer(10) = 34

	r := &footerReaderAt{
		footer:   footer,
		fileSize: fileSize,
	}

	t.Run("read PAR1 magic at offset 0", func(t *testing.T) {
		buf := make([]byte, 4)
		n, err := r.ReadAt(buf, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 4 {
			t.Fatalf("expected 4 bytes, got %d", n)
		}
		if string(buf) != "PAR1" {
			t.Fatalf("expected PAR1, got %q", string(buf))
		}
	})

	t.Run("read footer region", func(t *testing.T) {
		footerStart := fileSize - int64(len(footer)) // 24
		buf := make([]byte, len(footer))
		n, err := r.ReadAt(buf, footerStart)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != len(footer) {
			t.Fatalf("expected %d bytes, got %d", len(footer), n)
		}
		if string(buf) != "FOOTERDATA" {
			t.Fatalf("expected FOOTERDATA, got %q", string(buf))
		}
	})

	t.Run("read gap region returns zeros", func(t *testing.T) {
		buf := make([]byte, 5)
		n, err := r.ReadAt(buf, 10) // offset 10 is in the gap (4..24)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 5 {
			t.Fatalf("expected 5 bytes, got %d", n)
		}
		for i, b := range buf {
			if b != 0 {
				t.Fatalf("expected zero at index %d, got %d", i, b)
			}
		}
	})

	t.Run("read past EOF", func(t *testing.T) {
		buf := make([]byte, 4)
		_, err := r.ReadAt(buf, fileSize)
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
	})

	t.Run("negative offset", func(t *testing.T) {
		buf := make([]byte, 4)
		_, err := r.ReadAt(buf, -1)
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
	})

	t.Run("read crossing magic into gap", func(t *testing.T) {
		buf := make([]byte, 8)
		n, err := r.ReadAt(buf, 0) // 4 bytes magic + 4 bytes gap
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 8 {
			t.Fatalf("expected 8 bytes, got %d", n)
		}
		if string(buf[:4]) != "PAR1" {
			t.Fatalf("first 4 bytes should be PAR1, got %q", string(buf[:4]))
		}
		for i := 4; i < 8; i++ {
			if buf[i] != 0 {
				t.Fatalf("expected zero at index %d, got %d", i, buf[i])
			}
		}
	})

	t.Run("read crossing gap into footer", func(t *testing.T) {
		footerStart := fileSize - int64(len(footer)) // 24
		buf := make([]byte, 8)
		n, err := r.ReadAt(buf, footerStart-4) // 4 bytes gap + 4 bytes footer
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 8 {
			t.Fatalf("expected 8 bytes, got %d", n)
		}
		// First 4 bytes should be zeros (gap)
		for i := 0; i < 4; i++ {
			if buf[i] != 0 {
				t.Fatalf("expected zero at index %d, got %d", i, buf[i])
			}
		}
		// Last 4 bytes should be start of footer
		if string(buf[4:8]) != "FOOT" {
			t.Fatalf("expected FOOT, got %q", string(buf[4:8]))
		}
	})

	t.Run("read extending past file size truncates", func(t *testing.T) {
		buf := make([]byte, 20)
		n, err := r.ReadAt(buf, fileSize-5) // only 5 bytes available
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 5 {
			t.Fatalf("expected 5 bytes, got %d", n)
		}
		// Should be last 5 bytes of footer
		if string(buf[:5]) != "RDATA" {
			t.Fatalf("expected RDATA, got %q", string(buf[:5]))
		}
	})
}

// ---------------------------------------------------------------------------
// 6. ParseFooterFromData
// ---------------------------------------------------------------------------

func TestCoverage_ParseFooterFromData(t *testing.T) {
	t.Run("valid parquet file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.parquet")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		type simpleRow struct {
			Name string `parquet:"name"`
		}
		w := parquet.NewGenericWriter[simpleRow](f)
		if _, err := w.Write([]simpleRow{{Name: "test"}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}

		cf, pf, err := ParseFooterFromData("test-key", data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cf == nil {
			t.Fatal("expected non-nil CachedFooter")
		}
		if pf == nil {
			t.Fatal("expected non-nil parquet.File")
		}
		if cf.FileSize != int64(len(data)) {
			t.Fatalf("expected FileSize=%d, got %d", len(data), cf.FileSize)
		}
	})

	t.Run("invalid data", func(t *testing.T) {
		_, _, err := ParseFooterFromData("bad-key", []byte("not a parquet file"))
		if err == nil {
			t.Fatal("expected error for invalid data")
		}
	})

	t.Run("empty data", func(t *testing.T) {
		_, _, err := ParseFooterFromData("empty-key", []byte{})
		if err == nil {
			t.Fatal("expected error for empty data")
		}
	})
}

// ---------------------------------------------------------------------------
// 7. SetFlushCacheCallback
// ---------------------------------------------------------------------------

func TestCoverage_SetFlushCacheCallback(t *testing.T) {
	bw := &BatchWriter{}
	if bw.flushCacheCb != nil {
		t.Fatal("expected nil flushCacheCb initially")
	}

	var called bool
	cb := FlushCacheCallback(func(fileKey string, data []byte) {
		called = true
	})
	bw.SetFlushCacheCallback(cb)

	if bw.flushCacheCb == nil {
		t.Fatal("expected flushCacheCb to be set")
	}

	// Invoke it to verify it's the right callback
	bw.flushCacheCb("key", []byte("data"))
	if !called {
		t.Fatal("expected callback to be invoked")
	}
}

// ---------------------------------------------------------------------------
// Additional: ParseFooterFromData with bytes.Buffer-generated parquet
// ---------------------------------------------------------------------------

func TestCoverage_ParseFooterFromData_InMemory(t *testing.T) {
	type miniRow struct {
		Value int64 `parquet:"value"`
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[miniRow](&buf)
	if _, err := w.Write([]miniRow{{Value: 1}, {Value: 2}, {Value: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	cf, pf, err := ParseFooterFromData("in-memory-key", data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cf == nil || pf == nil {
		t.Fatal("expected non-nil results")
	}
	if len(pf.RowGroups()) == 0 {
		t.Fatal("expected at least one row group")
	}
}
