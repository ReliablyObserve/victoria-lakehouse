package schema

import (
	"bytes"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/parquet-go/parquet-go"
)

func createdByOf(t *testing.T) string {
	t.Helper()
	type row struct{ A int64 }
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf, ParquetCreatedBy())
	if _, err := w.Write([]row{{1}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	return f.Metadata().CreatedBy
}

// TestParquetCreatedBy pins the footer string: the Lakehouse application and
// the stamped release, "dev" when the binary was built without one. It must
// not depend on the Go build information, which differs between toolchains.
func TestParquetCreatedBy(t *testing.T) {
	saved := buildinfo.Version
	t.Cleanup(func() { buildinfo.Version = saved })

	buildinfo.Version = ""
	if got, want := createdByOf(t), "victoria-lakehouse version dev(build )"; got != want {
		t.Errorf("unstamped: created_by = %q, want %q", got, want)
	}
	buildinfo.Version = "v0.143.10"
	if got, want := createdByOf(t), "victoria-lakehouse version v0.143.10(build )"; got != want {
		t.Errorf("stamped: created_by = %q, want %q", got, want)
	}
}
