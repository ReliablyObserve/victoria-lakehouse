package schema

import (
	"cmp"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/parquet-go/parquet-go"
)

// ParquetWriterApp is the application name every Lakehouse Parquet writer
// records in the footer's created_by field.
const ParquetWriterApp = "victoria-lakehouse"

// ParquetCreatedBy is the created_by writer option shared by every Parquet
// writer (flush, compaction, delete rewrite) in both binaries:
// "victoria-lakehouse version <release>(build )", with <release> the
// buildinfo.Version stamped at link time ("dev" when unstamped).
//
// parquet-go's own default reads the library version from the Go build
// information, which is present or absent depending on the toolchain and
// build mode (Go 1.27 fills it for test binaries, Go 1.26 did not). The
// same rows then came out 23 bytes longer or shorter per file, so the files
// were not reproducible across builds. A fixed application string makes the
// bytes depend only on the rows, the writer options and the release.
func ParquetCreatedBy() parquet.WriterOption {
	return parquet.CreatedBy(ParquetWriterApp, cmp.Or(buildinfo.Version, "dev"), "")
}
