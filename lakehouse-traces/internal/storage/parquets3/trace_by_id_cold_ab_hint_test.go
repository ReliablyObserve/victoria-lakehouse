package parquets3

import (
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetfooter"
)

// tbidFooterHint records the writer's footer-tail length in fi, as the flush
// and compaction writers do (the builds that predate the field have an empty
// copy of this file).
func tbidFooterHint(fi *manifest.FileInfo, data []byte) {
	fi.FooterTail = parquetfooter.Inspect(data).Tail
}
