package parquets3

import (
	"context"
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
)

// idxKey fixes the group's trace_id_idx object key (once). It is the span object's
// key with the signal directory swapped (traces -> traces_idx) and the same
// <nonce>-<slice> file name, so the buffer view hides it with the span object's
// segment and a restarted window names the same object.
func (w *BatchWriter) idxKey(up *traceGroupUpload) string {
	if up.idxKey == "" {
		if up.batchID == "" {
			up.batchID = randomBatchID()
		}
		up.idxKey = fmt.Sprintf("%s%s/%s.parquet", tracesidx.PrefixFor(w.prefixForTenant(up.accountID, up.projectID)), up.partition, up.batchID)
	}
	return up.idxKey
}

// uploadIdxGroup stores the group's trace_id_idx rows as one object of the idx table
// and registers it with the manifest. Like uploadTraceGroup it never writes a key
// twice with different content: the rows are sorted and encoded deterministically, a
// key the manifest already has or has retired is settled without an upload.
func (w *BatchWriter) uploadIdxGroup(ctx context.Context, up *traceGroupUpload) error {
	key := w.idxKey(up)
	if w.manifest.HasKey(key) {
		logger.Infof("skipped %s: an earlier upload of it is already live in the manifest; rows=%d", key, len(up.idxRows))
		return nil
	}
	if w.manifest.IsRetired(key) {
		metrics.InsertRowsSuperseded.Add(len(up.idxRows))
		return nil
	}
	if up.idxData == nil {
		data, info, err := tracesidx.Marshal(up.idxRows, w.cfg.CompressionLevel)
		if err != nil {
			return fmt.Errorf("write trace_id_idx parquet: %w", err)
		}
		up.idxData, up.idxInfo = data, info
	}
	bucket, uploader := w.bucketForTenant(up.accountID, up.projectID)
	metrics.S3RequestsTotal.Inc("PUT")
	if err := uploader.Upload(ctx, key, up.idxData); err != nil {
		metrics.S3ErrorsTotal.Inc("PUT")
		return err
	}
	metrics.InsertBytesUploaded.Add(len(up.idxData))
	rows := int64(len(up.idxRows))
	fi := manifest.FileInfo{
		Key:               key,
		Bucket:            bucket,
		Size:              int64(len(up.idxData)),
		RowCount:          rows,
		MinTimeNs:         up.idxInfo.MinTime,
		MaxTimeNs:         up.idxInfo.MaxTime,
		RawBytes:          rows * tracesidx.RawBytesPerRow,
		SchemaFingerprint: schemaFingerprint(w.mode),
	}
	if !w.manifest.AddFileUnlessRetired(up.partition, fi) {
		w.manifest.Retire(key, "", true)
		metrics.InsertRowsSuperseded.Add(len(up.idxRows))
		return nil
	}
	w.totalBytes.Add(int64(len(up.idxData)))
	metrics.TraceIdxRowsFlushed.Add(int(rows))
	// The object's membership summary (fences, time bounds, bloom filter) goes to pmeta: the
	// lookups of the table skip the object, without a read, for an id it cannot hold.
	if sum, err := tracesidx.Summarize(up.idxData); err == nil {
		w.catalogObserver.OnIdxFlush(key, sum)
	} else {
		logger.Warnf("trace_id_idx summary of %s: %s", key, err)
	}
	logger.Infof("flushed trace_id_idx partition; partition=%s, tenant=%d:%d, rows=%d, bytes=%d, key=%s",
		up.partition, up.accountID, up.projectID, rows, len(up.idxData), key)
	return nil
}
