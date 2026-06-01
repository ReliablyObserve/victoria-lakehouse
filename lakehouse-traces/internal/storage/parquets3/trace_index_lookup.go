package parquets3

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go/format"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// LookupTraceIndex resolves a trace's start/end time bounds from the
// `_trace_idx` key-value metadata embedded in each trace Parquet file's
// footer. It mirrors VT's trace_id_idx_stream lookup but avoids the LogsQL
// stream-filter path entirely: each cold-tier trace file already carries a
// compact per-trace summary written by the BatchWriter at flush time
// (computeTraceIndex + marshalTraceIndex), so a lookup needs only the
// Parquet footers — no row-group scan, no full file download.
//
// Returns found=true with the aggregated (min start, max end) across all
// files that mention this trace ID; found=false when no file's index
// carries an entry for the trace (cold-start data, an evicted file, or a
// trace that simply never landed). On found=false the caller must fall
// back to a span-by-trace_id scan to remain VT-compliant — VT's
// /select/tempo/api/v2/traces/<id> path requires a (start, end) bound
// before it issues the span fetch, and we can't manufacture that without
// either the index or a scan.
//
// Footer access reuses the existing fetchFooterFile + footerCache machinery
// so repeated lookups for the same trace ID share cached metadata. Errors
// from individual files are logged and skipped (best-effort aggregation);
// the lookup as a whole only fails if no usable index was ever read.
func (s *Storage) LookupTraceIndex(ctx context.Context, traceID string) (startNs, endNs int64, found bool, err error) {
	if traceID == "" {
		metrics.TraceIndexLookups.Inc("miss")
		return 0, 0, false, nil
	}

	files := s.manifest.AllFiles()
	if len(files) == 0 {
		metrics.TraceIndexLookups.Inc("miss")
		return 0, 0, false, nil
	}

	// Track whether we hit a real error vs a clean miss so the metric
	// label reports the operationally interesting case.
	var anyErr error

	var hit bool
	var aggStart, aggEnd int64

	for _, partFiles := range files {
		for i := range partFiles {
			fi := partFiles[i]
			if entry, ok, lookupErr := s.lookupTraceIDInFile(ctx, fi, traceID); ok {
				if !hit || entry.StartNs < aggStart {
					aggStart = entry.StartNs
				}
				if !hit || entry.EndNs > aggEnd {
					aggEnd = entry.EndNs
				}
				hit = true
			} else if lookupErr != nil {
				anyErr = lookupErr
			}
		}
	}

	if hit {
		metrics.TraceIndexLookups.Inc("hit")
		return aggStart, aggEnd, true, nil
	}
	if anyErr != nil {
		metrics.TraceIndexLookups.Inc("error")
		// Return nil error to the caller: an error from a single footer
		// fetch shouldn't fail the whole trace-by-ID request. The metric
		// preserves the signal for an operator.
		logger.Warnf("trace-index lookup encountered footer errors; falling back to scan; trace_id=%s; last_err=%v", traceID, anyErr)
		return 0, 0, false, nil
	}
	metrics.TraceIndexLookups.Inc("miss")
	return 0, 0, false, nil
}

// lookupTraceIDInFile pulls the Parquet footer for one manifest entry and
// searches its `_trace_idx` KV metadata for the given trace ID.
func (s *Storage) lookupTraceIDInFile(ctx context.Context, fi manifest.FileInfo, traceID string) (TraceIndexEntry, bool, error) {
	f, err := s.fetchFooterFile(ctx, fi)
	if err != nil {
		return TraceIndexEntry{}, false, err
	}
	meta := f.Metadata()
	if meta == nil {
		return TraceIndexEntry{}, false, nil
	}
	return findTraceIDInFooterMeta(meta, traceID)
}

// findTraceIDInFooterMeta is split out so the lookup logic is testable
// without round-tripping through S3 or the footer cache.
func findTraceIDInFooterMeta(meta *format.FileMetaData, traceID string) (TraceIndexEntry, bool, error) {
	if meta == nil || len(meta.KeyValueMetadata) == 0 {
		return TraceIndexEntry{}, false, nil
	}
	for _, kv := range meta.KeyValueMetadata {
		if kv.Key != traceIndexMetadataKey {
			continue
		}
		entries, ok := traceIndexFromMetadata(map[string]string{traceIndexMetadataKey: kv.Value})
		if !ok {
			return TraceIndexEntry{}, false, nil
		}
		for _, e := range entries {
			if e.TraceID == traceID {
				return e, true, nil
			}
		}
		return TraceIndexEntry{}, false, nil
	}
	return TraceIndexEntry{}, false, nil
}
