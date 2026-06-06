package parquets3

import (
	"context"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go/format"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// traceIndexLookupParallelism caps the number of concurrent
// fetchFooterFile calls during LookupTraceIndex. The serial loop
// blew the Jaeger client's 30s budget on a non-existent trace ID at
// 589 files × ~50ms cached footer fetch each. Bounding to the same
// query.file-workers default keeps S3 pressure in line with the
// rest of the read path.
const traceIndexLookupParallelism = 16

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

	// Flatten so the worker pool sees one job per file rather than a
	// nested loop. Avoids the per-partition slice indirection inside
	// hot goroutines.
	var flatFiles []manifest.FileInfo
	for _, partFiles := range files {
		flatFiles = append(flatFiles, partFiles...)
	}

	// Parallel footer fetch with bounded concurrency. The serial
	// version was a 30s hang on a 589-file manifest for any
	// trace ID that didn't exist in any footer — Jaeger /api/traces
	// timed out before we ever issued a 404. The cancelCtx lets
	// the first goroutine that finds a hit (or the outer ctx
	// deadline) abort the rest so we don't keep fetching footers
	// after the answer is known.
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var mu sync.Mutex
	var anyErr error
	var hit bool
	var aggStart, aggEnd int64

	sem := make(chan struct{}, traceIndexLookupParallelism)
	var wg sync.WaitGroup
	for i := range flatFiles {
		fi := flatFiles[i]
		select {
		case sem <- struct{}{}:
		case <-cancelCtx.Done():
			// Outer ctx cancelled (or sibling already found a hit
			// and cancelled — but we don't cancel on hit, so this
			// is exclusively the deadline path); stop scheduling.
			break
		}
		if cancelCtx.Err() != nil {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			entry, ok, lookupErr := s.lookupTraceIDInFile(cancelCtx, fi, traceID)
			mu.Lock()
			defer mu.Unlock()
			if ok {
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
		}()
	}
	wg.Wait()

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
