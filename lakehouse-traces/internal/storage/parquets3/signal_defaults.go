package parquets3

// Per-signal defaults for the S3 read-path knobs whose right value depends
// on the signal's file/footer geometry (planned-fetch v2 research, Part II
// §II.1/§II.2). The TRACES values live here; the logs twin holds its own
// (internal/storage/parquets3/signal_defaults.go). Every other twin file
// stays byte-identical between the modules.
const (
	// defaultFooterPrefetchBytes (s3.footer_prefetch_bytes = 0) — traces.
	// EVERY live traces compacted-L2 footer measures 467-519KB (the trace
	// index lives in footer key-value metadata), so the previous shared
	// 64KB constant could never hold one: footer prefetch hit too_big,
	// the inline fetch failed, and traces-L2 projected reads ALWAYS fell
	// back to full downloads (fallback{reason="no-footer"}). 640KB fits
	// the measured footers with ~25% headroom in a single tail GET.
	defaultFooterPrefetchBytes = 640 * 1024

	// defaultWholeFileThresholdBytes (s3.whole_file_threshold_bytes = 0)
	// — traces. S* from the cost model over the live traces file-size
	// distribution: the multi-hundred-KB trace-index footer makes the
	// cold footer fetch dearer than on logs, shifting the whole-file
	// breakeven up to 8MB.
	defaultWholeFileThresholdBytes = 8 * 1024 * 1024
)

// footerPrefetchBytes resolves s3.footer_prefetch_bytes (0/absent = the
// per-signal default above).
func (s *Storage) footerPrefetchBytes() int64 {
	if s.cfg != nil && s.cfg.S3.FooterPrefetchBytes > 0 {
		return int64(s.cfg.S3.FooterPrefetchBytes)
	}
	return defaultFooterPrefetchBytes
}

// wholeFileThresholdBytes resolves s3.whole_file_threshold_bytes (0/absent
// = the per-signal default above).
func (s *Storage) wholeFileThresholdBytes() int64 {
	if s.cfg != nil && s.cfg.S3.WholeFileThresholdBytes > 0 {
		return int64(s.cfg.S3.WholeFileThresholdBytes)
	}
	return defaultWholeFileThresholdBytes
}
