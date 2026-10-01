package parquets3

// Per-signal defaults for the S3 read-path knobs whose right value depends
// on the signal's file/footer geometry (planned-fetch v2 research, Part II
// §II.1/§II.2). The LOGS values live here; the traces twin holds its own
// (lakehouse-traces/internal/storage/parquets3/signal_defaults.go). Every
// other twin file stays byte-identical between the modules.
const (
	// defaultFooterPrefetchBytes (s3.footer_prefetch_bytes = 0) — logs.
	// Measured logs L2 footers are ~46-50KB; 128KB holds them with >2x
	// headroom AND covers the page-index stripe, which ends 91-97KB from
	// EOF on every measured file — one tail GET serves footer + all
	// ColumnIndex/OffsetIndex sections. (The previous shared 64KB constant
	// left logs only ~25% headroom and could NEVER hold a traces L2
	// footer — see the traces twin for that side of the bug-class.)
	defaultFooterPrefetchBytes = 128 * 1024

	// defaultWholeFileThresholdBytes (s3.whole_file_threshold_bytes = 0)
	// — logs. S* from the cost model over the live logs file-size
	// distribution: below 5MB one whole-file GET (which doubles as the
	// footer-cache warmup) beats footer-fetch + span RTTs on a cold open.
	defaultWholeFileThresholdBytes = 5 * 1024 * 1024
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

const (
	// defaultFooterMaxBytes (cache.footer_max_bytes = 0) — logs. The footer
	// cache is bounded by resident bytes. A cached logs footer is the raw
	// tail (footer + page-index stripe: 46-50 KB measured on L2 files,
	// 43 KB on flush-sized files, 357-431 KB with the token-bloom KV on
	// 10 MB compacted objects) plus the decoded metadata, so an entry is
	// ~0.1-0.9 MB. 256 MiB holds the footers of ~300 compacted objects to
	// ~3,000 flush-sized ones: the working set of a hot query window.
	defaultFooterMaxBytes = 256 * 1024 * 1024
)
