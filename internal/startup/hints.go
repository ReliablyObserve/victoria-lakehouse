package startup

import (
	"fmt"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// HintInputs is the runtime state the post-startup hint emitter
// looks at to decide which tuning suggestions are worth logging.
// Kept narrow on purpose: the hints package mustn't pull in the
// storage/manifest packages or it'd create an import cycle —
// callers (main) gather the numbers and pass them in.
//
// Mirrors the VL/VM upstream pattern where the binary emits
// runtime-derived tuning hints at startup. The hints have to be
// actionable: every line names the config knob to change AND
// quantifies the gain. "Increase memory" without a target is
// useless to an operator under deploy pressure.
type HintInputs struct {
	// Files in the manifest after recovery + S3 refresh.
	ManifestFiles int64

	// File count threshold below which /ready=503 (0 = gate off).
	MinManifestFiles int64

	// Configured footer cache capacity (entries).
	FooterCacheMax int

	// Number of insert-pod peers visible to BufferBridge.
	BufferBridgePeers int

	// Wall-clock age of the snapshot we just loaded (0 if none).
	SnapshotAge time.Duration

	// Configured manifest persist interval.
	PersistInterval time.Duration

	// Time S3 refresh took.
	S3RefreshDuration time.Duration

	// Time the full background warmup took.
	WarmupDuration time.Duration
}

// EmitStartupHints writes structured warn-level tuning hints to
// the logger. Each hint is one log line operators can search for
// in their aggregator ("hint:" prefix). They fire only when
// something is genuinely off — a healthy startup logs no hints,
// matching VL's principle of "silence means OK".
//
// Hint categories (any may fire independently):
//   - **footer-cache** — cache cap small vs file count
//   - **snapshot-staleness** — disk snapshot older than expected
//   - **buffer-peers** — single insert peer; cluster has no
//     redundancy for the cold-buffer window during restart
//   - **warmup-time** — background warmup is slow vs typical
//   - **ready-gate** — MinManifestFiles=0 in a non-trivial cluster
//     (operator opted out of the honesty gate)
func EmitStartupHints(in HintInputs) {
	// Footer cache vs file count. If the cache holds <10% of
	// recent files, wide-window queries will pay an S3 round
	// trip per footer. At PB scale this becomes minutes per query.
	if in.FooterCacheMax > 0 && in.ManifestFiles > 0 {
		coverage := float64(in.FooterCacheMax) / float64(in.ManifestFiles)
		if coverage < 0.1 && in.ManifestFiles > 1000 {
			recommended := int(float64(in.ManifestFiles) * 0.2)
			memMB := recommended * 50 / 1024 // ~50KB/footer avg
			logger.Warnf("hint:footer-cache — cache holds %d entries vs %d manifest files (%.0f%% coverage). Wide-window queries will hit S3 footer fetches on every uncached file. Recommend cfg.cache.footer_max_items=%d (~%d MB RAM)",
				in.FooterCacheMax, in.ManifestFiles, coverage*100, recommended, memMB)
		}
	}

	// Snapshot staleness. If we loaded a snapshot older than
	// 6× the persist interval, something kept the pod from
	// persisting on schedule — long downtime, crash without
	// graceful shutdown, or a slow disk that timed out the
	// shutdown persist.
	if in.SnapshotAge > 0 && in.PersistInterval > 0 {
		threshold := 6 * in.PersistInterval
		if in.SnapshotAge > threshold {
			logger.Warnf("hint:snapshot-staleness — loaded snapshot is %v old (%.1fx the persist interval %v). Queries during background warmup will miss files written by other peers in that window. Consider cfg.shutdown.persist_timeout increase if SaveTo timed out on previous shutdown",
				in.SnapshotAge.Round(time.Second), float64(in.SnapshotAge)/float64(in.PersistInterval), in.PersistInterval)
		}
	}

	// Single insert peer. BufferBridge falls back to this one
	// peer's buffer — if that peer is the same pod that just
	// restarted, the cluster has NO warm buffer anywhere and
	// queries during the cold-start window return only flushed
	// data. Scale-out the insert tier (≥2 pods) to avoid this.
	if in.BufferBridgePeers <= 1 {
		logger.Warnf("hint:buffer-peers — BufferBridge sees %d insert peer(s). On restart, the buffered-but-unflushed window (~%v before flush) is invisible until ingest resumes. Scale the insert tier to ≥2 pods for cluster-wide cold-start resilience",
			in.BufferBridgePeers, in.PersistInterval)
	}

	// Slow warmup. If warmup took >2× the snapshot age, the
	// S3 refresh is doing much more work than the typical delta
	// since last persist — symptom of orphaned files, missing
	// snapshot, or compaction-heavy partitions. Operators should
	// shorten manifest.refresh_interval or increase persist
	// frequency.
	if in.WarmupDuration > 30*time.Second {
		logger.Warnf("hint:warmup-time — background warmup took %v (recovery: %v, refresh: %v). At PB scale this delays full /ready=200 routing decisions. Tune cfg.cache.warmup_partitions / warmup_max_files, or increase cfg.manifest.persist_interval frequency",
			in.WarmupDuration.Round(time.Millisecond),
			(in.WarmupDuration - in.S3RefreshDuration).Round(time.Millisecond),
			in.S3RefreshDuration.Round(time.Millisecond))
	}

	// MinManifestFiles gate off but cluster is sizeable. The
	// /ready honesty gate prevents a fresh pod from lying about
	// readiness with an empty manifest. At >10k files the
	// trade-off favours strictness; <10k may legitimately be a
	// dev/CI cluster where the gate adds friction.
	if in.MinManifestFiles == 0 && in.ManifestFiles >= 10000 {
		gate := in.ManifestFiles / 10
		logger.Warnf("hint:ready-gate — cfg.startup.min_manifest_files=0 but manifest holds %d files. On fresh-PVC restart the pod would lie /ready=200 with empty cold tier for 3-5 min while background S3 LIST runs. Recommend cfg.startup.min_manifest_files=%d (10%% of current count)",
			in.ManifestFiles, gate)
	}
}

// FormatHint returns the hint text without emitting. Useful for
// tests that need to inspect the message body without parsing
// log output. Mirror of EmitStartupHints' content with the same
// formatting contract.
func FormatHint(category string, msg string) string {
	return fmt.Sprintf("hint:%s — %s", category, msg)
}
