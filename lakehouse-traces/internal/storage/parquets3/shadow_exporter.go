package parquets3

import (
	"context"
	"fmt"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// ShadowExporter runs the Option B buffer→Parquet export (P5) in parallel with
// the authoritative legacy flush, writing the result to a SHADOW S3 prefix that
// is NOT registered in the manifest. It exists to validate, over real traffic,
// that the buffer-sourced Parquet matches the legacy Parquet — before the
// cutover that retires the legacy []TraceRow path. Zero production impact: it
// only adds shadow objects an operator can diff, plus metrics
// (lakehouse_buffer_shadow_export_*).
type ShadowExporter struct {
	store            LocalBuffer
	uploader         PoolWriter
	prefix           string // e.g. "0/0/traces_shadow/" — parallel to the live prefix
	rowGroupSize     int
	compressionLevel int
}

// NewShadowExporter builds a shadow exporter. prefix should be a dedicated
// shadow location (never the live data prefix) so shadow objects can never be
// picked up by the manifest/query path.
func NewShadowExporter(store LocalBuffer, uploader PoolWriter, prefix string, rowGroupSize, compressionLevel int) *ShadowExporter {
	return &ShadowExporter{
		store:            store,
		uploader:         uploader,
		prefix:           prefix,
		rowGroupSize:     rowGroupSize,
		compressionLevel: compressionLevel,
	}
}

// ExportTenantOnce exports the buffer window [startNs, endNs] for one tenant to
// the shadow prefix and records metrics. Returns the number of rows exported (0
// when the window is empty for the tenant). Errors are counted and returned but
// never propagated to ingestion (the caller runs this off the hot path).
func (se *ShadowExporter) ExportTenantOnce(ctx context.Context, tenant logstorage.TenantID, startNs, endNs int64) (int, error) {
	data, _, n, err := ExportBufferToParquet(ctx, se.store, tenant, startNs, endNs, se.rowGroupSize, se.compressionLevel)
	if err != nil {
		metrics.BufferShadowExportErrors.Inc()
		return 0, err
	}
	if n == 0 {
		return 0, nil
	}
	key := fmt.Sprintf("%s%d_%d/%s/%s.parquet", se.prefix, tenant.AccountID, tenant.ProjectID,
		partitionFromNano(startNs), randomBatchID())
	if err := se.uploader.Upload(ctx, key, data); err != nil {
		metrics.BufferShadowExportErrors.Inc()
		return 0, err
	}
	metrics.BufferShadowExportRows.Add(n)
	metrics.BufferShadowExportFiles.Inc()
	metrics.BufferShadowExportBytes.Add(len(data))
	return n, nil
}

// Run loops on a ticker, exporting each tenant's just-elapsed window to the
// shadow prefix until ctx is cancelled. tenantsFn supplies the tenants with data
// in [startNs, endNs] (e.g. via the buffer's GetTenantIDs). Errors are swallowed
// (counted) so a transient export problem never stalls the loop.
func (se *ShadowExporter) Run(ctx context.Context, interval time.Duration, tenantsFn func(startNs, endNs int64) []logstorage.TenantID) {
	if interval <= 0 {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	last := time.Now().Add(-interval).UnixNano()
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-t.C:
			now := tick.UnixNano()
			for _, tenant := range tenantsFn(last, now) {
				_, _ = se.ExportTenantOnce(ctx, tenant, last, now)
			}
			last = now
		}
	}
}
