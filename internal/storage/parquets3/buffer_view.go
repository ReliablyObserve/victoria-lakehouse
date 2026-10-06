package parquets3

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// bufferView is the insert buffer as ONE query sees it, taken before the
// query's object list is used:
//
//   - the co-located segments, held (membuffer.Snapshot) until release, on a
//     node without peers; or the rows every insert peer returned through the
//     buffer bridge, with the nonces of the segments each peer read them from;
//   - nonces: the segments those rows come from. A cold object whose key
//     carries one of them (manifest.SegmentNonceOfKey) holds rows the view serves
//     itself, so it is dropped from the scan (exclude).
//
// Rows are therefore answered exactly once at every instant — before, during
// and after a segment's flush, and after a restart — with no time watermark:
// a segment is removed only once its objects are committed and no view holds
// it, and a peer keeps a committed segment readable for the grace period, so a
// select node has listed its objects before the peer stops serving the rows.
type bufferView struct {
	local     *membuffer.Snapshot
	nonces    map[string]struct{}
	bridged   bool
	logRows   []schema.LogRow
	traceRows []schema.TraceRow
}

// useLocalBuffer reports whether this node answers from its own segments: it
// has them and no insert peer (with peers every pod's rows, this one's
// included, come through the bridge).
func (s *Storage) useLocalBuffer() bool {
	return s.localBuffer != nil && (s.bufferBridge == nil || !s.bufferBridge.HasPeers())
}

// testHookBetweenViewAndList, when set by a test, runs after a read path has
// taken its buffer view and before it lists the objects: the instant a refresh
// and a reap could slip in (#379).
var testHookBetweenViewAndList func()

// openBufferView takes the query's view of the insert buffer. Call release
// when the query is done with it.
func (s *Storage) openBufferView(ctx context.Context, startNs, endNs int64, tenantIDs []logstorage.TenantID) *bufferView {
	if s.useLocalBuffer() {
		snap := s.localBuffer.Snapshot()
		return &bufferView{local: snap, nonces: snap.Nonces()}
	}
	if s.bufferBridge == nil {
		return &bufferView{}
	}
	scope := scopeFor(ctx, tenantIDs)
	v := &bufferView{bridged: true}
	switch s.cfg.Mode {
	case config.ModeLogs:
		v.logRows, v.nonces = s.bufferBridge.QueryLogs(ctx, startNs, endNs, scope)
	case config.ModeTraces:
		v.traceRows, v.nonces = s.bufferBridge.QueryTraces(ctx, startNs, endNs, scope)
	}
	return v
}

func (v *bufferView) release() {
	if v != nil && v.local != nil {
		v.local.Release()
		v.local = nil
	}
}

// exclude drops the objects of the view's segments from files.
func (v *bufferView) exclude(files []manifest.FileInfo) []manifest.FileInfo {
	if v == nil || len(v.nonces) == 0 || len(files) == 0 {
		return files
	}
	out := files[:0:0]
	for _, f := range files {
		if n := manifest.SegmentNonceOfKey(f.Key); n != "" {
			if _, live := v.nonces[n]; live {
				metrics.BufferViewExcludedObjects.Inc()
				continue
			}
		}
		out = append(out, f)
	}
	return out
}

// tenantIDs is the tenant list the local segments are queried for: the
// request's own tenants, or, for a validated cross-tenant read, every tenant
// the segments hold in the window.
func (v *bufferView) tenantIDs(ctx context.Context, tenantIDs []logstorage.TenantID, startNs, endNs int64) []logstorage.TenantID {
	own := tenantIDs
	if len(own) == 0 {
		own = []logstorage.TenantID{{}}
	}
	if !scopeFor(ctx, tenantIDs).all || v.local == nil {
		return own
	}
	ids, err := v.local.GetTenantIDs(ctx, startNs, endNs)
	if err != nil {
		logger.Warnf("global read: cannot enumerate buffered tenants, serving the request's own tenant from the buffer: %s", err)
		return own
	}
	return ids
}

// serveBufferView emits the view's rows of the request's tenants in
// [startNs, endNs]: raw rows, every tenant through its own tombstone filter
// (sink.forTenant). The caller's pipes run on top, as for rows read from
// Parquet.
func (s *Storage) serveBufferView(ctx context.Context, v *bufferView, startNs, endNs int64, maxRows int64, rowsEmitted *atomic.Int64, q *logstorage.Query, tenantIDs []logstorage.TenantID, sink *tombstoneSink) {
	if v == nil || (maxRows > 0 && rowsEmitted.Load() >= maxRows) {
		return
	}
	if v.local != nil {
		for _, id := range v.tenantIDs(ctx, tenantIDs, startNs, endNs) {
			qBuf := q.CloneWithTimeFilter(q.GetTimestamp(), startNs, endNs)
			qBuf.DropAllPipes()
			qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, []logstorage.TenantID{id}, qBuf, false, nil)
			if err := v.local.RunQuery(qctx, sink.forTenant(id)); err != nil {
				logger.Warnf("insert buffer query failed (the unflushed rows are missing from this answer): %s", err)
			}
		}
		return
	}
	if !v.bridged {
		return
	}
	scope := scopeFor(ctx, tenantIDs)
	switch s.cfg.Mode {
	case config.ModeLogs:
		s.emitBridgeLogRows(scope, rowsInWindow(v.logRows, startNs, endNs, func(r *schema.LogRow) int64 { return r.TimestampUnixNano }), sink)
	case config.ModeTraces:
		s.emitBridgeTraceRows(scope, rowsInWindow(v.traceRows, startNs, endNs, func(r *schema.TraceRow) int64 { return r.TimestampUnixNano }), sink)
	}
}

// rowsInWindow keeps the rows with a timestamp in [startNs, endNs].
func rowsInWindow[T any](rows []T, startNs, endNs int64, ts func(*T) int64) []T {
	out := rows[:0:0]
	for i := range rows {
		if t := ts(&rows[i]); t >= startNs && t <= endNs {
			out = append(out, rows[i])
		}
	}
	return out
}

// BridgeSource serves this insert pod's segments to the buffer bridge
// (/internal/buffer/query): the rows of every live segment and the segments'
// nonces, read from one snapshot.
type BridgeSource struct {
	Segments *membuffer.Segments
}

// ReadBuffer implements buffer.Source.
func (b BridgeSource) ReadBuffer(ctx context.Context, sel buffer.Selection, startNs, endNs int64, mode string) (buffer.Answer, error) {
	snap := b.Segments.Snapshot()
	defer snap.Release()
	var ans buffer.Answer
	for n := range snap.Nonces() {
		ans.Nonces = append(ans.Nonces, n)
	}
	tenants := []logstorage.TenantID{{AccountID: sel.AccountID, ProjectID: sel.ProjectID}}
	if sel.All {
		ids, err := snap.GetTenantIDs(ctx, startNs, endNs)
		if err != nil {
			return ans, err
		}
		tenants = ids
	}
	q, err := logstorage.ParseQueryAtTimestamp("*", endNs)
	if err != nil {
		return ans, err
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), startNs, endNs)
	if mode != string(config.ModeLogs) {
		return ans, nil // the logs binary holds no spans
	}
	for _, tenant := range tenants {
		qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)
		var rows []schema.LogRow
		var mu sync.Mutex
		err := snap.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			conv := vlstorage.DataBlockToLogRows(db, tenant)
			mu.Lock()
			rows = append(rows, conv...)
			mu.Unlock()
		})
		if err != nil {
			return ans, err
		}
		ans.Logs = append(ans.Logs, rows...)
	}
	return ans, nil
}

// bufferTenantAccountIDs lists the AccountIDs with rows in the local segments.
func (s *Storage) bufferTenantAccountIDs(seen map[uint32]struct{}) {
	if s.localBuffer == nil {
		return
	}
	snap := s.localBuffer.Snapshot()
	defer snap.Release()
	ids, err := snap.GetTenantIDs(context.Background(), 0, math.MaxInt64)
	if err != nil {
		return
	}
	for _, t := range ids {
		seen[t.AccountID] = struct{}{}
	}
}

// BufferedRows reports the insert buffer as the select path sees it for the
// admin parity check: every tenant's rows with _time in [startNs, endNs] held
// by the live segments (the co-located ones, or every insert peer's through the
// buffer bridge), and the nonces of those segments. A query counts these rows
// from the buffer and skips the objects of the same nonces, so the caller
// compares them with the manifest rows of those objects. Co-located segments
// are reported one by one (with their committed state); peers' only as a total.
// A peer that fails is an error, with the rows of the others in the report: a
// smaller answer must not read as a whole one. A node with no buffer returns an
// empty report.
func (s *Storage) BufferedRows(ctx context.Context, startNs, endNs int64) (buffer.WindowReport, error) {
	if s.useLocalBuffer() {
		snap := s.localBuffer.Snapshot()
		defer snap.Release()
		segs, err := snap.SegmentRowsInWindow(ctx, startNs, endNs, "")
		rep := buffer.WindowReport{Nonces: snap.Nonces(), Segments: make([]buffer.SegmentRows, 0, len(segs))}
		for _, g := range segs {
			rep.Rows += g.Rows
			rep.Segments = append(rep.Segments, buffer.SegmentRows{Nonce: g.Nonce, Committed: g.Committed, Rows: g.Rows, Dropped: g.Dropped})
		}
		return rep, err
	}
	if s.bufferBridge == nil {
		return buffer.WindowReport{}, nil
	}
	ctx = storage.WithGlobalRead(ctx)
	scope := scopeFor(ctx, nil)
	var rep buffer.WindowReport
	var errs []error
	switch s.cfg.Mode {
	case config.ModeLogs:
		var rows []schema.LogRow
		rows, rep.Nonces, errs = queryPeersChecked[schema.LogRow](s.bufferBridge, ctx, startNs, endNs, scope)
		rep.Rows = int64(len(rows))
	case config.ModeTraces:
		var rows []schema.TraceRow
		rows, rep.Nonces, errs = queryPeersChecked[schema.TraceRow](s.bufferBridge, ctx, startNs, endNs, scope)
		rep.Rows = int64(len(rows))
	}
	return rep, errors.Join(errs...)
}
