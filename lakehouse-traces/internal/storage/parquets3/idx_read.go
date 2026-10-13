package parquets3

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// idxRoute says which tables a query reads: the span objects, the trace_id_idx
// objects, or both. Correctness never depends on it (VictoriaLogs' own filter runs on
// every row that is read); it decides cost, and a wrong "spans only" would hide index
// rows, so that answer is given only when the filter provably cannot match an index
// row.
type idxRoute int

const (
	routeBoth idxRoute = iota
	routeSpansOnly
	routeIdxOnly
)

// idxStreamQueryRe matches a query that starts with a selector of VictoriaTraces'
// index stream (after any number of time filters: the select handler adds its own
// latency-offset bound to the caller's range): `{trace_id_idx_stream="N"} AND trace_id_idx:=...` (trace-by-ID, stage
// 1) and `{trace_id_idx_stream!=""} AND duration:>... ` (the TraceQL duration join).
var idxStreamQueryRe = regexp.MustCompile(`^(?:_time:\S+ )*\{` + tracesidx.StreamField)

// spanStreamQueryRe matches a query that starts with a stream selector.
var spanStreamQueryRe = regexp.MustCompile(`^(?:_time:\S+ )*\{`)

// idxRouteOf classifies queryStr. A NOT, an OR or an `if (...)` anywhere makes the
// filter's shape unknown (noFooterBloomFrom), so only the cost-neutral answer (both
// tables) is given for it.
func (s *Storage) idxRouteOf(ctx context.Context, queryStr string) idxRoute {
	return s.idxRouteOfQuery(queryStr, noFooterBloomFrom(ctx))
}

// idxRouteOfQuery is idxRouteOf for a caller that has no read context (the field and
// stream enumerations); unknownShape is true for a NOT, an OR or an `if (...)`.
func (s *Storage) idxRouteOfQuery(queryStr string, unknownShape bool) idxRoute {
	if !tracesidx.Enabled {
		return routeSpansOnly
	}
	if unknownShape {
		return routeBoth
	}
	if idxStreamQueryRe.MatchString(queryStr) {
		return routeIdxOnly
	}
	if spanStreamQueryRe.MatchString(queryStr) {
		return routeSpansOnly // a selector of a span stream: index rows are in another stream
	}
	// An index row has none of the span fields: a positive exact filter on one of
	// them (the same predicates the file-level bloom prunes on) cannot match it.
	f := parseFilterFromQueryStr(queryStr)
	if requiresSpanField(f) {
		return routeSpansOnly
	}
	for _, col := range s.registry.PromotedColumns() {
		if !col.HasBloom {
			continue
		}
		if f != nil {
			if len(FilterExtractFieldValues(f, col.InternalName)) > 0 || len(FilterExtractFieldValues(f, col.ParquetColumn)) > 0 {
				return routeSpansOnly
			}
		}
	}
	return routeBoth
}

// idxSource is a tracesidx.Source over one idx object, read through the cache tiers.
type idxSource struct {
	s  *Storage
	fi manifest.FileInfo
}

func (x *idxSource) Key() string { return x.fi.Key }
func (x *idxSource) Size() int64 { return x.fi.Size }

// ReadRange reads a range of the object through the cache tiers: memory (L1), the disk
// of the owner of the range (L2), the owner over the peer ring (L3), then the object
// store. The footers, index slices and pages of the table are ordinary cache entries
// ("<object>#<offset>+<length>"), so the disk tier keeps them across restarts and the
// ring shares them.
func (x *idxSource) ReadRange(ctx context.Context, kind tracesidx.ReadKind, off, n int64) ([]byte, error) {
	s := x.s
	key := x.fi.Key + "#" + strconv.FormatInt(off, 10) + "+" + strconv.FormatInt(n, 10)
	fetch := func(c context.Context) ([]byte, error) {
		d, err := s.pool.DownloadRangeDedup(c, "idx_"+kind.String(), x.fi.Key, off, n)
		if err == nil && int64(len(d)) != n {
			err = fmt.Errorf("short range read of %s: %d of %d bytes", x.fi.Key, len(d), n)
		}
		return d, err
	}
	var (
		data []byte
		tier string
		err  error
	)
	if s.smartCache != nil {
		data, tier, err = s.smartCache.GetRange(ctx, key, fetch)
	} else {
		data, tier, err = s.rangeThroughTiers(ctx, key, fetch)
	}
	if err != nil {
		return nil, err
	}
	metrics.TraceIdxRangeReads.Inc(kind.String() + "_" + tier)
	metrics.TraceIdxRangeBytes.Add(kind.String()+"_"+tier, len(data))
	return data, nil
}

// rangeThroughTiers is the cache chain of getFileData for a range, for nodes without
// the smart-cache controller.
func (s *Storage) rangeThroughTiers(ctx context.Context, key string, fetch func(context.Context) ([]byte, error)) ([]byte, string, error) {
	if s.memCache != nil {
		if d, ok := s.memCache.Get(key); ok {
			return d, "memory", nil
		}
	}
	if s.diskCache != nil {
		if path, ok := s.diskCache.Get(key); ok {
			if d, err := os.ReadFile(path); err == nil {
				if s.memCache != nil {
					s.memCache.PutNoCopy(key, d)
				}
				return d, "disk", nil
			}
			s.diskCache.Delete(key)
		}
	}
	if s.peerCache != nil {
		if peer, isLocal := s.peerCache.Lookup(key); !isLocal {
			if d, found, err := s.peerCache.Fetch(ctx, peer, key); err == nil && found {
				if s.memCache != nil {
					s.memCache.PutNoCopy(key, d)
				}
				return d, "peer", nil
			}
		}
	}
	d, err := fetch(ctx)
	if err != nil {
		return nil, "", err
	}
	if s.memCache != nil {
		s.memCache.Put(key, d)
	}
	if s.diskCache != nil {
		if _, perr := s.diskCache.Put(key, d); perr != nil {
			logger.Warnf("disk cache put failed: %s; key=%s", perr, key)
		}
	}
	return d, "s3", nil
}

// idxTenant is the numeric tenant of an idx object's rows (it goes into the derived
// _stream_id). A request scoped to one tenant reads only that tenant's objects, so the
// request's own (numeric) tenant is right even when the keys carry an alias; any wider
// scope reads the tenant from each key.
func (s *Storage) idxTenant(scope tenantScope, tenants []logstorage.TenantID, key string) logstorage.TenantID {
	if scope.single() && len(tenants) == 1 {
		return tenants[0]
	}
	return s.tenantOfKey(key)
}

// tenantOfKey is the tenant an idx object belongs to (the numeric form of its key).
func (s *Storage) tenantOfKey(key string) logstorage.TenantID {
	var t logstorage.TenantID
	if s.manifest == nil {
		return t
	}
	account, project, ok := s.manifest.KeyTenant(key)
	if !ok {
		return t
	}
	if a, err := strconv.ParseUint(account, 10, 32); err == nil {
		t.AccountID = uint32(a)
	}
	if p, err := strconv.ParseUint(project, 10, 32); err == nil {
		t.ProjectID = uint32(p)
	}
	return t
}

// idxWant maps the fields a query needs onto the columns of a lookup. The time is
// always read: the query's time range is applied to it.
func idxWant(needed []string) tracesidx.Want {
	if needed == nil || containsWildcard(needed) {
		return tracesidx.WantAll
	}
	w := tracesidx.WantTime
	for _, f := range needed {
		switch f {
		case tracesidx.StartField:
			w |= tracesidx.WantStart
		case tracesidx.EndField:
			w |= tracesidx.WantEnd
		case tracesidx.DurationField:
			w |= tracesidx.WantDuration
		case tracesidx.StreamField, "_stream", "_stream_id":
			w |= tracesidx.WantStream
		}
	}
	return w
}

// idxBlock turns rows of one tenant's idx object into a DataBlock with upstream's
// column names. _stream and _stream_id are derived with upstream's StreamTags code and
// stream-id rule, _msg is upstream's constant.
func idxBlock(tenant logstorage.TenantID, rows []tracesidx.Row, want tracesidx.Want, streams map[int32][3]string) *logstorage.DataBlock {
	n := len(rows)
	col := func() []string { return make([]string, n) }
	tm, sid, st, msg, strm, id := col(), col(), col(), col(), col(), col()
	var start, end, dur []string
	if want&tracesidx.WantStart != 0 {
		start = col()
	}
	if want&tracesidx.WantEnd != 0 {
		end = col()
	}
	if want&tracesidx.WantDuration != 0 {
		dur = col()
	}
	for i := range rows {
		r := &rows[i]
		tm[i] = time.Unix(0, r.Time).UTC().Format(time.RFC3339Nano)
		v, ok := streams[r.Stream]
		if !ok {
			text, canonical := vlstorage.IdxStream(r.Stream)
			v = [3]string{text, vlstorage.StreamIDFor(tenant, canonical), strconv.Itoa(int(r.Stream))}
			streams[r.Stream] = v
		}
		st[i], sid[i], strm[i] = v[0], v[1], v[2]
		msg[i] = tracesidx.MessageValue
		id[i] = r.TraceID
		if start != nil {
			start[i] = strconv.FormatInt(r.Start, 10)
		}
		if end != nil {
			end[i] = strconv.FormatInt(r.End, 10)
		}
		if dur != nil {
			dur[i] = strconv.FormatInt(r.Duration, 10)
		}
	}
	cols := []logstorage.BlockColumn{
		{Name: "_time", Values: tm},
		{Name: "_stream_id", Values: sid},
		{Name: "_stream", Values: st},
		{Name: "_msg", Values: msg},
		{Name: tracesidx.StreamField, Values: strm},
		{Name: tracesidx.IDField, Values: id},
	}
	if start != nil {
		cols = append(cols, logstorage.BlockColumn{Name: tracesidx.StartField, Values: start})
	}
	if end != nil {
		cols = append(cols, logstorage.BlockColumn{Name: tracesidx.EndField, Values: end})
	}
	if dur != nil {
		cols = append(cols, logstorage.BlockColumn{Name: tracesidx.DurationField, Values: dur})
	}
	db := &logstorage.DataBlock{}
	db.SetColumns(cols)
	return db
}

// idxObjectsSmall: an object at most this big is read whole, in one GET, instead of
// footer + index + pages (each ranged read costs a request).
const idxObjectsSmall = 128 << 10

// scanIdxObjects emits the rows of the trace_id_idx objects files that match the
// query's filter, through sink (which applies the row filter and the tombstones). With
// an exact trace id in the query each object is asked for that id only (Locator); any
// other query reads the whole object.
func (s *Storage) scanIdxObjects(ctx context.Context, scope tenantScope, tenants []logstorage.TenantID, files []manifest.FileInfo, startNs, endNs int64, queryStr string, rowsEmitted *atomic.Int64, maxRows int64, sink *tombstoneSink) {
	if len(files) == 0 {
		return
	}
	var ids []string
	if !noFooterBloomFrom(ctx) {
		ids = extractFilterValuesAST(queryStr, tracesidx.IDField)
	}
	want := idxWant(neededFieldsFrom(ctx))
	workers := min(max(s.queryFileWorkers(len(files)), 1), len(files))
	ch := make(chan manifest.FileInfo, len(files))
	for _, fi := range files {
		ch <- fi
	}
	close(ch)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			streams := map[int32][3]string{}
			for fi := range ch {
				if ctx.Err() != nil || (maxRows > 0 && rowsEmitted.Load() >= maxRows) {
					return
				}
				if fi.MaxTimeNs < startNs || fi.MinTimeNs > endNs {
					continue
				}
				rows, err := s.readIdxObject(ctx, fi, ids, want)
				if err != nil {
					metrics.QueryFileErrorsTotal.Inc()
					logger.Warnf("trace_id_idx object read error: %s; key=%s", err, fi.Key)
					continue
				}
				kept := rows[:0]
				for _, r := range rows {
					if r.Time >= startNs && r.Time <= endNs {
						kept = append(kept, r)
					}
				}
				if len(kept) == 0 {
					continue
				}
				const blockRows = 8192
				for lo := 0; lo < len(kept); lo += blockRows {
					hi := min(lo+blockRows, len(kept))
					sink.forKey(fi.Key)(0, idxBlock(s.idxTenant(scope, tenants, fi.Key), kept[lo:hi], want, streams))
				}
			}
		}()
	}
	wg.Wait()
}

// readIdxObject returns the rows of one idx object: those of the given ids, or all.
func (s *Storage) readIdxObject(ctx context.Context, fi manifest.FileInfo, ids []string, want tracesidx.Want) ([]tracesidx.Row, error) {
	if len(ids) == 0 {
		data, err := s.getFileData(ctx, fi.Key, fi.Size) // one GET for the whole object, through the file cache tiers
		if err != nil {
			return nil, err
		}
		return tracesidx.ReadAll(data)
	}
	var src tracesidx.Source = &idxSource{s: s, fi: fi}
	if fi.Size <= idxObjectsSmall {
		// A small object is read whole, in one GET, the first time a range of it is
		// needed (never when the cached footer and filters already rule the id out).
		src = &wholeObjectSource{s: s, fi: fi}
	}
	var out []tracesidx.Row
	for _, id := range ids {
		rows, st, err := s.locator().Lookup(ctx, src, id, want)
		if err != nil {
			return nil, err
		}
		metrics.TraceIdxLookupBloomSkips.Add(st.BloomSkips)
		out = append(out, rows...)
	}
	return out, nil
}

// wholeObjectSource serves ranges of a small object from one whole-object read, made
// on the first range asked for and cached by the file tiers.
type wholeObjectSource struct {
	s    *Storage
	fi   manifest.FileInfo
	once sync.Once
	data []byte
	err  error
}

func (w *wholeObjectSource) Key() string { return w.fi.Key }
func (w *wholeObjectSource) Size() int64 { return w.fi.Size }
func (w *wholeObjectSource) ReadRange(ctx context.Context, _ tracesidx.ReadKind, off, n int64) ([]byte, error) {
	w.once.Do(func() { w.data, w.err = w.s.getFileData(ctx, w.fi.Key, w.fi.Size) })
	if w.err != nil {
		return nil, w.err
	}
	if off < 0 || n < 0 || off+n > int64(len(w.data)) {
		return nil, fmt.Errorf("range %d+%d outside the %d-byte object", off, n, len(w.data))
	}
	return w.data[off : off+n], nil
}

// collectIdxValues adds the trace_id_idx objects' rows to a field or stream enumeration,
// as hot VictoriaTraces counts its index rows in field_values, streams and stream_ids:
// through the request's filter and tombstones, a row without the field under "" when
// the request counts the empty value.
func (s *Storage) collectIdxValues(ctx context.Context, r fieldValuesRequest, seen map[string]uint64) {
	if r.query == nil || r.field == "" || !tracesidx.Enabled {
		return
	}
	scope := scopeFor(ctx, r.tenantIDs)
	files := r.view.exclude(s.idxFilesForScope(r.op, r.startNs, r.endNs, scope))
	if len(files) == 0 {
		return
	}
	queryStr := r.query.String()
	unknown := FilterContainsNotOr(r.filter) || strings.Contains(queryStr, " if (")
	if s.idxRouteOfQuery(queryStr, unknown) == routeSpansOnly {
		return
	}
	var mu sync.Mutex
	count := func(tss []tombstone) logstorage.WriteDataBlockFunc {
		return newFieldValueCounterOpts(r.field, r.filter, tss, &mu, seen, r.emptyBucket)
	}
	sink := newTombstoneSink(scope, r.tombstones, r.parse, s.AccountOnlyTenantKeys(), count)
	var emitted atomic.Int64
	s.scanIdxObjects(withNoFooterBloom(ctx, unknown), scope, r.tenantIDs, files, r.startNs, r.endNs, queryStr, &emitted, 0, sink)
}

// idxRowFieldNames are the fields an index row has (VictoriaTraces writes only these).
var idxRowFieldNames = map[string]struct{}{
	"_time": {}, "_stream": {}, "_stream_id": {}, "_msg": {},
	tracesidx.StreamField: {}, tracesidx.IDField: {}, tracesidx.StartField: {}, tracesidx.EndField: {}, tracesidx.DurationField: {},
}

// requiresSpanField reports whether the AND-chain of f contains `field:*` (the field is
// not empty) on a field an index row does not have: such a filter cannot match an index
// row, so the query does not need the table. It is the shape of the span queries of the
// dashboards and of VictoriaTraces' own (`span_id:*`).
func requiresSpanField(f *logstorage.Filter) bool {
	if f == nil {
		return false
	}
	found := false
	var visit func(v reflect.Value)
	visit = func(v reflect.Value) {
		v = derefValue(v)
		switch astTypeName(v) {
		case astTypeAnd:
			filters := v.FieldByName("filters")
			if filters.IsValid() && filters.Kind() == reflect.Slice {
				for i := 0; i < filters.Len(); i++ {
					visit(filters.Index(i))
				}
			}
		case astTypeGeneric:
			name := stringField(v, "fieldName")
			if _, ok := idxRowFieldNames[name]; ok || name == "" {
				return
			}
			inner := derefValue(v.FieldByName("f"))
			if astTypeName(inner) == astTypePrefix && stringField(inner, "prefix") == "" {
				found = true
			}
		}
	}
	visit(filterInner(f))
	return found
}
