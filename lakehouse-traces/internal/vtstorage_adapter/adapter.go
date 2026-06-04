package vtstorageadapter

import (
	"context"
	"fmt"
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

var _ vtstorage.ExternalStorage = (*Adapter)(nil)

// TenantLister returns the (account, project) pairs present in cold
// storage for files whose time range overlaps [startNs, endNs].
// Used to power vtstorage.GetTenantIDs so VT's per-tenant background
// tasks (notably servicegraph) see every tenant the LH process owns.
// nil = adapter falls back to the legacy "single zero tenant" shape.
type TenantLister func(startNs, endNs int64) []logstorage.TenantID

type Adapter struct {
	store        storage.Storage
	tenantLister TenantLister
}

// Init installs the adapter as VT's external storage. The optional
// tenantLister enables per-tenant iteration in upstream VT tasks
// (servicegraph). When omitted the adapter reports a single
// {0,0} tenant, preserving pre-multi-tenant behavior.
func Init(store storage.Storage, opts ...InitOption) {
	a := &Adapter{store: store}
	for _, opt := range opts {
		opt(a)
	}
	vtstorage.SetExternalStorage(a)
}

// InitOption tunes Init at construction time.
type InitOption func(*Adapter)

// WithTenantLister wires a callback for GetTenantIDs.
func WithTenantLister(f TenantLister) InitOption {
	return func(a *Adapter) { a.tenantLister = f }
}

func (a *Adapter) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	if qctx.Query == nil {
		return nil
	}

	// Trace-index fast path: VT's tempo handler issues a stats query
	// against the trace_id_idx_stream to bound the subsequent span fetch
	// (see vtselect/traces/tempo/query.go GetTrace). Every cold trace
	// Parquet file embeds a `_trace_idx` footer index that already carries
	// (start_time, end_time) per trace ID, so we can answer the bound
	// without ever opening a row group. On a footer miss we fall through
	// to the existing rewriteTraceIndexQuery span-scan rewrite so VT-side
	// semantics are preserved bit-for-bit.
	if lookup, ok := a.store.(traceIndexLookup); ok {
		if traceID, isLookup := traceIndexLookupTraceID(qctx.Query); isLookup && traceID != "" {
			startNs, endNs, found, _ := lookup.LookupTraceIndex(qctx.Context, traceID)
			if found {
				emitTraceIndexBlock(writeBlock, startNs, endNs)
				return nil
			}
		}
	}

	// Queries with field-enumerating pipes (field_names, field_values,
	// facets, block_stats) must bypass projection narrowing entirely —
	// those pipes report what fields a row carries, so handing them a
	// pre-projected DataBlock would truncate the answer. The adapter
	// rewrites such queries with a hint context value the parquets3
	// storage checks at RunQuery entry. See QueryNeedsAllFields in
	// patches/vl-traces/external_query.go.src for the pipe list.
	if logstorage.QueryNeedsAllFields(qctx.Query) {
		ctx := storage.WithAllFieldsHint(qctx.Context)
		searchFn := func(wb logstorage.WriteDataBlockFunc) error {
			return a.store.RunQuery(ctx, qctx.TenantIDs, qctx.Query, wb)
		}
		return logstorage.RunQueryExternalWithSubqueries(qctx, searchFn, a.RunQuery, writeBlock)
	}

	// IMPORTANT: pass the FULL query (with pipes intact) to a.store.RunQuery.
	// Our storage's queryColumns() consults logstorage.GetQueryPipeFields() to
	// expand the parquet column projection to cover fields referenced only by
	// pipes (e.g. `| fields _time, trace_id` or `| partition by (trace_id)`).
	// If we strip pipes here, the projection misses those fields, the emitted
	// DataBlocks don't carry them, and downstream pipes (e.g. `partition by`)
	// silently drop every row.
	//
	// Stripping pipes for actual row matching happens inside RunQuery itself
	// via parseFilterFromQuery (Clone + DropAllPipes), so passing the full
	// query here is safe — pipes only inform column projection planning.

	if rewritten, ok := rewriteTraceIndexQuery(qctx.Query); ok {
		newQctx := qctx.WithQuery(rewritten)
		searchFn := func(wb logstorage.WriteDataBlockFunc) error {
			return a.store.RunQuery(qctx.Context, qctx.TenantIDs, rewritten, wb)
		}
		return logstorage.RunQueryExternalWithSubqueries(newQctx, searchFn, a.RunQuery, writeBlock)
	}

	if rewritten, ok := stripTraceIndexStream(qctx.Query); ok {
		newQctx := qctx.WithQuery(rewritten)
		if logstorage.QueryHasPipes(rewritten) {
			searchFn := func(wb logstorage.WriteDataBlockFunc) error {
				return a.store.RunQuery(qctx.Context, qctx.TenantIDs, rewritten, wb)
			}
			return logstorage.RunQueryExternalWithSubqueries(newQctx, searchFn, a.RunQuery, writeBlock)
		}
		return a.store.RunQuery(qctx.Context, qctx.TenantIDs, rewritten, writeBlock)
	}

	if logstorage.QueryHasPipes(qctx.Query) {
		searchFn := func(wb logstorage.WriteDataBlockFunc) error {
			return a.store.RunQuery(qctx.Context, qctx.TenantIDs, qctx.Query, wb)
		}
		return logstorage.RunQueryExternalWithSubqueries(qctx, searchFn, a.RunQuery, writeBlock)
	}

	return a.store.RunQuery(qctx.Context, qctx.TenantIDs, qctx.Query, writeBlock)
}

// stripTraceIndexStream detects VT's Tempo search queries that use the
// {trace_id_idx_stream="..."} stream selector. Lakehouse doesn't have this
// index, so we strip the stream selector and let the query run against
// actual span data. Preserves pipes and time filters from the original query.
func stripTraceIndexStream(q *logstorage.Query) (*logstorage.Query, bool) {
	queryStr := q.String()
	if !strings.Contains(queryStr, `trace_id_idx_stream`) {
		return nil, false
	}

	cleaned := stripIndexStreamSelector(queryStr)
	if cleaned == queryStr {
		return nil, false
	}

	rewritten, err := logstorage.ParseQueryAtTimestamp(cleaned, q.GetTimestamp())
	if err != nil {
		logger.Warnf("failed to parse rewritten query %q: %s", cleaned, err)
		return nil, false
	}

	startNs, endNs := q.GetFilterTimeRange()
	if startNs > 0 || endNs > 0 {
		rewritten.AddTimeFilter(startNs, endNs)
	}

	return rewritten, true
}

// stripIndexStreamSelector removes {trace_id_idx_stream="..."} from a query
// string and cleans up leftover AND operators.
func stripIndexStreamSelector(s string) string {
	for {
		idx := strings.Index(s, `{trace_id_idx_stream`)
		if idx < 0 {
			break
		}
		end := strings.IndexByte(s[idx:], '}')
		if end < 0 {
			break
		}
		s = s[:idx] + s[idx+end+1:]
	}

	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "AND ")
	s = strings.ReplaceAll(s, "  ", " ")
	s = strings.TrimSpace(s)
	if s == "" {
		s = "*"
	}
	return s
}

// rewriteTraceIndexQuery detects VT's trace_id_idx_stream index queries and
// rewrites them to query span data directly. VT's GetTrace first queries:
//
//	{trace_id_idx_stream="<hash>"} AND trace_id_idx:="<traceID>"
//	| stats min(_time) _time, min(start_time) start_time, max(end_time) end_time
//
// Lakehouse doesn't have the index stream, so we rewrite to:
//
//	trace_id:="<traceID>"
//	| stats min(_time) _time, min(start_time_unix_nano) start_time, max(end_time_unix_nano) end_time
func rewriteTraceIndexQuery(q *logstorage.Query) (*logstorage.Query, bool) {
	queryStr := q.String()
	if !strings.Contains(queryStr, `trace_id_idx:=`) {
		return nil, false
	}

	traceID := extractTraceIDFromIndexQuery(queryStr)
	if traceID == "" {
		return nil, false
	}

	rewrittenStr := fmt.Sprintf(
		`trace_id:=%q | stats min(_time) _time, min(start_time_unix_nano) start_time, max(end_time_unix_nano) end_time`,
		traceID,
	)

	rewritten, err := logstorage.ParseQueryAtTimestamp(rewrittenStr, q.GetTimestamp())
	if err != nil {
		return nil, false
	}

	startNs, endNs := q.GetFilterTimeRange()
	if startNs > 0 || endNs > 0 {
		rewritten.AddTimeFilter(startNs, endNs)
	}

	return rewritten, true
}

// extractTraceIDFromIndexQuery extracts the trace ID from a query containing
// trace_id_idx:="<value>" or trace_id_idx:=<value>. Returns empty string if not found.
func extractTraceIDFromIndexQuery(queryStr string) string {
	const marker = `trace_id_idx:=`
	idx := strings.Index(queryStr, marker)
	if idx < 0 {
		return ""
	}
	start := idx + len(marker)
	rest := queryStr[start:]
	if len(rest) == 0 {
		return ""
	}

	if rest[0] == '"' {
		end := strings.IndexByte(rest[1:], '"')
		if end < 0 {
			return ""
		}
		return rest[1 : 1+end]
	}

	end := strings.IndexAny(rest, " |)")
	if end < 0 {
		return rest
	}
	return rest[:end]
}

// GetFieldNames / GetFieldValues / GetStreamFieldNames / GetStreamFieldValues
// gained a `filter string` parameter in VT v0.9.2 (upstream commit on
// app/vtstorage/main.go that renamed the local/netstorage call signatures).
// The shared root-module storage.Storage interface does not carry the filter
// — applying the substring filter is the adapter's responsibility, mirroring
// the logs-side adapter (internal/vlstorage/vlstorage.go filterValuesBySubstring).
func (a *Adapter) GetFieldNames(qctx *logstorage.QueryContext, filter string) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
	if err != nil {
		return nil, err
	}
	return filterValuesBySubstring(results, filter), nil
}

func (a *Adapter) GetFieldValues(qctx *logstorage.QueryContext, fieldName, filter string, limit uint64) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
	if err != nil {
		return nil, err
	}
	return filterValuesBySubstring(results, filter), nil
}

func (a *Adapter) GetStreamFieldNames(qctx *logstorage.QueryContext, filter string) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetStreamFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
	if err != nil {
		return nil, err
	}
	return filterValuesBySubstring(results, filter), nil
}

func (a *Adapter) GetStreamFieldValues(qctx *logstorage.QueryContext, fieldName, filter string, limit uint64) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetStreamFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
	if err != nil {
		return nil, err
	}
	return filterValuesBySubstring(results, filter), nil
}

// filterValuesBySubstring narrows results to entries whose Value contains
// filter. Empty filter is a no-op. Matches the substring semantics VT v0.9.2
// applies in app/vtstorage/main.go's GetFieldNames family (which the upstream
// docs describe as "values containing the filter substring").
func filterValuesBySubstring(results []logstorage.ValueWithHits, filter string) []logstorage.ValueWithHits {
	if filter == "" {
		return results
	}
	filtered := make([]logstorage.ValueWithHits, 0, len(results))
	for _, v := range results {
		if strings.Contains(v.Value, filter) {
			filtered = append(filtered, v)
		}
	}
	return filtered
}

func (a *Adapter) GetStreams(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreams(qctx.Context, qctx.TenantIDs, qctx.Query, limit)
}

func (a *Adapter) GetStreamIDs(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreamIDs(qctx.Context, qctx.TenantIDs, qctx.Query, limit)
}

func (a *Adapter) GetTenantIDs(_ context.Context, start, end int64) ([]logstorage.TenantID, error) {
	if !a.store.HasDataForRange(start, end) {
		return nil, nil
	}
	if a.tenantLister != nil {
		tenants := a.tenantLister(start, end)
		if len(tenants) > 0 {
			return tenants, nil
		}
	}
	// Single-tenant deployments (no lister wired) keep the legacy
	// "report one zero-tenant" answer so VT tasks still see exactly
	// one tenant to iterate.
	return []logstorage.TenantID{{AccountID: 0, ProjectID: 0}}, nil
}
