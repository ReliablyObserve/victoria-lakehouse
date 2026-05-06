package internalselect

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/zstd"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netselect"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

type Handler struct {
	store   storage.Storage
	timeout time.Duration
}

func NewHandler(store storage.Storage, timeout time.Duration) *Handler {
	vlstorage.SetStorage(store)
	return &Handler{
		store:   store,
		timeout: timeout,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/internal/select/query", h.wrap(processQueryRequest))
	mux.HandleFunc("/internal/select/field_names", h.wrap(processFieldNamesRequest))
	mux.HandleFunc("/internal/select/field_values", h.wrap(processFieldValuesRequest))
	mux.HandleFunc("/internal/select/stream_field_names", h.wrap(processStreamFieldNamesRequest))
	mux.HandleFunc("/internal/select/stream_field_values", h.wrap(processStreamFieldValuesRequest))
	mux.HandleFunc("/internal/select/streams", h.wrap(processStreamsRequest))
	mux.HandleFunc("/internal/select/stream_ids", h.wrap(processStreamIDsRequest))
	mux.HandleFunc("/internal/select/tenant_ids", h.wrap(processTenantIDsRequest))

	mux.HandleFunc("/internal/delete/run_task", h.wrap(processDeleteRunTask))
	mux.HandleFunc("/internal/delete/stop_task", h.wrap(processDeleteStopTask))
	mux.HandleFunc("/internal/delete/active_tasks", h.wrap(processDeleteActiveTasks))
}

func (h *Handler) wrap(fn func(ctx context.Context, w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()

		if err := fn(ctx, w, r); err != nil {
			logger.Errorf("error processing %s: %s", r.URL.Path, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func processQueryRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.QueryProtocolVersion)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	var wLock sync.Mutex
	var dataLenBuf []byte

	sendBuf := func(bb *bytesutil.ByteBuffer) error {
		if len(bb.B) == 0 {
			return nil
		}

		data := bb.B
		if !cp.DisableCompression {
			bufLen := len(bb.B)
			bb.B = zstd.CompressLevel(bb.B, bb.B, 1)
			data = bb.B[bufLen:]
		}

		wLock.Lock()
		dataLenBuf = encoding.MarshalUint64(dataLenBuf[:0], uint64(len(data)))
		_, err := w.Write(dataLenBuf)
		if err == nil {
			_, err = w.Write(data)
		}
		wLock.Unlock()

		bb.Reset()
		return err
	}

	var bufs atomicutil.Slice[bytesutil.ByteBuffer]
	var errGlobal atomic.Pointer[error]

	writeBlock := func(workerID uint, db *logstorage.DataBlock) {
		if errGlobal.Load() != nil {
			return
		}

		bb := bufs.Get(workerID)
		bb.B = append(bb.B, 0)
		bb.B = db.Marshal(bb.B)

		if len(bb.B) < 1024*1024 {
			return
		}

		if err := sendBuf(bb); err != nil {
			errGlobal.CompareAndSwap(nil, &err)
		}
	}

	qctx := cp.NewQueryContext(ctx)

	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		return err
	}
	if errP := errGlobal.Load(); errP != nil {
		return *errP
	}

	for _, bb := range bufs.All() {
		if err := sendBuf(bb); err != nil {
			return err
		}
	}

	bb := bufs.Get(0)
	bb.B = append(bb.B, 1)
	bb.B = marshalQueryStatsBlock(bb.B, qctx)
	return sendBuf(bb)
}

func processFieldNamesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.FieldNamesProtocolVersion)
	if err != nil {
		return err
	}

	filter := r.FormValue("filter")

	qctx := cp.NewQueryContext(ctx)

	fieldNames, err := vlstorage.GetFieldNames(qctx, filter)
	if err != nil {
		return fmt.Errorf("cannot obtain field names: %w", err)
	}

	return writeValuesWithHits(w, qctx, fieldNames, cp.DisableCompression)
}

func processFieldValuesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.FieldValuesProtocolVersion)
	if err != nil {
		return err
	}

	fieldName := r.FormValue("field")
	filter := r.FormValue("filter")

	limit, err := getInt64FromRequest(r, "limit")
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)

	fieldValues, err := vlstorage.GetFieldValues(qctx, fieldName, filter, uint64(limit))
	if err != nil {
		return fmt.Errorf("cannot obtain field values: %w", err)
	}

	return writeValuesWithHits(w, qctx, fieldValues, cp.DisableCompression)
}

func processStreamFieldNamesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.StreamFieldNamesProtocolVersion)
	if err != nil {
		return err
	}

	filter := r.FormValue("filter")

	qctx := cp.NewQueryContext(ctx)

	fieldNames, err := vlstorage.GetStreamFieldNames(qctx, filter)
	if err != nil {
		return fmt.Errorf("cannot obtain stream field names: %w", err)
	}

	return writeValuesWithHits(w, qctx, fieldNames, cp.DisableCompression)
}

func processStreamFieldValuesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.StreamFieldValuesProtocolVersion)
	if err != nil {
		return err
	}

	fieldName := r.FormValue("field")
	filter := r.FormValue("filter")

	limit, err := getInt64FromRequest(r, "limit")
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)

	fieldValues, err := vlstorage.GetStreamFieldValues(qctx, fieldName, filter, uint64(limit))
	if err != nil {
		return fmt.Errorf("cannot obtain stream field values: %w", err)
	}

	return writeValuesWithHits(w, qctx, fieldValues, cp.DisableCompression)
}

func processStreamsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.StreamsProtocolVersion)
	if err != nil {
		return err
	}

	limit, err := getInt64FromRequest(r, "limit")
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)

	streams, err := vlstorage.GetStreams(qctx, uint64(limit))
	if err != nil {
		return fmt.Errorf("cannot obtain streams: %w", err)
	}

	return writeValuesWithHits(w, qctx, streams, cp.DisableCompression)
}

func processStreamIDsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.StreamIDsProtocolVersion)
	if err != nil {
		return err
	}

	limit, err := getInt64FromRequest(r, "limit")
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)

	streamIDs, err := vlstorage.GetStreamIDs(qctx, uint64(limit))
	if err != nil {
		return fmt.Errorf("cannot obtain stream IDs: %w", err)
	}

	return writeValuesWithHits(w, qctx, streamIDs, cp.DisableCompression)
}

func processDeleteRunTask(ctx context.Context, _ http.ResponseWriter, r *http.Request) error {
	if err := checkProtocolVersion(r, netselect.DeleteRunTaskProtocolVersion); err != nil {
		return err
	}

	taskID := r.FormValue("task_id")
	if taskID == "" {
		return fmt.Errorf("missing task_id arg")
	}

	timestamp, err := getInt64FromRequest(r, "timestamp")
	if err != nil {
		return err
	}

	tenantIDsStr := r.FormValue("tenant_ids")
	tenantIDs, err := logstorage.UnmarshalTenantIDsFromJSON([]byte(tenantIDsStr))
	if err != nil {
		return fmt.Errorf("cannot unmarshal tenant_ids=%q: %w", tenantIDsStr, err)
	}

	fStr := r.FormValue("filter")
	f, err := logstorage.ParseFilter(fStr)
	if err != nil {
		return fmt.Errorf("cannot unmarshal filter=%q: %w", fStr, err)
	}

	return vlstorage.DeleteRunTask(ctx, taskID, timestamp, tenantIDs, f)
}

func processDeleteStopTask(ctx context.Context, _ http.ResponseWriter, r *http.Request) error {
	if err := checkProtocolVersion(r, netselect.DeleteStopTaskProtocolVersion); err != nil {
		return err
	}

	taskID := r.FormValue("task_id")
	if taskID == "" {
		return fmt.Errorf("missing task_id arg")
	}

	return vlstorage.DeleteStopTask(ctx, taskID)
}

func processDeleteActiveTasks(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	if err := checkProtocolVersion(r, netselect.DeleteActiveTasksProtocolVersion); err != nil {
		return err
	}

	tasks, err := vlstorage.DeleteActiveTasks(ctx)
	if err != nil {
		return err
	}

	data := logstorage.MarshalDeleteTasksToJSON(tasks)
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(data)
	return err
}

func processTenantIDsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	start, err := getInt64FromRequest(r, "start")
	if err != nil {
		return err
	}
	end, err := getInt64FromRequest(r, "end")
	if err != nil {
		return err
	}

	tenantIDs, err := vlstorage.GetTenantIDs(ctx, start, end)
	if err != nil {
		return fmt.Errorf("cannot obtain tenant IDs: %w", err)
	}

	data, err := json.Marshal(tenantIDs)
	if err != nil {
		return fmt.Errorf("cannot marshal tenantIDs: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(data)
	return err
}

type commonParams struct {
	TenantIDs            []logstorage.TenantID
	Query                *logstorage.Query
	DisableCompression   bool
	AllowPartialResponse bool
	HiddenFieldsFilters  []string

	qs logstorage.QueryStats
}

func (cp *commonParams) NewQueryContext(ctx context.Context) *logstorage.QueryContext {
	return logstorage.NewQueryContext(ctx, &cp.qs, cp.TenantIDs, cp.Query, cp.AllowPartialResponse, cp.HiddenFieldsFilters)
}

func getCommonParams(r *http.Request, expectedProtocolVersion string) (*commonParams, error) {
	if err := checkProtocolVersion(r, expectedProtocolVersion); err != nil {
		return nil, err
	}

	tenantIDsStr := r.FormValue("tenant_ids")
	tenantIDs, err := logstorage.UnmarshalTenantIDsFromJSON([]byte(tenantIDsStr))
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal tenant_ids=%q: %w", tenantIDsStr, err)
	}

	timestamp, err := getInt64FromRequest(r, "timestamp")
	if err != nil {
		return nil, err
	}

	qStr := r.FormValue("query")
	q, err := logstorage.ParseQueryAtTimestamp(qStr, timestamp)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal query=%q: %w", qStr, err)
	}

	disableCompression, err := getBoolFromRequest(r, "disable_compression")
	if err != nil {
		return nil, err
	}

	allowPartialResponse, err := getBoolFromRequest(r, "allow_partial_response")
	if err != nil {
		return nil, err
	}

	hiddenFieldsFilters, err := getStringSliceFromRequest(r, "hidden_fields_filters")
	if err != nil {
		return nil, err
	}

	cp := &commonParams{
		TenantIDs:            tenantIDs,
		Query:                q,
		DisableCompression:   disableCompression,
		AllowPartialResponse: allowPartialResponse,
		HiddenFieldsFilters:  hiddenFieldsFilters,
	}
	return cp, nil
}

func checkProtocolVersion(r *http.Request, expectedProtocolVersion string) error {
	version := r.FormValue("version")
	if version != expectedProtocolVersion {
		return fmt.Errorf("unexpected protocol version=%q; want %q", version, expectedProtocolVersion)
	}
	return nil
}

func writeValuesWithHits(w http.ResponseWriter, qctx *logstorage.QueryContext, vhs []logstorage.ValueWithHits, disableCompression bool) error {
	var b []byte

	b = encoding.MarshalUint64(b, uint64(len(vhs)))
	for i := range vhs {
		b = vhs[i].Marshal(b)
	}

	b = marshalQueryStatsBlock(b, qctx)

	if !disableCompression {
		b = zstd.CompressLevel(nil, b, 1)
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, err := w.Write(b)
	return err
}

func marshalQueryStatsBlock(dst []byte, qctx *logstorage.QueryContext) []byte {
	queryDurationNsecs := qctx.QueryDurationNsecs()
	db := qctx.QueryStats.CreateDataBlock(queryDurationNsecs)
	dst = db.Marshal(dst)
	return dst
}

func getInt64FromRequest(r *http.Request, argName string) (int64, error) {
	s := r.FormValue(argName)
	if s == "" {
		return 0, fmt.Errorf("missing the required arg %s", argName)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %s=%q: %w", argName, s, err)
	}
	return n, nil
}

func getBoolFromRequest(r *http.Request, argName string) (bool, error) {
	s := r.FormValue(argName)
	if s == "" {
		return false, fmt.Errorf("missing the required arg %s", argName)
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false, fmt.Errorf("cannot parse %s=%q as bool: %w", argName, s, err)
	}
	return b, nil
}

func getStringSliceFromRequest(r *http.Request, argName string) ([]string, error) {
	s := r.FormValue(argName)
	if s == "" {
		return nil, fmt.Errorf("missing the required arg %s", argName)
	}

	var a []string
	if err := json.Unmarshal([]byte(s), &a); err != nil {
		return nil, fmt.Errorf("cannot unmarshal JSON array from %s=%q: %w", argName, s, err)
	}
	return a, nil
}
