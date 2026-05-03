package selectapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

type Handler struct {
	store   storage.Storage
	logger  *slog.Logger
	cfg     *config.Config
	timeout time.Duration
}

func NewHandler(store storage.Storage, logger *slog.Logger, cfg *config.Config) *Handler {
	return &Handler{
		store:   store,
		logger:  logger.With("component", "selectapi"),
		cfg:     cfg,
		timeout: cfg.Query.Timeout,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/select/logsql/query", h.handleQuery)
	mux.HandleFunc("/select/logsql/field_names", h.handleFieldNames)
	mux.HandleFunc("/select/logsql/field_values", h.handleFieldValues)
	mux.HandleFunc("/select/logsql/stream_field_names", h.handleStreamFieldNames)
	mux.HandleFunc("/select/logsql/stream_field_values", h.handleStreamFieldValues)
	mux.HandleFunc("/select/logsql/streams", h.handleStreams)
	mux.HandleFunc("/select/logsql/stream_ids", h.handleStreamIDs)
	mux.HandleFunc("/select/logsql/hits", h.handleHits)
	mux.HandleFunc("/select/logsql/stats_query", h.handleStatsQuery)
	mux.HandleFunc("/select/logsql/stats_query_range", h.handleStatsQueryRange)
	mux.HandleFunc("/select/logsql/tail", h.handleTailNoop)

	if h.cfg.Mode == config.ModeTraces {
		mux.HandleFunc("/select/jaeger/api/traces/", h.handleJaegerTrace)
		mux.HandleFunc("/select/jaeger/api/traces", h.handleJaegerSearch)
		mux.HandleFunc("/select/jaeger/api/services", h.handleJaegerServices)
		mux.HandleFunc("/select/jaeger/api/services/", h.handleJaegerOperations)
		mux.HandleFunc("/select/jaeger/api/dependencies", h.handleJaegerDependencies)
		mux.HandleFunc("/api/traces/", h.handleJaegerTrace)
		mux.HandleFunc("/api/traces", h.handleJaegerSearch)
		mux.HandleFunc("/api/services", h.handleJaegerServices)
		mux.HandleFunc("/api/services/", h.handleJaegerOperations)
		mux.HandleFunc("/api/dependencies", h.handleJaegerDependencies)
	}
}

func (h *Handler) parseQueryContext(r *http.Request) *storage.QueryContext {
	q := r.URL.Query()

	query := q.Get("query")
	if query == "" {
		query = "*"
	}

	startNs := parseTimeParam(q.Get("start"), q.Get("time"), 0)
	endNs := parseTimeParam(q.Get("end"), "", time.Now().UnixNano())

	if startNs == 0 {
		startNs = time.Now().Add(-24 * time.Hour).UnixNano()
	}

	return &storage.QueryContext{
		StartNs: startNs,
		EndNs:   endNs,
		Query:   query,
	}
}

func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request) {
	qctx := h.parseQueryContext(r)

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	w.Header().Set("Content-Type", "application/stream+json")

	limit := 1000
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	filters := parseSimpleFilters(qctx.Query)

	count := 0
	err := h.store.RunQuery(ctx, qctx, func(workerID uint, db *storage.DataBlock) {
		if count >= limit {
			return
		}

		colMap := make(map[string][]string, len(db.Columns))
		for _, col := range db.Columns {
			colMap[col.Name] = col.Values
		}

		for rowIdx := 0; rowIdx < db.RowsCount && count < limit; rowIdx++ {
			if !matchFilters(colMap, rowIdx, filters) {
				continue
			}
			record := make(map[string]string, len(db.Columns))
			for _, col := range db.Columns {
				if rowIdx < len(col.Values) {
					record[col.Name] = col.Values[rowIdx]
				}
			}
			line, _ := json.Marshal(record)
			_, _ = w.Write(line)
			_, _ = w.Write([]byte("\n"))
			count++
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	})

	if err != nil {
		h.logger.Error("query error", "error", err, "query", qctx.Query)
	}
}

func (h *Handler) handleFieldNames(w http.ResponseWriter, r *http.Request) {
	qctx := h.parseQueryContext(r)

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	results, err := h.store.GetFieldNames(ctx, qctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.writeValuesJSON(w, results)
}

func (h *Handler) handleFieldValues(w http.ResponseWriter, r *http.Request) {
	qctx := h.parseQueryContext(r)
	fieldName := r.URL.Query().Get("field")
	if fieldName == "" {
		fieldName = r.URL.Query().Get("field_name")
	}
	if fieldName == "" {
		http.Error(w, "field parameter required", http.StatusBadRequest)
		return
	}

	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	results, err := h.store.GetFieldValues(ctx, qctx, fieldName, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.writeValuesJSON(w, results)
}

func (h *Handler) handleStreamFieldNames(w http.ResponseWriter, r *http.Request) {
	qctx := h.parseQueryContext(r)

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	results, err := h.store.GetStreamFieldNames(ctx, qctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.writeValuesJSON(w, results)
}

func (h *Handler) handleStreamFieldValues(w http.ResponseWriter, r *http.Request) {
	qctx := h.parseQueryContext(r)
	fieldName := r.URL.Query().Get("field")
	if fieldName == "" {
		fieldName = r.URL.Query().Get("field_name")
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	results, err := h.store.GetStreamFieldValues(ctx, qctx, fieldName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.writeValuesJSON(w, results)
}

func (h *Handler) handleStreams(w http.ResponseWriter, r *http.Request) {
	qctx := h.parseQueryContext(r)

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	results, err := h.store.GetStreams(ctx, qctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.writeValuesJSON(w, results)
}

func (h *Handler) handleStreamIDs(w http.ResponseWriter, r *http.Request) {
	qctx := h.parseQueryContext(r)

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	results, err := h.store.GetStreamIDs(ctx, qctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.writeValuesJSON(w, results)
}

func (h *Handler) handleHits(w http.ResponseWriter, r *http.Request) {
	qctx := h.parseQueryContext(r)
	groupField := r.URL.Query().Get("field")

	step := 60 * time.Second
	if s := r.URL.Query().Get("step"); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			step = d
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	grouped := make(map[string]map[int64]int)
	var mu sync.Mutex

	err := h.store.RunQuery(ctx, qctx, func(workerID uint, db *storage.DataBlock) {
		var timeVals []string
		var groupVals []string
		for _, col := range db.Columns {
			if col.Name == "_time" || col.Name == "timestamp_unix_nano" {
				timeVals = col.Values
			}
			if groupField != "" && col.Name == groupField {
				groupVals = col.Values
			}
		}
		if timeVals == nil {
			return
		}

		mu.Lock()
		defer mu.Unlock()
		for i, v := range timeVals {
			ns, parseErr := strconv.ParseInt(v, 10, 64)
			if parseErr != nil {
				continue
			}
			t := time.Unix(0, ns).Truncate(step).UnixNano()
			gv := ""
			if groupField != "" && i < len(groupVals) {
				gv = groupVals[i]
			}
			if grouped[gv] == nil {
				grouped[gv] = make(map[int64]int)
			}
			grouped[gv][t]++
		}
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	hits := make([]map[string]any, 0, len(grouped))
	for gv, buckets := range grouped {
		sorted := make([]int64, 0, len(buckets))
		for ns := range buckets {
			sorted = append(sorted, ns)
		}
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		timestamps := make([]string, 0, len(sorted))
		values := make([]int, 0, len(sorted))
		for _, ns := range sorted {
			timestamps = append(timestamps, time.Unix(0, ns).UTC().Format(time.RFC3339))
			values = append(values, buckets[ns])
		}

		fields := map[string]string{}
		if groupField != "" {
			fields[groupField] = gv
		}
		hits = append(hits, map[string]any{
			"fields": fields, "timestamps": timestamps, "values": values,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"hits": hits,
	})
}

func (h *Handler) handleStatsQuery(w http.ResponseWriter, r *http.Request) {
	qctx := h.parseQueryContext(r)

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	var total int
	err := h.store.RunQuery(ctx, qctx, func(workerID uint, db *storage.DataBlock) {
		total += db.RowsCount
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "vector",
			"result": []map[string]any{
				{
					"metric": map[string]string{},
					"value":  []any{float64(time.Now().Unix()), fmt.Sprintf("%d", total)},
				},
			},
		},
	})
}

func (h *Handler) handleStatsQueryRange(w http.ResponseWriter, r *http.Request) {
	h.handleStatsQuery(w, r)
}

func (h *Handler) handleTailNoop(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "live tail not supported on cold storage", http.StatusNotImplemented)
}

func (h *Handler) writeValuesJSON(w http.ResponseWriter, values []storage.ValueWithHits) {
	w.Header().Set("Content-Type", "application/json")

	type entry struct {
		Value string `json:"value"`
		Hits  uint64 `json:"hits"`
	}

	entries := make([]entry, len(values))
	for i, v := range values {
		entries[i] = entry{Value: v.Value, Hits: v.Hits}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"values": entries,
	})
}

// Jaeger API handlers for trace mode

func (h *Handler) handleJaegerServices(w http.ResponseWriter, r *http.Request) {
	qctx := &storage.QueryContext{
		StartNs: time.Now().Add(-720 * time.Hour).UnixNano(),
		EndNs:   time.Now().UnixNano(),
		Query:   "*",
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	// Use Parquet column name — works for both logs and traces mode
	results, err := h.store.GetFieldValues(ctx, qctx, "service.name", 1000)
	if err == nil && len(results) == 0 {
		results, err = h.store.GetFieldValues(ctx, qctx, "resource_attr:service.name", 1000)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	services := make([]string, 0, len(results))
	for _, v := range results {
		if v.Value != "" {
			services = append(services, v.Value)
		}
	}
	sort.Strings(services)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(jaegerListResponse{
		Data:   services,
		Total:  len(services),
		Limit:  0,
		Offset: 0,
	})
}

func (h *Handler) handleJaegerOperations(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/select/jaeger/api/services/")
	trimmed = strings.TrimPrefix(trimmed, "/api/services/")
	if trimmed == r.URL.Path {
		trimmed = strings.TrimPrefix(r.URL.Path, "/api/services/")
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) < 1 || parts[0] == "" {
		http.Error(w, "service name required", http.StatusBadRequest)
		return
	}

	if len(parts) >= 2 && parts[1] == "operations" {
		qctx := &storage.QueryContext{
			StartNs: time.Now().Add(-720 * time.Hour).UnixNano(),
			EndNs:   time.Now().UnixNano(),
			Query:   fmt.Sprintf(`service.name:="%s"`, parts[0]),
		}

		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()

		results, err := h.store.GetFieldValues(ctx, qctx, "span.name", 1000)
		if err == nil && len(results) == 0 {
			results, err = h.store.GetFieldValues(ctx, qctx, "name", 1000)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		ops := make([]string, 0, len(results))
		for _, v := range results {
			if v.Value != "" {
				ops = append(ops, v.Value)
			}
		}
		sort.Strings(ops)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jaegerListResponse{
			Data:   ops,
			Total:  len(ops),
			Limit:  0,
			Offset: 0,
		})
		return
	}

	http.Error(w, "not found", http.StatusNotFound)
}

func (h *Handler) handleJaegerTrace(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.TrimRight(r.URL.Path, "/"), "/")
	traceID := pathParts[len(pathParts)-1]
	if traceID == "" || traceID == "traces" {
		http.Error(w, "trace ID required", http.StatusBadRequest)
		return
	}

	qctx := &storage.QueryContext{
		StartNs: 0,
		EndNs:   time.Now().Add(time.Hour).UnixNano(),
		Query:   fmt.Sprintf(`trace_id:="%s"`, traceID),
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	var spans []jaegerSpan
	err := h.store.RunQuery(ctx, qctx, func(workerID uint, db *storage.DataBlock) {
		colMap := make(map[string][]string, len(db.Columns))
		for _, col := range db.Columns {
			colMap[col.Name] = col.Values
		}

		for i := 0; i < db.RowsCount; i++ {
			rowTraceID := getVal(colMap, "trace_id", i)
			if rowTraceID != traceID {
				continue
			}

			span := jaegerSpan{
				TraceID: rowTraceID,
				SpanID:  getVal(colMap, "span_id", i),
			}
			span.ParentSpanID = getVal(colMap, "parent_span_id", i)
			span.OperationName = getValAny(colMap, i, "name", "span.name")
			span.ServiceName = getValAny(colMap, i, "resource_attr:service.name", "service.name")

			startStr := getValAny(colMap, i, "start_time_unix_nano")
			if startNs, err := strconv.ParseInt(startStr, 10, 64); err == nil {
				span.StartTime = startNs / 1000
			}
			durStr := getValAny(colMap, i, "duration", "duration_ns")
			if durNs, err := strconv.ParseInt(durStr, 10, 64); err == nil {
				span.Duration = durNs / 1000
			}

			span.Tags = []jaegerTag{}
			var processTags []jaegerTag

			knownFields := map[string]bool{
				"trace_id": true, "span_id": true, "parent_span_id": true,
				"name": true, "span.name": true, "_time": true,
				"start_time_unix_nano": true, "duration": true, "duration_ns": true,
				"resource_attr:service.name": true, "service.name": true,
				"_stream": true, "_stream_id": true, "timestamp_unix_nano": true,
			}

			for colName, vals := range colMap {
				if knownFields[colName] || i >= len(vals) || vals[i] == "" {
					continue
				}
				v := vals[i]
				switch colName {
				case "status_code", "status.code":
					if v != "0" {
						statusStr := v
						switch v {
						case "1":
							statusStr = "STATUS_CODE_OK"
						case "2":
							statusStr = "STATUS_CODE_ERROR"
							span.Tags = append(span.Tags, jaegerTag{Key: "error", Type: "bool", Value: "true"})
						}
						span.Tags = append(span.Tags, jaegerTag{Key: "otel.status_code", Type: "string", Value: statusStr})
					}
				case "status_message", "status.message":
					span.Tags = append(span.Tags, jaegerTag{Key: "otel.status_description", Type: "string", Value: v})
				case "kind", "span.kind":
					span.Tags = append(span.Tags, jaegerTag{Key: "span.kind", Type: "string", Value: spanKindName(v)})
				default:
					if strings.HasPrefix(colName, "resource_attr:") {
						processTags = append(processTags, jaegerTag{
							Key: strings.TrimPrefix(colName, "resource_attr:"), Type: "string", Value: v,
						})
					} else if strings.HasPrefix(colName, "scope_attr:") {
						span.Tags = append(span.Tags, jaegerTag{
							Key: colName, Type: "string", Value: v,
						})
					} else {
						span.Tags = append(span.Tags, jaegerTag{Key: colName, Type: "string", Value: v})
					}
				}
			}

			span.Logs = []any{}
			span.ProcessID = "p1"
			span.processTags = processTags

			if span.ParentSpanID != "" {
				span.References = []jaegerReference{
					{RefType: "CHILD_OF", TraceID: span.TraceID, SpanID: span.ParentSpanID},
				}
			}

			spans = append(spans, span)
		}
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(spans) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(jaegerTracesResponse{
			Data:   []jaegerTraceData{},
			Errors: []map[string]any{{"code": 404, "msg": "trace not found"}},
			Total:  0,
			Limit:  0,
			Offset: 0,
		})
		return
	}

	processes := make(map[string]jaegerProcess)
	for i := range spans {
		pid := fmt.Sprintf("p%d", len(processes)+1)
		svcName := spans[i].ServiceName
		if svcName == "" {
			svcName = "unknown"
		}
		found := false
		for k, p := range processes {
			if p.ServiceName == svcName {
				pid = k
				found = true
				break
			}
		}
		if !found {
			tags := spans[i].processTags
			if tags == nil {
				tags = []jaegerTag{}
			}
			processes[pid] = jaegerProcess{ServiceName: svcName, Tags: tags}
		}
		spans[i].ProcessID = pid
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(jaegerTracesResponse{
		Data: []jaegerTraceData{
			{
				TraceID:   traceID,
				Spans:     spans,
				Processes: processes,
				Warnings:  nil,
			},
		},
		Total:  1,
		Limit:  0,
		Offset: 0,
	})
}

func (h *Handler) handleJaegerSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	service := q.Get("service")
	if service == "" {
		http.Error(w, "service name is required", http.StatusBadRequest)
		return
	}
	operation := q.Get("operation")
	lookback := q.Get("lookback")

	endNs := time.Now().UnixNano()
	startNs := time.Now().Add(-24 * time.Hour).UnixNano()
	if lookback != "" {
		if d, err := time.ParseDuration(lookback); err == nil {
			startNs = time.Now().Add(-d).UnixNano()
		}
	}
	if s := q.Get("start"); s != "" {
		if us, err := strconv.ParseInt(s, 10, 64); err == nil {
			startNs = us * 1000
		}
	}
	if s := q.Get("end"); s != "" {
		if us, err := strconv.ParseInt(s, 10, 64); err == nil {
			endNs = us * 1000
		}
	}

	query := "*"
	var queryParts []string
	if service != "" {
		queryParts = append(queryParts, fmt.Sprintf(`service.name:="%s"`, service))
	}
	if operation != "" {
		queryParts = append(queryParts, fmt.Sprintf(`span.name:="%s"`, operation))
	}

	var minDurNs, maxDurNs int64
	if s := q.Get("minDuration"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			minDurNs = d.Nanoseconds()
		}
	}
	if s := q.Get("maxDuration"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			maxDurNs = d.Nanoseconds()
		}
	}

	var tagFilters map[string]string
	if tags := q.Get("tags"); tags != "" {
		tagFilters = make(map[string]string)
		_ = json.Unmarshal([]byte(tags), &tagFilters)
		for k, v := range tagFilters {
			if mapped, ok := jaegerSpanAttrMap[k]; ok {
				delete(tagFilters, k)
				switch k {
				case "error":
					v = jaegerErrorStatusMap[v]
				case "span.kind":
					v = jaegerSpanKindToCodeMap[v]
				}
				tagFilters[mapped] = v
			}
		}
		for k, v := range tagFilters {
			queryParts = append(queryParts, fmt.Sprintf(`%s:="%s"`, k, v))
		}
	}

	if len(queryParts) > 0 {
		query = strings.Join(queryParts, " AND ")
	}

	limit := 20
	if l := q.Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			if parsed > 1000 {
				parsed = 1000
			}
			limit = parsed
		}
	}

	qctx := &storage.QueryContext{
		StartNs: startNs,
		EndNs:   endNs,
		Query:   query,
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	traceMap := make(map[string][]map[string]string)
	err := h.store.RunQuery(ctx, qctx, func(workerID uint, db *storage.DataBlock) {
		colMap := make(map[string][]string, len(db.Columns))
		for _, col := range db.Columns {
			colMap[col.Name] = col.Values
		}

		for i := 0; i < db.RowsCount; i++ {
			tid := getVal(colMap, "trace_id", i)
			if tid == "" {
				continue
			}

			durStr := getValAny(colMap, i, "duration", "duration_ns")
			if minDurNs > 0 || maxDurNs > 0 {
				if durNs, err := strconv.ParseInt(durStr, 10, 64); err == nil {
					if minDurNs > 0 && durNs < minDurNs {
						continue
					}
					if maxDurNs > 0 && durNs > maxDurNs {
						continue
					}
				}
			}

			span := map[string]string{
				"trace_id":             tid,
				"span_id":             getVal(colMap, "span_id", i),
				"service.name":        getValAny(colMap, i, "resource_attr:service.name", "service.name"),
				"span.name":           getValAny(colMap, i, "name", "span.name"),
				"start_time_unix_nano": getValAny(colMap, i, "start_time_unix_nano"),
				"duration_ns":         durStr,
			}
			traceMap[tid] = append(traceMap[tid], span)
		}
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var traces []jaegerTraceData
	for tid, rawSpans := range traceMap {
		if len(traces) >= limit {
			break
		}

		processHashMap := make(map[string]string)
		processes := make(map[string]jaegerProcess)

		jaegerSpans := make([]jaegerSpan, 0, len(rawSpans))
		for _, s := range rawSpans {
			startUs := int64(0)
			if ns, err := strconv.ParseInt(s["start_time_unix_nano"], 10, 64); err == nil {
				startUs = ns / 1000
			}
			durUs := int64(0)
			if ns, err := strconv.ParseInt(s["duration_ns"], 10, 64); err == nil {
				durUs = ns / 1000
			}

			svcName := s["service.name"]
			if svcName == "" {
				svcName = "unknown"
			}
			pid, ok := processHashMap[svcName]
			if !ok {
				pid = fmt.Sprintf("p%d", len(processHashMap)+1)
				processHashMap[svcName] = pid
				processes[pid] = jaegerProcess{ServiceName: svcName, Tags: []jaegerTag{}}
			}

			jaegerSpans = append(jaegerSpans, jaegerSpan{
				TraceID:       tid,
				SpanID:        s["span_id"],
				OperationName: s["span.name"],
				StartTime:     startUs,
				Duration:      durUs,
				Tags:          []jaegerTag{},
				Logs:          []any{},
				ProcessID:     pid,
			})
		}

		traces = append(traces, jaegerTraceData{
			TraceID:   tid,
			Spans:     jaegerSpans,
			Processes: processes,
			Warnings:  nil,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(jaegerTracesResponse{
		Data:   traces,
		Total:  len(traces),
		Limit:  0,
		Offset: 0,
	})
}

type jaegerListResponse struct {
	Data   []string `json:"data"`
	Errors any      `json:"errors"`
	Total  int      `json:"total"`
	Limit  int      `json:"limit"`
	Offset int      `json:"offset"`
}

type jaegerTracesResponse struct {
	Data   any `json:"data"`
	Errors any `json:"errors"`
	Total  int `json:"total"`
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

type jaegerTraceData struct {
	TraceID   string                    `json:"traceID"`
	Spans     []jaegerSpan              `json:"spans"`
	Processes map[string]jaegerProcess  `json:"processes"`
	Warnings  any                       `json:"warnings"`
}

type jaegerSpan struct {
	TraceID       string            `json:"traceID"`
	SpanID        string            `json:"spanID"`
	ParentSpanID  string            `json:"parentSpanID,omitempty"`
	OperationName string            `json:"operationName"`
	ServiceName   string            `json:"-"`
	StartTime     int64             `json:"startTime"`
	Duration      int64             `json:"duration"`
	Tags          []jaegerTag       `json:"tags"`
	Logs          []any             `json:"logs"`
	ProcessID     string            `json:"processID"`
	References    []jaegerReference `json:"references,omitempty"`
	Warnings      any               `json:"warnings"`
	processTags   []jaegerTag
}

type jaegerTag struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

type jaegerReference struct {
	RefType string `json:"refType"`
	TraceID string `json:"traceID"`
	SpanID  string `json:"spanID"`
}

type jaegerProcess struct {
	ServiceName string      `json:"serviceName"`
	Tags        []jaegerTag `json:"tags"`
}

var jaegerSpanAttrMap = map[string]string{
	"error":                   "status_code",
	"span.kind":               "kind",
	"otel.status_description": "status_message",
}

var jaegerErrorStatusMap = map[string]string{
	"unset": "0",
	"true":  "2",
	"false": "1",
}

var jaegerSpanKindToCodeMap = map[string]string{
	"internal": "1",
	"server":   "2",
	"client":   "3",
	"producer": "4",
	"consumer": "5",
}

func (h *Handler) handleJaegerDependencies(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(jaegerListResponse{
		Data:   []string{},
		Total:  0,
		Limit:  0,
		Offset: 0,
	})
}

func getVal(colMap map[string][]string, col string, idx int) string {
	if vals, ok := colMap[col]; ok && idx < len(vals) {
		return vals[idx]
	}
	return ""
}

func getValAny(colMap map[string][]string, idx int, names ...string) string {
	for _, name := range names {
		if vals, ok := colMap[name]; ok && idx < len(vals) && vals[idx] != "" {
			return vals[idx]
		}
	}
	return ""
}

func spanKindName(code string) string {
	switch code {
	case "1":
		return "internal"
	case "2":
		return "server"
	case "3":
		return "client"
	case "4":
		return "producer"
	case "5":
		return "consumer"
	default:
		return code
	}
}

type simpleFilter struct {
	field string
	value string
	exact bool
}

func parseSimpleFilters(query string) []simpleFilter {
	if query == "" || query == "*" {
		return nil
	}

	var filters []simpleFilter
	parts := strings.Fields(query)
	for _, part := range parts {
		if part == "AND" || part == "OR" || part == "NOT" {
			continue
		}
		if idx := strings.Index(part, `:="`); idx > 0 {
			field := part[:idx]
			val := strings.TrimSuffix(strings.TrimPrefix(part[idx+3:], ""), `"`)
			filters = append(filters, simpleFilter{field: field, value: val, exact: true})
		} else if idx := strings.Index(part, `:`); idx > 0 {
			field := part[:idx]
			val := strings.Trim(part[idx+1:], `"`)
			if val != "" && val != "*" {
				filters = append(filters, simpleFilter{field: field, value: val, exact: false})
			}
		}
	}
	return filters
}

func matchFilters(colMap map[string][]string, rowIdx int, filters []simpleFilter) bool {
	for _, f := range filters {
		val := ""
		if vals, ok := colMap[f.field]; ok && rowIdx < len(vals) {
			val = vals[rowIdx]
		}
		if f.exact {
			if val != f.value {
				return false
			}
		} else {
			if !strings.Contains(val, f.value) {
				return false
			}
		}
	}
	return true
}

func parseTimeParam(primary, secondary string, defaultVal int64) int64 {
	for _, s := range []string{primary, secondary} {
		if s == "" {
			continue
		}
		if ns, err := strconv.ParseInt(s, 10, 64); err == nil {
			switch {
			case ns < 1e12:
				return ns * int64(time.Second)
			case ns < 1e15:
				return ns * int64(time.Millisecond)
			case ns < 1e18:
				return ns * 1000
			default:
				return ns
			}
		}
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t.UnixNano()
		}
		if d, err := time.ParseDuration(s); err == nil {
			return time.Now().Add(-d).UnixNano()
		}
	}
	return defaultVal
}
