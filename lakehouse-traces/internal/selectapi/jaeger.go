package selectapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Jaeger API handlers for trace mode

func (h *Handler) handleJaegerServices(w http.ResponseWriter, r *http.Request) {
	q, err := logstorage.ParseQuery("*")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	q.AddTimeFilter(time.Now().Add(-720*time.Hour).UnixNano(), time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	results, err := h.store.GetFieldValues(ctx, nil, q, "service.name", 1000)
	if err == nil && len(results) == 0 {
		results, err = h.store.GetFieldValues(ctx, nil, q, "resource_attr:service.name", 1000)
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
		Data:  services,
		Total: len(services),
	})
}

func (h *Handler) handleJaegerOperations(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/select/jaeger/api/services/")
	trimmed = strings.TrimPrefix(trimmed, "/api/services/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 1 || parts[0] == "" {
		http.Error(w, "service name required", http.StatusBadRequest)
		return
	}

	if len(parts) >= 2 && parts[1] == "operations" {
		q, err := logstorage.ParseQuery(fmt.Sprintf(`"resource_attr:service.name":="%s"`, parts[0]))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		q.AddTimeFilter(time.Now().Add(-720*time.Hour).UnixNano(), time.Now().UnixNano())

		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()

		results, err := h.store.GetFieldValues(ctx, nil, q, "span.name", 1000)
		if err == nil && len(results) == 0 {
			results, err = h.store.GetFieldValues(ctx, nil, q, "name", 1000)
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
			Data:  ops,
			Total: len(ops),
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

	q, err := logstorage.ParseQuery(fmt.Sprintf(`trace_id:="%s"`, traceID))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	q.AddTimeFilter(0, time.Now().Add(time.Hour).UnixNano())

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	var spans []jaegerSpan
	err = h.store.RunQuery(ctx, nil, q, func(_ uint, db *logstorage.DataBlock) {
		columns := db.GetColumns(false)
		colMap := make(map[string][]string, len(columns))
		for _, col := range columns {
			colMap[col.Name] = col.Values
		}

		for i := 0; i < db.RowsCount(); i++ {
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

			startStr := getValAny(colMap, i, "start_time", "start_time_unix_nano")
			if startNs, ok := parseTimestampNanos(startStr); ok {
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
				"name": true, "span.name": true, "_time": true, "start_time": true,
				"start_time_unix_nano": true, "timestamp_unix_nano": true,
				"duration": true, "duration_ns": true,
				"resource_attr:service.name": true, "service.name": true,
				"_stream": true, "_stream_id": true,
				"scope.name": true, "severity_number": true,
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
					} else if strings.HasPrefix(colName, "span_attr:") {
						span.Tags = append(span.Tags, jaegerTag{
							Key: strings.TrimPrefix(colName, "span_attr:"), Type: "string", Value: v,
						})
					} else if strings.HasPrefix(colName, "scope_attr:") {
						span.Tags = append(span.Tags, jaegerTag{
							Key: strings.TrimPrefix(colName, "scope_attr:"), Type: "string", Value: v,
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
			{TraceID: traceID, Spans: spans, Processes: processes},
		},
		Total: 1,
	})
}

func (h *Handler) handleJaegerSearch(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	service := params.Get("service")
	if service == "" {
		http.Error(w, "service name is required", http.StatusBadRequest)
		return
	}
	operation := params.Get("operation")

	endNs := time.Now().UnixNano()
	startNs := time.Now().Add(-24 * time.Hour).UnixNano()
	if lookback := params.Get("lookback"); lookback != "" {
		if d, err := time.ParseDuration(lookback); err == nil {
			startNs = time.Now().Add(-d).UnixNano()
		}
	}
	if s := params.Get("start"); s != "" {
		if us, err := strconv.ParseInt(s, 10, 64); err == nil {
			startNs = us * 1000
		}
	}
	if s := params.Get("end"); s != "" {
		if us, err := strconv.ParseInt(s, 10, 64); err == nil {
			endNs = us * 1000
		}
	}

	var queryParts []string
	queryParts = append(queryParts, fmt.Sprintf(`"resource_attr:service.name":="%s"`, service))
	if operation != "" {
		queryParts = append(queryParts, fmt.Sprintf(`name:="%s"`, operation))
	}

	var minDurNs, maxDurNs int64
	if s := params.Get("minDuration"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			minDurNs = d.Nanoseconds()
		}
	}
	if s := params.Get("maxDuration"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			maxDurNs = d.Nanoseconds()
		}
	}

	if tags := params.Get("tags"); tags != "" {
		var tagFilters map[string]string
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
			if strings.ContainsRune(k, ':') {
				queryParts = append(queryParts, fmt.Sprintf(`"%s":="%s"`, k, v))
			} else {
				queryParts = append(queryParts, fmt.Sprintf(`%s:="%s"`, k, v))
			}
		}
	}

	limit := 20
	if l := params.Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			if parsed > 1000 {
				parsed = 1000
			}
			limit = parsed
		}
	}

	q, err := logstorage.ParseQuery(strings.Join(queryParts, " AND "))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	q.AddTimeFilter(startNs, endNs)

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	traceMap := make(map[string][]map[string]string)
	err = h.store.RunQuery(ctx, nil, q, func(_ uint, db *logstorage.DataBlock) {
		columns := db.GetColumns(false)
		colMap := make(map[string][]string, len(columns))
		for _, col := range columns {
			colMap[col.Name] = col.Values
		}

		for i := 0; i < db.RowsCount(); i++ {
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
				"trace_id":    tid,
				"span_id":     getVal(colMap, "span_id", i),
				"service.name": getValAny(colMap, i, "resource_attr:service.name", "service.name"),
				"span.name":   getValAny(colMap, i, "name", "span.name"),
				"start_time":  getValAny(colMap, i, "start_time", "start_time_unix_nano"),
				"duration_ns": durStr,
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
			if ns, ok := parseTimestampNanos(s["start_time"]); ok {
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
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(jaegerTracesResponse{
		Data:  traces,
		Total: len(traces),
	})
}

func (h *Handler) handleJaegerDependencies(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"data": []any{},
	})
}

// Jaeger types

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
	TraceID   string                   `json:"traceID"`
	Spans     []jaegerSpan             `json:"spans"`
	Processes map[string]jaegerProcess `json:"processes"`
	Warnings  any                      `json:"warnings"`
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

func parseTimestampNanos(s string) (int64, bool) {
	if ns, err := strconv.ParseInt(s, 10, 64); err == nil {
		return ns, true
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UnixNano(), true
	}
	return 0, false
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
