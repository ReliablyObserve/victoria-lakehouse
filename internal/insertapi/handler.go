package insertapi

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type LogStore interface {
	MustAddLogRows(rows []schema.LogRow)
	CanWriteData() error
}

type Handler struct {
	store          LogStore
	logger         *slog.Logger
	cfg            *config.Config
	promotedFields map[string]bool
	bufferHandler  *BufferHandler
}

func NewHandler(store LogStore, logger *slog.Logger, cfg *config.Config, bq ...BufferQuerier) *Handler {
	promoted := make(map[string]bool, len(promotedLogFields)+len(cfg.Schema.ExtraPromoted))
	for k, v := range promotedLogFields {
		promoted[k] = v
	}
	for _, ep := range cfg.Schema.ExtraPromoted {
		promoted[ep.Name] = true
	}
	h := &Handler{
		store:          store,
		logger:         logger.With("component", "insertapi"),
		cfg:            cfg,
		promotedFields: promoted,
	}
	if len(bq) > 0 && bq[0] != nil {
		h.bufferHandler = NewBufferHandler(bq[0])
	}
	return h
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/insert/jsonline", h.handleJSONLine)
	mux.HandleFunc("/insert/loki/api/v1/push", h.handleLokiPush)
	mux.HandleFunc("/insert/elasticsearch/_bulk", h.handleESBulk)
	if h.bufferHandler != nil {
		mux.Handle("/internal/buffer/query", h.bufferHandler)
	}
}

// handleJSONLine accepts VL-compatible JSON line format.
// Each line: {"_time":"...","_msg":"...","field":"value",...}
func (h *Handler) handleJSONLine(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.store.CanWriteData(); err != nil {
		http.Error(w, "cannot write: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)

	var rows []schema.LogRow
	var count int

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			h.logger.Debug("skipping invalid JSON line", "error", err)
			continue
		}

		row := jsonFieldsToLogRow(fields, h.promotedFields)
		rows = append(rows, row)
		count++
	}

	if err := scanner.Err(); err != nil {
		h.logger.Error("scanner error", "error", err)
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}

	if len(rows) > 0 {
		h.store.MustAddLogRows(rows)
	}

	w.WriteHeader(http.StatusNoContent)
	h.logger.Debug("jsonline insert", "rows", count)
}

// handleLokiPush accepts Loki push API format (JSON).
// POST /insert/loki/api/v1/push
// Body: {"streams":[{"stream":{"label":"value"},"values":[["nanosecond_ts","line"],...]}]}
func (h *Handler) handleLokiPush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.store.CanWriteData(); err != nil {
		http.Error(w, "cannot write: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	var push lokiPushRequest
	if err := json.Unmarshal(body, &push); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	var rows []schema.LogRow
	for _, stream := range push.Streams {
		for _, entry := range stream.Values {
			if len(entry) < 2 {
				continue
			}
			tsNano, err := strconv.ParseInt(entry[0], 10, 64)
			if err != nil {
				tsNano = time.Now().UnixNano()
			}

			row := schema.LogRow{
				TimestampUnixNano: tsNano,
				Body:              entry[1],
			}
			applyStreamLabels(&row, stream.Stream)
			rows = append(rows, row)
		}
	}

	if len(rows) > 0 {
		h.store.MustAddLogRows(rows)
	}

	w.WriteHeader(http.StatusNoContent)
	h.logger.Debug("loki push", "rows", len(rows))
}

// handleESBulk accepts Elasticsearch bulk format (index + doc lines).
func (h *Handler) handleESBulk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.store.CanWriteData(); err != nil {
		http.Error(w, "cannot write: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)

	var rows []schema.LogRow
	expectDoc := false

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		if !expectDoc {
			expectDoc = true
			continue
		}

		expectDoc = false
		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			continue
		}

		row := jsonFieldsToLogRow(fields, h.promotedFields)
		rows = append(rows, row)
	}

	if len(rows) > 0 {
		h.store.MustAddLogRows(rows)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"errors":false,"items":[]}`)
	h.logger.Debug("es bulk", "rows", len(rows))
}

type lokiPushRequest struct {
	Streams []lokiStream `json:"streams"`
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

var promotedLogFields = map[string]bool{
	"_time": true, "_msg": true, "level": true,
	"service.name": true, "k8s.namespace.name": true, "k8s.pod.name": true,
	"k8s.deployment.name": true, "k8s.node.name": true,
	"deployment.environment": true, "cloud.region": true, "host.name": true,
	"trace_id": true, "span_id": true, "scope.name": true,
}

func jsonFieldsToLogRow(fields map[string]any, promoted map[string]bool) schema.LogRow {
	row := schema.LogRow{
		TimestampUnixNano: time.Now().UnixNano(),
	}

	if v, ok := fields["_time"]; ok {
		switch t := v.(type) {
		case string:
			if parsed, err := time.Parse(time.RFC3339Nano, t); err == nil {
				row.TimestampUnixNano = parsed.UnixNano()
			}
		case float64:
			row.TimestampUnixNano = int64(t)
		}
	}
	if v, ok := fields["_msg"].(string); ok {
		row.Body = v
	}
	if v, ok := fields["level"].(string); ok {
		row.SeverityText = v
	}
	if v, ok := fields["service.name"].(string); ok {
		row.ServiceName = v
	}
	if v, ok := fields["k8s.namespace.name"].(string); ok {
		row.K8sNamespaceName = v
	}
	if v, ok := fields["k8s.pod.name"].(string); ok {
		row.K8sPodName = v
	}
	if v, ok := fields["k8s.deployment.name"].(string); ok {
		row.K8sDeploymentName = v
	}
	if v, ok := fields["k8s.node.name"].(string); ok {
		row.K8sNodeName = v
	}
	if v, ok := fields["deployment.environment"].(string); ok {
		row.DeployEnv = v
	}
	if v, ok := fields["cloud.region"].(string); ok {
		row.CloudRegion = v
	}
	if v, ok := fields["host.name"].(string); ok {
		row.HostName = v
	}
	if v, ok := fields["trace_id"].(string); ok {
		row.TraceID = v
	}
	if v, ok := fields["span_id"].(string); ok {
		row.SpanID = v
	}
	if v, ok := fields["scope.name"].(string); ok {
		row.ScopeName = v
	}

	for k, v := range fields {
		if promoted[k] {
			continue
		}
		s, ok := v.(string)
		if !ok {
			s = fmt.Sprintf("%v", v)
		}
		if row.LogAttributes == nil {
			row.LogAttributes = make(map[string]string)
		}
		row.LogAttributes[k] = s
	}

	if row.ServiceName != "" || row.K8sNamespaceName != "" {
		row.Stream = fmt.Sprintf(`{service.name=%q,k8s.namespace.name=%q}`, row.ServiceName, row.K8sNamespaceName)
	}

	return row
}

func applyStreamLabels(row *schema.LogRow, labels map[string]string) {
	for k, v := range labels {
		switch k {
		case "service", "service.name":
			row.ServiceName = v
		case "namespace", "k8s.namespace.name":
			row.K8sNamespaceName = v
		case "pod", "k8s.pod.name":
			row.K8sPodName = v
		case "deployment", "k8s.deployment.name":
			row.K8sDeploymentName = v
		case "node", "k8s.node.name":
			row.K8sNodeName = v
		case "env", "deployment.environment":
			row.DeployEnv = v
		case "region", "cloud.region":
			row.CloudRegion = v
		case "host", "host.name":
			row.HostName = v
		case "level":
			row.SeverityText = v
		default:
			if row.ResourceAttributes == nil {
				row.ResourceAttributes = make(map[string]string)
			}
			row.ResourceAttributes[k] = v
		}
	}

	var streamParts []string
	for k, v := range labels {
		streamParts = append(streamParts, fmt.Sprintf("%s=%q", k, v))
	}
	if len(streamParts) > 0 {
		row.Stream = "{" + strings.Join(streamParts, ",") + "}"
	}
}
