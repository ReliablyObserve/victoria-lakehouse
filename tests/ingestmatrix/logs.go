package ingestmatrix

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

const (
	rows3    = 3
	svcName  = "ingest-matrix"
	ctJSON   = "application/json"
	ctNDJSON = "application/stream+json"
	ctProto  = "application/x-protobuf"
)

var bothForms = []Form{Numeric, Alias}

var sevGap = []string{"cold-read-adds-severity-number"}

func logsCases() []Case {
	return []Case{
		{
			ID: "jsonline", Signal: Logs, Gaps: sevGap, Title: "/insert/jsonline: newline-delimited JSON", Transport: HTTP,
			Forms: bothForms, Rows: rows3, Routes: []string{"/insert/jsonline"},
			Counter: `vl_rows_ingested_total{type="jsonline"}`, Build: buildJSONLine,
		},
		{
			ID: "native", Signal: Logs, Gaps: sevGap, Title: "/insert/native: VictoriaLogs native binary protocol", Transport: HTTP,
			Forms: bothForms, Rows: rows3, Routes: []string{"/insert/native"},
			Counter: `vl_rows_ingested_total{type="nativeinsert"}`, Build: buildNative(false),
		},
		{
			ID: "multitenant_native", Signal: Logs, Gaps: sevGap, Title: "/insert/multitenant/native: native protocol, tenant inside each row", Transport: HTTP,
			Forms: []Form{Numeric}, FormNote: "the tenant is carried by each row of the payload and request tenant headers are ignored upstream, so there is no header or alias form",
			Rows: rows3, Routes: []string{"/insert/multitenant/native"}, NoTenantHeaders: true,
			Counter: `vl_rows_ingested_total{type="nativemultitenant"}`, Build: buildNative(true),
		},
		{
			ID: "internal_insert", Signal: Logs, Gaps: sevGap, Title: "/internal/insert: storage-node ingest used by a vlinsert tier (native rows)", Transport: HTTP,
			Forms: []Form{Numeric}, FormNote: "the tenant is carried by each row of the payload and request tenant headers are ignored upstream, so there is no header or alias form",
			Rows: rows3, Routes: []string{"/internal/insert"}, NoTenantHeaders: true,
			Counter: `vl_rows_ingested_total{type="internalinsert"}`, Build: buildNativeAt("/internal/insert"),
		},
		{
			ID: "loki_json", Signal: Logs, Gaps: sevGap, Title: "/insert/loki/api/v1/push: Loki push, JSON", Transport: HTTP,
			Forms: bothForms, Rows: rows3, Routes: []string{"/insert/loki/api/v1/push"},
			Counter: `vl_rows_ingested_total{type="loki_json"}`, Build: buildLokiJSON,
		},
		{
			ID: "loki_protobuf", Signal: Logs, Gaps: sevGap, Title: "/insert/loki/api/v1/push: Loki push, snappy protobuf", Transport: HTTP,
			Forms: bothForms, Rows: rows3, Routes: []string{"/insert/loki/api/v1/push"},
			Counter: `vl_rows_ingested_total{type="loki_protobuf"}`, Build: buildLokiProtobuf,
		},
		{
			ID: "elasticsearch_bulk", Signal: Logs, Gaps: sevGap, Title: "/insert/elasticsearch/_bulk: Elasticsearch bulk", Transport: HTTP,
			Forms: bothForms, Rows: rows3, Routes: []string{"/insert/elasticsearch/_bulk"},
			Counter: `vl_rows_ingested_total{type="elasticsearch_bulk"}`, Build: buildESBulk,
			NormalizeResponse: dropJSONKey("took"),
		},
		{
			ID: "splunk_event", Signal: Logs, Gaps: sevGap, Title: "Splunk HEC events: four route spellings", Transport: HTTP,
			Forms: bothForms, Rows: 4, Routes: []string{
				"/insert/splunk/services/collector/event", "/insert/splunk/services/collector/event/1.0",
				"/services/collector/event", "/services/collector/event/1.0",
			},
			Counter: `vl_rows_ingested_total{type="splunk"}`, Build: buildSplunk,
		},
		{
			ID: "datadog_logs", Signal: Logs, Gaps: sevGap, Title: "Datadog v2 logs intake: both route spellings", Transport: HTTP,
			Forms: bothForms, Rows: rows3, Routes: []string{"/insert/datadog/api/v2/logs", "/api/v2/logs"},
			Counter: `vl_rows_ingested_total{type="datadog"}`, Build: buildDatadog,
		},
		{
			ID: "journald", Signal: Logs, Gaps: sevGap, Title: "/insert/journald/upload: systemd-journal-upload export format", Transport: HTTP,
			Forms: bothForms, Rows: rows3, Routes: []string{"/insert/journald/upload"},
			Counter: `vl_rows_ingested_total{type="journald"}`, Build: buildJournald,
		},
		{
			ID: "otlp_logs_protobuf", Signal: Logs, Gaps: []string{"cold-read-renames-severity-text-to-level"}, Title: "/insert/opentelemetry/v1/logs: OTLP/HTTP logs, protobuf", Transport: HTTP,
			Forms: bothForms, Rows: rows3, Routes: []string{"/insert/opentelemetry/v1/logs"},
			Counter: `vl_rows_ingested_total{type="opentelemetry_protobuf"}`, Build: buildOTLPLogsProtobuf,
		},
		{
			ID: "otlp_logs_json", Signal: Logs, Title: "/insert/opentelemetry/v1/logs: OTLP/HTTP logs, JSON (refused upstream)", Transport: HTTP,
			Forms: bothForms, Rows: 0, Rejected: true, Routes: []string{"/insert/opentelemetry/v1/logs"},
			Build: buildOTLPLogsJSON,
		},
		{
			ID: "syslog_tcp_rfc3164", Signal: Logs, Gaps: sevGap, Title: "syslog over TCP, RFC3164 lines", Transport: TCP,
			Forms: []Form{Numeric}, FormNote: "upstream syslog pins the tenant per listener (-syslog.tenantID.tcp), there is no per-message tenant, header or alias form",
			Rows: rows3, Flags: []string{"syslog.listenAddr.tcp"},
			Counter: `vl_rows_ingested_total{type="syslog_tcp"}`, Lines: syslogLines(false),
		},
		{
			ID: "syslog_tcp_rfc5424", Signal: Logs, Gaps: sevGap, Title: "syslog over TCP, RFC5424 lines", Transport: TCP,
			Forms: []Form{Numeric}, FormNote: "see syslog_tcp_rfc3164",
			Rows: rows3, Flags: []string{"syslog.listenAddr.tcp"},
			Counter: `vl_rows_ingested_total{type="syslog_tcp"}`, Lines: syslogLines(true),
		},
		{
			ID: "syslog_udp_rfc3164", Signal: Logs, Gaps: sevGap, Title: "syslog over UDP, RFC3164 datagrams", Transport: UDP,
			Forms: []Form{Numeric}, FormNote: "see syslog_tcp_rfc3164",
			Rows: rows3, Flags: []string{"syslog.listenAddr.udp"},
			Counter: `vl_rows_ingested_total{type="syslog_udp"}`, Lines: syslogLines(false),
		},
		{
			ID: "syslog_udp_rfc5424", Signal: Logs, Gaps: sevGap, Title: "syslog over UDP, RFC5424 datagrams", Transport: UDP,
			Forms: []Form{Numeric}, FormNote: "see syslog_tcp_rfc3164",
			Rows: rows3, Flags: []string{"syslog.listenAddr.udp"},
			Counter: `vl_rows_ingested_total{type="syslog_udp"}`, Lines: syslogLines(true),
		},
	}
}

// Probes are the non-data ingest routes: readiness, health
// and the Elasticsearch/Datadog/Splunk compatibility stubs.
func Probes() []Probe {
	get := func(s Signal, route string) Probe {
		return Probe{Signal: s, Route: route, Method: "GET", Path: route}
	}
	ps := []Probe{
		get(Logs, "/insert/ready"),
		get(Logs, "/insert/elasticsearch"),
		get(Logs, "/insert/elasticsearch/"),
		get(Logs, "/insert/elasticsearch/_ilm/policy"),
		get(Logs, "/insert/elasticsearch/_index_template"),
		get(Logs, "/insert/elasticsearch/_ingest"),
		get(Logs, "/insert/elasticsearch/_license"),
		get(Logs, "/insert/elasticsearch/_logstash"),
		get(Logs, "/insert/elasticsearch/_nodes"),
		get(Logs, "/insert/elasticsearch/_rollup"),
		get(Logs, "/insert/elasticsearch/logstash"),
		get(Logs, "/insert/loki/ready"),
		get(Logs, "/insert/splunk/services/collector/health"),
		get(Logs, "/services/collector/health"),
		get(Logs, "/insert/datadog/api/v1/validate"),
		get(Logs, "/api/v1/validate"),
		get(Traces, "/insert/ready"),
	}
	return ps
}

// Exclusions lists the upstream ingest routes and listeners the matrix does not
// send data to. Every entry names an upstream inventory item and a reason; the
// drift gate deletes none silently: an entry that stops matching fails.
func Exclusions() []Exclusion {
	return []Exclusion{
		{Logs, "flag:syslog.listenAddr.unix", "a unix-socket listener cannot be published by the compose file or the Helm Service; same code path as the TCP listener"},
		{Traces, "route:/internal/insert", "known gap, tracked in https://github.com/ReliablyObserve/victoria-lakehouse/issues/334: lakehouse-traces does not mount VictoriaTraces' storage-node ingest route (404 where hot VT answers 200); the registry row vt.internal.insert.count stays pending"},
	}
}

type kv struct{ k, v string }

// rowAttrs are the non-message fields of row i, in a fixed order.
func rowAttrs(p Params, i int) []kv {
	return []kv{
		{"level", Level(i)},
		{"service.name", svcName},
		{"row", strconv.Itoa(i)},
		{"http.status", strconv.Itoa(200 + i)},
		{"env", "e2e"},
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func buildJSONLine(p Params) []Request {
	var b bytes.Buffer
	for i := 1; i <= rows3; i++ {
		fmt.Fprintf(&b, `{"_time":%s,"_msg":%s`, jsonString(p.RowTime(i, rows3).UTC().Format(time.RFC3339Nano)), jsonString(p.Msg(i)))
		for _, a := range rowAttrs(p, i) {
			fmt.Fprintf(&b, `,%s:%s`, jsonString(a.k), jsonString(a.v))
		}
		b.WriteString(`,"payload":{"nested":"value","n":` + strconv.Itoa(i) + "}}\n")
	}
	return []Request{{Method: "POST", Path: "/insert/jsonline", Query: "_stream_fields=service.name",
		Header: map[string]string{"Content-Type": ctNDJSON}, Body: b.Bytes(), Rows: rows3}}
}

func nativeRows(p Params, tenant Tenant) []byte {
	var out []byte
	for i := 1; i <= rows3; i++ {
		st := logstorage.GetStreamTags()
		st.Add("service.name", svcName)
		r := logstorage.InsertRow{
			TenantID:            logstorage.TenantID{AccountID: tenant.Account, ProjectID: tenant.Project},
			StreamTagsCanonical: string(st.MarshalCanonical(nil)),
			Timestamp:           p.RowTime(i, rows3).UnixNano(),
		}
		logstorage.PutStreamTags(st)
		r.Fields = append(r.Fields, logstorage.Field{Name: "_msg", Value: p.Msg(i)})
		r.Fields = append(r.Fields, logstorage.Field{Name: "service.name", Value: svcName})
		for _, a := range rowAttrs(p, i) {
			if a.k == "service.name" {
				continue
			}
			r.Fields = append(r.Fields, logstorage.Field{Name: a.k, Value: a.v})
		}
		out = r.Marshal(out)
	}
	return out
}

func buildNative(multitenant bool) func(Params) []Request {
	if multitenant {
		return buildNativeAt("/insert/multitenant/native")
	}
	return buildNativeAt("/insert/native")
}

func buildNativeAt(path string) func(Params) []Request {
	return func(p Params) []Request {
		return []Request{{Method: "POST", Path: path, Query: "version=v1",
			Header: map[string]string{"Content-Type": "application/octet-stream"}, Body: nativeRows(p, p.Tenant), Rows: rows3}}
	}
}

func buildLokiJSON(p Params) []Request {
	var streams []string
	for i := 1; i <= rows3; i++ {
		streams = append(streams, fmt.Sprintf(`{"stream":{"service_name":%s,"level":%s,"env":"e2e"},"values":[[%s,%s,{"row":%s,"http_status":%s}]]}`,
			jsonString(svcName), jsonString(Level(i)),
			jsonString(strconv.FormatInt(p.RowTime(i, rows3).UnixNano(), 10)), jsonString(p.Msg(i)),
			jsonString(strconv.Itoa(i)), jsonString(strconv.Itoa(200+i))))
	}
	body := `{"streams":[` + strings.Join(streams, ",") + `]}`
	return []Request{{Method: "POST", Path: "/insert/loki/api/v1/push", Header: map[string]string{"Content-Type": ctJSON}, Body: []byte(body), Rows: rows3}}
}

// lokiPush marshals a Loki logproto.PushRequest (the wire format the upstream
// parser decodes) and snappy-compresses it as Loki clients do.
func lokiPush(p Params) []byte {
	var req []byte
	for i := 1; i <= rows3; i++ {
		labels := fmt.Sprintf(`{service_name=%q, level=%q, env="e2e"}`, svcName, Level(i))
		ts := p.RowTime(i, rows3)
		var tsMsg []byte
		tsMsg = protowire.AppendTag(tsMsg, 1, protowire.VarintType)
		tsMsg = protowire.AppendVarint(tsMsg, uint64(ts.Unix()))
		tsMsg = protowire.AppendTag(tsMsg, 2, protowire.VarintType)
		tsMsg = protowire.AppendVarint(tsMsg, uint64(ts.Nanosecond()))
		var meta []byte
		for _, a := range []kv{{"row", strconv.Itoa(i)}, {"http_status", strconv.Itoa(200 + i)}} {
			var pair []byte
			pair = protowire.AppendTag(pair, 1, protowire.BytesType)
			pair = protowire.AppendString(pair, a.k)
			pair = protowire.AppendTag(pair, 2, protowire.BytesType)
			pair = protowire.AppendString(pair, a.v)
			meta = protowire.AppendTag(meta, 3, protowire.BytesType)
			meta = protowire.AppendBytes(meta, pair)
		}
		var entry []byte
		entry = protowire.AppendTag(entry, 1, protowire.BytesType)
		entry = protowire.AppendBytes(entry, tsMsg)
		entry = protowire.AppendTag(entry, 2, protowire.BytesType)
		entry = protowire.AppendString(entry, p.Msg(i))
		entry = append(entry, meta...)

		var stream []byte
		stream = protowire.AppendTag(stream, 1, protowire.BytesType)
		stream = protowire.AppendString(stream, labels)
		stream = protowire.AppendTag(stream, 2, protowire.BytesType)
		stream = protowire.AppendBytes(stream, entry)

		req = protowire.AppendTag(req, 1, protowire.BytesType)
		req = protowire.AppendBytes(req, stream)
	}
	return snappy.Encode(nil, req)
}

func buildLokiProtobuf(p Params) []Request {
	return []Request{{Method: "POST", Path: "/insert/loki/api/v1/push", Header: map[string]string{"Content-Type": ctProto}, Body: lokiPush(p), Rows: rows3}}
}

func buildESBulk(p Params) []Request {
	var b bytes.Buffer
	for i := 1; i <= rows3; i++ {
		b.WriteString(`{"create":{}}` + "\n")
		fmt.Fprintf(&b, `{"@timestamp":%s,"message":%s`, jsonString(p.RowTime(i, rows3).UTC().Format(time.RFC3339Nano)), jsonString(p.Msg(i)))
		for _, a := range rowAttrs(p, i) {
			fmt.Fprintf(&b, `,%s:%s`, jsonString(a.k), jsonString(a.v))
		}
		b.WriteString("}\n")
	}
	return []Request{{Method: "POST", Path: "/insert/elasticsearch/_bulk",
		Query:  "_time_field=@timestamp&_msg_field=message&_stream_fields=service.name",
		Header: map[string]string{"Content-Type": "application/x-ndjson"}, Body: b.Bytes(), Rows: rows3}}
}

func buildSplunk(p Params) []Request {
	paths := []string{
		"/insert/splunk/services/collector/event", "/insert/splunk/services/collector/event/1.0",
		"/services/collector/event", "/services/collector/event/1.0",
	}
	var reqs []Request
	for i, path := range paths {
		row := i + 1
		fields := fmt.Sprintf(`{"level":%s,"row":%s,"env":"e2e"}`, jsonString(Level(row)), jsonString(strconv.Itoa(row)))
		body := fmt.Sprintf(`{"time":%d,"event":%s,"host":"ingest-host","source":"ingest-source","sourcetype":"ingest-type","fields":%s}`,
			p.RowTime(row, 4).Unix(), jsonString(p.Msg(row)), fields)
		reqs = append(reqs, Request{Method: "POST", Path: path, Header: map[string]string{"Content-Type": ctJSON}, Body: []byte(body), Rows: 1})
	}
	return reqs
}

func buildDatadog(p Params) []Request {
	paths := []string{"/insert/datadog/api/v2/logs", "/api/v2/logs", "/insert/datadog/api/v2/logs"}
	var reqs []Request
	for i := 1; i <= rows3; i++ {
		body := fmt.Sprintf(`[{"message":%s,"ddsource":"ingest-source","ddtags":"env:e2e,row:%d","hostname":"ingest-host","service":%s,"status":%s}]`,
			jsonString(p.Msg(i)), i, jsonString(svcName), jsonString(strings.ToLower(Level(i))))
		reqs = append(reqs, Request{Method: "POST", Path: paths[i-1],
			Header: map[string]string{"Content-Type": ctJSON, "dd-message-timestamp": strconv.FormatInt(p.RowTime(i, rows3).UnixMilli(), 10)},
			Body:   []byte(body), Rows: 1})
	}
	return reqs
}

func buildJournald(p Params) []Request {
	var b bytes.Buffer
	for i := 1; i <= rows3; i++ {
		fmt.Fprintf(&b, "__REALTIME_TIMESTAMP=%d\n", p.RowTime(i, rows3).UnixMicro())
		fmt.Fprintf(&b, "MESSAGE=%s\n", p.Msg(i))
		fmt.Fprintf(&b, "PRIORITY=%d\n", 3+i)
		b.WriteString("_HOSTNAME=ingest-host\n_SYSTEMD_UNIT=ingest-matrix.service\n_MACHINE_ID=0123456789abcdef0123456789abcdef\n")
		fmt.Fprintf(&b, "ROW=%d\n\n", i)
	}
	return []Request{{Method: "POST", Path: "/insert/journald/upload",
		Header: map[string]string{"Content-Type": "application/vnd.fdo.journal"}, Body: b.Bytes(), Rows: rows3}}
}

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func buildOTLPLogsProtobuf(p Params) []Request {
	var recs []*logspb.LogRecord
	for i := 1; i <= rows3; i++ {
		recs = append(recs, &logspb.LogRecord{
			TimeUnixNano:   uint64(p.RowTime(i, rows3).UnixNano()),
			SeverityNumber: logspb.SeverityNumber(9 + 4*(i-1)),
			SeverityText:   Level(i),
			Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: p.Msg(i)}},
			Attributes:     []*commonpb.KeyValue{strAttr("row", strconv.Itoa(i)), strAttr("env", "e2e")},
		})
	}
	req := &collogs.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource:  &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strAttr("service.name", svcName)}},
		ScopeLogs: []*logspb.ScopeLogs{{Scope: &commonpb.InstrumentationScope{Name: "ingest-matrix"}, LogRecords: recs}},
	}}}
	body, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}
	return []Request{{Method: "POST", Path: "/insert/opentelemetry/v1/logs", Header: map[string]string{"Content-Type": ctProto}, Body: body, Rows: rows3}}
}

func buildOTLPLogsJSON(p Params) []Request {
	var recs []string
	for i := 1; i <= rows3; i++ {
		recs = append(recs, fmt.Sprintf(`{"timeUnixNano":%s,"severityNumber":9,"severityText":%s,"body":{"stringValue":%s},"attributes":[{"key":"row","value":{"stringValue":%s}}]}`,
			jsonString(strconv.FormatInt(p.RowTime(i, rows3).UnixNano(), 10)), jsonString(Level(i)), jsonString(p.Msg(i)), jsonString(strconv.Itoa(i))))
	}
	body := `{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"` + svcName + `"}}]},"scopeLogs":[{"logRecords":[` + strings.Join(recs, ",") + `]}]}]}`
	return []Request{{Method: "POST", Path: "/insert/opentelemetry/v1/logs", Header: map[string]string{"Content-Type": ctJSON}, Body: []byte(body), Rows: rows3}}
}

// syslogLines builds one message per row. The marker sits at the start of the
// free-text message so the word filter finds it in either dialect.
func syslogLines(rfc5424 bool) func(Params) [][]byte {
	return func(p Params) [][]byte {
		var out [][]byte
		for i := 1; i <= rows3; i++ {
			ts := p.RowTime(i, rows3).UTC()
			pri := 8*1 + 3 + i // user facility, error..critical severities
			if rfc5424 {
				out = append(out, []byte(fmt.Sprintf(`<%d>1 %s ingest-host ingest-app 123%d ID47 [exampleSDID@32473 iut="3" eventSource="Application"] %s`,
					pri, ts.Format("2006-01-02T15:04:05.000Z"), i, p.Msg(i))))
			} else {
				out = append(out, []byte(fmt.Sprintf(`<%d>%s ingest-host ingest-app[123%d]: %s`,
					pri, ts.Format("Jan _2 15:04:05"), i, p.Msg(i))))
			}
		}
		return out
	}
}

func dropJSONKey(key string) func([]byte) []byte {
	return func(body []byte) []byte {
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			return body
		}
		delete(m, key)
		out, err := json.Marshal(m)
		if err != nil {
			return body
		}
		return out
	}
}
