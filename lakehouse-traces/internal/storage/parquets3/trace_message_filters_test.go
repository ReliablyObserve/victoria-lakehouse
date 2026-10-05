package parquets3

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestTraceMessageFiltersAndRestart(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	bw.catalogObserver = &catalogObserver{store: s.catalog, pool: s.pool}
	uploadTraceRows(t, bw, []schema.TraceRow{{TimestampUnixNano: base.UnixNano(), TraceID: "trace", SpanID: "span", Body: "needlebodyspecial", SpanName: "nameonlytoken", SpanAttributes: map[string]string{"_msg": "customertoken"}}})
	run := coldSelectRunner(t, s, base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano())
	if rows := run("*"); len(rows) != 1 || rows[0]["_msg"] != "needlebodyspecial" || rows[0]["span_attr:_msg"] != "customertoken" {
		t.Fatalf("native/customer fixture=%v", rows)
	}
	if rows := run(`_msg:needlebodyspecial OR trace_id:=missing`); len(rows) != 1 {
		t.Fatalf("OR control bypassing bloom=%v", rows)
	}
	path := filepath.Join(t.TempDir(), "manifest.bin")
	if err := s.manifest.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	fresh := testStorageWithS3(t, mock.url())
	fresh.cfg.Mode = config.ModeTraces
	fresh.registry = schema.NewRegistry(schema.TracesProfile)
	if err := fresh.manifest.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	run = coldSelectRunner(t, fresh, base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano())
	for _, q := range []string{`_msg:needlebodyspecial`, `_msg:=needlebodyspecial`, `"span_attr:_msg":=customertoken`} {
		t.Run(q, func(t *testing.T) {
			if rows := run(q); len(rows) != 1 {
				t.Fatalf("message filter %q dropped preserved row: %v", q, rows)
			}
		})
	}
	for _, q := range []string{`_msg:nameonlytoken`, `_msg:customertoken`, `_msg:=missing`} {
		if rows := run(q); len(rows) != 0 {
			t.Fatalf("message false positive %q: %v", q, rows)
		}
	}
}

func TestTraceMessageTypedBridgeReservedAttributes(t *testing.T) {
	s := &Storage{cfg: testConfig(), registry: schema.NewRegistry(schema.TracesProfile)}
	row := schema.TraceRow{TimestampUnixNano: 123, Body: "native", ResourceAttributes: map[string]string{"_msg": "resource", "body": "resourcebody"}, SpanAttributes: map[string]string{"_msg": "span", "body": "spanbody"}, ScopeAttributes: map[string]string{"_msg": "scope", "body": "scopebody"}}
	db := typedRowsToDataBlock(s, []schema.TraceRow{row}, 0, 1000, traceRowToFields)
	got := map[string]string{}
	for _, c := range db.GetColumns(false) {
		if len(c.Values) > 0 {
			got[c.Name] = c.Values[0]
		}
	}
	for name, want := range map[string]string{"_msg": "native", "resource_attr:_msg": "resource", "span_attr:_msg": "span", "scope_attr:_msg": "scope", "resource_attr:body": "resourcebody", "span_attr:body": "spanbody", "scope_attr:body": "scopebody"} {
		if got[name] != want {
			t.Fatalf("typed message provenance %s=%q want=%q: %v", name, got[name], want, got)
		}
	}
}

func TestTraceMessagePeerBridgeReservedAttributes(t *testing.T) {
	s := &Storage{cfg: testConfig(), registry: schema.NewRegistry(schema.TracesProfile)}
	row := schema.TraceRow{TimestampUnixNano: 123, Body: "native", ResourceAttributes: map[string]string{"_msg": "resource", "body": "resourcebody"}, SpanAttributes: map[string]string{"_msg": "span", "body": "spanbody"}, ScopeAttributes: map[string]string{"_msg": "scope", "body": "scopebody"}}
	db := s.traceRowsToDataBlock(resolveTenantScope(nil), "", []schema.TraceRow{row})
	got := map[string]string{}
	for _, c := range db.GetColumns(false) {
		if len(c.Values) > 0 {
			got[c.Name] = c.Values[0]
		}
	}
	for name, want := range map[string]string{"_msg": "native", "resource_attr:_msg": "resource", "span_attr:_msg": "span", "scope_attr:_msg": "scope", "resource_attr:body": "resourcebody", "span_attr:body": "spanbody", "scope_attr:body": "scopebody"} {
		if got[name] != want {
			t.Fatalf("typed message provenance %s=%q want=%q: %v", name, got[name], want, got)
		}
	}
}

func TestTraceMessageLiteralReservedNamesAcrossReaders(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	attrs := map[string]string{}
	for _, key := range []string{"_msg", "body", "span_attr:_msg", "resource_attr:_msg", "scope_attr:_msg", "span_attr:span_attr:_msg", "span_attr:body"} {
		attrs[key] = key + " value"
	}
	row := schema.TraceRow{TimestampUnixNano: base.UnixNano(), Body: "native", SpanAttributes: attrs, ResourceAttributes: attrs, ScopeAttributes: attrs}
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	uploadTraceRows(t, bw, []schema.TraceRow{row})
	run := coldSelectRunner(t, s, base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano())
	for _, text := range []string{"*", "* | fields _msg, span_attr:*, resource_attr:*, scope_attr:*"} {
		got := run(text)
		if len(got) != 1 || got[0]["_msg"] != "native" {
			t.Fatalf("native body lost: %v", got)
		}
		for _, prefix := range []string{"resource_attr:", "span_attr:", "scope_attr:"} {
			for key, val := range attrs {
				if got[0][prefix+key] != val {
					t.Fatalf("%s query=%q field=%s got=%v", prefix, text, key, got)
				}
			}
		}
	}
	db := typedRowsToDataBlock(s, []schema.TraceRow{row}, base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano(), traceRowToFields)
	got := map[string]string{}
	for _, c := range db.GetColumns(false) {
		if len(c.Values) > 0 {
			got[c.Name] = c.Values[0]
		}
	}
	for _, prefix := range []string{"resource_attr:", "span_attr:", "scope_attr:"} {
		for key, val := range attrs {
			if got[prefix+key] != val {
				t.Fatalf("typed field=%s got=%v", prefix+key, got)
			}
		}
	}
}

func TestTraceMessageLiteralReservedNamesPeer(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	attrs := map[string]string{}
	for _, key := range []string{"_msg", "body", "span_attr:_msg", "resource_attr:_msg", "scope_attr:_msg", "span_attr:span_attr:_msg", "span_attr:body"} {
		attrs[key] = key + " value"
	}
	row := schema.TraceRow{TimestampUnixNano: base.UnixNano(), Body: "native", SpanAttributes: attrs, ResourceAttributes: attrs, ScopeAttributes: attrs}
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	uploadTraceRows(t, bw, []schema.TraceRow{row})
	run := coldSelectRunner(t, s, base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano())
	for _, text := range []string{"*", "* | fields _msg, span_attr:*, resource_attr:*, scope_attr:*"} {
		got := run(text)
		if len(got) != 1 || got[0]["_msg"] != "native" {
			t.Fatalf("native body lost: %v", got)
		}
		for _, prefix := range []string{"resource_attr:", "span_attr:", "scope_attr:"} {
			for key, val := range attrs {
				if got[0][prefix+key] != val {
					t.Fatalf("%s query=%q field=%s got=%v", prefix, text, key, got)
				}
			}
		}
	}
	db := s.traceRowsToDataBlock(resolveTenantScope(nil), "", []schema.TraceRow{row})
	got := map[string]string{}
	for _, c := range db.GetColumns(false) {
		if len(c.Values) > 0 {
			got[c.Name] = c.Values[0]
		}
	}
	for _, prefix := range []string{"resource_attr:", "span_attr:", "scope_attr:"} {
		for key, val := range attrs {
			if got[prefix+key] != val {
				t.Fatalf("typed field=%s got=%v", prefix+key, got)
			}
		}
	}
}
