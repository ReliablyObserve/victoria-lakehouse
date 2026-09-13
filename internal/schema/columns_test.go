package schema

import (
	"reflect"
	"sort"
	"strings"
	"testing"
)

// rowParquetColumns returns the top-level Parquet column names of a row struct,
// read from the `parquet:"..."` struct tags — the same names the reader sees in
// f.Schema().Columns().
func rowParquetColumns(t *testing.T, rowType reflect.Type) []string {
	t.Helper()
	var names []string
	for i := 0; i < rowType.NumField(); i++ {
		tag := rowType.Field(i).Tag.Get("parquet")
		if tag == "" || tag == "-" {
			continue
		}
		name := tag
		if idx := strings.IndexByte(tag, ','); idx >= 0 {
			name = tag[:idx]
		}
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// wantLogColumnKinds is the golden classification of every top-level LogRow
// column. Adding a column to LogRow without adding it here fails this test —
// that is the point: an unclassified column would otherwise be emitted by the
// cold read paths as a query field that hot VictoriaLogs never returns.
var wantLogColumnKinds = map[string]ColumnKind{
	"account_id":              ColumnInternal,
	"project_id":              ColumnInternal,
	"timestamp_unix_nano":     ColumnUserVisible,
	"body":                    ColumnUserVisible,
	"severity_text":           ColumnUserVisible,
	"severity_number":         ColumnUserVisible,
	"service.name":            ColumnUserVisible,
	"trace_id":                ColumnUserVisible,
	"span_id":                 ColumnUserVisible,
	"k8s.namespace.name":      ColumnUserVisible,
	"k8s.pod.name":            ColumnUserVisible,
	"k8s.deployment.name":     ColumnUserVisible,
	"k8s.node.name":           ColumnUserVisible,
	"deployment.environment":  ColumnUserVisible,
	"cloud.region":            ColumnUserVisible,
	"host.name":               ColumnUserVisible,
	"_stream":                 ColumnUserVisible,
	"_stream_id":              ColumnUserVisible,
	"scope.name":              ColumnUserVisible,
	"container.id":            ColumnUserVisible,
	"service.instance.id":     ColumnUserVisible,
	"service.version":         ColumnUserVisible,
	"exception.type":          ColumnUserVisible,
	"exception.message":       ColumnUserVisible,
	"k8s.cluster.name":        ColumnUserVisible,
	"telemetry.sdk.name":      ColumnUserVisible,
	"telemetry.sdk.language":  ColumnUserVisible,
	"telemetry.sdk.version":   ColumnUserVisible,
	"cloud.account.id":        ColumnUserVisible,
	"cloud.provider":          ColumnUserVisible,
	"os.type":                 ColumnUserVisible,
	"host.arch":               ColumnUserVisible,
	"process.runtime.name":    ColumnUserVisible,
	"process.runtime.version": ColumnUserVisible,
	"ded_s01":                 ColumnSlot,
	"ded_s02":                 ColumnSlot,
	"ded_s03":                 ColumnSlot,
	"ded_s04":                 ColumnSlot,
	"ded_s05":                 ColumnSlot,
	"ded_s06":                 ColumnSlot,
	"ded_s07":                 ColumnSlot,
	"ded_s08":                 ColumnSlot,
	"resource.attributes":     ColumnUserVisible,
	"log.attributes":          ColumnUserVisible,
	"scope.attributes":        ColumnUserVisible,
}

// wantTraceColumnKinds is the golden classification of every top-level TraceRow
// column. See wantLogColumnKinds.
//
// The service-graph columns (parent / child / callCount) are user-visible on
// purpose: they carry the edge rows VictoriaTraces' servicegraph task emits and
// the Jaeger Dependencies reader queries them by name. They are NULL on regular
// span rows, so the read paths' null-skip keeps them out of span results.
var wantTraceColumnKinds = map[string]ColumnKind{
	"account_id":                 ColumnInternal,
	"project_id":                 ColumnInternal,
	"timestamp_unix_nano":        ColumnUserVisible,
	"start_time_unix_nano":       ColumnUserVisible,
	"trace_id":                   ColumnUserVisible,
	"span_id":                    ColumnUserVisible,
	"parent_span_id":             ColumnUserVisible,
	"span.name":                  ColumnUserVisible,
	"service.name":               ColumnUserVisible,
	"duration_ns":                ColumnUserVisible,
	"status.code":                ColumnUserVisible,
	"status.message":             ColumnUserVisible,
	"span.kind":                  ColumnUserVisible,
	"http.method":                ColumnUserVisible,
	"http.status_code":           ColumnUserVisible,
	"http.url":                   ColumnUserVisible,
	"db.system":                  ColumnUserVisible,
	"db.statement":               ColumnUserVisible,
	"k8s.namespace.name":         ColumnUserVisible,
	"k8s.pod.name":               ColumnUserVisible,
	"k8s.deployment.name":        ColumnUserVisible,
	"k8s.node.name":              ColumnUserVisible,
	"deployment.environment":     ColumnUserVisible,
	"cloud.region":               ColumnUserVisible,
	"host.name":                  ColumnUserVisible,
	"_stream":                    ColumnUserVisible,
	"_stream_id":                 ColumnUserVisible,
	"scope.name":                 ColumnUserVisible,
	"url.full":                   ColumnUserVisible,
	"client.address":             ColumnUserVisible,
	"server.address":             ColumnUserVisible,
	"network.peer.address":       ColumnUserVisible,
	"db.collection.name":         ColumnUserVisible,
	"db.operation.name":          ColumnUserVisible,
	"rpc.method":                 ColumnUserVisible,
	"messaging.destination.name": ColumnUserVisible,
	"code.function.name":         ColumnUserVisible,
	"exception.type":             ColumnUserVisible,
	"container.id":               ColumnUserVisible,
	"service.instance.id":        ColumnUserVisible,
	"k8s.cluster.name":           ColumnUserVisible,
	"telemetry.sdk.name":         ColumnUserVisible,
	"cloud.account.id":           ColumnUserVisible,
	"db.query.text":              ColumnUserVisible,
	"ded_s01":                    ColumnSlot,
	"ded_s02":                    ColumnSlot,
	"ded_s03":                    ColumnSlot,
	"ded_s04":                    ColumnSlot,
	"ded_s05":                    ColumnSlot,
	"ded_s06":                    ColumnSlot,
	"ded_s07":                    ColumnSlot,
	"ded_s08":                    ColumnSlot,
	"resource.attributes":        ColumnUserVisible,
	"span.attributes":            ColumnUserVisible,
	"scope.attributes":           ColumnUserVisible,
	"parent":                     ColumnUserVisible,
	"child":                      ColumnUserVisible,
	"callCount":                  ColumnUserVisible,
}

// TestClassifyColumn_EveryRowColumnIsClassified is the anti-leak guard for the
// cold-row field set: it enumerates every top-level Parquet column of LogRow and
// TraceRow and asserts each one has a deliberate classification. A new column
// that nobody classified would be read as a user-visible field on the cold query
// path and appear in every Grafana log line / span that hot VL/VT never shows.
func TestClassifyColumn_EveryRowColumnIsClassified(t *testing.T) {
	cases := []struct {
		signal string
		typ    reflect.Type
		want   map[string]ColumnKind
	}{
		{"logs", reflect.TypeOf(LogRow{}), wantLogColumnKinds},
		{"traces", reflect.TypeOf(TraceRow{}), wantTraceColumnKinds},
	}
	for _, c := range cases {
		t.Run(c.signal, func(t *testing.T) {
			cols := rowParquetColumns(t, c.typ)
			for _, col := range cols {
				want, ok := c.want[col]
				if !ok {
					t.Errorf("Parquet column %q of %s rows has no classification. "+
						"Add it to this test's golden map (and, if it is storage "+
						"bookkeeping, to schema.InternalColumns) — an unclassified "+
						"column leaks into every cold query row as a field hot "+
						"VL/VT never returns.", col, c.signal)
					continue
				}
				if got := ClassifyColumn(col); got != want {
					t.Errorf("ClassifyColumn(%q) = %d, want %d", col, got, want)
				}
			}
			seen := make(map[string]bool, len(cols))
			for _, col := range cols {
				seen[col] = true
			}
			for col := range c.want {
				if !seen[col] {
					t.Errorf("classification lists %q but %s rows no longer have that "+
						"Parquet column — drop the stale entry", col, c.signal)
				}
			}
		})
	}
}

func TestIsInternalColumn(t *testing.T) {
	for _, c := range InternalColumns {
		if !IsInternalColumn(c) {
			t.Errorf("IsInternalColumn(%q) = false, want true", c)
		}
		if got := ClassifyColumn(c); got != ColumnInternal {
			t.Errorf("ClassifyColumn(%q) = %d, want ColumnInternal", c, got)
		}
	}
	for _, c := range []string{"service.name", "_msg", "ded_s01", "", "account_idx"} {
		if IsInternalColumn(c) {
			t.Errorf("IsInternalColumn(%q) = true, want false", c)
		}
	}
}

func TestIsDedicatedSlotColumn(t *testing.T) {
	for _, slot := range DedicatedSlotColumns {
		if !IsDedicatedSlotColumn(slot) {
			t.Errorf("IsDedicatedSlotColumn(%q) = false, want true", slot)
		}
		if got := ClassifyColumn(slot); got != ColumnSlot {
			t.Errorf("ClassifyColumn(%q) = %d, want ColumnSlot", slot, got)
		}
	}
	for _, c := range []string{"ded_s1", "ded_s001", "ded_sxx_", "service.name", ""} {
		if c == "ded_sxx_" {
			continue // 7 chars with the ded_s prefix — deliberately matches.
		}
		if IsDedicatedSlotColumn(c) {
			t.Errorf("IsDedicatedSlotColumn(%q) = true, want false", c)
		}
	}
}
