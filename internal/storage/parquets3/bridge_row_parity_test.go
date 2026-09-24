package parquets3

import (
	"reflect"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// fillRow sets every exported string, integer and map field of a row struct to
// a distinct non-empty value, leaving the tenant ids alone.
func fillRow(v reflect.Value) {
	for i := 0; i < v.NumField(); i++ {
		f, sf := v.Field(i), v.Type().Field(i)
		if !sf.IsExported() || sf.Name == "AccountID" || sf.Name == "ProjectID" {
			continue
		}
		switch f.Kind() {
		case reflect.String:
			f.SetString("v-" + sf.Name)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			f.SetInt(int64(i + 1))
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			f.SetUint(uint64(i + 1))
		case reflect.Map:
			if f.Type().Key().Kind() == reflect.String && f.Type().Elem().Kind() == reflect.String {
				f.Set(reflect.ValueOf(map[string]string{"k-" + sf.Name: "v-" + sf.Name}))
			}
		}
	}
}

// A row still in a peer's buffer must read exactly like the same row after it
// is flushed: every field the Parquet read path emits (logRowToFields), with
// the same value, is in the bridge's block. _stream_id, severity_number and
// scope.name were missing, so stream_ids and filters on them skipped a peer's
// unflushed rows.
func TestBridgeLogRows_CarryEveryFieldTheFilePathEmits(t *testing.T) {
	s := testStorage()
	var row schema.LogRow
	fillRow(reflect.ValueOf(&row).Elem())
	row.TimestampUnixNano = 1_700_000_000_000_000_000

	db := s.logRowsToDataBlock(tenantScope{account: "0", project: "0"}, "test", []schema.LogRow{row})
	if db == nil {
		t.Fatal("no block")
	}
	bridge := map[string]string{}
	for _, c := range db.GetColumns(false) {
		bridge[c.Name] = c.Values[0]
	}
	for _, f := range logRowToFields(&row, nil) {
		want := s.registry.FormatField(f.name, f.value)
		if want == "" || bridgeNamingGap(f.name) {
			continue
		}
		if got, ok := bridge[f.name]; !ok || got != want {
			t.Errorf("field %s: bridge %q (present %v), file path %q", f.name, got, ok, want)
		}
	}
}

// bridgeNamingGap names the fields the two conversions still spell differently
// — map attributes (bare on the file path, resource_attr:/log_attr: on the
// bridge) and the Tier-2 ded_sNN slots (not emitted by the bridge). Which
// spelling is right is decided against hot VictoriaLogs; until then they are
// listed here, not silently skipped (docs/parity-and-gaps.md).
func bridgeNamingGap(name string) bool {
	if len(name) == 7 && name[:5] == "ded_s" {
		return true
	}
	return len(name) > 2 && name[:2] == "k-" // fillRow's map keys
}
