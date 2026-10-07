package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// fvPropMap is one attribute MAP column of the generated schema: the Parquet
// column name and the prefix its attributes carry in a query (none in logs).
type fvPropMap struct{ col, prefix string }

var fvPropMaps = []fvPropMap{{"resource.attributes", ""}, {"log.attributes", ""}}

// fvPropRow is one generated row: the scalars and, per map column, its map
// (nil = null map, empty = empty map).
type fvPropRow struct {
	ts     int64
	level  string
	svc    string
	maps   map[string]map[string]string
	hasMap map[string]bool
}

// fvPropFile builds a Parquet file whose columns are in the given order, so a
// MAP column can sit anywhere among the scalars; rows are written in groups so
// the file has several row groups.
func fvPropFile(t *testing.T, order []string, rows []fvPropRow, groupSize int) []byte {
	t.Helper()
	var fields []reflect.StructField
	for i, name := range order {
		var typ reflect.Type
		tag := name
		switch name {
		case "timestamp_unix_nano":
			typ = reflect.TypeOf(int64(0))
		case "severity_text", "service.name":
			typ = reflect.TypeOf("")
		default:
			typ = reflect.TypeOf(map[string]string{})
			tag += ",optional"
		}
		fields = append(fields, reflect.StructField{
			Name: fmt.Sprintf("F%d", i), Type: typ, Tag: reflect.StructTag(`parquet:"` + tag + `"`),
		})
	}
	st := reflect.StructOf(fields)
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, parquet.SchemaOf(reflect.New(st).Interface()))
	for n, r := range rows {
		v := reflect.New(st).Elem()
		for i, name := range order {
			f := v.Field(i)
			switch name {
			case "timestamp_unix_nano":
				f.SetInt(r.ts)
			case "severity_text":
				f.SetString(r.level)
			case "service.name":
				f.SetString(r.svc)
			default:
				if r.hasMap[name] {
					f.Set(reflect.ValueOf(r.maps[name]))
				}
			}
		}
		if err := w.Write(v.Addr().Interface()); err != nil {
			t.Fatal(err)
		}
		if groupSize > 0 && (n+1)%groupSize == 0 {
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// fvPropName is the field name a query uses for key k of map column m.
func fvPropName(m fvPropMap, k string) string { return m.prefix + k }

// TestFieldValues_MapAttributes_Property generates files with random column
// order (MAP columns anywhere among the scalars, several maps, empty and null
// maps, rows lacking a key, keys missing from whole row groups, several files
// with different layouts) and asserts that field_values of every attribute —
// unfiltered, filtered on a scalar and filtered on another attribute — equals
// the answer computed from the generated rows.
func TestFieldValues_MapAttributes_Property(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			mock := newMockS3Server()
			t.Cleanup(mock.close)
			s := testStorageWithS3(t, mock.url())
			s.cfg.Mode = fvPropMode()
			base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)

			var all []fvPropRow
			for fileNo := 0; fileNo < 3; fileNo++ {
				cols := []string{"timestamp_unix_nano", "severity_text", "service.name"}
				for _, m := range fvPropMaps {
					cols = append(cols, m.col)
				}
				rng.Shuffle(len(cols), func(i, j int) { cols[i], cols[j] = cols[j], cols[i] })
				var rows []fvPropRow
				for i := 0; i < 9+rng.Intn(10); i++ {
					r := fvPropRow{
						ts:     base.Add(time.Duration(len(all)+len(rows)) * time.Second).UnixNano(),
						level:  []string{"INFO", "WARN", "ERROR"}[rng.Intn(3)],
						svc:    []string{"a", "b"}[rng.Intn(2)],
						maps:   map[string]map[string]string{},
						hasMap: map[string]bool{},
					}
					for mi, m := range fvPropMaps {
						switch rng.Intn(4) {
						case 0: // null map
						case 1:
							r.hasMap[m.col], r.maps[m.col] = true, map[string]string{}
						default:
							r.hasMap[m.col] = true
							mm := map[string]string{}
							// Key names are distinct across maps, plus one key per
							// map every file shares and one only some files carry.
							for ki := 0; ki < 3; ki++ {
								if rng.Intn(3) == 0 {
									continue
								}
								if ki == 2 && fileNo != 1 {
									continue
								}
								mm[fmt.Sprintf("m%dk%d", mi, ki)] = fmt.Sprintf("v%d", rng.Intn(4))
							}
							r.maps[m.col] = mm
						}
					}
					rows = append(rows, r)
				}
				data := fvPropFile(t, cols, rows, 5)
				key := fmt.Sprintf("%s/dt=%s/hour=%02d/prop%d.parquet", fvPropPrefix(), base.Format("2006-01-02"), base.Hour(), fileNo)
				fi := registerFileInMockS3(t, s, mock, key, data, base)
				_ = fi
				all = append(all, rows...)
			}
			// Widen the registered bounds to the generated rows.
			start, end := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()

			type filt struct {
				query string
				keep  func(fvPropRow) bool
			}
			filters := []filt{
				{"*", func(fvPropRow) bool { return true }},
				{`level:=ERROR`, func(r fvPropRow) bool { return r.level == "ERROR" }},
			}
			if f := fvPropFilterQuery(); f != "" {
				filters[1] = filt{f, func(r fvPropRow) bool { return r.level == "ERROR" }}
			}
			// A filter on another map attribute.
			m0 := fvPropMaps[0]
			filters = append(filters, filt{
				fmt.Sprintf("`%s`:=v1", fvPropName(m0, "m0k0")),
				func(r fvPropRow) bool { return r.maps[m0.col]["m0k0"] == "v1" },
			})

			for _, f := range filters {
				for _, m := range fvPropMaps {
					for ki := 0; ki < 3; ki++ {
						k := fmt.Sprintf("m%dk%d", indexOfMap(m), ki)
						want := map[string]uint64{}
						for _, r := range all {
							if f.keep(r) {
								want[r.maps[m.col][k]]++
							}
						}
						q := mustParseQueryWithTime(t, f.query, start, end)
						got, err := s.GetFieldValues(context.Background(), nil, q, fvPropName(m, k), 0)
						if err != nil {
							t.Fatalf("field_values(%s) q=%s: %v", k, f.query, err)
						}
						gm := map[string]uint64{}
						for _, v := range got {
							gm[v.Value] = v.Hits
						}
						if !reflect.DeepEqual(gm, want) && (len(gm) != 0 || len(want) != 0) {
							t.Errorf("field_values(%s) q=%s = %v, want %v", fvPropName(m, k), f.query, fvSortedKV(gm), fvSortedKV(want))
						}
					}
				}
				// Scalar target under the same filter (a scalar placed after MAP columns).
				want := map[string]uint64{}
				for _, r := range all {
					if f.keep(r) {
						want[r.level]++
					}
				}
				q := mustParseQueryWithTime(t, f.query, start, end)
				got, err := s.GetFieldValues(context.Background(), nil, q, "level", 0)
				if err != nil {
					t.Fatal(err)
				}
				gm := map[string]uint64{}
				for _, v := range got {
					gm[v.Value] = v.Hits
				}
				if !reflect.DeepEqual(gm, want) && (len(gm) != 0 || len(want) != 0) {
					t.Errorf("field_values(level) q=%s = %v, want %v", f.query, fvSortedKV(gm), fvSortedKV(want))
				}
			}
		})
	}
}

func indexOfMap(m fvPropMap) int {
	for i, x := range fvPropMaps {
		if x == m {
			return i
		}
	}
	return -1
}

func fvSortedKV(m map[string]uint64) string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	out := ""
	for _, k := range ks {
		out += fmt.Sprintf("%s=%d ", k, m[k])
	}
	return "[" + out + "]"
}

func fvPropMode() config.Mode { return config.ModeLogs }
func fvPropPrefix() string    { return "logs" }
func fvPropFilterQuery() string {
	return ""
}
