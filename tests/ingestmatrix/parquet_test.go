package ingestmatrix

import (
	"bytes"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// These fixtures use the open format directly, without the Lakehouse writer or
// query reader. Each mutation leaves marker/count intact.
func TestRawParquetTruthRejectsCorruption(t *testing.T) {
	for _, sig := range []Signal{Logs, Traces} {
		t.Run(string(sig), func(t *testing.T) {
			type row struct {
				Account    uint32            `parquet:"account_id"`
				Project    uint32            `parquet:"project_id"`
				Time       int64             `parquet:"timestamp_unix_nano"`
				Body       string            `parquet:"body"`
				Service    string            `parquet:"service.name"`
				Attributes map[string]string `parquet:"resource.attributes"`
			}
			stamp := time.Date(2026, 10, 5, 0, 0, 0, 123456789, time.UTC)
			hot := map[string]string{"_time": stamp.Format(time.RFC3339Nano), "_msg": "marker", "service.name": "correct", "custom": "spilled"}
			if sig == Traces {
				hot["resource_attr:service.name"] = hot["service.name"]
				delete(hot, "service.name")
				hot["resource_attr:custom"] = hot["custom"]
				delete(hot, "custom")
			}
			original := row{4401, 1, stamp.UnixNano(), "marker", "correct", map[string]string{"custom": "spilled"}}
			for _, mutation := range []string{"none", "account", "project", "time", "body", "promoted", "spilled", "field_name"} {
				t.Run(mutation, func(t *testing.T) {
					r := original
					r.Attributes = map[string]string{"custom": "spilled"}
					switch mutation {
					case "account":
						r.Account++
					case "project":
						r.Project++
					case "time":
						r.Time++
					case "body":
						r.Body = "wrong"
					case "promoted":
						r.Service = "wrong"
					case "spilled":
						r.Attributes["custom"] = "wrong"
					case "field_name":
						delete(r.Attributes, "custom")
						r.Attributes["wrong_custom"] = "spilled"
					}
					var b bytes.Buffer
					if err := parquet.Write(&b, []row{r}); err != nil {
						t.Fatal(err)
					}
					rows, err := ReadRawParquet(b.Bytes())
					if err != nil {
						t.Fatal(err)
					}
					if len(rows) != 1 {
						t.Fatalf("rows %d", len(rows))
					}
					err = rows[0].CheckHot(sig, NumericTenant, hot)
					if mutation == "none" && err != nil {
						t.Fatal(err)
					}
					if mutation != "none" && err == nil {
						t.Fatal("corrupt raw fields accepted")
					}
				})
			}
		})
	}
}

func TestRawTruthRetainsAttributeProvenanceAndExactKeys(t *testing.T) {
	base := RawParquetRow{"account_id": {"4401"}, "project_id": {"1"}, "body": {"native"}}
	for _, sig := range []Signal{Logs, Traces} {
		hot := map[string]string{"_msg": "native"}
		for _, variant := range []string{"extra_scalar", "extra_map", "wrong_family"} {
			t.Run(string(sig)+"/"+variant, func(t *testing.T) {
				r := RawParquetRow{}
				for k, v := range base {
					r[k] = v
				}
				expected := map[string]string{}
				for k, v := range hot {
					expected[k] = v
				}
				switch variant {
				case "extra_scalar":
					r["invented"] = []string{"extra"}
				case "extra_map":
					r["resource.attributes.key_value.key"] = []string{"invented"}
					r["resource.attributes.key_value.value"] = []string{"extra"}
				case "wrong_family":
					if sig == Logs {
						return
					}
					r["review"] = []string{"x"}
					expected["span_attr:review"] = "x"
				}
				if err := r.CheckHot(sig, NumericTenant, expected); err == nil {
					t.Fatal("unexpected fields/provenance accepted")
				}
			})
		}
	}
}

func TestRawTruthRejectsMalformedMaps(t *testing.T) {
	for _, sig := range []Signal{Logs, Traces} {
		for _, duplicate := range []bool{false, true} {
			r := RawParquetRow{"account_id": {"4401"}, "project_id": {"1"}, "body": {"native"}, "resource.attributes.key_value.value": {"x"}}
			hot := map[string]string{"_msg": "native"}
			if duplicate {
				r["resource.attributes.key_value.key"] = []string{"review", "review"}
				r["resource.attributes.key_value.value"] = []string{"x", "x"}
				key := "review"
				if sig == Traces {
					key = "resource_attr:review"
				}
				hot[key] = "x"
			}
			if err := r.CheckHot(sig, NumericTenant, hot); err == nil {
				t.Fatalf("%s malformed map accepted (duplicate=%t)", sig, duplicate)
			}
		}
	}
}

func TestRawTruthChecksEveryPromotedRepresentation(t *testing.T) {
	for _, sig := range []Signal{Logs, Traces} {
		key := "service.name"
		if sig == Traces {
			key = "resource_attr:service.name"
		}
		hot := map[string]string{"_msg": "native", key: "correct"}
		for _, scalar := range [][]string{{"correct"}, {"wrong"}, {"correct", "correct"}} {
			r := RawParquetRow{"account_id": {"4401"}, "project_id": {"1"}, "body": {"native"}, "service.name": scalar, "resource.attributes.key_value.key": {"service.name"}, "resource.attributes.key_value.value": {"correct"}}
			err := r.CheckHot(sig, NumericTenant, hot)
			valid := len(scalar) == 1 && scalar[0] == "correct"
			if (err == nil) != valid {
				t.Fatalf("%s scalar %v: %v", sig, scalar, err)
			}
		}
	}
}
func TestRawTruthChecksNativeSpanMetadataCopies(t *testing.T) {
	for _, key := range []string{"start_time_unix_nano", "end_time_unix_nano", "flags", "dropped_attributes_count", "dropped_events_count", "dropped_links_count", "trace_state", "scope_version"} {
		hot := map[string]string{"_msg": "native", key: "123"}
		r := RawParquetRow{"account_id": {"4401"}, "project_id": {"1"}, "body": {"native"}, "span.attributes.key_value.key": {key}, "span.attributes.key_value.value": {"123"}}
		if err := r.CheckHot(Traces, NumericTenant, hot); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		r["span.attributes.key_value.value"] = []string{"wrong"}
		if err := r.CheckHot(Traces, NumericTenant, hot); err == nil {
			t.Fatalf("%s conflicting metadata accepted", key)
		}
	}
}
