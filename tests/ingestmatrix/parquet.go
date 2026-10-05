package ingestmatrix

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

// RawParquetRow retains physical leaf names and values, including MAP keys.
// This is a plain format oracle; it never invokes a Lakehouse row converter.
type RawParquetRow map[string][]string

func ReadRawParquet(data []byte) ([]RawParquetRow, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	paths := f.Schema().Columns()
	var out []RawParquetRow
	for _, rg := range f.RowGroups() {
		r := rg.Rows()
		buf := make([]parquet.Row, 256)
		for {
			n, readErr := r.ReadRows(buf)
			for _, row := range buf[:n] {
				values := RawParquetRow{}
				for _, v := range row {
					if v.IsNull() {
						continue
					}
					column := v.Column()
					if column < 0 || column >= len(paths) {
						_ = r.Close()
						return nil, fmt.Errorf("invalid column %d", column)
					}
					name := strings.Join(paths[column], ".")
					value := v.String()
					if v.Kind() == parquet.ByteArray {
						value = string(v.ByteArray())
					}
					values[name] = append(values[name], value)
				}
				out = append(out, values)
			}
			if readErr != nil {
				if readErr != io.EOF {
					_ = r.Close()
					return nil, readErr
				}
				break
			}
		}
		if err := r.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (r RawParquetRow) CheckHot(sig Signal, tenant Tenant, hot map[string]string) error {
	if err := r.CheckTenant(tenant); err != nil {
		return err
	}
	return r.checkFields(sig, hot)
}

// CheckTenant applies to every physical row, including trace-index rows.
func (r RawParquetRow) CheckTenant(tenant Tenant) error {
	for column, want := range map[string]string{"account_id": strconv.FormatUint(uint64(tenant.Account), 10), "project_id": strconv.FormatUint(uint64(tenant.Project), 10)} {
		if got := r[column]; len(got) != 1 || got[0] != want {
			return fmt.Errorf("raw tenant column %s=%v, want %s", column, got, want)
		}
	}
	return nil
}

func (r RawParquetRow) checkFields(sig Signal, hot map[string]string) error {
	if len(hot) == 0 {
		return fmt.Errorf("raw truth requires nonempty hot fields")
	}
	for field, want := range hot {
		if field == "_time" {
			stamp, err := time.Parse(time.RFC3339Nano, want)
			if err != nil {
				return err
			}
			want = strconv.FormatInt(stamp.UnixNano(), 10)
		}
		if !r.hasField(sig, field, want) {
			return fmt.Errorf("raw field %q lacks expected hot value %q (physical leaves %v)", field, want, r)
		}
	}
	for leaf, values := range r {
		if strings.Contains(leaf, ".attributes.") {
			if strings.HasSuffix(leaf, ".value") {
				keys, ok := r[strings.TrimSuffix(leaf, ".value")+".key"]
				if !ok || len(keys) != len(values) {
					return fmt.Errorf("unpaired raw map values %s", leaf)
				}
			}
			if !strings.HasSuffix(leaf, ".key") {
				continue
			}
			family := strings.SplitN(leaf, ".", 2)[0]
			mapValues := r[strings.TrimSuffix(leaf, ".key")+".value"]
			if len(values) != len(mapValues) {
				return fmt.Errorf("malformed raw map %s", leaf)
			}
			seen := map[string]bool{}
			for i, key := range values {
				if seen[key] {
					return fmt.Errorf("duplicate raw map key %s:%s", leaf, key)
				}
				seen[key] = true
				public := key
				if sig == Traces {
					public = family + "_attr:" + key
					// VT native span metadata remains bare; its physical MAP
					// value must agree with the corresponding hot field.
					if family == "span" && nativeTraceMetadata(key) {
						if _, ok := hot[key]; ok {
							public = key
						}
					}
				}
				if want, ok := hot[public]; !ok || want != mapValues[i] {
					return fmt.Errorf("unexpected raw map field %s=%q", public, mapValues[i])
				}
			}
			continue
		}
		if leaf == "account_id" || leaf == "project_id" {
			continue
		}
		public := rawColumnPublicName(sig, leaf)
		if leaf == "severity_text" {
			if _, ok := hot["severity_text"]; ok {
				public = "severity_text"
			}
		}
		for _, value := range values {
			if _, expected := hot[public]; value == "" && !expected {
				continue
			}
			if _, expected := hot[public]; !expected && value == "0" && (leaf == "severity_number" || leaf == "duration_ns" || leaf == "status.code" || leaf == "span.kind" || leaf == "start_time_unix_nano") {
				continue
			}
			want, ok := hot[public]
			if !ok {
				return fmt.Errorf("unexpected raw scalar field %s=%q", leaf, value)
			}
			if public == "_time" {
				stamp, err := time.Parse(time.RFC3339Nano, want)
				if err != nil {
					return err
				}
				want = strconv.FormatInt(stamp.UnixNano(), 10)
			}
			if len(values) != 1 || value != want {
				return fmt.Errorf("raw scalar %s=%v, want one value %q", leaf, values, want)
			}
		}
	}
	return nil
}

// Public names below are the documented open-format mappings. Deliberately do
// not import schema.Registry or use a query converter: a shared mapping bug
// must be caught by comparing the physical file to the hot upstream answer.
func (r RawParquetRow) hasField(sig Signal, field, want string) bool {
	column := field
	switch field {
	case "_time":
		column = "timestamp_unix_nano"
	case "_msg":
		column = "body"
	case "level", "severity_text":
		column = "severity_text"
	}
	if sig == Traces {
		switch field {
		case "name":
			column = "span.name"
		case "duration":
			column = "duration_ns"
		case "status_code":
			column = "status.code"
		case "status_message":
			column = "status.message"
		case "kind":
			column = "span.kind"
		case "scope_name":
			column = "scope.name"
		}
		if field == "end_time_unix_nano" {
			if len(r["start_time_unix_nano"]) == 1 && len(r["duration_ns"]) == 1 {
				a, e1 := strconv.ParseInt(r["start_time_unix_nano"][0], 10, 64)
				b, e2 := strconv.ParseInt(r["duration_ns"][0], 10, 64)
				if e1 == nil && e2 == nil && strconv.FormatInt(a+b, 10) == want {
					return true
				}
			}
		}
		for _, prefix := range []string{"resource_attr:", "span_attr:", "scope_attr:"} {
			if strings.HasPrefix(column, prefix) {
				candidate := strings.TrimPrefix(column, prefix)
				if rawColumnPublicName(sig, candidate) == field {
					column = candidate
				} else {
					column = ""
				}
				break
			}
		}
	}
	if values := r[column]; len(values) == 1 && values[0] == want {
		return true
	}
	for _, family := range []string{"resource", "log", "span", "scope"} {
		key := field
		if sig == Traces {
			prefix := family + "_attr:"
			if !strings.HasPrefix(field, prefix) && !(family == "span" && nativeTraceMetadata(field)) {
				continue
			}
			key = strings.TrimPrefix(field, prefix)
		}
		for leaf, keys := range r {
			if !strings.HasPrefix(leaf, family+".attributes.") || !strings.HasSuffix(leaf, ".key") {
				continue
			}
			values := r[strings.TrimSuffix(leaf, ".key")+".value"]
			for i, k := range keys {
				if k == key && i < len(values) && values[i] == want {
					return true
				}
			}
		}
	}
	return false
}

func rawColumnPublicName(sig Signal, column string) string {
	switch column {
	case "body":
		return "_msg"
	case "timestamp_unix_nano":
		return "_time"
	case "severity_text":
		return "level"
	}
	if sig != Traces {
		return column
	}
	switch column {
	case "span.name":
		return "name"
	case "duration_ns":
		return "duration"
	case "status.code":
		return "status_code"
	case "status.message":
		return "status_message"
	case "span.kind":
		return "kind"
	case "scope.name":
		return "scope_name"
	}
	// Promoted resource and span families are fixed by the documented schema;
	// arbitrary prefixed customer names cannot borrow an unrelated scalar.
	resources := strings.Fields("service.name deployment.environment cloud.region host.name k8s.namespace.name k8s.pod.name k8s.deployment.name k8s.node.name container.id service.instance.id k8s.cluster.name telemetry.sdk.name cloud.account.id")
	spans := strings.Fields("http.method http.status_code http.url db.system db.statement url.full client.address server.address network.peer.address db.collection.name db.operation.name rpc.method messaging.destination.name code.function.name exception.type db.query.text")
	for _, name := range resources {
		if name == column {
			return "resource_attr:" + column
		}
	}
	for _, name := range spans {
		if name == column {
			return "span_attr:" + column
		}
	}
	return column
}

// Names defined by VictoriaTraces protoparser/opentelemetry/pb/trace_fields.go.
// Keep this independent of Lakehouse naming helpers so a shared mapping bug
// cannot make the physical oracle agree with the query reader.
func nativeTraceMetadata(key string) bool {
	switch key {
	case "start_time_unix_nano", "end_time_unix_nano", "trace_state", "flags", "dropped_attributes_count", "dropped_events_count", "dropped_links_count", "scope_version":
		return true
	default:
		return false
	}
}
