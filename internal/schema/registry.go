package schema

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type FieldOrigin int

const (
	OriginPromoted     FieldOrigin = iota // top-level Parquet column
	OriginResourceMap                     // inside resource.attributes MAP
	OriginLogAttrMap                      // inside log.attributes MAP
	OriginSpanAttrMap                     // inside span.attributes MAP
	OriginScopeAttrMap                    // inside scope.attributes MAP
)

type FieldType int

const (
	TypeString        FieldType = iota // default: passthrough string
	TypeTimestampNano                  // int64 nanoseconds → RFC3339Nano
	TypeInt32                          // int32/int → decimal string
	TypeInt64                          // int64 → decimal string
	TypeFloat64                        // float64 → %g string
	TypeBool                           // bool → "true"/"false"
)

func (ft FieldType) FormatValue(v any) string {
	switch ft {
	case TypeTimestampNano:
		if n, ok := v.(int64); ok {
			return time.Unix(0, n).UTC().Format(time.RFC3339Nano)
		}
	case TypeInt32:
		switch n := v.(type) {
		case int32:
			return strconv.FormatInt(int64(n), 10)
		case int64:
			return strconv.FormatInt(n, 10)
		case int:
			return strconv.Itoa(n)
		}
	case TypeInt64:
		switch n := v.(type) {
		case int64:
			return strconv.FormatInt(n, 10)
		case int32:
			return strconv.FormatInt(int64(n), 10)
		}
	case TypeFloat64:
		if n, ok := v.(float64); ok {
			return strconv.FormatFloat(n, 'g', -1, 64)
		}
	case TypeBool:
		if b, ok := v.(bool); ok {
			if b {
				return "true"
			}
			return "false"
		}
	case TypeString:
		if s, ok := v.(string); ok {
			return s
		}
	}
	return fmt.Sprintf("%v", v)
}

func ParseFieldType(s string) FieldType {
	switch s {
	case "int32":
		return TypeInt32
	case "int64":
		return TypeInt64
	case "float64":
		return TypeFloat64
	case "bool":
		return TypeBool
	case "timestamp_nano":
		return TypeTimestampNano
	default:
		return TypeString
	}
}

type FieldMapping struct {
	ParquetColumn string
	InternalName  string
	Type          FieldType
	Origin        FieldOrigin
	MapColumn     string // parent MAP column name when Origin != OriginPromoted
	MapKey        string // key inside the MAP
	HasBloom      bool
}

type Profile struct {
	Promoted     []FieldMapping
	MapColumns   []string // MAP column names to scan for unknown fields
	StreamFields []string // fields that define a stream identity
}

type ExtraPromoted struct {
	Name  string
	Type  string
	Bloom bool
}

type Registry struct {
	profile       Profile
	extraPromoted []ExtraPromoted
	byInternal    map[string]*FieldMapping
	byParquet     map[string]*FieldMapping
}

var LogsProfile = Profile{
	Promoted: []FieldMapping{
		{ParquetColumn: "timestamp_unix_nano", InternalName: "_time", Type: TypeTimestampNano, Origin: OriginPromoted},
		{ParquetColumn: "body", InternalName: "_msg", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "severity_text", InternalName: "level", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "severity_number", InternalName: "severity_number", Type: TypeInt32, Origin: OriginPromoted},
		{ParquetColumn: "service.name", InternalName: "service.name", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "k8s.namespace.name", InternalName: "k8s.namespace.name", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "k8s.pod.name", InternalName: "k8s.pod.name", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "trace_id", InternalName: "trace_id", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "span_id", InternalName: "span_id", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "k8s.deployment.name", InternalName: "k8s.deployment.name", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "k8s.node.name", InternalName: "k8s.node.name", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "deployment.environment", InternalName: "deployment.environment", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "cloud.region", InternalName: "cloud.region", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "host.name", InternalName: "host.name", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "_stream", InternalName: "_stream", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "_stream_id", InternalName: "_stream_id", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "scope.name", InternalName: "scope.name", Type: TypeString, Origin: OriginPromoted},
	},
	MapColumns:   []string{"resource.attributes", "log.attributes"},
	StreamFields: []string{"service.name", "k8s.namespace.name", "k8s.pod.name"},
}

var TracesProfile = Profile{
	Promoted: []FieldMapping{
		{ParquetColumn: "timestamp_unix_nano", InternalName: "_time", Type: TypeTimestampNano, Origin: OriginPromoted},
		{ParquetColumn: "start_time_unix_nano", InternalName: "start_time", Type: TypeTimestampNano, Origin: OriginPromoted},
		{ParquetColumn: "trace_id", InternalName: "trace_id", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "span_id", InternalName: "span_id", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "parent_span_id", InternalName: "parent_span_id", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "span.name", InternalName: "name", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "span.kind", InternalName: "kind", Type: TypeInt32, Origin: OriginPromoted},
		{ParquetColumn: "status.code", InternalName: "status_code", Type: TypeInt32, Origin: OriginPromoted},
		{ParquetColumn: "status.message", InternalName: "status_message", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "duration_ns", InternalName: "duration", Type: TypeInt64, Origin: OriginPromoted},
		{ParquetColumn: "service.name", InternalName: "resource_attr:service.name", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "scope.name", InternalName: "scope_attr:otel.library.name", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "_stream", InternalName: "_stream", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "_stream_id", InternalName: "_stream_id", Type: TypeString, Origin: OriginPromoted},
	},
	MapColumns:   []string{"resource.attributes", "span.attributes", "scope.attributes"},
	StreamFields: []string{"resource_attr:service.name", "name"},
}

func NewRegistry(profile Profile, extra ...ExtraPromoted) *Registry {
	r := &Registry{
		profile:       profile,
		extraPromoted: extra,
		byInternal:    make(map[string]*FieldMapping, len(profile.Promoted)+len(extra)),
		byParquet:     make(map[string]*FieldMapping, len(profile.Promoted)+len(extra)),
	}
	for i := range profile.Promoted {
		m := &profile.Promoted[i]
		r.byInternal[m.InternalName] = m
		r.byParquet[m.ParquetColumn] = m
	}
	for _, ep := range extra {
		m := &FieldMapping{
			ParquetColumn: ep.Name,
			InternalName:  ep.Name,
			Type:          ParseFieldType(ep.Type),
			Origin:        OriginPromoted,
			HasBloom:      ep.Bloom,
		}
		r.byInternal[ep.Name] = m
		r.byParquet[ep.Name] = m
	}
	return r
}

func (r *Registry) FormatField(internalName string, v any) string {
	if m, ok := r.byInternal[internalName]; ok {
		return m.Type.FormatValue(v)
	}
	return TypeString.FormatValue(v)
}

func (r *Registry) ExtraPromoted() []ExtraPromoted {
	return r.extraPromoted
}

func (r *Registry) IsPromoted(fieldName string) bool {
	_, ok := r.byInternal[fieldName]
	return ok
}

func (r *Registry) ResolveToParquet(internalName string) *FieldMapping {
	if m, ok := r.byInternal[internalName]; ok {
		return m
	}

	// VT prefix convention: resource_attr:X -> resource.attributes MAP
	if key, ok := strings.CutPrefix(internalName, "resource_attr:"); ok {
		return &FieldMapping{
			ParquetColumn: "resource.attributes",
			InternalName:  internalName,
			Origin:        OriginResourceMap,
			MapColumn:     "resource.attributes",
			MapKey:        key,
		}
	}
	if key, ok := strings.CutPrefix(internalName, "span_attr:"); ok {
		return &FieldMapping{
			ParquetColumn: "span.attributes",
			InternalName:  internalName,
			Origin:        OriginSpanAttrMap,
			MapColumn:     "span.attributes",
			MapKey:        key,
		}
	}
	if key, ok := strings.CutPrefix(internalName, "scope_attr:"); ok {
		return &FieldMapping{
			ParquetColumn: "scope.attributes",
			InternalName:  internalName,
			Origin:        OriginScopeAttrMap,
			MapColumn:     "scope.attributes",
			MapKey:        key,
		}
	}
	if key, ok := strings.CutPrefix(internalName, "log_attr:"); ok {
		return &FieldMapping{
			ParquetColumn: "log.attributes",
			InternalName:  internalName,
			Origin:        OriginLogAttrMap,
			MapColumn:     "log.attributes",
			MapKey:        key,
		}
	}

	// Fallback: unrecognized fields map to the first MAP column (resource.attributes
	// for traces, log.attributes for logs). Fields that shouldn't hit this fallback
	// must be registered as Promoted in the profile.
	if len(r.profile.MapColumns) > 0 {
		mapCol := r.profile.MapColumns[0]
		origin := mapColumnToOrigin(mapCol)
		return &FieldMapping{
			ParquetColumn: mapCol,
			InternalName:  internalName,
			Origin:        origin,
			MapColumn:     mapCol,
			MapKey:        internalName,
		}
	}

	return nil
}

func (r *Registry) ResolveFromParquet(parquetColumn string) *FieldMapping {
	if m, ok := r.byParquet[parquetColumn]; ok {
		return m
	}
	return nil
}

func (r *Registry) PromotedColumns() []FieldMapping {
	return r.profile.Promoted
}

func (r *Registry) MapColumns() []string {
	return r.profile.MapColumns
}

func (r *Registry) StreamFields() []string {
	return r.profile.StreamFields
}

func (r *Registry) TimestampColumn() string {
	return "timestamp_unix_nano"
}

func mapColumnToOrigin(col string) FieldOrigin {
	switch col {
	case "resource.attributes":
		return OriginResourceMap
	case "log.attributes":
		return OriginLogAttrMap
	case "span.attributes":
		return OriginSpanAttrMap
	case "scope.attributes":
		return OriginScopeAttrMap
	default:
		return OriginResourceMap
	}
}
