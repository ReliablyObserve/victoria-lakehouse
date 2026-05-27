package schema

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// fastFormatTimestampNano formats nanosecond-epoch int64 to RFC3339Nano
// without the per-call overhead of time.Time allocation + time.Format.
// Uses direct digit extraction into a fixed-size buffer.
func fastFormatTimestampNano(ns int64) string {
	const rfc3339NanoLen = len("2006-01-02T15:04:05.999999999Z")
	t := time.Unix(0, ns).UTC()
	y, mo, d := t.Date()
	h, mi, s := t.Clock()
	nsec := t.Nanosecond()

	var buf [rfc3339NanoLen]byte
	buf[0] = byte('0' + y/1000)
	buf[1] = byte('0' + (y/100)%10)
	buf[2] = byte('0' + (y/10)%10)
	buf[3] = byte('0' + y%10)
	buf[4] = '-'
	buf[5] = byte('0' + mo/10)
	buf[6] = byte('0' + mo%10)
	buf[7] = '-'
	buf[8] = byte('0' + d/10)
	buf[9] = byte('0' + d%10)
	buf[10] = 'T'
	buf[11] = byte('0' + h/10)
	buf[12] = byte('0' + h%10)
	buf[13] = ':'
	buf[14] = byte('0' + mi/10)
	buf[15] = byte('0' + mi%10)
	buf[16] = ':'
	buf[17] = byte('0' + s/10)
	buf[18] = byte('0' + s%10)
	buf[19] = '.'

	// nanosecond digits
	for i := 28; i >= 20; i-- {
		buf[i] = byte('0' + nsec%10)
		nsec /= 10
	}

	// Trim trailing zeros to match time.RFC3339Nano behavior.
	end := 29
	for end > 20 && buf[end-1] == '0' {
		end--
	}
	if end == 20 {
		// All zeros — omit the decimal point too.
		buf[19] = 'Z'
		return string(buf[:20])
	}
	buf[end] = 'Z'
	return string(buf[:end+1])
}

// fastFormatInt64 avoids strconv.FormatInt allocation for small values
// by using a stack-allocated buffer.
func fastFormatInt64(n int64) string {
	if n >= 0 && n < 1000 {
		return smallPosStr[n]
	}
	var buf [20]byte
	neg := n < 0
	if neg {
		n = -n
	}
	i := len(buf)
	for n >= 10 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	i--
	buf[i] = byte('0' + n)
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

var smallPosStr [1000]string

func init() {
	for i := range smallPosStr {
		smallPosStr[i] = strconv.Itoa(i)
	}
}

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
			return fastFormatTimestampNano(n)
		}
	case TypeInt32:
		switch n := v.(type) {
		case int32:
			return fastFormatInt64(int64(n))
		case int64:
			return fastFormatInt64(n)
		case int:
			return fastFormatInt64(int64(n))
		}
	case TypeInt64:
		switch n := v.(type) {
		case int64:
			return fastFormatInt64(n)
		case int32:
			return fastFormatInt64(int64(n))
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
	Promoted        []FieldMapping
	MapColumns      []string        // MAP column names to scan for unknown fields
	StreamFields    []string        // fields that define a stream identity
	TopLevelMapKeys map[string]bool // MAP keys emitted without prefix (VT top-level span attrs)
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
	MapColumns: []string{"resource.attributes", "log.attributes"},
	StreamFields: []string{
		"service.name", "k8s.namespace.name", "k8s.pod.name",
		"k8s.deployment.name", "deployment.environment", "cloud.region",
		"host.name", "k8s.node.name", "level",
	},
}

var TracesProfile = Profile{
	Promoted: []FieldMapping{
		{ParquetColumn: "timestamp_unix_nano", InternalName: "_time", Type: TypeTimestampNano, Origin: OriginPromoted},
		{ParquetColumn: "start_time_unix_nano", InternalName: "start_time_unix_nano", Type: TypeInt64, Origin: OriginPromoted},
		{ParquetColumn: "trace_id", InternalName: "trace_id", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "span_id", InternalName: "span_id", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "parent_span_id", InternalName: "parent_span_id", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "span.name", InternalName: "name", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "span.kind", InternalName: "kind", Type: TypeInt32, Origin: OriginPromoted},
		{ParquetColumn: "status.code", InternalName: "status_code", Type: TypeInt32, Origin: OriginPromoted},
		{ParquetColumn: "status.message", InternalName: "status_message", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "duration_ns", InternalName: "duration", Type: TypeInt64, Origin: OriginPromoted},
		{ParquetColumn: "service.name", InternalName: "resource_attr:service.name", Type: TypeString, Origin: OriginPromoted, HasBloom: true},
		{ParquetColumn: "deployment.environment", InternalName: "resource_attr:deployment.environment", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "cloud.region", InternalName: "resource_attr:cloud.region", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "host.name", InternalName: "resource_attr:host.name", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "k8s.namespace.name", InternalName: "resource_attr:k8s.namespace.name", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "k8s.pod.name", InternalName: "resource_attr:k8s.pod.name", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "k8s.deployment.name", InternalName: "resource_attr:k8s.deployment.name", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "k8s.node.name", InternalName: "resource_attr:k8s.node.name", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "http.method", InternalName: "span_attr:http.method", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "http.status_code", InternalName: "span_attr:http.status_code", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "http.url", InternalName: "span_attr:http.url", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "db.system", InternalName: "span_attr:db.system", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "db.statement", InternalName: "span_attr:db.statement", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "scope.name", InternalName: "scope_name", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "_stream", InternalName: "_stream", Type: TypeString, Origin: OriginPromoted},
		{ParquetColumn: "_stream_id", InternalName: "_stream_id", Type: TypeString, Origin: OriginPromoted},
	},
	MapColumns:      []string{"resource.attributes", "span.attributes", "scope.attributes"},
	StreamFields:    []string{"resource_attr:service.name", "name"},
	TopLevelMapKeys: VTTopLevelSpanAttrKeys,
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

func (r *Registry) TopLevelMapKeys() map[string]bool {
	return r.profile.TopLevelMapKeys
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
