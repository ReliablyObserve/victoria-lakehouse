package schema

import (
	"fmt"
	"runtime"
	"testing"
)

func schemaForceGC() {
	runtime.GC()
	runtime.GC()
}

func schemaHeapInUse() uint64 {
	var m runtime.MemStats
	schemaForceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func TestMemLeak_Registry_NewLogsRegistry(t *testing.T) {
	// Warm up
	for i := 0; i < 20; i++ {
		_ = NewRegistry(LogsProfile)
	}
	schemaForceGC()

	before := schemaHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		r := NewRegistry(LogsProfile)
		_ = r.IsPromoted("service.name")
	}

	schemaForceGC()
	after := schemaHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d NewRegistry(LogsProfile) cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Registry_NewTracesRegistry(t *testing.T) {
	// Warm up
	for i := 0; i < 20; i++ {
		_ = NewRegistry(TracesProfile)
	}
	schemaForceGC()

	before := schemaHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		r := NewRegistry(TracesProfile)
		_ = r.IsPromoted("trace_id")
	}

	schemaForceGC()
	after := schemaHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d NewRegistry(TracesProfile) cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Registry_ResolveToParquet(t *testing.T) {
	r := NewRegistry(LogsProfile)

	fields := []string{
		"service.name", "trace_id", "_msg", "level",
		"resource_attr:host.name", "log_attr:custom",
		"span_attr:http.method", "unknown-field",
	}

	// Warm up
	for i := 0; i < 100; i++ {
		for _, f := range fields {
			_ = r.ResolveToParquet(f)
		}
	}
	schemaForceGC()

	before := schemaHeapInUse()

	const iterations = 50000
	for i := 0; i < iterations; i++ {
		f := fields[i%len(fields)]
		m := r.ResolveToParquet(f)
		if m != nil {
			_ = m.ParquetColumn
		}
	}

	schemaForceGC()
	after := schemaHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d ResolveToParquet cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Registry_FormatField(t *testing.T) {
	r := NewRegistry(LogsProfile)

	// Warm up
	for i := 0; i < 100; i++ {
		_ = r.FormatField("service.name", "my-svc")
		_ = r.FormatField("_time", int64(1234567890000000000))
		_ = r.FormatField("severity_number", int32(9))
	}
	schemaForceGC()

	before := schemaHeapInUse()

	const iterations = 100000
	for i := 0; i < iterations; i++ {
		switch i % 3 {
		case 0:
			_ = r.FormatField("service.name", fmt.Sprintf("svc-%d", i%100))
		case 1:
			_ = r.FormatField("_time", int64(1234567890000000000+i))
		case 2:
			_ = r.FormatField("severity_number", int32(i%10))
		}
	}

	schemaForceGC()
	after := schemaHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d FormatField cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_FieldType_FormatValue(t *testing.T) {
	types := []struct {
		ft  FieldType
		val any
	}{
		{TypeString, "hello world"},
		{TypeInt64, int64(9876543210)},
		{TypeInt32, int32(42)},
		{TypeFloat64, float64(3.14159)},
		{TypeBool, true},
		{TypeTimestampNano, int64(1700000000000000000)},
	}

	// Warm up
	for i := 0; i < 100; i++ {
		for _, tc := range types {
			_ = tc.ft.FormatValue(tc.val)
		}
	}
	schemaForceGC()

	before := schemaHeapInUse()

	const iterations = 200000
	for i := 0; i < iterations; i++ {
		tc := types[i%len(types)]
		_ = tc.ft.FormatValue(tc.val)
	}

	schemaForceGC()
	after := schemaHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d FieldType.FormatValue cycles (max %d)", growth, iterations, maxAllowed)
	}
}
