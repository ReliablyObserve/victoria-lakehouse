package parquets3

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// mlForceGC runs two GC cycles to ensure all unreachable objects are collected.
func mlForceGC() {
	runtime.GC()
	runtime.GC()
}

// mlHeapInUse returns current HeapInuse after forcing GC.
func mlHeapInUse() uint64 {
	var m runtime.MemStats
	mlForceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// --- FooterCache ---

func TestMemLeak_FooterCache_PutGet(t *testing.T) {
	fc := NewFooterCache(50) // small capacity to force eviction

	// Warm up
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("file-%d.parquet", i)
		fc.Put(key, &CachedFooter{FileSize: int64(i)})
		_, _ = fc.Get(key)
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("file-%d.parquet", i%200) // cycle through 200 keys with cap=50
		fc.Put(key, &CachedFooter{FileSize: int64(i)})
		_, _ = fc.Get(key)
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024) // 10 MB
	if growth > maxAllowed {
		t.Errorf("FooterCache memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}

	if fc.Len() > 50 {
		t.Errorf("FooterCache len %d exceeds capacity 50", fc.Len())
	}
}

func TestMemLeak_FooterCache_Remove(t *testing.T) {
	fc := NewFooterCache(100)

	// Warm up
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("k%d", i)
		fc.Put(key, &CachedFooter{FileSize: int64(i)})
		fc.Remove(key)
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("k%d", i)
		fc.Put(key, &CachedFooter{FileSize: int64(i)})
		fc.Remove(key)
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("FooterCache Remove memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}

	if fc.Len() != 0 {
		t.Errorf("FooterCache len = %d after all removes, want 0", fc.Len())
	}
}

// --- LRU memCache ---

func TestMemLeak_LRUCache_StoreRetrieve(t *testing.T) {
	// 1 MB capacity — forces eviction
	c := cache.NewLRU(1 * 1024 * 1024)

	// Warm up
	for i := 0; i < 200; i++ {
		c.Put(fmt.Sprintf("key-%d", i), make([]byte, 4096))
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("key-%d", i%500)
		c.Put(key, make([]byte, 2048))
		_, _ = c.Get(key)
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("LRU cache memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}

	if c.Size() > c.MaxSize() {
		t.Errorf("LRU size %d exceeds max %d", c.Size(), c.MaxSize())
	}
}

// --- PushDownFilter building ---

func TestMemLeak_BuildPushDownFilter(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)

	// Warm up
	for i := 0; i < 100; i++ {
		q := fmt.Sprintf(`service.name:="svc-%d"`, i)
		_ = buildPushDownFilter(q, reg)
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		q := fmt.Sprintf(`service.name:="svc-%d" span.name:="GET /api"`, i%50)
		_ = buildPushDownFilter(q, reg)
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("buildPushDownFilter memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// --- BloomIndex AddColumns ---

func TestMemLeak_BloomIndex_AddColumns(t *testing.T) {
	idx := bloomindex.New()

	// Warm up
	for i := 0; i < 100; i++ {
		cols := map[string]*bloomindex.Filter{
			"service.name": bloomindex.NewFilter(10, 0.01),
			"trace_id":     bloomindex.NewFilter(10, 0.01),
		}
		cols["service.name"].Add(fmt.Sprintf("svc-%d", i))
		cols["trace_id"].Add(fmt.Sprintf("trace-%d", i))
		idx.AddColumns(fmt.Sprintf("file-%d.parquet", i), cols)
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		cols := map[string]*bloomindex.Filter{
			"service.name": bloomindex.NewFilter(5, 0.01),
		}
		cols["service.name"].Add(fmt.Sprintf("svc-%d", i%20))
		// Use bounded set of file keys to prevent unbounded growth
		idx.AddColumns(fmt.Sprintf("file-%d.parquet", i%100), cols)
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("BloomIndex.AddColumns memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// --- TokenBloom creation ---

func TestMemLeak_TokenBloom_Creation(t *testing.T) {
	// Warm up
	for i := 0; i < 100; i++ {
		b := NewTokenBloom(1000, 0.01)
		b.Add(fmt.Sprintf("token-%d", i))
		_ = b.Test(fmt.Sprintf("token-%d", i))
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		b := NewTokenBloom(100, 0.01)
		for j := 0; j < 10; j++ {
			b.Add(fmt.Sprintf("word-%d", j))
		}
		_ = b.Test("word-5")
		// b goes out of scope and should be GC'd
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("TokenBloom creation memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_TokenBloom_MarshalUnmarshal(t *testing.T) {
	// Warm up
	for i := 0; i < 50; i++ {
		b := NewTokenBloom(50, 0.01)
		b.Add("foo")
		data, _ := b.MarshalBinary()
		b2 := &TokenBloom{}
		_ = b2.UnmarshalBinary(data)
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 5000
	for i := 0; i < iterations; i++ {
		b := NewTokenBloom(50, 0.01)
		b.Add(fmt.Sprintf("token-%d", i%100))
		data, err := b.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		b2 := &TokenBloom{}
		if err := b2.UnmarshalBinary(data); err != nil {
			t.Fatalf("UnmarshalBinary: %v", err)
		}
		_ = b2.Test("token-5")
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("TokenBloom marshal/unmarshal memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// --- queryColumns / PushDownFilter extraction (traces profile) ---

func TestMemLeak_QueryColumns_Extraction(t *testing.T) {
	// Warm up
	for i := 0; i < 100; i++ {
		q := fmt.Sprintf(`service.name:="svc-%d" span.name:="GET"`, i%10)
		reg := schema.NewRegistry(schema.TracesProfile)
		f := buildPushDownFilter(q, reg)
		_ = f
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	reg := schema.NewRegistry(schema.TracesProfile)
	for i := 0; i < iterations; i++ {
		queries := []string{
			fmt.Sprintf(`service.name:="svc-%d"`, i%20),
			`span.name:="GET /api"`,
			`k8s.namespace.name:="prod"`,
			`*`,
		}
		q := queries[i%len(queries)]
		f := buildPushDownFilter(q, reg)
		if f != nil {
			_ = len(f.Checks)
		}
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("queryColumns extraction memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// --- extractTraceLabels ---

func TestMemLeak_ExtractTraceLabels(t *testing.T) {
	rows := make([]schema.TraceRow, 50)
	for i := range rows {
		rows[i] = schema.TraceRow{
			TimestampUnixNano: time.Now().UnixNano(),
			ServiceName:       fmt.Sprintf("svc-%d", i%5),
			SpanName:          fmt.Sprintf("GET /api/%d", i%10),
			TraceID:           fmt.Sprintf("trace-%d", i),
		}
	}

	// Warm up
	for i := 0; i < 100; i++ {
		_ = extractTraceLabels(rows)
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		labels := extractTraceLabels(rows)
		_ = labels
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("extractTraceLabels memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// --- Manifest GetFilesForRange ---

func TestMemLeak_Manifest_GetFilesForRange(t *testing.T) {
	m := manifest.New("test-bucket", "traces/")

	// Populate with 30 days × 24 hours of data
	for d := 0; d < 30; d++ {
		for h := 0; h < 24; h++ {
			date := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d)
			partition := fmt.Sprintf("dt=%s/hour=%02d", date.Format("2006-01-02"), h)
			for f := 0; f < 5; f++ {
				m.AddFile(partition, manifest.FileInfo{
					Key:  fmt.Sprintf("traces/%s/file-%d.parquet", partition, f),
					Size: 1024,
				})
			}
		}
	}

	startNs := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC).UnixNano()

	// Warm up
	for i := 0; i < 100; i++ {
		_ = m.GetFilesForRange(startNs, endNs)
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		files := m.GetFilesForRange(startNs, endNs)
		_ = len(files)
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("Manifest.GetFilesForRange memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// --- LabelIndex updates ---

func TestMemLeak_LabelIndex_Updates(t *testing.T) {
	idx := cache.NewLabelIndex()

	// Warm up
	for i := 0; i < 100; i++ {
		idx.Add("service.name", []string{fmt.Sprintf("svc-%d", i%10)})
		idx.Add("span.name", []string{"GET /api", "POST /api", "DELETE /api"})
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		// Reuse same field names (bounded cardinality) — should not grow
		idx.Add("service.name", []string{fmt.Sprintf("svc-%d", i%20)})
		idx.Add("k8s.namespace.name", []string{fmt.Sprintf("ns-%d", i%5)})
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("LabelIndex updates memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// --- FooterCache capacity enforcement ---

func TestMemLeak_FooterCache_CapacityEnforced(t *testing.T) {
	const cap = 100
	fc := NewFooterCache(cap)

	// Warm up: insert more than capacity
	for i := 0; i < 200; i++ {
		fc.Put(fmt.Sprintf("file-%d.parquet", i), &CachedFooter{FileSize: int64(i * 100)})
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		// Keys rotate well beyond cap to force constant eviction
		fc.Put(fmt.Sprintf("file-%d.parquet", i), &CachedFooter{FileSize: int64(1024)})
	}

	mlForceGC()
	after := mlHeapInUse()

	if fc.Len() > cap {
		t.Errorf("FooterCache len %d exceeds capacity %d", fc.Len(), cap)
	}

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("FooterCache capacity enforcement memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// --- extractTraceBloomValues ---

func TestMemLeak_ExtractTraceBloomValues(t *testing.T) {
	rows := make([]schema.TraceRow, 100)
	for i := range rows {
		rows[i] = schema.TraceRow{
			TimestampUnixNano: time.Now().UnixNano(),
			ServiceName:       fmt.Sprintf("svc-%d", i%10),
			TraceID:           fmt.Sprintf("trace-%d", i),
		}
	}

	// Warm up
	for i := 0; i < 100; i++ {
		_ = extractTraceBloomValues(rows)
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		m := extractTraceBloomValues(rows)
		_ = m
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("extractTraceBloomValues memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// --- FileBloomIndex creation ---

func TestMemLeak_FileBloomIndex_Creation(t *testing.T) {
	columnValues := map[string][]string{
		"service.name": {"svc-a", "svc-b", "svc-c"},
		"trace_id":     {"t1", "t2", "t3"},
		"span.name":    {"GET /api", "POST /api"},
	}

	// Warm up
	for i := 0; i < 100; i++ {
		idx := bloomindex.NewFileBloomIndex(columnValues, 0.01)
		_ = idx.Len()
	}
	mlForceGC()

	before := mlHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		idx := bloomindex.NewFileBloomIndex(columnValues, 0.01)
		_ = idx.Len()
		// idx goes out of scope
	}

	mlForceGC()
	after := mlHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("FileBloomIndex creation memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}
