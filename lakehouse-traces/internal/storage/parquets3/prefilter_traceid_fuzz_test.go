package parquets3

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/smartcache"
)

// FuzzPreFilterFiles_TraceID fuzzes the trace_id narrowing edge cases the
// recently-flushed parity bug (bd838e9 + 392ba6f) touched. The fix removed
// smartCache-based narrowing from preFilterFiles — it now only runs
// filterFilesByLabels + filterFilesByBloomIndex, and the deterministic,
// footer-backed filterFilesByTraceIdx is the sole authority on trace_id
// narrowing (and runs LATER, on whatever preFilterFiles keeps).
//
// The INVARIANT this fuzz pins, for ANY trace_id query shape:
//
//  1. preFilterFiles never panics.
//  2. fileB — a manifest file that is NEVER in the smartCache (the
//     recently-flushed / never-warmed file) — is ALWAYS present in the
//     output. The smartCache is a LOWER BOUND on the relevant file set;
//     dropping fileB because the cache doesn't list it is exactly the
//     cold-tier 0-vs-N regression class (cold Jaeger 0 traces vs hot VT N
//     for minutes-old spans, and the multi-id in() partial-hit narrowing).
//  3. The output is a SUBSET of the input files (no fabricated files,
//     no duplicates beyond the input set).
//
// The smartCache is deliberately seeded so SOME trace_ids map to fileA
// (cache hit) while fileB has no entry at all (permanent cache miss). A
// regression that re-introduces FindFilesByTraceID-based narrowing would
// union the cache-hit keys and drop fileB on partial/zero hits — caught
// here as an invariant-2 violation across the fuzzed query corpus.
func FuzzPreFilterFiles_TraceID(f *testing.F) {
	const (
		keyA = "traces/dt=2026-05-10/hour=14/cache-hit.parquet"
		keyB = "traces/dt=2026-05-10/hour=14/cache-miss-recent.parquet"
		tidA = "trace-A-cached-id-aaaaaaaaaaaa"
		tidB = "trace-B-uncached-id-bbbbbbbbbb"
	)

	// Diverse trace_id query shapes. Each is the `query` arg.
	f.Add(`trace_id:"` + tidA + `"`)             // single quoted, cached id
	f.Add(`trace_id:=` + tidA)                   // single field-eq, unquoted, cached
	f.Add(`trace_id:"` + tidB + `"`)             // single quoted, UNcached id
	f.Add(`trace_id:=` + tidB)                   // single field-eq, uncached
	f.Add(`trace_id:in(` + tidA + `,` + tidB + `)`) // mixed cached+uncached in()
	f.Add(`trace_id:in(` + tidA + `)`)           // single-element in(), cached
	f.Add(`trace_id:in(` + tidB + `)`)           // single-element in(), uncached
	f.Add(`trace_id:in(a,b,c)`)                  // small in(), none cached
	f.Add(`trace_id:in()`)                       // empty in()
	f.Add(`trace_id:in(`)                        // unclosed in()
	f.Add(`trace_id:in(,,,)`)                    // only separators
	f.Add(`trace_id:in(` + strings.Repeat("x,", 1000) + `x)`) // 1001 ids
	f.Add(`trace_id:""`)                         // empty quoted id
	f.Add(`trace_id:=`)                          // empty field-eq value
	f.Add(`trace_id:"héllo-世界-trace-id-长长长"`)    // unicode id
	f.Add(`trace_id:"` + strings.Repeat("z", 4096) + `"`) // very long id
	f.Add(`trace_id:"unclosed`)                  // malformed quote
	f.Add(`trace_id:"a\"b"`)                      // escaped quote inside value
	f.Add("trace_id:\"a\x00b\"")                 // embedded NUL
	f.Add(`trace_id:in("` + tidA + `","` + tidB + `")`) // quoted ids in in()
	f.Add(`trace_id:"` + tidA + `" OR trace_id:"` + tidB + `"`) // OR of two
	f.Add(`trace_id:"` + tidA + `" AND trace_id:"` + tidB + `"`) // AND of two
	f.Add(`_stream:{resource_attr:service.name="x"} AND trace_id:"` + tidB + `"`) // combined
	f.Add(`*`)                                   // wildcard (no trace_id)
	f.Add(``)                                    // empty query
	f.Add(`trace_id:`)                           // dangling colon
	f.Add(`:in(a,b)`)                            // missing field name
	f.Add(`trace_id:in(` + tidA + `,,` + tidB + `,)`) // empty elements mixed

	f.Fuzz(func(t *testing.T, query string) {
		s := testStorage()

		// Seed smartCache metadata so fileA is "known" for tidA, but
		// fileB has NO metadata entry (permanent cache miss). Mirrors the
		// cold/hot parity unit tests' setup exactly.
		meta := smartcache.NewMetadataMap()
		meta.Set(keyA, smartcache.EntryMeta{
			Signal:   "traces",
			Size:     1,
			TraceIDs: []string{tidA},
		})
		s.smartCache = smartcache.NewController(smartcache.ControllerConfig{
			L1:          &mockL1{},
			L2:          &mockL2{},
			PeerLookup:  &mockPeerLookup{},
			S3Fetcher:   &mockS3Fetcher{},
			Metadata:    meta,
			GracePeriod: 5 * time.Minute,
		})
		// Also record via the public API so a regression that consults
		// the recorded-traceIDs index (rather than raw metadata) is still
		// exercised against fileA-only knowledge.
		s.smartCache.RecordTraceIDs(keyA, []string{tidA})

		files := []manifest.FileInfo{
			{Key: keyA, Size: 1},
			{Key: keyB, Size: 1},
		}

		// (1) Must not panic.
		got := s.preFilterFiles(files, query)

		// (3) Output must be a subset of the input file set (by key),
		// with no key appearing more than once.
		inputKeys := map[string]bool{keyA: true, keyB: true}
		seen := map[string]bool{}
		gotKeys := make(map[string]bool, len(got))
		for _, fi := range got {
			if !inputKeys[fi.Key] {
				t.Fatalf("preFilterFiles fabricated a file not in the input set: %q "+
					"(query=%q). Output must be a subset of input.", fi.Key, query)
			}
			if seen[fi.Key] {
				t.Fatalf("preFilterFiles emitted duplicate file %q (query=%q). "+
					"Output must be a set-subset of input.", fi.Key, query)
			}
			seen[fi.Key] = true
			gotKeys[fi.Key] = true
		}

		// (2) The load-bearing invariant: fileB (never in smartCache,
		// i.e. the recently-flushed file) must ALWAYS survive. With a bare
		// testStorage() the only legitimate dropper is filterFilesByLabels
		// (column-stats / label index) — and these FileInfos carry no
		// Labels and no ColumnStats, so neither can drop fileB. The only
		// way fileB disappears is a regression that re-introduces
		// smartCache lower-bound narrowing into preFilterFiles.
		if !gotKeys[keyB] {
			t.Fatalf("preFilterFiles DROPPED the never-cached file %q for query=%q. "+
				"This is the recently-flushed cold-tier parity regression: smartCache "+
				"is only a LOWER BOUND on the relevant file set, so a file it doesn't "+
				"list must never be removed by preFilterFiles. The deterministic "+
				"filterFilesByTraceIdx (run later) is the authority and can only see "+
				"files preFilterFiles keeps. got keys: %v", keyB, query, gotKeys)
		}
	})
}

// FuzzExtractFilterValuesAST_TraceID fuzzes the trace_id-specific value
// extraction that preFilterFiles' downstream trace_idx narrowing depends
// on: the in()/quoted/unquoted shapes for the `trace_id` field. The general
// FuzzExtractFilterValuesAST (filter_fuzz_test.go) covers mixed fields; this
// one concentrates the corpus on trace_id batching edge cases (empty in(),
// huge in(), malformed quotes, mixed quoting) so a crash in the in()
// extraction path that only manifests for trace_id-shaped values is caught.
//
// Invariant: extractFilterValuesAST must never panic and must return a
// (possibly empty) slice for any (query, fieldName) pair.
func FuzzExtractFilterValuesAST_TraceID(f *testing.F) {
	const field = "trace_id"
	f.Add(`trace_id:"abc123"`, field)
	f.Add(`trace_id:=abc123`, field)
	f.Add(`trace_id:in(a,b,c)`, field)
	f.Add(`trace_id:in("a","b","c")`, field)
	f.Add(`trace_id:in()`, field)
	f.Add(`trace_id:in(`, field)
	f.Add(`trace_id:in(,,,)`, field)
	f.Add(`trace_id:in(`+strings.Repeat("id,", 1000)+`id)`, field)
	f.Add(`trace_id:""`, field)
	f.Add(`trace_id:"unclosed`, field)
	f.Add(`trace_id:"a\"b"`, field)
	f.Add("trace_id:\"a\x00b\"", field)
	f.Add(`trace_id:"héllo-世界"`, field)
	f.Add(`trace_id:in(a,,b,)`, field)
	f.Add(`trace_id:"x" OR trace_id:"y"`, field)
	f.Add(`trace_id:"x" AND trace_id:"y"`, field)
	f.Add(`_stream:{resource_attr:service.name="s"} AND trace_id:in(a,b)`, field)
	f.Add(``, field)
	f.Add(`*`, field)
	f.Add(`trace_id:`, field)
	f.Add(fmt.Sprintf(`trace_id:in(%s)`, strings.Repeat("\"q\",", 500)), field)

	f.Fuzz(func(t *testing.T, query, fieldName string) {
		// Must not panic; result may be empty.
		_ = extractFilterValuesAST(query, fieldName)
	})
}
