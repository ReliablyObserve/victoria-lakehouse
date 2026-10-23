package traceindex

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// legacyMarshal is the writer as it was before #768: the trace ID length is
// truncated to uint16 without a check. It is byte-for-byte the encoder of
// every earlier release (checked with git show: the traces-module copy at
// b04d9a40 and a7d98086, the logs-module copy up to #765, the shared package
// at 365a2b50): 5 + sum(20 + len(id)) bytes, no padding, checksum or trailer,
// so exact consumption cannot reject a valid historical index.
func legacyMarshal(entries []Entry) []byte {
	size := 1 + 4
	for _, e := range entries {
		size += 2 + len(e.TraceID) + 2 + 8 + 8
	}
	buf := make([]byte, size)
	buf[0] = Version
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(entries)))
	off := 5
	for _, e := range entries {
		binary.LittleEndian.PutUint16(buf[off:off+2], uint16(len(e.TraceID)))
		off += 2
		copy(buf[off:], e.TraceID)
		off += len(e.TraceID)
		binary.LittleEndian.PutUint16(buf[off:off+2], e.Partition)
		off += 2
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(e.StartNs))
		off += 8
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(e.EndNs))
		off += 8
	}
	return buf
}

// legacyUnmarshal is the decoder as it was before #768 (after #765's byte
// bound): it never checks that it consumed the whole payload.
func legacyUnmarshal(data []byte) ([]Entry, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("short")
	}
	if data[0] != Version {
		return nil, fmt.Errorf("version")
	}
	count := int(binary.LittleEndian.Uint32(data[1:5]))
	if count > 1<<20 {
		return nil, fmt.Errorf("count")
	}
	if remaining := len(data) - 5; count > remaining/minEntrySize {
		return nil, fmt.Errorf("count exceeds payload")
	}
	entries := make([]Entry, 0, count)
	off := 5
	for i := 0; i < count; i++ {
		if off+2 > len(data) {
			return nil, fmt.Errorf("truncated")
		}
		tidLen := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+tidLen+2+8+8 > len(data) {
			return nil, fmt.Errorf("truncated")
		}
		tid := string(data[off : off+tidLen])
		off += tidLen
		p := binary.LittleEndian.Uint16(data[off : off+2])
		off += 2
		s := int64(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
		e := int64(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
		entries = append(entries, Entry{TraceID: tid, Partition: p, StartNs: s, EndNs: e})
	}
	return entries, nil
}

// longThenNormal returns a long ID that sorts before a normal one, the shape
// the #765 review reproduced.
func longThenNormal(longLen int) []Entry {
	return []Entry{
		{TraceID: strings.Repeat("0", longLen), Partition: 1, StartNs: 10, EndNs: 20},
		{TraceID: "aaaabbbbccccdddd", Partition: 2, StartNs: 30, EndNs: 40},
	}
}

// TestMarshal_TraceIDAboveUint16OmitsIndex pins #768: a trace ID of 65,536
// bytes or more must not produce a wrapped index. Marshal returns nil (no
// index), and the omission is counted.
func TestMarshal_TraceIDAboveUint16OmitsIndex(t *testing.T) {
	for _, n := range []int{MaxTraceIDLen + 1, MaxTraceIDLen + 2, 1<<16 + 5, 1 << 17, 3<<16 + 7} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			before := metrics.WriterTraceIdxOmittedTotal.Get("trace_id_too_long")
			raw := Marshal(longThenNormal(n))
			if raw != nil {
				// On the pre-fix code this decodes successfully into other entries.
				got, err := Unmarshal(raw)
				t.Fatalf("Marshal of a %d-byte trace ID returned a %d-byte index (decodes to %d entries, err=%v)", n, len(raw), len(got), err)
			}
			if d := metrics.WriterTraceIdxOmittedTotal.Get("trace_id_too_long") - before; d != 1 {
				t.Fatalf("omission counted %d times, want 1", d)
			}
		})
	}
}

// TestMarshal_TraceIDAtUint16LimitRoundTrips: the longest representable ID
// (65,535 bytes) is still indexed.
func TestMarshal_TraceIDAtUint16LimitRoundTrips(t *testing.T) {
	in := longThenNormal(MaxTraceIDLen)
	raw := Marshal(in)
	if raw == nil {
		t.Fatal("a 65535-byte trace ID is representable and must be indexed")
	}
	got, err := Unmarshal(raw)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("round trip mismatch")
	}
}

// TestUnmarshal_RejectsLegacyWrappedIndex: an index written by a release
// before #768, carrying a trace ID of 65,536+ bytes with a wrapped length,
// decoded successfully into garbage. It must now be refused (exact
// consumption), so readers fall back to the span scan.
func TestUnmarshal_RejectsLegacyWrappedIndex(t *testing.T) {
	for _, n := range []int{1<<16 + 1, 1<<16 + 5, 1<<16 + 40, 1 << 17, 3<<16 + 7} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			raw := legacyMarshal(longThenNormal(n))
			if got, err := legacyUnmarshal(raw); err == nil && reflect.DeepEqual(got, longThenNormal(n)) {
				t.Fatal("test setup: the legacy decode round-tripped, the index did not wrap")
			}
			if got, err := Unmarshal(raw); err == nil {
				t.Fatalf("a wrapped legacy index decoded successfully into %d garbage entries: %+v", len(got), got)
			}
		})
	}
}

// TestFromMetadata_LegacyWrappedIndexIsNoIndex: through the reader entry
// point, a wrapped legacy index is "no usable index" (ok=false), which both
// the file pre-filter and the footer lookup treat as keep-the-file / miss.
func TestFromMetadata_LegacyWrappedIndexIsNoIndex(t *testing.T) {
	raw := legacyMarshal(longThenNormal(1<<16 + 5))
	if entries, ok := FromMetadata(map[string]string{MetadataKey: string(raw)}); ok {
		t.Fatalf("FromMetadata accepted a wrapped legacy index: %+v", entries)
	}
}

// TestUnmarshal_RejectsTrailingBytes: bytes after the last declared entry
// mean the payload was not produced by Marshal.
func TestUnmarshal_RejectsTrailingBytes(t *testing.T) {
	good := Marshal([]Entry{{TraceID: "t1", Partition: 1, StartNs: 1, EndNs: 2}})
	for _, extra := range [][]byte{{0}, {1, 2, 3}, make([]byte, 100)} {
		if _, err := Unmarshal(append(append([]byte{}, good...), extra...)); err == nil || !strings.Contains(err.Error(), "trailing") {
			t.Fatalf("trailing %d bytes: want a trailing-bytes error, got %v", len(extra), err)
		}
	}
	if _, err := Unmarshal(good); err != nil {
		t.Fatalf("the exact payload must decode: %v", err)
	}
}

// TestMarshal_EntryCountAgreesWithDecoder (the #768 minor): the writer and
// the decoder agree on the entry cap. At the cap the index round-trips; one
// above it Marshal omits the index (the decoder would refuse it).
func TestMarshal_EntryCountAgreesWithDecoder(t *testing.T) {
	mk := func(n int) []Entry {
		in := make([]Entry, n)
		for i := range in {
			in[i] = Entry{Partition: uint16(i), StartNs: int64(i), EndNs: int64(i) + 1}
		}
		return in
	}
	at := mk(MaxEntries)
	raw := Marshal(at)
	if raw == nil {
		t.Fatal("an index at the cap must be written")
	}
	if got, err := Unmarshal(raw); err != nil || len(got) != MaxEntries {
		t.Fatalf("at the cap: got %d entries, err=%v", len(got), err)
	}

	before := metrics.WriterTraceIdxOmittedTotal.Get("too_many_entries")
	if raw := Marshal(mk(MaxEntries + 1)); raw != nil {
		_, err := Unmarshal(raw)
		t.Fatalf("Marshal wrote an index of %d entries the decoder refuses (err=%v)", MaxEntries+1, err)
	}
	if d := metrics.WriterTraceIdxOmittedTotal.Get("too_many_entries") - before; d != 1 {
		t.Fatalf("omission counted %d times, want 1", d)
	}
}

// TestUnmarshal_NoDivergenceFromLegacyOnWriterOutput: exact consumption must
// not reject anything a writer produces, now or in any release before. The
// new decoder is compared with the previous decoder over random and extreme
// outputs of the new and the legacy writer (IDs within the uint16 limit):
// 0 divergences.
func TestUnmarshal_NoDivergenceFromLegacyOnWriterOutput(t *testing.T) {
	rng := rand.New(rand.NewSource(768))
	lens := []int{0, 1, 2, 31, 32, 33, 255, 256, 1000, 65534, MaxTraceIDLen}
	check := func(in []Entry) {
		t.Helper()
		for name, raw := range map[string][]byte{"new": Marshal(in), "legacy": legacyMarshal(in)} {
			want, werr := legacyUnmarshal(raw)
			got, gerr := Unmarshal(raw)
			if (werr == nil) != (gerr == nil) || !reflect.DeepEqual(want, got) {
				t.Fatalf("%s writer, %d entries: legacy=(%d entries, %v) new=(%d entries, %v)", name, len(in), len(want), werr, len(got), gerr)
			}
			if gerr != nil {
				t.Fatalf("%s writer output refused: %v", name, gerr)
			}
		}
	}
	check(nil)
	check([]Entry{})
	for iter := 0; iter < 300; iter++ {
		n := rng.Intn(40)
		in := make([]Entry, n)
		for i := range in {
			l := rng.Intn(80)
			if rng.Intn(10) == 0 {
				l = lens[rng.Intn(len(lens))]
			}
			b := make([]byte, l)
			rng.Read(b)
			in[i] = Entry{TraceID: string(b), Partition: uint16(rng.Intn(1 << 16)), StartNs: rng.Int63() - rng.Int63(), EndNs: rng.Int63() - rng.Int63()}
		}
		check(in)
	}
	for _, l := range lens {
		check([]Entry{{TraceID: strings.Repeat("x", l)}, {TraceID: strings.Repeat("y", l)}})
	}
}
