package parquetwriter

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestTokenizeSplitsLowercasesAndDeduplicates(t *testing.T) {
	for _, c := range []struct {
		in   string
		want []string
	}{
		{"Hello, WORLD hello", []string{"hello", "world"}},
		{"a b cc", []string{"cc"}},                   // one-character tokens are dropped
		{"req-123/ok", []string{"req", "123", "ok"}}, // non-alphanumerics split
		{"café résumé", []string{"café", "résumé"}},  // letters are Unicode letters
		{"", []string{}},
		{"!!! ??? ...", []string{}},
	} {
		if got := Tokenize(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("Tokenize(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// A token bloom never has a false negative: every token added tests true, before
// and after the marshal round trip.
func TestTokenBloomHasNoFalseNegativesAndRoundTrips(t *testing.T) {
	var tokens []string
	for i := 0; i < 2000; i++ {
		tokens = append(tokens, fmt.Sprintf("token%dx", i))
	}
	b := NewTokenBloom(len(tokens), 0.01)
	for _, tok := range tokens {
		b.Add(tok)
	}
	data, err := b.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var back TokenBloom
	if err := back.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	for _, tok := range tokens {
		if !b.Test(tok) || !back.Test(tok) {
			t.Fatalf("token %q: false negative (original %v, restored %v)", tok, b.Test(tok), back.Test(tok))
		}
	}
	absent := 0
	for i := 0; i < 2000; i++ {
		if back.Test(fmt.Sprintf("absent%dy", i)) {
			absent++
		}
	}
	if absent > 100 { // about 1 % expected
		t.Errorf("%d of 2000 absent tokens pass the bloom, want about 1 %%", absent)
	}
	again, _ := back.MarshalBinary()
	if string(again) != string(data) {
		t.Error("a restored bloom marshals to different bytes")
	}
}

func TestTokenBloomEdgeCases(t *testing.T) {
	// A non-positive size and an out-of-range false-positive rate fall back to a
	// usable bloom instead of an empty or degenerate one.
	for _, b := range []*TokenBloom{NewTokenBloom(0, 0.01), NewTokenBloom(-5, 0.01), NewTokenBloom(10, 0), NewTokenBloom(10, 1), NewTokenBloom(10, 7)} {
		b.Add("alpha")
		if !b.Test("alpha") {
			t.Fatal("a bloom built from degenerate parameters lost a token")
		}
	}
	var empty TokenBloom
	if empty.Test("alpha") {
		t.Error("a zero-value bloom must answer false (definitely absent)")
	}
}

func TestTokenBloomUnmarshalRejectsShortAndTruncatedData(t *testing.T) {
	var b TokenBloom
	if err := b.UnmarshalBinary([]byte{1, 2, 3}); err == nil {
		t.Error("UnmarshalBinary accepted 3 bytes")
	}
	full, _ := NewTokenBloom(100, 0.01).MarshalBinary()
	if err := b.UnmarshalBinary(full[:len(full)-1]); err == nil {
		t.Error("UnmarshalBinary accepted a truncated bloom")
	}
	if err := b.UnmarshalBinary(full); err != nil {
		t.Errorf("UnmarshalBinary of a complete bloom: %v", err)
	}
}

func TestBuildTokenBloomMetadata(t *testing.T) {
	key, value := BuildTokenBloomMetadata([]string{"processed request 42", "request 43 failed"}, 7)
	if key != TokenBloomKey(7) || !strings.HasPrefix(key, BodyBloomEntryPrefix) || key != "_bloom_body_rg_7" {
		t.Errorf("key = %q", key)
	}
	var b TokenBloom
	if err := b.UnmarshalBinary(value); err != nil {
		t.Fatal(err)
	}
	for _, tok := range []string{"processed", "request", "42", "43", "failed"} {
		if !b.Test(tok) {
			t.Errorf("token %q missing from the row group's bloom", tok)
		}
	}
	// Bodies with no token of two letters or more still give a valid bloom.
	if _, v := BuildTokenBloomMetadata([]string{"a", "b"}, 0); len(v) == 0 {
		t.Error("an all-short body set gave an empty bloom value")
	}
}

func TestTokenBloomDegenerate(t *testing.T) {
	var zero TokenBloom
	if !zero.Degenerate() {
		t.Error("a zero-value bloom is degenerate")
	}
	var noHash TokenBloom
	if err := noHash.UnmarshalBinary(make([]byte, 6+8)); err != nil { // k=0, one word
		t.Fatal(err)
	}
	if !noHash.Degenerate() {
		t.Error("a bloom with no hash function is degenerate")
	}
	var noWords TokenBloom
	if err := noWords.UnmarshalBinary([]byte{3, 0, 0, 0, 0, 0}); err != nil { // k=3, zero words
		t.Fatal(err)
	}
	if !noWords.Degenerate() {
		t.Error("a bloom with no words is degenerate")
	}
	if NewTokenBloom(10, 0.01).Degenerate() {
		t.Error("a sized bloom is not degenerate")
	}
}
