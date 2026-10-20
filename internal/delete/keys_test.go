package delete

import (
	"strings"
	"testing"
)

func TestKeys_FormatAndParse(t *testing.T) {
	prefix := TombstonePrefix("1002/0/logs/")
	if prefix != "1002/0/logs/_tombstones/" {
		t.Fatalf("prefix = %q", prefix)
	}
	cases := []struct {
		name string
		k    tombKey
		want string
	}{
		{"definition", tombKey{kind: kindDefinition, timeNs: 0x1234, id: "task-1"}, "1002/0/logs/_tombstones/t0000000000001234-task-1.json"},
		{"undo", tombKey{kind: kindUndo, timeNs: 1 << 62, id: "a", defNs: 0x1234}, "1002/0/logs/_tombstones/t4000000000000000-0000000000001234-a.undo"},
		{"id with dots and a suffix lookalike", tombKey{kind: kindDefinition, timeNs: 7, id: "x.json"}, "1002/0/logs/_tombstones/t0000000000000007-x%2Ejson.json"},
		{"id with slash and space", tombKey{kind: kindUndo, timeNs: 7, id: "a/b c", defNs: 5}, "1002/0/logs/_tombstones/t0000000000000007-0000000000000005-a%2Fb%20c.undo"},
		{"uuid", tombKey{kind: kindDefinition, timeNs: 7, id: "8f14e45f-ceea-467a-9b3e-0c5e7d8f1a22"}, "1002/0/logs/_tombstones/t0000000000000007-8f14e45f-ceea-467a-9b3e-0c5e7d8f1a22.json"},
		{"utf8 id", tombKey{kind: kindDefinition, timeNs: 7, id: "żółć"}, "1002/0/logs/_tombstones/t0000000000000007-%C5%BC%C3%B3%C5%82%C4%87.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := formatKey(prefix, tc.k)
			if got != tc.want {
				t.Fatalf("formatKey = %q, want %q", got, tc.want)
			}
			back, ok := parseKey(prefix, got)
			if !ok || back != tc.k {
				t.Fatalf("parseKey(%q) = %+v %v, want %+v", got, back, ok, tc.k)
			}
		})
	}
}

// The order of keys is the order of their times: a StartAfter listing returns
// exactly the keys at or after a time.
func TestKeys_LexicalOrderIsTimeOrder(t *testing.T) {
	prefix := TombstonePrefix("logs/")
	times := []int64{1, 15, 16, 255, 256, 1 << 40, 1<<62 + 5, 1<<63 - 1}
	var prev string
	for _, ns := range times {
		key := formatKey(prefix, tombKey{kind: kindDefinition, timeNs: ns, id: "x"})
		if prev != "" && prev >= key {
			t.Fatalf("key for %d (%q) does not sort after the previous (%q)", ns, key, prev)
		}
		if sa := startAfterKey(prefix, ns); sa >= key {
			t.Fatalf("startAfter(%d) = %q does not precede %q", ns, sa, key)
		}
		if ns > 1 {
			if before := formatKey(prefix, tombKey{kind: kindDefinition, timeNs: ns - 1, id: "zzzz"}); before >= startAfterKey(prefix, ns) {
				t.Fatalf("a key at %d must not be after startAfter(%d)", ns-1, ns)
			}
		}
		prev = key
	}
	if startAfterKey(prefix, 0) != "" || startAfterKey(prefix, -5) != "" {
		t.Fatal("a non-positive time resumes from the start")
	}
}

func TestKeys_ParseRejectsEverythingElse(t *testing.T) {
	prefix := TombstonePrefix("logs/")
	good := prefix + "t0000000000000001-x.json"
	if _, ok := parseKey(prefix, good); !ok {
		t.Fatal("fixture: good key rejected")
	}
	bad := []string{
		"",
		"garbage",
		prefix + "t0000000000000001-x",                                     // no kind
		prefix + "t0000000000000001-x.JSON",                                // kind case
		prefix + "t0000000000000001-.json",                                 // empty id
		prefix + "T0000000000000001-x.json",                                // prefix letter case
		prefix + "t000000000000001-x.json",                                 // 15 digits
		prefix + "t00000000000000001-x.json",                               // 17 digits
		prefix + "t000000000000000G-x.json",                                // not hex
		prefix + "t000000000000000A-x.json",                                // upper-case hex
		prefix + "t0000000000000000-x.json",                                // time zero
		prefix + "tffffffffffffffff-x.json",                                // beyond int64
		prefix + "t0000000000000001-x/y.json",                              // unescaped slash
		prefix + "t0000000000000001-%2f.json",                              // lower-case escape
		prefix + "t0000000000000001-%41.json",                              // escape of a byte that is never escaped
		prefix + "t0000000000000001-%4.json",                               // truncated escape
		prefix + "t0000000000000001-%.json",                                // bare percent
		prefix + "t0000000000000001-" + strings.Repeat("a", 401) + ".json", // too long
		prefix + "t0000000000000001-x.undo",                                // an undo names no definition
		prefix + "t0000000000000001-0000000000000000-x.undo",               // definition time zero
		prefix + "t0000000000000001-000000000000000g-x.undo",               // definition time not hex
		prefix + "t0000000000000001-00000000000000001-x.undo",              // definition time 17 digits
		prefix + "t0000000000000001-0000000000000002.undo",                 // no id after the definition time
		prefix + "_snap/abc.json",
		prefix + "t0000000000000001-x.json.tmp",
		"other/_tombstones/t0000000000000001-x.json", // another prefix
	}
	for _, key := range bad {
		if k, ok := parseKey(prefix, key); ok {
			t.Errorf("parseKey accepted %q as %+v", key, k)
		}
	}
	// An id whose bytes are not valid UTF-8 cannot be a key.
	if _, ok := parseKey(prefix, prefix+"t0000000000000001-%FF.json"); ok {
		t.Error("an id that is not UTF-8 was accepted")
	}
}

func TestDefinition_CanonicalBytes(t *testing.T) {
	a := Tombstone{ID: "t", Query: "level:error", StartNs: 1, EndNs: 9, Mode: "hide", FilterAt: 5,
		Tenants: []TenantRef{{AccountID: 9, ProjectID: 1}, {AccountID: 2}, {AccountID: 9, ProjectID: 1}}}
	b := a
	b.Tenants = []TenantRef{{AccountID: 2}, {AccountID: 9, ProjectID: 1}}
	ba, err := encodeDefinition(a)
	if err != nil {
		t.Fatal(err)
	}
	bb, _ := encodeDefinition(b)
	if string(ba) != string(bb) {
		t.Fatalf("tenant order or duplicates changed the bytes:\n%s\n%s", ba, bb)
	}
	want := `{"v":1,"id":"t","tenants":[{"a":2,"p":0},{"a":9,"p":1}],"filter":"level:error","filter_at":5,"start_ns":1,"end_ns":9,"mode":"hide"}`
	if string(ba) != want {
		t.Fatalf("definition bytes =\n%s\nwant\n%s", ba, want)
	}
	// `by` is the audit trail of an API delete; an upstream task has none.
	a.CreatedBy = "1001:0"
	withBy, _ := encodeDefinition(a)
	if !strings.Contains(string(withBy), `"by":"1001:0"`) {
		t.Fatalf("by missing: %s", withBy)
	}
	back, err := decodeDefinition(withBy)
	if err != nil || back.CreatedBy != "1001:0" || len(back.Tenants) != 2 {
		t.Fatalf("decode = %+v, %v", back, err)
	}
}

// Records of the previous (mutable) protocol, and anything else that is not a
// version-1 definition, are rejected, never read.
func TestDefinition_OldShapesAreRejected(t *testing.T) {
	for name, body := range map[string]string{
		"previous protocol record": `{"ID":"x","Query":"*","StartNs":0,"EndNs":5,"AffectedKeys":["k"],"Reaped":{"k":true},"Tenants":[{"AccountID":1,"ProjectID":0}]}`,
		"unknown version":          `{"v":2,"id":"x","tenants":[{"a":1,"p":0}],"filter":"*","end_ns":5,"mode":"hide"}`,
		"no version":               `{"id":"x","tenants":[{"a":1,"p":0}],"filter":"*","end_ns":5,"mode":"hide"}`,
		"no tenant":                `{"v":1,"id":"x","tenants":[],"filter":"*","end_ns":5,"mode":"hide"}`,
		"bad filter":               `{"v":1,"id":"x","tenants":[{"a":1,"p":0}],"filter":"level:(","end_ns":5,"mode":"hide"}`,
		"inverted range":           `{"v":1,"id":"x","tenants":[{"a":1,"p":0}],"filter":"*","start_ns":9,"end_ns":5,"mode":"hide"}`,
		"not json":                 `<xml/>`,
		"empty":                    ``,
	} {
		if _, err := decodeDefinition([]byte(body)); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
}

// The key time of a task id: upstream's ids are the creating node's clock in
// nanoseconds, so the same task always maps to the same key; any other id is not
// a time.
func TestIDTime(t *testing.T) {
	for _, c := range []struct {
		id   string
		want int64
		ok   bool
	}{
		{"1792310275123456789", 1792310275123456789, true},
		{"1000000000000000000", 1000000000000000000, true},
		{"4000000000000000000", 4000000000000000000, true},
		{"999999999999999999", 0, false},  // before 2001
		{"4000000000000000001", 0, false}, // after 2096
		{"42", 0, false},
		{"", 0, false},
		{"01792310275123456789", 0, false}, // one spelling per id
		{"+1792310275123456789", 0, false},
		{"1792310275123456789x", 0, false},
		{"8f14e45f-ceea-467a-9b3e-0c5e7d8f1a22", 0, false},
		{"-1792310275123456789", 0, false},
	} {
		got, ok := idTime(c.id)
		if ok != c.ok || got != c.want {
			t.Errorf("idTime(%q) = %d, %v; want %d, %v", c.id, got, ok, c.want, c.ok)
		}
	}
}

// An unreadable definition names the tenants it closes the gate for; a body that
// does not say leaves the gate closed for everybody.
func TestNamedTenants(t *testing.T) {
	got, ok := namedTenants([]byte(`{"v":99,"future":true,"tenants":[{"a":7,"p":1},{"a":3,"p":0},{"a":7,"p":1}]}`))
	if !ok || len(got) != 2 || got[0] != (TenantRef{AccountID: 3}) || got[1] != (TenantRef{AccountID: 7, ProjectID: 1}) {
		t.Fatalf("namedTenants = %v, %v; want the two distinct tenants, sorted", got, ok)
	}
	for _, body := range []string{``, `<xml/>`, `{}`, `{"tenants":[]}`, `{"tenants":"all"}`, `{"tenants":[{"a":"x"}]}`, `[]`} {
		if got, ok := namedTenants([]byte(body)); ok {
			t.Errorf("namedTenants(%q) = %v, want none", body, got)
		}
	}
}
