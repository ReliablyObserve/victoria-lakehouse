package manifest

import (
	"testing"
	"time"
)

// The read path leans on three manifest guarantees: a tenant only ever gets its
// own objects back, objects written under the pre-tenant static prefix are
// findable as the default tenant's data, and the manifest can name the one
// tenant it holds objects for (which is what decides whether a global, not
// tenant-keyed, in-RAM index may answer a query).

const tsPart = "dt=2026-05-10/hour=14"

func TestGetFilesForRangeUntenanted(t *testing.T) {
	m := New("test-bucket", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	base := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC).UnixNano()
	add := func(key string) {
		m.AddFile(tsPart, FileInfo{Key: key, Size: 10, RowCount: 1, MinTimeNs: base, MaxTimeNs: base})
	}
	add("0/0/logs/" + tsPart + "/a.parquet")
	add("1001/0/logs/" + tsPart + "/b.parquet")
	add("logs/" + tsPart + "/legacy.parquet")

	lo, hi := base-int64(time.Hour), base+int64(time.Hour)

	legacy := m.GetFilesForRangeUntenanted(lo, hi)
	if len(legacy) != 1 || legacy[0].Key != "logs/"+tsPart+"/legacy.parquet" {
		t.Fatalf("GetFilesForRangeUntenanted = %+v, want only the legacy object", legacy)
	}

	if got := m.GetFilesForRangeTenant(lo, hi, "0", "0"); len(got) != 1 || got[0].Key != "0/0/logs/"+tsPart+"/a.parquet" {
		t.Errorf("tenant 0:0 = %+v, want only its own object (the legacy one is returned separately)", got)
	}
	if got := m.GetFilesForRangeTenant(lo, hi, "1001", "0"); len(got) != 1 {
		t.Errorf("tenant 1001:0 = %+v, want exactly its own object", got)
	}
	if got := m.GetFilesForRangeTenant(lo, hi, "77", "0"); len(got) != 0 {
		t.Errorf("unknown tenant = %+v, want nothing", got)
	}

	// Out of range: the legacy walk must honour the time window too.
	if got := m.GetFilesForRangeUntenanted(base+int64(48*time.Hour), base+int64(49*time.Hour)); len(got) != 0 {
		t.Errorf("legacy objects outside the window were returned: %+v", got)
	}
}

func TestGetFilesForRangeUntenanted_RemoveKeepsBookkeeping(t *testing.T) {
	m := New("test-bucket", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	base := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC).UnixNano()
	fi := FileInfo{Key: "logs/" + tsPart + "/legacy.parquet", Size: 10, RowCount: 1, MinTimeNs: base, MaxTimeNs: base}
	m.AddFile(tsPart, fi)

	lo, hi := base-int64(time.Hour), base+int64(time.Hour)
	if len(m.GetFilesForRangeUntenanted(lo, hi)) != 1 {
		t.Fatal("legacy object not tracked after AddFile")
	}
	m.RemoveFile(tsPart, fi.Key)
	if got := m.GetFilesForRangeUntenanted(lo, hi); len(got) != 0 {
		t.Errorf("legacy object still tracked after RemoveFile: %+v", got)
	}
	if acc, proj, ok := m.SoleTenant(); ok {
		t.Errorf("SoleTenant = %s:%s after removing the only object, want none", acc, proj)
	}
}

func TestSoleTenant(t *testing.T) {
	base := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC).UnixNano()
	cases := []struct {
		name     string
		template string
		keys     []string
		wantAcc  string
		wantProj string
		wantOK   bool
	}{
		{"empty", "{AccountID}/{ProjectID}/", nil, "", "", false},
		{"one tenant", "{AccountID}/{ProjectID}/", []string{"1001/0/logs/" + tsPart + "/a.parquet"}, "1001", "0", true},
		{"same tenant twice", "{AccountID}/{ProjectID}/", []string{"3/4/logs/" + tsPart + "/a.parquet", "3/4/logs/" + tsPart + "/b.parquet"}, "3", "4", true},
		{"two tenants", "{AccountID}/{ProjectID}/", []string{"0/0/logs/" + tsPart + "/a.parquet", "1/2/logs/" + tsPart + "/b.parquet"}, "", "", false},
		// Legacy untenanted objects are 0:0's data.
		{"legacy only", "{AccountID}/{ProjectID}/", []string{"logs/" + tsPart + "/a.parquet"}, "0", "0", true},
		{"legacy without a template", "", []string{"logs/" + tsPart + "/a.parquet"}, "0", "0", true},
		{"default tenant plus legacy", "{AccountID}/{ProjectID}/", []string{"0/0/logs/" + tsPart + "/a.parquet", "logs/" + tsPart + "/b.parquet"}, "0", "0", true},
		{"other tenant plus legacy", "{AccountID}/{ProjectID}/", []string{"1001/0/logs/" + tsPart + "/a.parquet", "logs/" + tsPart + "/b.parquet"}, "", "", false},
		{"default account other project plus legacy", "{AccountID}/{ProjectID}/", []string{"0/5/logs/" + tsPart + "/a.parquet", "logs/" + tsPart + "/b.parquet"}, "", "", false},
		{"orgid-shaped key", "{OrgID}/", []string{"acme/logs/" + tsPart + "/a.parquet"}, "acme", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := New("test-bucket", "")
			if tc.template != "" {
				m.SetPrefixTemplate(tc.template)
			}
			for _, k := range tc.keys {
				m.AddFile(tsPart, FileInfo{Key: k, Size: 10, RowCount: 1, MinTimeNs: base, MaxTimeNs: base})
			}
			acc, proj, ok := m.SoleTenant()
			if acc != tc.wantAcc || proj != tc.wantProj || ok != tc.wantOK {
				t.Errorf("SoleTenant = (%q, %q, %v), want (%q, %q, %v)", acc, proj, ok, tc.wantAcc, tc.wantProj, tc.wantOK)
			}
		})
	}
}

// TestSoleTenant_FollowsRemovals: removing the last object of the second tenant
// makes the remaining tenant the sole tenant again.
func TestSoleTenant_FollowsRemovals(t *testing.T) {
	base := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC).UnixNano()
	m := New("test-bucket", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	m.AddFile(tsPart, FileInfo{Key: "1001/0/logs/" + tsPart + "/a.parquet", Size: 10, RowCount: 1, MinTimeNs: base, MaxTimeNs: base})
	m.AddFile(tsPart, FileInfo{Key: "2002/7/logs/" + tsPart + "/b.parquet", Size: 10, RowCount: 1, MinTimeNs: base, MaxTimeNs: base})
	if _, _, ok := m.SoleTenant(); ok {
		t.Fatal("two tenants reported a sole tenant")
	}
	m.RemoveFile(tsPart, "2002/7/logs/"+tsPart+"/b.parquet")
	if acc, proj, ok := m.SoleTenant(); !ok || acc != "1001" || proj != "0" {
		t.Fatalf("after removing 2002:7, SoleTenant = (%q, %q, %v), want 1001:0", acc, proj, ok)
	}
}

func TestTenantKeyParser(t *testing.T) {
	m := New("test-bucket", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	parse := m.TenantKeyParser()

	cases := []struct {
		key               string
		wantAcc, wantProj string
		wantOK            bool
	}{
		{"0/0/logs/" + tsPart + "/a.parquet", "0", "0", true},
		{"1001/7/logs/" + tsPart + "/a.parquet", "1001", "7", true},
		{"logs/" + tsPart + "/a.parquet", "", "", false},
		{"a.parquet", "", "", false},
		// Path traversal and encoded separators must never be accepted as a
		// tenant segment — they would let a key claim another tenant's prefix.
		{"0/../1/logs/a.parquet", "", "", false},
		{"0/0%2f1/logs/a.parquet", "", "", false},
		{"0/ 1/logs/a.parquet", "", "", false},
	}
	for _, tc := range cases {
		acc, proj, ok := parse(tc.key)
		if ok != tc.wantOK || acc != tc.wantAcc || proj != tc.wantProj {
			t.Errorf("parse(%q) = (%q, %q, %v), want (%q, %q, %v)", tc.key, acc, proj, ok, tc.wantAcc, tc.wantProj, tc.wantOK)
		}
		// KeyTenant is the single-key convenience wrapper; it must agree.
		acc2, proj2, ok2 := m.KeyTenant(tc.key)
		if acc2 != acc || proj2 != proj || ok2 != ok {
			t.Errorf("KeyTenant(%q) disagrees with TenantKeyParser", tc.key)
		}
	}
}

func TestTenantKeyPrefix(t *testing.T) {
	m := New("test-bucket", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	if got := m.TenantKeyPrefix("1001", "7"); got != "1001/7/" {
		t.Errorf("TenantKeyPrefix = %q, want %q", got, "1001/7/")
	}
	m.SetPrefixTemplate("{OrgID}/")
	if got := m.TenantKeyPrefix("acme", ""); got != "acme/" {
		t.Errorf("single-segment TenantKeyPrefix = %q, want %q", got, "acme/")
	}
}

func TestTenantSummaries_ExcludesLegacyObjects(t *testing.T) {
	// The legacy accumulator must stay out of the tenant list: those objects
	// have no tenant identity of their own to report.
	m := New("test-bucket", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	base := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC).UnixNano()
	m.AddFile(tsPart, FileInfo{Key: "0/0/logs/" + tsPart + "/a.parquet", Size: 10, RowCount: 1, MinTimeNs: base, MaxTimeNs: base})
	m.AddFile(tsPart, FileInfo{Key: "logs/" + tsPart + "/b.parquet", Size: 10, RowCount: 1, MinTimeNs: base, MaxTimeNs: base})

	summaries := m.TenantSummaries()
	if len(summaries) != 1 {
		t.Fatalf("TenantSummaries = %+v, want exactly the one real tenant", summaries)
	}
	if summaries[0].AccountID != "0" || summaries[0].ProjectID != "0" {
		t.Errorf("TenantSummaries[0] = %+v, want tenant 0:0", summaries[0])
	}
}
