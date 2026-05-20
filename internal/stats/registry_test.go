package stats

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCopyMapStringInt_Nil(t *testing.T) {
	result := copyMapStringInt(nil)
	if result != nil {
		t.Errorf("expected nil for nil input, got %v", result)
	}
}

func TestCopyMapStringInt_NonNil(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2}
	cp := copyMapStringInt(m)
	if len(cp) != 2 {
		t.Errorf("expected 2 entries, got %d", len(cp))
	}
	if cp["a"] != 1 || cp["b"] != 2 {
		t.Errorf("unexpected values: %v", cp)
	}
	// Modification of copy should not affect original
	cp["a"] = 99
	if m["a"] != 1 {
		t.Error("modifying copy should not affect original")
	}
}

func TestRegistryRecordWrite(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("acme:proj1", 1024, 2048, 10, "STANDARD")

	ts := reg.Get("acme:proj1")
	if ts == nil {
		t.Fatal("expected tenant stats, got nil")
	}
	if ts.AccountID != "acme" {
		t.Errorf("AccountID = %q, want %q", ts.AccountID, "acme")
	}
	if ts.ProjectID != "proj1" {
		t.Errorf("ProjectID = %q, want %q", ts.ProjectID, "proj1")
	}
	if ts.TotalBytes != 1024 {
		t.Errorf("TotalBytes = %d, want %d", ts.TotalBytes, 1024)
	}
	if ts.RawBytes != 2048 {
		t.Errorf("RawBytes = %d, want %d", ts.RawBytes, 2048)
	}
	if ts.TotalRows != 10 {
		t.Errorf("TotalRows = %d, want %d", ts.TotalRows, 10)
	}
	if ts.TotalFiles != 1 {
		t.Errorf("TotalFiles = %d, want %d", ts.TotalFiles, 1)
	}
	if ts.BytesByClass["STANDARD"] != 1024 {
		t.Errorf("BytesByClass[STANDARD] = %d, want %d", ts.BytesByClass["STANDARD"], 1024)
	}
	if ts.FilesByClass["STANDARD"] != 1 {
		t.Errorf("FilesByClass[STANDARD] = %d, want %d", ts.FilesByClass["STANDARD"], 1)
	}
	if ts.LastWriteAt.IsZero() {
		t.Error("LastWriteAt should not be zero")
	}
	if ts.MinTimeNs == 0 {
		t.Error("MinTimeNs should not be zero")
	}
	if ts.MaxTimeNs == 0 {
		t.Error("MaxTimeNs should not be zero")
	}
	if ts.NodeContribs["node-1"] != 1024 {
		t.Errorf("NodeContribs[node-1] = %d, want %d", ts.NodeContribs["node-1"], 1024)
	}
}

func TestRegistryRecordQuery(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("acme:proj1", 100, 200, 1, "STANDARD")

	before := time.Now()
	reg.RecordQuery("acme:proj1")

	ts := reg.Get("acme:proj1")
	if ts == nil {
		t.Fatal("expected tenant stats")
	}
	if ts.LastQueryAt.Before(before) {
		t.Error("LastQueryAt should be >= before")
	}
}

func TestRegistryMultipleWrites(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("acme:proj1", 100, 200, 5, "STANDARD")
	reg.RecordWrite("acme:proj1", 300, 400, 10, "GLACIER")
	reg.RecordWrite("acme:proj1", 50, 100, 2, "STANDARD")

	ts := reg.Get("acme:proj1")
	if ts == nil {
		t.Fatal("expected tenant stats")
	}
	if ts.TotalBytes != 450 {
		t.Errorf("TotalBytes = %d, want %d", ts.TotalBytes, 450)
	}
	if ts.RawBytes != 700 {
		t.Errorf("RawBytes = %d, want %d", ts.RawBytes, 700)
	}
	if ts.TotalRows != 17 {
		t.Errorf("TotalRows = %d, want %d", ts.TotalRows, 17)
	}
	if ts.TotalFiles != 3 {
		t.Errorf("TotalFiles = %d, want %d", ts.TotalFiles, 3)
	}
	if ts.BytesByClass["STANDARD"] != 150 {
		t.Errorf("BytesByClass[STANDARD] = %d, want %d", ts.BytesByClass["STANDARD"], 150)
	}
	if ts.BytesByClass["GLACIER"] != 300 {
		t.Errorf("BytesByClass[GLACIER] = %d, want %d", ts.BytesByClass["GLACIER"], 300)
	}
	if ts.FilesByClass["STANDARD"] != 2 {
		t.Errorf("FilesByClass[STANDARD] = %d, want %d", ts.FilesByClass["STANDARD"], 2)
	}
	if ts.FilesByClass["GLACIER"] != 1 {
		t.Errorf("FilesByClass[GLACIER] = %d, want %d", ts.FilesByClass["GLACIER"], 1)
	}
}

func TestRegistryListAll(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("small:s", 100, 100, 1, "STANDARD")
	reg.RecordWrite("big:b", 9000, 9000, 50, "STANDARD")
	reg.RecordWrite("mid:m", 500, 500, 5, "STANDARD")

	all := reg.All()
	if len(all) != 3 {
		t.Fatalf("len(All()) = %d, want 3", len(all))
	}
	// Sorted by TotalBytes desc.
	if all[0].TotalBytes != 9000 {
		t.Errorf("all[0].TotalBytes = %d, want 9000", all[0].TotalBytes)
	}
	if all[1].TotalBytes != 500 {
		t.Errorf("all[1].TotalBytes = %d, want 500", all[1].TotalBytes)
	}
	if all[2].TotalBytes != 100 {
		t.Errorf("all[2].TotalBytes = %d, want 100", all[2].TotalBytes)
	}
}

func TestRegistryCRDTMerge(t *testing.T) {
	// Node A writes tenant "acme:proj1".
	regA := NewTenantRegistry("node-A")
	regA.RecordWrite("acme:proj1", 1000, 2000, 10, "STANDARD")

	// Node B writes same tenant.
	regB := NewTenantRegistry("node-B")
	regB.RecordWrite("acme:proj1", 500, 800, 5, "STANDARD")

	// Build delta from A, merge into B.
	deltaA := regA.BuildDelta(0)
	regB.Merge(deltaA)

	ts := regB.Get("acme:proj1")
	if ts == nil {
		t.Fatal("expected merged stats")
	}
	// Combined: node-A(1000) + node-B(500) = 1500.
	if ts.TotalBytes != 1500 {
		t.Errorf("TotalBytes = %d, want 1500", ts.TotalBytes)
	}
	if ts.TotalRows != 15 {
		t.Errorf("TotalRows = %d, want 15", ts.TotalRows)
	}
	if ts.TotalFiles != 2 {
		t.Errorf("TotalFiles = %d, want 2", ts.TotalFiles)
	}
}

func TestRegistryCRDTMergeTimestampExtrema(t *testing.T) {
	regA := NewTenantRegistry("node-A")
	regB := NewTenantRegistry("node-B")

	regA.RecordWrite("t:1", 100, 100, 1, "STANDARD")
	// Short sleep to ensure different timestamps.
	time.Sleep(2 * time.Millisecond)
	regB.RecordWrite("t:1", 200, 200, 2, "STANDARD")

	tsA := regA.Get("t:1")
	tsB := regB.Get("t:1")

	// B wrote later, so LastWriteAt should be later.
	if !tsB.LastWriteAt.After(tsA.LastWriteAt) {
		t.Error("node-B LastWriteAt should be after node-A")
	}

	deltaB := regB.BuildDelta(0)
	regA.Merge(deltaB)

	merged := regA.Get("t:1")
	// After merge, LastWriteAt = max(A, B) = B's time.
	if !merged.LastWriteAt.Equal(tsB.LastWriteAt) {
		t.Errorf("merged LastWriteAt = %v, want %v", merged.LastWriteAt, tsB.LastWriteAt)
	}
	// MinTimeNs = min(A, B) = A's time.
	if merged.MinTimeNs != tsA.MinTimeNs {
		t.Errorf("merged MinTimeNs = %d, want %d", merged.MinTimeNs, tsA.MinTimeNs)
	}
}

func TestRegistryCRDTMergeIdempotent(t *testing.T) {
	regA := NewTenantRegistry("node-A")
	regA.RecordWrite("acme:proj1", 1000, 2000, 10, "STANDARD")

	regB := NewTenantRegistry("node-B")
	regB.RecordWrite("acme:proj1", 500, 800, 5, "STANDARD")

	delta := regA.BuildDelta(0)

	// Merge once.
	regB.Merge(delta)
	ts1 := regB.Get("acme:proj1")

	// Merge the same delta again.
	regB.Merge(delta)
	ts2 := regB.Get("acme:proj1")

	// Values must be identical.
	if ts1.TotalBytes != ts2.TotalBytes {
		t.Errorf("TotalBytes changed: %d -> %d", ts1.TotalBytes, ts2.TotalBytes)
	}
	if ts1.TotalRows != ts2.TotalRows {
		t.Errorf("TotalRows changed: %d -> %d", ts1.TotalRows, ts2.TotalRows)
	}
	if ts1.TotalFiles != ts2.TotalFiles {
		t.Errorf("TotalFiles changed: %d -> %d", ts1.TotalFiles, ts2.TotalFiles)
	}
}

func TestRegistryGeneration(t *testing.T) {
	reg := NewTenantRegistry("node-1")

	if g := reg.Generation(); g != 0 {
		t.Errorf("initial generation = %d, want 0", g)
	}

	reg.RecordWrite("a:1", 100, 100, 1, "STANDARD")
	if g := reg.Generation(); g != 1 {
		t.Errorf("generation after write = %d, want 1", g)
	}

	reg.RecordQuery("a:1")
	if g := reg.Generation(); g != 2 {
		t.Errorf("generation after query = %d, want 2", g)
	}

	reg.RecordWrite("b:2", 200, 200, 2, "STANDARD")
	if g := reg.Generation(); g != 3 {
		t.Errorf("generation after second write = %d, want 3", g)
	}
}

func TestRegistryConcurrent(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	const goroutines = 100
	const writesPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			tenant := fmt.Sprintf("tenant:%d", id%10) // 10 distinct tenants
			for j := 0; j < writesPerGoroutine; j++ {
				reg.RecordWrite(tenant, 100, 200, 1, "STANDARD")
				_ = reg.Get(tenant)
				_ = reg.All()
				_ = reg.GlobalAggregates()
				_ = reg.Generation()
				_ = reg.TenantCount()
			}
		}(i)
	}
	wg.Wait()

	// Verify consistency.
	if reg.TenantCount() != 10 {
		t.Errorf("TenantCount = %d, want 10", reg.TenantCount())
	}

	// Each tenant should have goroutines/10 * writesPerGoroutine = 10*50 = 500 files.
	all := reg.All()
	for _, ts := range all {
		if ts.TotalFiles != 500 {
			t.Errorf("tenant %s:%s TotalFiles = %d, want 500", ts.AccountID, ts.ProjectID, ts.TotalFiles)
		}
		if ts.TotalBytes != 50000 {
			t.Errorf("tenant %s:%s TotalBytes = %d, want 50000", ts.AccountID, ts.ProjectID, ts.TotalBytes)
		}
	}

	expectedGen := uint64(goroutines * writesPerGoroutine)
	if reg.Generation() != expectedGen {
		t.Errorf("Generation = %d, want %d", reg.Generation(), expectedGen)
	}
}

func TestRegistryBuildDeltaSinceGeneration(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 100, 100, 1, "STANDARD") // gen 1
	reg.RecordWrite("b:2", 200, 200, 2, "STANDARD") // gen 2

	midGen := reg.Generation() // 2

	reg.RecordWrite("c:3", 300, 300, 3, "STANDARD") // gen 3
	reg.RecordWrite("a:1", 50, 50, 1, "STANDARD")   // gen 4 (updates a:1)

	delta := reg.BuildDelta(midGen)
	if len(delta.Tenants) != 2 {
		t.Errorf("delta has %d tenants, want 2 (c:3 and a:1)", len(delta.Tenants))
	}
	if _, ok := delta.Tenants["c:3"]; !ok {
		t.Error("delta missing tenant c:3")
	}
	if _, ok := delta.Tenants["a:1"]; !ok {
		t.Error("delta missing tenant a:1")
	}
	if _, ok := delta.Tenants["b:2"]; ok {
		t.Error("delta should NOT include b:2 (unchanged)")
	}
}

func TestRegistryGlobalAggregates(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 100, 200, 5, "STANDARD")
	reg.RecordWrite("b:2", 300, 500, 10, "GLACIER")
	reg.RecordWrite("a:1", 50, 100, 2, "GLACIER")

	gs := reg.GlobalAggregates()
	if gs.TenantCount != 2 {
		t.Errorf("TenantCount = %d, want 2", gs.TenantCount)
	}
	if gs.TotalBytes != 450 {
		t.Errorf("TotalBytes = %d, want 450", gs.TotalBytes)
	}
	if gs.RawBytes != 800 {
		t.Errorf("RawBytes = %d, want 800", gs.RawBytes)
	}
	if gs.TotalRows != 17 {
		t.Errorf("TotalRows = %d, want 17", gs.TotalRows)
	}
	if gs.TotalFiles != 3 {
		t.Errorf("TotalFiles = %d, want 3", gs.TotalFiles)
	}
	if gs.BytesByClass["STANDARD"] != 100 {
		t.Errorf("BytesByClass[STANDARD] = %d, want 100", gs.BytesByClass["STANDARD"])
	}
	if gs.BytesByClass["GLACIER"] != 350 {
		t.Errorf("BytesByClass[GLACIER] = %d, want 350", gs.BytesByClass["GLACIER"])
	}
}

func TestRegistryGetNonExistent(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	if ts := reg.Get("does:not-exist"); ts != nil {
		t.Errorf("expected nil for non-existent tenant, got %+v", ts)
	}
}

func TestRegistryEmptyDelta(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	delta := reg.BuildDelta(0)
	if len(delta.Tenants) != 0 {
		t.Errorf("empty registry delta has %d tenants, want 0", len(delta.Tenants))
	}
	if delta.NodeID != "node-1" {
		t.Errorf("delta NodeID = %q, want %q", delta.NodeID, "node-1")
	}
}

func TestRegistryMergeEmptyDelta(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 100, 100, 1, "STANDARD")

	genBefore := reg.Generation()
	tsBefore := reg.Get("a:1")

	// Merge nil delta.
	reg.Merge(nil)
	// Merge empty delta.
	reg.Merge(&TenantDelta{
		NodeID:  "node-2",
		Tenants: map[string]*TenantStats{},
	})

	if reg.Generation() != genBefore {
		t.Errorf("generation changed from %d to %d after empty merge", genBefore, reg.Generation())
	}
	tsAfter := reg.Get("a:1")
	if tsBefore.TotalBytes != tsAfter.TotalBytes {
		t.Error("TotalBytes changed after empty merge")
	}
}

func TestRegistryRecordWriteZeroValues(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("zero:tenant", 0, 0, 0, "")

	ts := reg.Get("zero:tenant")
	if ts == nil {
		t.Fatal("zero-value write should still create tenant")
	}
	if ts.TotalBytes != 0 {
		t.Errorf("TotalBytes = %d, want 0", ts.TotalBytes)
	}
	if ts.TotalFiles != 1 {
		t.Errorf("TotalFiles = %d, want 1 (file is still counted)", ts.TotalFiles)
	}
}

func TestRegistryMergeSameNodeIdempotent(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 1000, 2000, 10, "STANDARD")

	// Build and merge own delta (self-merge).
	delta := reg.BuildDelta(0)
	reg.Merge(delta)

	ts := reg.Get("a:1")
	// Should not double-count own bytes since nodeBytes["node-1"] uses max.
	if ts.TotalBytes != 1000 {
		t.Errorf("TotalBytes after self-merge = %d, want 1000", ts.TotalBytes)
	}
	if ts.TotalRows != 10 {
		t.Errorf("TotalRows after self-merge = %d, want 10", ts.TotalRows)
	}
	if ts.TotalFiles != 1 {
		t.Errorf("TotalFiles after self-merge = %d, want 1", ts.TotalFiles)
	}
}

func TestRegistryDeepCopyIsolation(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 1000, 2000, 10, "STANDARD")

	ts := reg.Get("a:1")
	// Mutate the returned copy.
	ts.TotalBytes = 999999
	ts.Labels["mutated"] = 42
	ts.BytesByClass["MUTATED"] = 99

	// Verify original is unchanged.
	original := reg.Get("a:1")
	if original.TotalBytes != 1000 {
		t.Errorf("TotalBytes mutated: got %d, want 1000", original.TotalBytes)
	}
	if _, ok := original.Labels["mutated"]; ok {
		t.Error("Labels map was mutated through returned copy")
	}
	if _, ok := original.BytesByClass["MUTATED"]; ok {
		t.Error("BytesByClass map was mutated through returned copy")
	}
}

func TestRegistryMergeNewTenant(t *testing.T) {
	regA := NewTenantRegistry("node-A")
	regA.RecordWrite("only-on-a:1", 500, 500, 5, "STANDARD")

	regB := NewTenantRegistry("node-B")
	regB.RecordWrite("only-on-b:1", 200, 200, 2, "STANDARD")

	deltaA := regA.BuildDelta(0)
	regB.Merge(deltaA)

	// B should now have both tenants.
	if regB.TenantCount() != 2 {
		t.Errorf("TenantCount = %d, want 2", regB.TenantCount())
	}
	ts := regB.Get("only-on-a:1")
	if ts == nil {
		t.Fatal("merged tenant only-on-a:1 not found in regB")
	}
	if ts.TotalBytes != 500 {
		t.Errorf("TotalBytes = %d, want 500", ts.TotalBytes)
	}
}

func TestRegistryMarshalSnapshotRoundTrip(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 1000, 2000, 10, "STANDARD")
	reg.RecordWrite("b:2", 500, 800, 5, "GLACIER")
	reg.RecordQuery("a:1")

	data, err := reg.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}

	// Verify it's valid JSON.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("snapshot is not valid JSON: %v", err)
	}

	// Load into a new registry.
	reg2 := NewTenantRegistry("node-2")
	if err := reg2.LoadSnapshot("node-1", data); err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	if reg2.TenantCount() != 2 {
		t.Errorf("loaded TenantCount = %d, want 2", reg2.TenantCount())
	}

	ts := reg2.Get("a:1")
	if ts == nil {
		t.Fatal("loaded tenant a:1 not found")
	}
	if ts.TotalBytes != 1000 {
		t.Errorf("loaded TotalBytes = %d, want 1000", ts.TotalBytes)
	}
	if ts.TotalRows != 10 {
		t.Errorf("loaded TotalRows = %d, want 10", ts.TotalRows)
	}
}

func TestRegistryThreeNodeConvergence(t *testing.T) {
	// Three nodes each write to the same tenant.
	regA := NewTenantRegistry("node-A")
	regB := NewTenantRegistry("node-B")
	regC := NewTenantRegistry("node-C")

	regA.RecordWrite("shared:t", 100, 200, 1, "STANDARD")
	regB.RecordWrite("shared:t", 200, 300, 2, "STANDARD")
	regC.RecordWrite("shared:t", 300, 400, 3, "STANDARD")

	// Pairwise merge: A->B, B->C, C->A, A->B, B->C (full gossip round).
	deltaA := regA.BuildDelta(0)
	deltaB := regB.BuildDelta(0)
	deltaC := regC.BuildDelta(0)

	regB.Merge(deltaA)
	regC.Merge(deltaB)
	regA.Merge(deltaC)

	// Second round to propagate fully.
	deltaA2 := regA.BuildDelta(0)
	deltaB2 := regB.BuildDelta(0)
	deltaC2 := regC.BuildDelta(0)

	regB.Merge(deltaC2)
	regC.Merge(deltaA2)
	regA.Merge(deltaB2)

	// All three should converge to the same totals.
	expected := int64(600) // 100 + 200 + 300
	for name, reg := range map[string]*TenantRegistry{"A": regA, "B": regB, "C": regC} {
		ts := reg.Get("shared:t")
		if ts == nil {
			t.Fatalf("node %s: tenant not found", name)
		}
		if ts.TotalBytes != expected {
			t.Errorf("node %s: TotalBytes = %d, want %d", name, ts.TotalBytes, expected)
		}
		if ts.TotalRows != 6 {
			t.Errorf("node %s: TotalRows = %d, want 6", name, ts.TotalRows)
		}
		if ts.TotalFiles != 3 {
			t.Errorf("node %s: TotalFiles = %d, want 3", name, ts.TotalFiles)
		}
	}
}
