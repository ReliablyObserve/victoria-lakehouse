package smartcache

import (
	"testing"
	"time"
)

func TestSizingCalculator_IngestionBased(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})

	calc.RecordIngestion(1024 * 1024 * 1024) // 1GB in this interval
	calc.SetIngestionInterval(1 * time.Hour)

	est := calc.IngestionEstimate()
	// 1GB/hour * 24h = 24GB
	expected := int64(24 * 1024 * 1024 * 1024)
	tolerance := int64(float64(expected) * 0.01)
	if est < expected-tolerance || est > expected+tolerance {
		t.Errorf("ingestion estimate = %d, want ~%d", est, expected)
	}
}

func TestSizingCalculator_QueryBased(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})

	for i := 0; i < 10; i++ {
		calc.RecordQueryRead(int64(i), 100*1024*1024)
	}

	est := calc.QueryEstimate()
	expected := int64(10 * 100 * 1024 * 1024)
	if est != expected {
		t.Errorf("query estimate = %d, want %d", est, expected)
	}
}

func TestSizingCalculator_QueryBased_Deduplicates(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})

	for i := 0; i < 5; i++ {
		calc.RecordQueryRead(42, 100*1024*1024)
	}

	est := calc.QueryEstimate()
	expected := int64(100 * 1024 * 1024)
	if est != expected {
		t.Errorf("query estimate = %d, want %d (should deduplicate)", est, expected)
	}
}

func TestSizingCalculator_BlendedEstimate(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})

	calc.RecordIngestion(2 * 1024 * 1024 * 1024)
	calc.SetIngestionInterval(1 * time.Hour)

	for i := 0; i < 5; i++ {
		calc.RecordQueryRead(int64(i), 200*1024*1024)
	}

	est0 := calc.BlendedEstimate(0)
	ingEst := calc.IngestionEstimate()
	if est0 != ingEst {
		t.Errorf("blended at hour 0 = %d, want ingestion estimate %d", est0, ingEst)
	}

	est12 := calc.BlendedEstimate(12 * time.Hour)
	qEst := calc.QueryEstimate()
	if est12 != qEst {
		t.Errorf("blended at hour 12 = %d, want query estimate %d", est12, qEst)
	}

	est6 := calc.BlendedEstimate(6 * time.Hour)
	if est6 <= qEst || est6 >= ingEst {
		t.Errorf("blended at hour 6 = %d, expected between %d and %d", est6, qEst, ingEst)
	}
}

func TestSizingCalculator_FleetDivision(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})

	calc.RecordIngestion(3 * 1024 * 1024 * 1024)
	calc.SetIngestionInterval(1 * time.Hour)

	single := calc.RecommendedPerNode(0, 1)
	perNode := calc.RecommendedPerNode(0, 3)

	expected := single / 3
	tolerance := int64(float64(expected) * 0.01)
	if perNode < expected-tolerance || perNode > expected+tolerance {
		t.Errorf("per-node estimate = %d, want ~%d", perNode, expected)
	}
}

func TestSizingCalculator_ZeroTargetHours_DefaultsTo24(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 0})
	calc.RecordIngestion(1024 * 1024 * 1024)
	calc.SetIngestionInterval(1 * time.Hour)
	est := calc.IngestionEstimate()
	expected := int64(24 * 1024 * 1024 * 1024)
	tolerance := int64(float64(expected) * 0.01)
	if est < expected-tolerance || est > expected+tolerance {
		t.Errorf("with zero target hours, estimate = %d, want ~%d (should default to 24)", est, expected)
	}
}

func TestSizingCalculator_NoData_ZeroEstimates(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})
	if calc.IngestionEstimate() != 0 {
		t.Errorf("empty ingestion estimate should be 0")
	}
	if calc.QueryEstimate() != 0 {
		t.Errorf("empty query estimate should be 0")
	}
	if calc.BlendedEstimate(time.Hour) != 0 {
		t.Errorf("empty blended estimate should be 0")
	}
}

func TestSizingCalculator_OnlyIngestion_BlendedReturnsIngestion(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})
	calc.RecordIngestion(1024 * 1024 * 1024)
	calc.SetIngestionInterval(1 * time.Hour)
	// No query reads recorded
	blended := calc.BlendedEstimate(6 * time.Hour)
	ingEst := calc.IngestionEstimate()
	if blended != ingEst {
		t.Errorf("blended with only ingestion = %d, want %d", blended, ingEst)
	}
}

func TestSizingCalculator_OnlyQuery_BlendedReturnsQuery(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})
	// No ingestion recorded
	calc.RecordQueryRead(1, 100*1024*1024)
	blended := calc.BlendedEstimate(6 * time.Hour)
	qEst := calc.QueryEstimate()
	if blended != qEst {
		t.Errorf("blended with only query = %d, want %d", blended, qEst)
	}
}

func TestSizingCalculator_RecommendedPerNode_SingleNode(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})
	calc.RecordIngestion(1024 * 1024 * 1024)
	calc.SetIngestionInterval(1 * time.Hour)
	single := calc.RecommendedPerNode(0, 1)
	total := calc.BlendedEstimate(0)
	if single != total {
		t.Errorf("single node should get full estimate: got %d, want %d", single, total)
	}
}
