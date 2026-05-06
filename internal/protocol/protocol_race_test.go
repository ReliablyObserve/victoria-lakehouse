package protocol

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestMarshalUnmarshal_Race_MaxGoroutines(t *testing.T) {
	const goroutines = 300
	const ops = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				rows := rng.Intn(50) + 1
				cols := rng.Intn(5) + 1
				blockCols := make([]logstorage.BlockColumn, cols)
				for c := range blockCols {
					blockCols[c].Name = fmt.Sprintf("col-%d", c)
					vals := make([]string, rows)
					for r := range vals {
						vals[r] = fmt.Sprintf("v-%d-%d-%d", id, i, r)
					}
					blockCols[c].Values = vals
				}
				db := makeDataBlock(blockCols)
				data := MarshalDataBlock(db)
				got, err := UnmarshalDataBlock(data)
				if err != nil {
					t.Errorf("unmarshal error: %v", err)
					return
				}
				if got.RowsCount() != db.RowsCount() {
					t.Errorf("rows mismatch: got %d want %d", got.RowsCount(), db.RowsCount())
				}
				if i%50 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestValueWithHits_Race_MaxGoroutines(t *testing.T) {
	const goroutines = 300
	const ops = 300
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				count := rng.Intn(50) + 1
				vals := make([]logstorage.ValueWithHits, count)
				for j := range vals {
					vals[j] = logstorage.ValueWithHits{
						Value: fmt.Sprintf("v-%d-%d", id, j),
						Hits:  uint64(rng.Int63()),
					}
				}
				data := MarshalValueWithHits(vals)
				got, err := UnmarshalValueWithHits(data)
				if err != nil {
					t.Errorf("unmarshal error: %v", err)
					return
				}
				if len(got) != count {
					t.Errorf("count mismatch: got %d want %d", len(got), count)
				}
				if i%50 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestStream_Race_MaxGoroutines(t *testing.T) {
	const goroutines = 200
	const ops = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				rows := rng.Intn(20) + 1
				vals := make([]string, rows)
				for r := range vals {
					vals[r] = fmt.Sprintf("val-%d", r)
				}
				db := makeDataBlock([]logstorage.BlockColumn{
					{Name: "col", Values: vals},
				})

				var buf bytes.Buffer
				if err := WriteDataBlockStream(&buf, db); err != nil {
					t.Errorf("write error: %v", err)
					return
				}
				got, err := ReadDataBlockStream(&buf)
				if err != nil {
					t.Errorf("read error: %v", err)
					return
				}
				if got.RowsCount() != db.RowsCount() {
					t.Errorf("rows mismatch: got %d want %d", got.RowsCount(), db.RowsCount())
				}
				if i%25 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestTenantIDs_Race_MaxGoroutines(t *testing.T) {
	const goroutines = 300
	const ops = 300
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				count := rng.Intn(50) + 1
				ids := make([]logstorage.TenantID, count)
				for j := range ids {
					ids[j] = logstorage.TenantID{
						AccountID: uint32(rng.Int31()),
						ProjectID: uint32(rng.Int31()),
					}
				}
				data := MarshalTenantIDs(ids)
				got, err := UnmarshalTenantIDs(data)
				if err != nil {
					t.Errorf("unmarshal error: %v", err)
					return
				}
				if len(got) != count {
					t.Errorf("count mismatch: got %d want %d", len(got), count)
				}
				if i%50 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()
}
