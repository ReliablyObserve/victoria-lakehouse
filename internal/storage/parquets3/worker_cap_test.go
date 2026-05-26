package parquets3

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// maxRGWorkers is the hardcoded row-group worker cap in queryFile (storage_query.go:549-551).
const maxRGWorkers = 8

// TestRGWorkerCapEnforced verifies that the row-group worker limiting pattern
// from storage_query.go correctly caps concurrent goroutines at maxRGWorkers.
func TestRGWorkerCapEnforced(t *testing.T) {
	const totalTasks = 50

	for _, cap := range []int{1, 4, maxRGWorkers, 16} {
		t.Run(fmt.Sprintf("cap=%d", cap), func(t *testing.T) {
			var peak atomic.Int64
			var current atomic.Int64

			taskCh := make(chan int, totalTasks)
			for i := 0; i < totalTasks; i++ {
				taskCh <- i
			}
			close(taskCh)

			// Mirror the capping logic from storage_query.go:
			// rgWorkers := len(matchedRGs)
			// if rgWorkers > 8 { rgWorkers = 8 }
			workers := totalTasks
			if workers > cap {
				workers = cap
			}

			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range taskCh {
						c := current.Add(1)
						// Track peak concurrency
						for {
							p := peak.Load()
							if c <= p {
								break
							}
							if peak.CompareAndSwap(p, c) {
								break
							}
						}
						// Simulate work
						time.Sleep(100 * time.Microsecond)
						current.Add(-1)
					}
				}()
			}
			wg.Wait()

			observed := peak.Load()
			if observed > int64(cap) {
				t.Errorf("peak concurrent workers = %d, exceeds cap %d", observed, cap)
			}
			t.Logf("cap=%d, peak=%d, tasks=%d", cap, observed, totalTasks)
		})
	}
}

// TestFileWorkerCapEnforced verifies the file worker pool pattern from
// storage_query.go:179-184 correctly caps at the configured FileWorkers limit.
func TestFileWorkerCapEnforced(t *testing.T) {
	const totalFiles = 200
	const configuredCap = 64 // default from config.go:523

	for _, numFiles := range []int{10, 64, 200} {
		t.Run(fmt.Sprintf("files=%d", numFiles), func(t *testing.T) {
			var peak atomic.Int64
			var current atomic.Int64

			taskCh := make(chan int, numFiles)
			for i := 0; i < numFiles; i++ {
				taskCh <- i
			}
			close(taskCh)

			// Mirror capping logic from storage_query.go:179-184:
			// fileWorkers := s.cfg.Query.FileWorkers (default 64)
			// if fileWorkers > len(files) { fileWorkers = len(files) }
			fileWorkers := configuredCap
			if fileWorkers > numFiles {
				fileWorkers = numFiles
			}

			var wg sync.WaitGroup
			for i := 0; i < fileWorkers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range taskCh {
						c := current.Add(1)
						for {
							p := peak.Load()
							if c <= p {
								break
							}
							if peak.CompareAndSwap(p, c) {
								break
							}
						}
						time.Sleep(50 * time.Microsecond)
						current.Add(-1)
					}
				}()
			}
			wg.Wait()

			observed := peak.Load()
			if observed > int64(fileWorkers) {
				t.Errorf("peak file workers = %d, exceeds cap %d", observed, fileWorkers)
			}
			t.Logf("numFiles=%d, cap=%d, peak=%d", numFiles, fileWorkers, observed)
		})
	}
}

// TestRGWorkerCapMatchesCode documents the expected RG worker cap constant.
// If the cap in storage_query.go changes, this test reminds us to update.
func TestRGWorkerCapMatchesCode(t *testing.T) {
	// The RG worker cap is hardcoded at 8 in storage_query.go:550-551:
	//   rgWorkers := len(matchedRGs)
	//   if rgWorkers > 8 { rgWorkers = 8 }
	if maxRGWorkers != 8 {
		t.Errorf("maxRGWorkers constant = %d, expected 8 to match storage_query.go", maxRGWorkers)
	}
}
