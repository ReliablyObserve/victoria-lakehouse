package prefetch

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type Type int

const (
	TypeCorrelated Type = iota
	TypeReadAhead
	TypeWarmup
)

func (t Type) String() string {
	switch t {
	case TypeCorrelated:
		return "correlated"
	case TypeReadAhead:
		return "read_ahead"
	case TypeWarmup:
		return "warmup"
	default:
		return "unknown"
	}
}

type Task struct {
	Key       string
	Type      Type
	Priority  int
	CreatedAt time.Time
}

type FetchFunc func(ctx context.Context, key string) error

type Engine struct {
	mu            sync.Mutex
	queue         []Task
	maxQueue      int
	maxConcurrent int
	active        atomic.Int32
	closed        bool
	fetchFn       FetchFunc
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup

	triggered atomic.Int64
	completed atomic.Int64
	errors    atomic.Int64
	useful    atomic.Int64

	usefulKeys sync.Map
}

func NewEngine(maxConcurrent, maxQueue int, fetchFn FetchFunc) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		maxQueue:      maxQueue,
		maxConcurrent: maxConcurrent,
		fetchFn:       fetchFn,
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (e *Engine) Enqueue(task Task) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return false
	}

	for _, t := range e.queue {
		if t.Key == task.Key {
			return false
		}
	}

	if len(e.queue) >= e.maxQueue {
		e.queue = e.queue[1:]
	}

	task.CreatedAt = time.Now()
	e.queue = append(e.queue, task)
	e.triggered.Add(1)

	e.startWorkerLocked()
	return true
}

func (e *Engine) EnqueueCorrelated(keys []string) int {
	enqueued := 0
	for _, key := range keys {
		if e.Enqueue(Task{Key: key, Type: TypeCorrelated, Priority: 1}) {
			enqueued++
		}
	}
	return enqueued
}

func (e *Engine) EnqueueReadAhead(keys []string) int {
	enqueued := 0
	for _, key := range keys {
		if e.Enqueue(Task{Key: key, Type: TypeReadAhead, Priority: 2}) {
			enqueued++
		}
	}
	return enqueued
}

func (e *Engine) EnqueueWarmup(keys []string) int {
	enqueued := 0
	for _, key := range keys {
		if e.Enqueue(Task{Key: key, Type: TypeWarmup, Priority: 3}) {
			enqueued++
		}
	}
	return enqueued
}

func (e *Engine) MarkUseful(key string) {
	if _, loaded := e.usefulKeys.LoadAndDelete(key); loaded {
		e.useful.Add(1)
	}
}

func (e *Engine) startWorkerLocked() {
	if int(e.active.Load()) >= e.maxConcurrent {
		return
	}

	task, ok := e.dequeue()
	if !ok {
		return
	}

	e.active.Add(1)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer e.active.Add(-1)
		e.processTask(task)

		e.mu.Lock()
		e.startWorkerLocked()
		e.mu.Unlock()
	}()
}

func (e *Engine) dequeue() (Task, bool) {
	if len(e.queue) == 0 {
		return Task{}, false
	}
	task := e.queue[0]
	e.queue = e.queue[1:]
	return task, true
}

func (e *Engine) processTask(task Task) {
	if err := e.ctx.Err(); err != nil {
		return
	}

	e.usefulKeys.Store(task.Key, struct{}{})

	if err := e.fetchFn(e.ctx, task.Key); err != nil {
		e.errors.Add(1)
		e.usefulKeys.Delete(task.Key)
		if e.ctx.Err() == nil {
			logger.Infof("prefetch failed; key=%s, type=%v, error=%s", task.Key, task.Type, err)
		}
		return
	}

	e.completed.Add(1)
	logger.Infof("prefetch complete; key=%s, type=%v", task.Key, task.Type)
}

func (e *Engine) QueueLen() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.queue)
}

func (e *Engine) Active() int {
	return int(e.active.Load())
}

func (e *Engine) Stats() (triggered, completed, errors, useful int64) {
	return e.triggered.Load(), e.completed.Load(), e.errors.Load(), e.useful.Load()
}

func (e *Engine) Close() {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	e.cancel()
	e.wg.Wait()
}
