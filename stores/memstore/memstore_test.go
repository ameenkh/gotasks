package memstore_test

// Acceptance: a real manager running end-to-end on the public memstore —
// the exact usage pattern a library user's unit tests follow. No MongoDB.

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/ameenkh/gotasks/stores/memstore"
)

func newManager(t *testing.T, st *memstore.Store, opts ...gotasks.Option) *gotasks.Manager {
	t.Helper()
	base := []gotasks.Option{
		gotasks.WithQueues(gotasks.QueuePolicy{Name: "jobs"}),
		gotasks.WithWorkers(2),
		gotasks.WithPollInterval(5 * time.Millisecond),
		gotasks.WithBackoff(gotasks.FixedBackoff(0)),
		gotasks.WithJanitor(gotasks.JanitorConfig{ReapInterval: 20 * time.Millisecond}),
	}
	m, err := gotasks.New(st, append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

func waitStatus(t *testing.T, st *memstore.Store, id string, want gotasks.Status) *gotasks.Task {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if task := st.Task(id); task != nil && task.Status == want {
			return task
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("task %s never reached %q (last: %+v)", id, want, st.Task(id))
	return nil
}

type payload struct {
	N int `json:"n"`
}

func TestEndToEndOnMemstore(t *testing.T) {
	st := memstore.New()
	m := newManager(t, st)

	var calls atomic.Int32
	err := gotasks.RegisterHandler(m, "work",
		func(ctx context.Context, task *gotasks.Task, p payload) (any, error) {
			if calls.Add(1) == 1 {
				return nil, errors.New("transient") // first attempt fails
			}
			return map[string]int{"n": p.N * 2}, nil
		})
	if err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	ctx := context.Background()
	id, err := gotasks.Enqueue(ctx, m, "jobs", "work", payload{N: 21})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = m.Stop(sctx)
	})

	task := waitStatus(t, st, id, gotasks.StatusDone)
	if task.Attempts != 2 || len(task.Errors) != 1 {
		t.Errorf("attempts=%d errors=%d, want 2/1 (retry exercised)", task.Attempts, len(task.Errors))
	}
	if !strings.Contains(string(task.Result), `"n":42`) {
		t.Errorf("result = %s, want n:42", task.Result)
	}

	// Unique keys and the drift guard work here too.
	id1, err := gotasks.Enqueue(ctx, m, "jobs", "work", payload{N: 1}, gotasks.TaskPolicy{UniqueKey: "k"})
	if err != nil {
		t.Fatalf("unique enqueue: %v", err)
	}
	if id2, err := gotasks.Enqueue(ctx, m, "jobs", "work", payload{N: 2}, gotasks.TaskPolicy{UniqueKey: "k"}); !errors.Is(err, gotasks.ErrDuplicateTask) || id2 != id1 {
		t.Errorf("dedup: id=%q err=%v", id2, err)
	}
	m2, err := gotasks.New(st, gotasks.WithQueues(gotasks.QueuePolicy{Name: "jobs", TTL: time.Hour}))
	if err != nil {
		t.Fatalf("New m2: %v", err)
	}
	if _, err := gotasks.Enqueue(ctx, m2, "jobs", "work", payload{N: 3}); !errors.Is(err, gotasks.ErrQueueConflict) {
		t.Errorf("registry drift guard: %v", err)
	}
}

func TestAtMostOnceAndRequeueOnMemstore(t *testing.T) {
	st := memstore.New()
	m := newManager(t, st)
	var calls atomic.Int32
	_ = gotasks.RegisterHandler(m, "once",
		func(ctx context.Context, task *gotasks.Task, _ struct{}) (any, error) {
			calls.Add(1)
			return nil, errors.New("boom")
		})
	ctx := context.Background()
	id, _ := gotasks.Enqueue(ctx, m, "jobs", "once", struct{}{}, gotasks.TaskPolicy{MaxAttempts: 1})
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = m.Stop(sctx)
	})

	waitStatus(t, st, id, gotasks.StatusDead)
	if calls.Load() != 1 {
		t.Errorf("at-most-once ran %d times", calls.Load())
	}
	if err := m.Requeue(ctx, id); err != nil {
		t.Fatalf("Requeue: %v", err)
	}
	task := waitStatus(t, st, id, gotasks.StatusDead) // dies again, fresh budget
	if len(task.Errors) != 2 {
		t.Errorf("errors after requeue = %d, want 2 (history kept)", len(task.Errors))
	}
}
