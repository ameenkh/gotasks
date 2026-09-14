package gotasks

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testManager(t *testing.T, store Store, opts ...Option) *Manager {
	t.Helper()
	base := []Option{
		WithWorkers(2),
		WithPollInterval(5 * time.Millisecond),
		WithLeaseTime(time.Second),
		WithBackoff(FixedBackoff(0)),
		WithReapInterval(20 * time.Millisecond),
	}
	m, err := New(store, append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

func startManager(t *testing.T, m *Manager) {
	t.Helper()
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = m.Stop(ctx)
	})
}

func waitForStatus(t *testing.T, fs *fakeStore, id string, want Status) *Task {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if task := fs.get(id); task != nil && task.Status == want {
			return task
		}
		time.Sleep(5 * time.Millisecond)
	}
	got := fs.get(id)
	t.Fatalf("task %s never reached %q (last: %+v)", id, want, got)
	return nil
}

type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func TestEnqueueAndComplete(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs)

	var got atomic.Value
	err := RegisterHandler(m, "email", func(ctx context.Context, task *Task, p emailPayload) (any, error) {
		got.Store(p)
		return map[string]string{"message_id": "abc-123"}, nil
	})
	if err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}

	id, err := Enqueue(context.Background(), m, "email", emailPayload{To: "a@b.c", Subject: "hi"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	startManager(t, m)

	task := waitForStatus(t, fs, id, StatusDone)
	if p, _ := got.Load().(emailPayload); p.To != "a@b.c" || p.Subject != "hi" {
		t.Errorf("payload mismatch: %+v", p)
	}
	if task.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", task.Attempts)
	}
	if !strings.Contains(string(task.Result), "abc-123") {
		t.Errorf("result not stored: %s", task.Result)
	}
}

func TestRetryThenTerminalFailure(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs)

	var calls atomic.Int32
	_ = RegisterHandler(m, "flaky", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		calls.Add(1)
		return nil, errors.New("boom")
	})

	id, err := Enqueue(context.Background(), m, "flaky", struct{}{}, WithMaxAttempts(2))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	startManager(t, m)

	task := waitForStatus(t, fs, id, StatusFailed)
	if task.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", task.Attempts)
	}
	if len(task.Errors) != 2 {
		t.Fatalf("errors = %d, want 2: %+v", len(task.Errors), task.Errors)
	}
	if task.Errors[0].Message != "boom" || task.Errors[0].Attempt != 1 || task.Errors[1].Attempt != 2 {
		t.Errorf("bad error entries: %+v", task.Errors)
	}
	if calls.Load() != 2 {
		t.Errorf("handler calls = %d, want 2", calls.Load())
	}
}

func TestRetrySucceedsSecondAttempt(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs)

	var calls atomic.Int32
	_ = RegisterHandler(m, "flaky", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("transient")
		}
		return nil, nil
	})

	id, _ := Enqueue(context.Background(), m, "flaky", struct{}{}, WithMaxAttempts(3))
	startManager(t, m)

	task := waitForStatus(t, fs, id, StatusDone)
	if task.Attempts != 2 || len(task.Errors) != 1 {
		t.Errorf("attempts=%d errors=%d, want 2/1", task.Attempts, len(task.Errors))
	}
}

func TestPanicIsAFailedAttempt(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs)

	_ = RegisterHandler(m, "panicky", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		panic("kaboom")
	})

	id, _ := Enqueue(context.Background(), m, "panicky", struct{}{}, WithMaxAttempts(1))
	startManager(t, m)

	task := waitForStatus(t, fs, id, StatusFailed)
	if len(task.Errors) != 1 || !strings.Contains(task.Errors[0].Message, "kaboom") {
		t.Errorf("panic not recorded: %+v", task.Errors)
	}
}

func TestHandlerTimeout(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs)

	_ = RegisterHandler(m, "slow", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, WithHandlerTimeout(30*time.Millisecond))

	id, _ := Enqueue(context.Background(), m, "slow", struct{}{}, WithMaxAttempts(1))
	startManager(t, m)

	task := waitForStatus(t, fs, id, StatusFailed)
	if !strings.Contains(task.Errors[0].Message, "deadline") {
		t.Errorf("expected deadline error, got: %+v", task.Errors)
	}
}

func TestScheduledTaskWaitsForRunAt(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs)

	_ = RegisterHandler(m, "later", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		return nil, nil
	})

	id, _ := Enqueue(context.Background(), m, "later", struct{}{}, WithDelay(150*time.Millisecond))
	startManager(t, m)

	time.Sleep(75 * time.Millisecond)
	if task := fs.get(id); task.Status != StatusPending {
		t.Fatalf("ran before run_at: %+v", task)
	}
	waitForStatus(t, fs, id, StatusDone)
}

func TestStaleTaskReclaimedThenCompletes(t *testing.T) {
	fs := newFakeStore()
	// First claim it directly (simulating a dead worker) with a tiny lease.
	ctx := context.Background()
	ids, err := fs.Enqueue(ctx, []*Task{{
		Type: "job", Status: StatusPending, MaxAttempts: 3,
		RunAt: time.Now().UTC(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	dead, err := fs.Claim(ctx, "dead-worker", nil, time.Millisecond)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	time.Sleep(10 * time.Millisecond) // let the lease expire

	m := testManager(t, fs)
	_ = RegisterHandler(m, "job", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		return nil, nil
	})
	startManager(t, m)

	task := waitForStatus(t, fs, ids[0], StatusDone)
	if task.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 (dead claim + reclaim)", task.Attempts)
	}
	// The dead worker's fenced write must fail: token rotated on reclaim.
	if err := fs.Complete(ctx, dead.ID, dead.LeaseToken, nil); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("stale worker write: got %v, want ErrLeaseLost", err)
	}
}

func TestAtMostOnceStaleIsReapedNotRetried(t *testing.T) {
	fs := newFakeStore()
	ctx := context.Background()
	ids, _ := fs.Enqueue(ctx, []*Task{{
		Type: "once", Status: StatusPending, MaxAttempts: 1,
		RunAt: time.Now().UTC(), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}})
	if _, err := fs.Claim(ctx, "dead-worker", nil, time.Millisecond); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	var calls atomic.Int32
	m := testManager(t, fs)
	_ = RegisterHandler(m, "once", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		calls.Add(1)
		return nil, nil
	})
	startManager(t, m)

	task := waitForStatus(t, fs, ids[0], StatusFailed) // reaper, not a worker
	if calls.Load() != 0 {
		t.Errorf("at-most-once task was re-run %d times", calls.Load())
	}
	if len(task.Errors) != 1 || !strings.Contains(task.Errors[0].Message, "lease expired") {
		t.Errorf("expected lease-expired error, got: %+v", task.Errors)
	}
}

func TestEnqueueManyAndTypeRestriction(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs)

	var done atomic.Int32
	_ = RegisterHandler(m, "bulk", func(ctx context.Context, task *Task, p map[string]int) (any, error) {
		done.Add(1)
		return nil, nil
	})

	payloads := []map[string]int{{"n": 1}, {"n": 2}, {"n": 3}}
	ids, err := EnqueueMany(context.Background(), m, "bulk", payloads)
	if err != nil || len(ids) != 3 {
		t.Fatalf("EnqueueMany: ids=%v err=%v", ids, err)
	}
	// A task type with no registered handler must never be claimed.
	otherID, _ := Enqueue(context.Background(), m, "other-service-type", struct{}{})
	startManager(t, m)

	for _, id := range ids {
		waitForStatus(t, fs, id, StatusDone)
	}
	time.Sleep(30 * time.Millisecond)
	if task := fs.get(otherID); task.Status != StatusPending || task.Attempts != 0 {
		t.Errorf("foreign task touched: %+v", task)
	}
}

func TestPayloadMustBeObject(t *testing.T) {
	m := testManager(t, newFakeStore())
	if _, err := Enqueue(context.Background(), m, "t", "just a string"); err == nil {
		t.Fatal("expected error for non-object payload")
	}
	if _, err := Enqueue(context.Background(), m, "t", 42); err == nil {
		t.Fatal("expected error for numeric payload")
	}
}

func TestRegisterAfterStartFails(t *testing.T) {
	m := testManager(t, newFakeStore())
	_ = RegisterHandler(m, "a", func(ctx context.Context, task *Task, _ struct{}) (any, error) { return nil, nil })
	startManager(t, m)
	if err := RegisterHandler(m, "b", func(ctx context.Context, task *Task, _ struct{}) (any, error) { return nil, nil }); err == nil {
		t.Fatal("expected error registering after Start")
	}
}

func TestStopDrainsInFlight(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs)

	started := make(chan struct{})
	release := make(chan struct{})
	_ = RegisterHandler(m, "slow", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		close(started)
		<-release
		return nil, nil
	})

	id, _ := Enqueue(context.Background(), m, "slow", struct{}{})
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-started

	stopDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		stopDone <- m.Stop(ctx)
	}()
	time.Sleep(20 * time.Millisecond) // Stop is now waiting on the in-flight task
	close(release)
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if task := fs.get(id); task.Status != StatusDone {
		t.Errorf("in-flight task not drained: %+v", task)
	}
}

func TestBackoffStrategies(t *testing.T) {
	if d := FixedBackoff(time.Second).Next(5); d != time.Second {
		t.Errorf("fixed: %v", d)
	}
	if d := LinearBackoff(10 * time.Second).Next(3); d != 30*time.Second {
		t.Errorf("linear: %v", d)
	}
	exp := ExponentialBackoff(time.Second, 10*time.Second)
	for attempt, base := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 10: 10 * time.Second} {
		d := exp.Next(attempt)
		if d < base || d > base+base/4 {
			t.Errorf("exp attempt %d: %v outside [%v, %v]", attempt, d, base, base+base/4)
		}
	}
}
