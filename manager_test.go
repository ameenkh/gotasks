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

	task := waitForStatus(t, fs, id, StatusDead)
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

	task := waitForStatus(t, fs, id, StatusDead)
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

	task := waitForStatus(t, fs, id, StatusDead)
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
	dead, err := fs.Claim(ctx, ClaimOptions{WorkerID: "dead-worker", Lease: time.Millisecond})
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
	if err := fs.Complete(ctx, dead, nil); !errors.Is(err, ErrLeaseLost) {
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
	if _, err := fs.Claim(ctx, ClaimOptions{WorkerID: "dead-worker", Lease: time.Millisecond}); err != nil {
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

	task := waitForStatus(t, fs, ids[0], StatusDead) // reaper, not a worker
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

func TestHeartbeatKeepsLongHandlerAlive(t *testing.T) {
	fs := newFakeStore()
	// Lease far shorter than the handler; the opted-in heartbeat (auto =
	// lease/3) must keep extending it so no other worker reclaims the task.
	m := testManager(t, fs, WithLeaseTime(60*time.Millisecond), WithHeartbeat())

	_ = RegisterHandler(m, "long", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		time.Sleep(250 * time.Millisecond)
		return nil, nil
	})

	id, _ := Enqueue(context.Background(), m, "long", struct{}{}, WithMaxAttempts(3))
	startManager(t, m)

	task := waitForStatus(t, fs, id, StatusDone)
	if task.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 (heartbeat should prevent reclaim)", task.Attempts)
	}
	if len(task.Errors) != 0 {
		t.Errorf("unexpected errors: %+v", task.Errors)
	}
}

func TestNoHeartbeatByDefaultLongHandlerIsReclaimed(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs, WithLeaseTime(40*time.Millisecond), WithWorkers(2))

	var calls atomic.Int32
	release := make(chan struct{})
	_ = RegisterHandler(m, "long", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		if calls.Add(1) == 1 {
			<-release // first run outlives its lease
			return nil, errors.New("stale worker finished late")
		}
		return nil, nil
	})

	id, _ := Enqueue(context.Background(), m, "long", struct{}{}, WithMaxAttempts(3))
	startManager(t, m)

	task := waitForStatus(t, fs, id, StatusDone) // second worker reclaims and finishes
	if task.Attempts < 2 {
		t.Errorf("attempts = %d, want >= 2 (reclaim expected without heartbeat)", task.Attempts)
	}
	close(release)
}

func TestHeartbeatLeaseLossCancelsHandler(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs,
		WithLeaseTime(500*time.Millisecond),
		WithHeartbeatInterval(10*time.Millisecond))

	cancelled := make(chan struct{})
	_ = RegisterHandler(m, "doomed", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		fs.forceLeaseLost.Store(true) // now every heartbeat sees ErrLeaseLost
		select {
		case <-ctx.Done():
			close(cancelled)
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
			return nil, errors.New("handler was never cancelled")
		}
	})

	_, _ = Enqueue(context.Background(), m, "doomed", struct{}{}, WithMaxAttempts(1))
	startManager(t, m)

	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("handler context was not cancelled on lease loss")
	}
	fs.forceLeaseLost.Store(false)
}

func TestDeadTaskRequeueRunsAgain(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs)

	var calls atomic.Int32
	_ = RegisterHandler(m, "flaky", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		if calls.Add(1) <= 2 {
			return nil, errors.New("boom")
		}
		return nil, nil
	})

	id, _ := Enqueue(context.Background(), m, "flaky", struct{}{}, WithMaxAttempts(2))
	startManager(t, m)

	waitForStatus(t, fs, id, StatusDead)
	if err := m.Requeue(context.Background(), id); err != nil {
		t.Fatalf("Requeue: %v", err)
	}
	task := waitForStatus(t, fs, id, StatusDone)
	if task.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 (requeue resets attempts)", task.Attempts)
	}
	if len(task.Errors) != 2 {
		t.Errorf("error history lost: %d entries, want 2", len(task.Errors))
	}
	// Requeueing a non-dead task must fail.
	if err := m.Requeue(context.Background(), id); !errors.Is(err, ErrNotFound) {
		t.Errorf("requeue done task: got %v, want ErrNotFound", err)
	}
}

func TestRequeueDeadByType(t *testing.T) {
	fs := newFakeStore()
	ctx := context.Background()
	now := time.Now().UTC()
	mk := func(taskType string) string {
		ids, _ := fs.Enqueue(ctx, []*Task{{
			Type: taskType, Status: StatusDead, MaxAttempts: 1, Attempts: 1,
			RunAt: now, CreatedAt: now, UpdatedAt: now,
		}})
		return ids[0]
	}
	aID, b1ID, b2ID := mk("a"), mk("b"), mk("b")

	m := testManager(t, fs)
	n, err := m.RequeueDead(ctx, "b")
	if err != nil || n != 2 {
		t.Fatalf("RequeueDead(b) = %d, %v; want 2, nil", n, err)
	}
	if fs.get(aID).Status != StatusDead {
		t.Error("type-a task requeued by mistake")
	}
	if fs.get(b1ID).Status != StatusPending || fs.get(b2ID).Status != StatusPending {
		t.Error("type-b tasks not requeued")
	}
	if n, _ := m.RequeueDead(ctx, ""); n != 1 { // remaining dead: the type-a one
		t.Errorf("RequeueDead(all) = %d, want 1", n)
	}
}

func TestUniqueKeyDedup(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs)
	ctx := context.Background()

	_ = RegisterHandler(m, "sync", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		return nil, nil
	})

	id1, err := Enqueue(ctx, m, "sync", struct{}{}, WithUniqueKey("tenant-42"))
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	// Duplicate: must return the existing id + ErrDuplicateTask.
	id2, err := Enqueue(ctx, m, "sync", struct{}{}, WithUniqueKey("tenant-42"))
	if !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("duplicate enqueue: got %v, want ErrDuplicateTask", err)
	}
	if id2 != id1 {
		t.Errorf("duplicate returned id %q, want existing %q", id2, id1)
	}
	// A different key is fine.
	if _, err := Enqueue(ctx, m, "sync", struct{}{}, WithUniqueKey("tenant-43")); err != nil {
		t.Fatalf("different key: %v", err)
	}

	startManager(t, m)
	waitForStatus(t, fs, id1, StatusDone)

	// Done released the key: same key enqueues fresh.
	id3, err := Enqueue(ctx, m, "sync", struct{}{}, WithUniqueKey("tenant-42"))
	if err != nil {
		t.Fatalf("re-enqueue after done: %v", err)
	}
	if id3 == id1 {
		t.Error("expected a fresh task after key release")
	}
}

func TestUniqueKeyRejectedForBatch(t *testing.T) {
	m := testManager(t, newFakeStore())
	_, err := EnqueueMany(context.Background(), m, "t",
		[]struct{}{{}, {}}, WithUniqueKey("k"))
	if err == nil {
		t.Fatal("expected error for WithUniqueKey on a batch")
	}
}

func TestBatchModeProcessesAllExactlyOnce(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs, WithPipelineMode(PipelineConfig{ClaimBatch: 10}), WithWorkers(4))

	var calls callCounts
	_ = RegisterHandler(m, "bulk", func(ctx context.Context, task *Task, p struct{ N int }) (any, error) {
		calls.hit(task.ID)
		return nil, nil
	})

	payloads := make([]struct{ N int }, 100)
	for i := range payloads {
		payloads[i].N = i
	}
	ids, err := EnqueueMany(context.Background(), m, "bulk", payloads)
	if err != nil || len(ids) != 100 {
		t.Fatalf("EnqueueMany: %d ids, %v", len(ids), err)
	}
	startManager(t, m)

	for _, id := range ids {
		task := waitForStatus(t, fs, id, StatusDone)
		if task.Attempts != 1 {
			t.Errorf("task %s attempts = %d, want 1", id, task.Attempts)
		}
	}
	if n := calls.multi(); n != 0 {
		t.Errorf("%d tasks handled more than once", n)
	}
}

// A task whose queue lease expires while buffered must be dropped locally
// (zero handler calls for that claim) and then reclaimed and completed on a
// later fetch — never run under an expired lease and never run twice.
func TestBatchModeQueueLeaseAgingDropsThenReclaims(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs,
		WithPipelineMode(PipelineConfig{
			ClaimBatch: 5,
			QueueLease: 20 * time.Millisecond, // tiny channel budget
			NoFinalize: true,
		}),
		WithWorkers(1),
		WithLeaseTime(time.Second),  // ample execution budget
		WithDefaultMaxAttempts(20),  // aging burns attempts; keep headroom
	)

	var calls callCounts
	_ = RegisterHandler(m, "slow", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		calls.hit(task.ID)
		time.Sleep(60 * time.Millisecond) // 3x the queue lease: the rest of the batch ages
		return nil, nil
	})

	ids, err := EnqueueMany(context.Background(), m, "slow", make([]struct{}, 5))
	if err != nil {
		t.Fatalf("EnqueueMany: %v", err)
	}
	startManager(t, m)

	sawReclaim := false
	for _, id := range ids {
		task := waitForStatus(t, fs, id, StatusDone)
		if task.Attempts > 1 {
			sawReclaim = true
		}
	}
	if !sawReclaim {
		t.Error("no task aged in the channel — scenario did not exercise the drop path")
	}
	if n := calls.multi(); n != 0 {
		t.Errorf("%d tasks handled more than once despite aging", n)
	}
}

func TestBatchModeForeignTypesUntouched(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs, WithPipelineMode(PipelineConfig{ClaimBatch: 8}))

	var done atomic.Int32
	_ = RegisterHandler(m, "mine", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		done.Add(1)
		return nil, nil
	})

	ids, _ := EnqueueMany(context.Background(), m, "mine", make([]struct{}, 10))
	otherID, _ := Enqueue(context.Background(), m, "other-service-type", struct{}{})
	startManager(t, m)

	for _, id := range ids {
		waitForStatus(t, fs, id, StatusDone)
	}
	time.Sleep(30 * time.Millisecond)
	if task := fs.get(otherID); task.Status != StatusPending || task.Attempts != 0 {
		t.Errorf("foreign task touched by batch fetcher: %+v", task)
	}
}

func TestPipelineValidation(t *testing.T) {
	if _, err := New(newFakeStore(), WithPipelineMode(PipelineConfig{})); err != nil {
		t.Errorf("zero-value PipelineConfig must be valid: %v", err)
	}
	if _, err := New(newFakeStore(), WithPipelineMode(PipelineConfig{ClaimBatch: 101})); err == nil {
		t.Error("ClaimBatch 101 accepted")
	}
	if _, err := New(newFakeStore(), WithPipelineMode(PipelineConfig{QueueLease: -time.Second})); err == nil {
		t.Error("negative QueueLease accepted")
	}
	if _, err := New(newFakeStore(), WithPipelineMode(PipelineConfig{FinalizeBatch: 1001})); err == nil {
		t.Error("FinalizeBatch 1001 accepted")
	}
}

func TestManagerServesOnlyItsQueues(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs, WithQueues("emails"))

	var done atomic.Int32
	_ = RegisterHandler(m, "send", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		done.Add(1)
		return nil, nil
	})

	ctx := context.Background()
	emailID, _ := Enqueue(ctx, m, "send", struct{}{}, WithQueue("emails"))
	reportID, _ := Enqueue(ctx, m, "send", struct{}{}, WithQueue("reports"))
	defaultID, _ := Enqueue(ctx, m, "send", struct{}{}) // DefaultQueue
	startManager(t, m)

	waitForStatus(t, fs, emailID, StatusDone)
	time.Sleep(30 * time.Millisecond)
	if task := fs.get(reportID); task.Status != StatusPending {
		t.Errorf("reports-queue task claimed by emails manager: %+v", task)
	}
	if task := fs.get(defaultID); task.Status != StatusPending {
		t.Errorf("default-queue task claimed by emails manager: %+v", task)
	}
	if task := fs.get(defaultID); task.Queue != DefaultQueue {
		t.Errorf("queue not defaulted: %q", task.Queue)
	}
	if done.Load() != 1 {
		t.Errorf("handled %d tasks, want 1", done.Load())
	}
}

// watchingFakeStore adds Watcher support to the fake store; nudges are sent
// manually by the test.
type watchingFakeStore struct {
	*fakeStore
	watch chan struct{}
}

func (s *watchingFakeStore) WatchRunnable(ctx context.Context, types, queues []string) (<-chan struct{}, error) {
	return s.watch, nil
}

// With a change stream active, polls stretch to FallbackPoll — so pickup
// within the test window proves the nudge path works, in both modes.
func TestChangeStreamNudgeWakesWorkers(t *testing.T) {
	for _, mode := range []struct {
		name string
		opts []Option
	}{
		{"single", nil},
		{"pipeline", []Option{WithPipelineMode(PipelineConfig{ClaimBatch: 4})}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			ws := &watchingFakeStore{fakeStore: newFakeStore(), watch: make(chan struct{}, 1)}
			opts := append([]Option{
				WithPollInterval(10 * time.Minute), // ensure polling can't explain pickup
				WithFallbackPoll(10 * time.Minute),
				WithReapInterval(0),
			}, mode.opts...)
			m := testManager(t, ws, opts...)

			var done atomic.Int32
			_ = RegisterHandler(m, "job", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
				done.Add(1)
				return nil, nil
			})
			startManager(t, m)
			time.Sleep(100 * time.Millisecond) // let workers reach their idle wait

			id, _ := Enqueue(context.Background(), m, "job", struct{}{})
			time.Sleep(150 * time.Millisecond)
			if task := ws.get(id); task.Status == StatusDone {
				t.Fatal("task ran without a nudge — polling should be idle for 10m")
			}
			ws.watch <- struct{}{}
			waitForStatus(t, ws.fakeStore, id, StatusDone)
			if done.Load() != 1 {
				t.Errorf("handled %d, want 1", done.Load())
			}
		})
	}
}

func TestWithoutChangeStreamIgnoresWatcher(t *testing.T) {
	ws := &watchingFakeStore{fakeStore: newFakeStore(), watch: make(chan struct{}, 1)}
	m := testManager(t, ws, WithoutChangeStream()) // PollInterval 5ms from testManager

	_ = RegisterHandler(m, "job", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		return nil, nil
	})
	id, _ := Enqueue(context.Background(), m, "job", struct{}{})
	startManager(t, m)
	waitForStatus(t, ws.fakeStore, id, StatusDone) // polling picked it up; no nudge ever sent
}

func TestFinalizeBatchFlushBySize(t *testing.T) {
	fs := newFakeStore()
	// Huge interval: only the size trigger (3) can explain a prompt flush.
	m := testManager(t, fs,
		WithLeaseTime(time.Minute),
		WithPipelineMode(PipelineConfig{ClaimBatch: 4, FinalizeBatch: 3, FinalizeInterval: 2 * time.Second}),
		WithWorkers(2))

	_ = RegisterHandler(m, "job", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		return map[string]bool{"ok": true}, nil
	})
	ids, _ := EnqueueMany(context.Background(), m, "job", make([]struct{}, 3))
	startManager(t, m)

	for _, id := range ids {
		task := waitForStatus(t, fs, id, StatusDone)
		if len(task.Result) == 0 {
			t.Errorf("result lost through batch finalize: %+v", task)
		}
	}
}

func TestFinalizeBatchFlushByInterval(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs,
		WithLeaseTime(time.Minute),
		WithPipelineMode(PipelineConfig{ClaimBatch: 4, FinalizeBatch: 1000, FinalizeInterval: 120 * time.Millisecond}), // size can't trigger
		WithWorkers(1))

	ran := make(chan struct{})
	_ = RegisterHandler(m, "job", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		close(ran)
		return nil, nil
	})
	id, _ := Enqueue(context.Background(), m, "job", struct{}{})
	startManager(t, m)

	<-ran
	// Right after the handler, the outcome is buffered: DB still says running.
	time.Sleep(20 * time.Millisecond)
	if task := fs.get(id); task.Status != StatusRunning {
		t.Logf("note: flushed faster than expected (status %s)", task.Status)
	}
	waitForStatus(t, fs, id, StatusDone) // interval flush lands
}

func TestFinalizeBatchAtMostOnceBypasses(t *testing.T) {
	fs := newFakeStore()
	// Interval so long that only the bypass can finish the task quickly.
	m := testManager(t, fs,
		WithLeaseTime(time.Minute),
		WithPipelineMode(PipelineConfig{FinalizeBatch: 1000, FinalizeInterval: 10 * time.Second}))

	_ = RegisterHandler(m, "once", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		return nil, nil
	})
	id, _ := Enqueue(context.Background(), m, "once", struct{}{}, WithMaxAttempts(1))
	startManager(t, m)
	waitForStatus(t, fs, id, StatusDone) // immediate path, no buffering
}

func TestFinalizeBatchLeaseMarginBypasses(t *testing.T) {
	fs := newFakeStore()
	// LeaseTime 1s < margin (2s): every outcome bypasses the buffer.
	m := testManager(t, fs,
		WithLeaseTime(time.Second),
		WithPipelineMode(PipelineConfig{FinalizeBatch: 1000, FinalizeInterval: 10 * time.Second}))

	_ = RegisterHandler(m, "job", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		return nil, nil
	})
	id, _ := Enqueue(context.Background(), m, "job", struct{}{})
	startManager(t, m)
	waitForStatus(t, fs, id, StatusDone)
}

func TestFinalizeBatchDrainsOnStop(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs,
		WithLeaseTime(time.Minute),
		WithPipelineMode(PipelineConfig{FinalizeBatch: 1000, FinalizeInterval: 10 * time.Second}), // neither size nor interval fires before Stop
		WithWorkers(2))

	var ran atomic.Int32
	_ = RegisterHandler(m, "job", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		ran.Add(1)
		return nil, nil
	})
	ids, _ := EnqueueMany(context.Background(), m, "job", make([]struct{}, 4))
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for ran.Load() < 4 {
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Stop's drain must have flushed the buffered outcomes.
	for _, id := range ids {
		if task := fs.get(id); task.Status != StatusDone {
			t.Errorf("outcome not drained on Stop: %+v", task)
		}
	}
}

func TestFinalizeBatchFailuresRetryAndDie(t *testing.T) {
	fs := newFakeStore()
	m := testManager(t, fs,
		WithLeaseTime(time.Minute),
		WithPipelineMode(PipelineConfig{FinalizeBatch: 2, FinalizeInterval: 30 * time.Millisecond}))

	var calls atomic.Int32
	_ = RegisterHandler(m, "flaky", func(ctx context.Context, task *Task, _ struct{}) (any, error) {
		calls.Add(1)
		return nil, errors.New("boom")
	})
	id, _ := Enqueue(context.Background(), m, "flaky", struct{}{}, WithMaxAttempts(2))
	startManager(t, m)

	task := waitForStatus(t, fs, id, StatusDead)
	if task.Attempts != 2 || len(task.Errors) != 2 {
		t.Errorf("attempts=%d errors=%d, want 2/2", task.Attempts, len(task.Errors))
	}
	if calls.Load() != 2 {
		t.Errorf("handler calls = %d, want 2", calls.Load())
	}
}
