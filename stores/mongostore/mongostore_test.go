package mongostore

// Integration tests — they run against a local MongoDB (mongodb://localhost:27017)
// and skip automatically when none is reachable. Each run uses a throwaway
// collection that is dropped afterwards.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ameenkh/gotasks"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	uri := os.Getenv("GOTASKS_TEST_MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	coll := fmt.Sprintf("tasks_test_%d", time.Now().UnixNano())
	s, err := New(ctx, uri,
		WithDatabase("gotasks_test"),
		WithCollection(coll),
		WithRetention(time.Hour),
	)
	if err != nil {
		t.Skipf("no MongoDB at %s: %v", uri, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.col.Drop(ctx)
		_ = s.Close(ctx)
	})
	return s
}

func enqueueOne(t *testing.T, s *Store, taskType string, payload string, maxAttempts int, runAt time.Time) string {
	t.Helper()
	now := time.Now().UTC()
	ids, err := s.Enqueue(context.Background(), []*gotasks.Task{{
		Type: taskType, Payload: []byte(payload), Status: gotasks.StatusPending,
		MaxAttempts: maxAttempts, RunAt: runAt, CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return ids[0]
}

func TestClaimCompleteRoundTrip(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	enqueueOne(t, s, "email", `{"to":"a@b.c","n":7}`, 3, now)

	task, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w1", Types: []string{"email"}, Lease: time.Minute})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if task.Status != gotasks.StatusRunning || task.Attempts != 1 || task.LeaseToken == "" {
		t.Fatalf("bad claimed task: %+v", task)
	}
	if !strings.Contains(string(task.Payload), `"to"`) || !strings.Contains(string(task.Payload), "a@b.c") {
		t.Fatalf("payload did not round-trip: %s", task.Payload)
	}

	if err := s.Complete(ctx, task, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	// Second complete with the (now cleared) token must be fenced off.
	if err := s.Complete(ctx, task, nil); !errors.Is(err, gotasks.ErrLeaseLost) {
		t.Fatalf("double complete: got %v, want ErrLeaseLost", err)
	}
	if _, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w1", Lease: time.Minute}); !errors.Is(err, gotasks.ErrNoTask) {
		t.Fatalf("claim on empty queue: got %v, want ErrNoTask", err)
	}
}

func TestConcurrentClaimsAreExclusive(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	const n = 20
	for i := 0; i < n; i++ {
		enqueueOne(t, s, "job", fmt.Sprintf(`{"i":%d}`, i), 3, now)
	}

	var mu sync.Mutex
	claimed := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				task, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: fmt.Sprintf("w%d", w), Lease: time.Minute})
				if errors.Is(err, gotasks.ErrNoTask) {
					return
				}
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				mu.Lock()
				claimed[task.ID]++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if len(claimed) != n {
		t.Fatalf("claimed %d distinct tasks, want %d", len(claimed), n)
	}
	for id, c := range claimed {
		if c != 1 {
			t.Errorf("task %s claimed %d times", id, c)
		}
	}
}

func TestFailRetryAndTerminal(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	enqueueOne(t, s, "flaky", `{"x":1}`, 2, now)

	task, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w1", Lease: time.Minute})
	if err != nil {
		t.Fatalf("Claim 1: %v", err)
	}
	te := gotasks.TaskError{At: now, Attempt: task.Attempts, Worker: "w1", Message: "boom"}
	if err := s.Fail(ctx, task, te, now, false); err != nil {
		t.Fatalf("Fail 1: %v", err)
	}

	task, err = s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w2", Lease: time.Minute})
	if err != nil {
		t.Fatalf("Claim 2 (retry): %v", err)
	}
	if task.Attempts != 2 || len(task.Errors) != 1 || task.Errors[0].Message != "boom" {
		t.Fatalf("retry state wrong: %+v", task)
	}
	te = gotasks.TaskError{At: now, Attempt: task.Attempts, Worker: "w2", Message: "boom again"}
	if err := s.Fail(ctx, task, te, now, true); err != nil {
		t.Fatalf("Fail terminal: %v", err)
	}
	if _, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w3", Lease: time.Minute}); !errors.Is(err, gotasks.ErrNoTask) {
		t.Fatalf("failed task still claimable: %v", err)
	}
}

func TestStaleReclaimRespectsMaxAttempts(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	retryable := enqueueOne(t, s, "jobA", `{"kind":"retryable"}`, 3, now)
	once := enqueueOne(t, s, "jobB", `{"kind":"once"}`, 1, now)

	// Claim both with an already-expired lease (dead workers). Claimed by
	// type: with a negative lease the first task is instantly stale again,
	// so an untyped second claim could just re-claim it.
	for _, taskType := range []string{"jobA", "jobB"} {
		if _, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "dead", Types: []string{taskType}, Lease: -time.Second}); err != nil {
			t.Fatalf("Claim %s: %v", taskType, err)
		}
	}

	// Only the retryable one may be reclaimed.
	task, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "alive", Lease: time.Minute})
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if task.ID != retryable || task.Attempts != 2 {
		t.Fatalf("wrong reclaim: %+v (want id %s)", task, retryable)
	}
	if _, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "alive", Lease: time.Minute}); !errors.Is(err, gotasks.ErrNoTask) {
		t.Fatalf("at-most-once task was reclaimed: %v", err)
	}

	// The reaper marks the at-most-once zombie failed, with its own fields
	// in the error entry.
	n, err := s.ReapExpired(ctx)
	if err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}
	if n != 1 {
		t.Fatalf("reaped %d, want 1", n)
	}
	var doc taskDoc
	oidOnce, _ := oid(once)
	if err := s.col.FindOne(ctx, map[string]any{"_id": oidOnce}).Decode(&doc); err != nil {
		t.Fatalf("find reaped: %v", err)
	}
	if doc.Status != string(gotasks.StatusDead) || len(doc.Errors) != 1 {
		t.Fatalf("reaped doc wrong: %+v", doc)
	}
	if doc.Errors[0].Worker != "dead" || doc.Errors[0].Attempt != 1 {
		t.Fatalf("reap error entry wrong: %+v", doc.Errors[0])
	}
}

func TestExtendLeaseAndFencing(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	enqueueOne(t, s, "long", `{"x":1}`, 3, now)
	task, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w1", Lease: time.Minute})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	until, err := s.ExtendLease(ctx, task, 5*time.Minute)
	if err != nil {
		t.Fatalf("ExtendLease: %v", err)
	}
	if until.Before(now.Add(4 * time.Minute)) {
		t.Fatalf("lease not extended: %v", until)
	}
	stale := *task
	stale.LeaseToken = "wrong-token"
	if _, err := s.ExtendLease(ctx, &stale, time.Minute); !errors.Is(err, gotasks.ErrLeaseLost) {
		t.Fatalf("wrong token extend: got %v, want ErrLeaseLost", err)
	}
}

func TestScheduledTaskNotClaimableEarly(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	enqueueOne(t, s, "later", `{"x":1}`, 3, time.Now().UTC().Add(time.Hour))
	if _, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w1", Lease: time.Minute}); !errors.Is(err, gotasks.ErrNoTask) {
		t.Fatalf("future task claimed: %v", err)
	}
}

func claimOne(t *testing.T, s *Store, worker string) *gotasks.Task {
	t.Helper()
	task, err := s.Claim(context.Background(), gotasks.ClaimOptions{WorkerID: worker, Lease: time.Minute})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return task
}

func TestUniqueKeyDedupAndRelease(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	mkTask := func(key string) *gotasks.Task {
		return &gotasks.Task{
			Type: "sync", UniqueKey: key, Payload: []byte(`{"x":1}`),
			Status: gotasks.StatusPending, MaxAttempts: 3,
			RunAt: now, CreatedAt: now, UpdatedAt: now,
		}
	}

	ids, err := s.Enqueue(ctx, []*gotasks.Task{mkTask("k1")})
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	// Duplicate must fail with the existing id.
	_, err = s.Enqueue(ctx, []*gotasks.Task{mkTask("k1")})
	var dup *gotasks.DuplicateTaskError
	if !errors.As(err, &dup) || !errors.Is(err, gotasks.ErrDuplicateTask) {
		t.Fatalf("duplicate enqueue: got %v, want DuplicateTaskError", err)
	}
	if dup.ExistingID != ids[0] || dup.Key != "k1" {
		t.Fatalf("dup details wrong: %+v (want id %s)", dup, ids[0])
	}
	// The key survives a retry (non-terminal fail) — still active.
	task := claimOne(t, s, "w1")
	te := gotasks.TaskError{At: now, Attempt: task.Attempts, Worker: "w1", Message: "boom"}
	if err := s.Fail(ctx, task, te, now, false); err != nil {
		t.Fatalf("Fail retry: %v", err)
	}
	if _, err = s.Enqueue(ctx, []*gotasks.Task{mkTask("k1")}); !errors.Is(err, gotasks.ErrDuplicateTask) {
		t.Fatalf("key released by retry: %v", err)
	}
	// Completing releases the key.
	task = claimOne(t, s, "w1")
	if err := s.Complete(ctx, task, nil); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if _, err := s.Enqueue(ctx, []*gotasks.Task{mkTask("k1")}); err != nil {
		t.Fatalf("enqueue after key release: %v", err)
	}
}

func TestRequeueDeadTask(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	id := enqueueOne(t, s, "job", `{"x":1}`, 1, now)
	task := claimOne(t, s, "w1")
	te := gotasks.TaskError{At: now, Attempt: task.Attempts, Worker: "w1", Message: "boom"}
	if err := s.Fail(ctx, task, te, now, true); err != nil {
		t.Fatalf("Fail terminal: %v", err)
	}

	// Dead task is not claimable; requeue resets it.
	if _, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w2", Lease: time.Minute}); !errors.Is(err, gotasks.ErrNoTask) {
		t.Fatalf("dead task claimable: %v", err)
	}
	if err := s.Requeue(ctx, id); err != nil {
		t.Fatalf("Requeue: %v", err)
	}
	task = claimOne(t, s, "w2")
	if task.ID != id || task.Attempts != 1 || len(task.Errors) != 1 {
		t.Fatalf("requeued task state wrong: %+v", task)
	}
	// expires_at must have been cleared so TTL won't eat the requeued task.
	var doc taskDoc
	o, _ := oid(id)
	if err := s.col.FindOne(ctx, map[string]any{"_id": o}).Decode(&doc); err != nil {
		t.Fatalf("find: %v", err)
	}
	if doc.ExpiresAt != nil {
		t.Fatalf("expires_at not cleared on requeue: %v", doc.ExpiresAt)
	}
	// Requeue of a non-dead task fails.
	if err := s.Requeue(ctx, id); !errors.Is(err, gotasks.ErrNotFound) {
		t.Fatalf("requeue running task: got %v, want ErrNotFound", err)
	}
}

func TestRequeueDeadByTypeMongo(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	kill := func(taskType string) {
		enqueueOne(t, s, taskType, `{"x":1}`, 1, now)
		task := claimOne(t, s, "w1")
		te := gotasks.TaskError{At: now, Attempt: task.Attempts, Worker: "w1", Message: "boom"}
		if err := s.Fail(ctx, task, te, now, true); err != nil {
			t.Fatalf("Fail: %v", err)
		}
	}
	kill("a")
	kill("b")
	kill("b")

	n, err := s.RequeueDead(ctx, "b")
	if err != nil || n != 2 {
		t.Fatalf("RequeueDead(b) = %d, %v; want 2", n, err)
	}
	n, err = s.RequeueDead(ctx, "")
	if err != nil || n != 1 {
		t.Fatalf("RequeueDead(all) = %d, %v; want 1", n, err)
	}
}

func TestPerTypeRetention(t *testing.T) {
	uri := os.Getenv("GOTASKS_TEST_MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	coll := fmt.Sprintf("tasks_test_%d", time.Now().UnixNano())
	s, err := New(ctx, uri,
		WithDatabase("gotasks_test"),
		WithCollection(coll),
		WithRetention(2*time.Hour),
		WithRetentionByType(map[string]time.Duration{"email": time.Hour, "audit": 0}),
	)
	if err != nil {
		t.Skipf("no MongoDB at %s: %v", uri, err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = s.col.Drop(ctx)
		_ = s.Close(ctx)
	})
	bg := context.Background()
	now := time.Now().UTC()

	expiresOf := func(id string) *time.Time {
		var doc taskDoc
		o, _ := oid(id)
		if err := s.col.FindOne(bg, map[string]any{"_id": o}).Decode(&doc); err != nil {
			t.Fatalf("find %s: %v", id, err)
		}
		return doc.ExpiresAt
	}
	within := func(got *time.Time, want time.Duration) bool {
		if got == nil {
			return false
		}
		d := got.Sub(now)
		return d > want-time.Minute && d < want+time.Minute
	}

	// Complete path: per-type override, default, and keep-forever.
	ids := map[string]string{}
	for _, typ := range []string{"email", "other", "audit"} {
		ids[typ] = enqueueOne(t, s, typ, `{"x":1}`, 3, now)
		task := claimOne(t, s, "w1")
		if err := s.Complete(bg, task, nil); err != nil {
			t.Fatalf("Complete %s: %v", typ, err)
		}
	}
	if e := expiresOf(ids["email"]); !within(e, time.Hour) {
		t.Errorf("email expires_at = %v, want ~now+1h", e)
	}
	if e := expiresOf(ids["other"]); !within(e, 2*time.Hour) {
		t.Errorf("other expires_at = %v, want ~now+2h (default)", e)
	}
	if e := expiresOf(ids["audit"]); e != nil {
		t.Errorf("audit expires_at = %v, want none (keep forever)", e)
	}

	// Reap path: the $switch pipeline must apply the same per-type rules.
	reapIDs := map[string]string{}
	for _, typ := range []string{"email", "other", "audit"} {
		reapIDs[typ] = enqueueOne(t, s, typ, `{"x":1}`, 1, now)
		claimOne(t, s, "dead") // claim then abandon
	}
	if _, err := s.col.UpdateMany(bg,
		map[string]any{"status": string(gotasks.StatusRunning)},
		map[string]any{"$set": map[string]any{"locked_until": now.Add(-time.Minute)}}); err != nil {
		t.Fatalf("expire leases: %v", err)
	}
	n, err := s.ReapExpired(bg)
	if err != nil || n != 3 {
		t.Fatalf("ReapExpired = %d, %v; want 3", n, err)
	}
	if e := expiresOf(reapIDs["email"]); !within(e, time.Hour) {
		t.Errorf("reaped email expires_at = %v, want ~now+1h", e)
	}
	if e := expiresOf(reapIDs["other"]); !within(e, 2*time.Hour) {
		t.Errorf("reaped other expires_at = %v, want ~now+2h", e)
	}
	if e := expiresOf(reapIDs["audit"]); e != nil {
		t.Errorf("reaped audit expires_at = %v, want none", e)
	}
}

func TestClaimBatchBasics(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	for i := 0; i < 10; i++ {
		enqueueOne(t, s, "bulk", fmt.Sprintf(`{"i":%d}`, i), 3, now)
	}

	batch, err := s.ClaimBatch(ctx, gotasks.ClaimOptions{WorkerID: "f1", Types: []string{"bulk"}, Lease: 90 * time.Second}, 4)
	if err != nil {
		t.Fatalf("ClaimBatch: %v", err)
	}
	if len(batch) != 4 {
		t.Fatalf("claimed %d, want 4", len(batch))
	}
	token := batch[0].LeaseToken
	for _, task := range batch {
		if task.Status != gotasks.StatusRunning || task.Attempts != 1 {
			t.Errorf("bad claimed task: %+v", task)
		}
		if task.LeaseToken != token {
			t.Errorf("batch tasks must share one lease token")
		}
		if task.LockedUntil.Before(now.Add(80 * time.Second)) {
			t.Errorf("queue lease not applied: %v", task.LockedUntil)
		}
	}

	// Short batch: only 6 remain although we ask for 20.
	batch, err = s.ClaimBatch(ctx, gotasks.ClaimOptions{WorkerID: "f1", Lease: time.Minute}, 20)
	if err != nil || len(batch) != 6 {
		t.Fatalf("short batch: %d tasks, %v; want 6", len(batch), err)
	}
	// Empty queue.
	if _, err := s.ClaimBatch(ctx, gotasks.ClaimOptions{WorkerID: "f1", Lease: time.Minute}, 5); !errors.Is(err, gotasks.ErrNoTask) {
		t.Fatalf("empty claim: got %v, want ErrNoTask", err)
	}
}

func TestClaimBatchConcurrentExclusive(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	const total = 60
	for i := 0; i < total; i++ {
		enqueueOne(t, s, "job", fmt.Sprintf(`{"i":%d}`, i), 3, now)
	}

	var mu sync.Mutex
	claimed := map[string]int{}
	var wg sync.WaitGroup
	for f := 0; f < 4; f++ {
		wg.Add(1)
		go func(f int) {
			defer wg.Done()
			for {
				batch, err := s.ClaimBatch(ctx, gotasks.ClaimOptions{WorkerID: fmt.Sprintf("f%d", f), Lease: time.Minute}, 7)
				if errors.Is(err, gotasks.ErrNoTask) {
					return
				}
				if err != nil {
					t.Errorf("ClaimBatch: %v", err)
					return
				}
				mu.Lock()
				for _, task := range batch {
					claimed[task.ID]++
				}
				mu.Unlock()
				// len(batch)==0 (all candidates stolen) → just retry.
			}
		}(f)
	}
	wg.Wait()

	if len(claimed) != total {
		t.Fatalf("claimed %d distinct tasks, want %d", len(claimed), total)
	}
	for id, c := range claimed {
		if c != 1 {
			t.Errorf("task %s claimed %d times", id, c)
		}
	}
}

func TestClaimBatchRespectsStaleAndAttempts(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	fresh := enqueueOne(t, s, "freshT", `{"kind":"fresh"}`, 3, now)
	stale := enqueueOne(t, s, "staleT", `{"kind":"stale-retryable"}`, 3, now)
	spent := enqueueOne(t, s, "spentT", `{"kind":"stale-exhausted"}`, 1, now)

	// Make the last two stale via type-targeted claims with an expired
	// lease (negative-lease tasks are instantly stale again, so untyped
	// setup claims would just re-take the same doc).
	for _, taskType := range []string{"staleT", "spentT"} {
		if _, err := s.ClaimBatch(ctx, gotasks.ClaimOptions{WorkerID: "dead", Types: []string{taskType}, Lease: -time.Second}, 1); err != nil {
			t.Fatalf("stale setup %s: %v", taskType, err)
		}
	}

	// Batch-claimable now: fresh (pending) and stale (stale reclaim,
	// attempts remaining). Spent (stale, attempts exhausted) must not be.
	batch, err := s.ClaimBatch(ctx, gotasks.ClaimOptions{WorkerID: "alive", Lease: time.Minute}, 10)
	if err != nil {
		t.Fatalf("ClaimBatch: %v", err)
	}
	got := map[string]int{}
	for _, task := range batch {
		got[task.ID] = task.Attempts
	}
	if len(batch) != 2 || got[fresh] != 1 || got[stale] != 2 {
		t.Fatalf("claimed %v, want {fresh:1 attempt, stale:2 attempts}", got)
	}
	if _, ok := got[spent]; ok {
		t.Fatalf("at-most-once stale task was batch-claimed")
	}
}

func TestFIFOClaimOrder(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour)

	// Distinct run_at values, enqueued out of order.
	for _, m := range []int{30, 10, 50, 20, 40} {
		enqueueOne(t, s, "job", fmt.Sprintf(`{"m":%d}`, m), 3, base.Add(time.Duration(m)*time.Minute))
	}
	var got []string
	for {
		task, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w1", FIFO: true, Lease: time.Minute})
		if errors.Is(err, gotasks.ErrNoTask) {
			break
		}
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		got = append(got, string(task.Payload))
	}
	want := []string{`{"m":10}`, `{"m":20}`, `{"m":30}`, `{"m":40}`, `{"m":50}`}
	if len(got) != len(want) {
		t.Fatalf("claimed %d tasks, want %d", len(got), len(want))
	}
	for i := range want {
		if !strings.Contains(got[i], want[i][1:len(want[i])-1]) {
			t.Fatalf("FIFO order broken: got %v, want ascending run_at %v", got, want)
		}
	}

	// FIFO batch claim preserves the order too.
	for _, m := range []int{3, 1, 2} {
		enqueueOne(t, s, "job2", fmt.Sprintf(`{"m":%d}`, m), 3, base.Add(time.Duration(m)*time.Minute))
	}
	batch, err := s.ClaimBatch(ctx, gotasks.ClaimOptions{WorkerID: "f1", Types: []string{"job2"}, FIFO: true, Lease: time.Minute}, 2)
	if err != nil || len(batch) != 2 {
		t.Fatalf("ClaimBatch: %d, %v", len(batch), err)
	}
	if !strings.Contains(string(batch[0].Payload), `"m":1`) || !strings.Contains(string(batch[1].Payload), `"m":2`) {
		t.Fatalf("FIFO batch order broken: %s, %s", batch[0].Payload, batch[1].Payload)
	}
}

func TestQueueFieldAndFiltering(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	mk := func(queue string) string {
		ids, err := s.Enqueue(ctx, []*gotasks.Task{{
			Queue: queue, Type: "job", Payload: []byte(`{"x":1}`),
			Status: gotasks.StatusPending, MaxAttempts: 3,
			RunAt: now, CreatedAt: now, UpdatedAt: now,
		}})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		return ids[0]
	}
	emailID := mk("emails")
	mk("reports")
	defaulted := mk("") // store defaults empty to DefaultQueue

	// A manager restricted to "emails" sees only that queue.
	task, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w1", Queues: []string{"emails"}, Lease: time.Minute})
	if err != nil || task.ID != emailID || task.Queue != "emails" {
		t.Fatalf("queue claim: %+v, %v (want %s)", task, err, emailID)
	}
	if _, err := s.Claim(ctx, gotasks.ClaimOptions{WorkerID: "w1", Queues: []string{"emails"}, Lease: time.Minute}); !errors.Is(err, gotasks.ErrNoTask) {
		t.Fatalf("emails queue should be empty: %v", err)
	}

	// Batch claim with a queue filter.
	batch, err := s.ClaimBatch(ctx, gotasks.ClaimOptions{WorkerID: "f1", Queues: []string{"reports", gotasks.DefaultQueue}, Lease: time.Minute}, 10)
	if err != nil || len(batch) != 2 {
		t.Fatalf("queue batch: %d, %v; want 2", len(batch), err)
	}
	for _, task := range batch {
		if task.ID == defaulted && task.Queue != gotasks.DefaultQueue {
			t.Errorf("empty queue not defaulted: %+v", task)
		}
	}
}
