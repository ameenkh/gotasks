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

	task, err := s.Claim(ctx, "w1", []string{"email"}, time.Minute)
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
	if _, err := s.Claim(ctx, "w1", nil, time.Minute); !errors.Is(err, gotasks.ErrNoTask) {
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
				task, err := s.Claim(ctx, fmt.Sprintf("w%d", w), nil, time.Minute)
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

	task, err := s.Claim(ctx, "w1", nil, time.Minute)
	if err != nil {
		t.Fatalf("Claim 1: %v", err)
	}
	te := gotasks.TaskError{At: now, Attempt: task.Attempts, Worker: "w1", Message: "boom"}
	if err := s.Fail(ctx, task, te, now, false); err != nil {
		t.Fatalf("Fail 1: %v", err)
	}

	task, err = s.Claim(ctx, "w2", nil, time.Minute)
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
	if _, err := s.Claim(ctx, "w3", nil, time.Minute); !errors.Is(err, gotasks.ErrNoTask) {
		t.Fatalf("failed task still claimable: %v", err)
	}
}

func TestStaleReclaimRespectsMaxAttempts(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	retryable := enqueueOne(t, s, "job", `{"kind":"retryable"}`, 3, now)
	once := enqueueOne(t, s, "job", `{"kind":"once"}`, 1, now)

	// Claim both with an already-expired lease (dead workers).
	for i := 0; i < 2; i++ {
		if _, err := s.Claim(ctx, "dead", nil, -time.Second); err != nil {
			t.Fatalf("Claim %d: %v", i, err)
		}
	}

	// Only the retryable one may be reclaimed.
	task, err := s.Claim(ctx, "alive", nil, time.Minute)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if task.ID != retryable || task.Attempts != 2 {
		t.Fatalf("wrong reclaim: %+v (want id %s)", task, retryable)
	}
	if _, err := s.Claim(ctx, "alive", nil, time.Minute); !errors.Is(err, gotasks.ErrNoTask) {
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
	task, err := s.Claim(ctx, "w1", nil, time.Minute)
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
	if _, err := s.Claim(ctx, "w1", nil, time.Minute); !errors.Is(err, gotasks.ErrNoTask) {
		t.Fatalf("future task claimed: %v", err)
	}
}

func claimOne(t *testing.T, s *Store, worker string) *gotasks.Task {
	t.Helper()
	task, err := s.Claim(context.Background(), worker, nil, time.Minute)
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
	if _, err := s.Claim(ctx, "w2", nil, time.Minute); !errors.Is(err, gotasks.ErrNoTask) {
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
