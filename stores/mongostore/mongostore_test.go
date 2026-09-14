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

	if err := s.Complete(ctx, task.ID, task.LeaseToken, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	// Second complete with the (now cleared) token must be fenced off.
	if err := s.Complete(ctx, task.ID, task.LeaseToken, nil); !errors.Is(err, gotasks.ErrLeaseLost) {
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
	if err := s.Fail(ctx, task.ID, task.LeaseToken, te, now, false); err != nil {
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
	if err := s.Fail(ctx, task.ID, task.LeaseToken, te, now, true); err != nil {
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
	if doc.Status != string(gotasks.StatusFailed) || len(doc.Errors) != 1 {
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
	until, err := s.ExtendLease(ctx, task.ID, task.LeaseToken, 5*time.Minute)
	if err != nil {
		t.Fatalf("ExtendLease: %v", err)
	}
	if until.Before(now.Add(4 * time.Minute)) {
		t.Fatalf("lease not extended: %v", until)
	}
	if _, err := s.ExtendLease(ctx, task.ID, "wrong-token", time.Minute); !errors.Is(err, gotasks.ErrLeaseLost) {
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
