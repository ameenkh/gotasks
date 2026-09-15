package gotasks

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// fakeStore is an in-memory Store used to test the manager without MongoDB.
// It honors the same claim/fencing/unique-key semantics as real stores.
type fakeStore struct {
	mu    sync.Mutex
	seq   int
	tasks map[string]*Task

	// forceLeaseLost makes ExtendLease report ErrLeaseLost, simulating the
	// task having been reclaimed while its handler runs.
	forceLeaseLost atomic.Bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{tasks: map[string]*Task{}}
}

func cloneTask(t *Task) *Task {
	c := *t
	c.Payload = slices.Clone(t.Payload)
	c.Result = slices.Clone(t.Result)
	c.Errors = slices.Clone(t.Errors)
	return &c
}

func (s *fakeStore) get(id string) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tasks[id]; ok {
		return cloneTask(t)
	}
	return nil
}

func (s *fakeStore) activeKeyHolder(key string) *Task {
	for _, t := range s.tasks {
		if t.UniqueKey == key && (t.Status == StatusPending || t.Status == StatusRunning) {
			return t
		}
	}
	return nil
}

func (s *fakeStore) Enqueue(_ context.Context, tasks []*Task) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, len(tasks))
	for i, t := range tasks {
		if t.UniqueKey != "" {
			if holder := s.activeKeyHolder(t.UniqueKey); holder != nil {
				return nil, &DuplicateTaskError{Key: t.UniqueKey, ExistingID: holder.ID}
			}
		}
		s.seq++
		c := cloneTask(t)
		c.ID = fmt.Sprintf("task-%d", s.seq)
		s.tasks[c.ID] = c
		ids[i] = c.ID
	}
	return ids, nil
}

func (s *fakeStore) Claim(_ context.Context, workerID string, types []string, lease time.Duration) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var best *Task
	for _, t := range s.tasks {
		if len(types) > 0 && !slices.Contains(types, t.Type) {
			continue
		}
		runnable := (t.Status == StatusPending && !t.RunAt.After(now)) ||
			(t.Status == StatusRunning && t.LockedUntil.Before(now) && t.Attempts < t.MaxAttempts)
		if !runnable {
			continue
		}
		if best == nil || t.RunAt.Before(best.RunAt) || (t.RunAt.Equal(best.RunAt) && t.ID < best.ID) {
			best = t
		}
	}
	if best == nil {
		return nil, ErrNoTask
	}
	s.seq++
	best.Status = StatusRunning
	best.Attempts++
	best.LockedBy = workerID
	best.LeaseToken = fmt.Sprintf("lease-%d", s.seq)
	best.LockedUntil = now.Add(lease)
	best.UpdatedAt = now
	return cloneTask(best), nil
}

func (s *fakeStore) held(id, leaseToken string) *Task {
	t, ok := s.tasks[id]
	if !ok || t.Status != StatusRunning || t.LeaseToken != leaseToken {
		return nil
	}
	return t
}

func (s *fakeStore) Complete(_ context.Context, task *Task, result json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.held(task.ID, task.LeaseToken)
	if t == nil {
		return ErrLeaseLost
	}
	t.Status = StatusDone
	t.Result = slices.Clone(result)
	t.LockedBy, t.LeaseToken, t.UniqueKey = "", "", ""
	t.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *fakeStore) Fail(_ context.Context, task *Task, taskErr TaskError, retryAt time.Time, terminal bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.held(task.ID, task.LeaseToken)
	if t == nil {
		return ErrLeaseLost
	}
	t.Errors = append(t.Errors, taskErr)
	if terminal {
		t.Status = StatusDead
		t.UniqueKey = ""
	} else {
		t.Status = StatusPending
		t.RunAt = retryAt
	}
	t.LockedBy, t.LeaseToken = "", ""
	t.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *fakeStore) ExtendLease(_ context.Context, task *Task, lease time.Duration) (time.Time, error) {
	if s.forceLeaseLost.Load() {
		return time.Time{}, ErrLeaseLost
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.held(task.ID, task.LeaseToken)
	if t == nil {
		return time.Time{}, ErrLeaseLost
	}
	t.LockedUntil = time.Now().UTC().Add(lease)
	return t.LockedUntil, nil
}

func (s *fakeStore) requeue(t *Task, now time.Time) {
	t.Status = StatusPending
	t.Attempts = 0
	t.RunAt = now
	t.Result = nil
	t.LockedBy, t.LeaseToken = "", ""
	t.UpdatedAt = now
}

func (s *fakeStore) Requeue(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok || t.Status != StatusDead {
		return fmt.Errorf("%w: no dead task with id %q", ErrNotFound, id)
	}
	s.requeue(t, time.Now().UTC())
	return nil
}

func (s *fakeStore) RequeueDead(_ context.Context, taskType string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var n int64
	for _, t := range s.tasks {
		if t.Status == StatusDead && (taskType == "" || t.Type == taskType) {
			s.requeue(t, now)
			n++
		}
	}
	return n, nil
}

func (s *fakeStore) ReapExpired(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var n int64
	for _, t := range s.tasks {
		if t.Status == StatusRunning && t.LockedUntil.Before(now) && t.Attempts >= t.MaxAttempts {
			t.Status = StatusDead
			t.Errors = append(t.Errors, TaskError{
				At: now, Attempt: t.Attempts, Worker: t.LockedBy,
				Message: "lease expired before completion; attempts exhausted",
			})
			t.LockedBy, t.LeaseToken, t.UniqueKey = "", "", ""
			n++
		}
	}
	return n, nil
}

func (s *fakeStore) Close(context.Context) error { return nil }
