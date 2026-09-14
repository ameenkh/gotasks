package gotasks

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"
)

// fakeStore is an in-memory Store used to test the manager without MongoDB.
// It honors the same claim/fencing semantics as real stores.
type fakeStore struct {
	mu    sync.Mutex
	seq   int
	tasks map[string]*Task
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

func (s *fakeStore) Enqueue(_ context.Context, tasks []*Task) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, len(tasks))
	for i, t := range tasks {
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

func (s *fakeStore) Complete(_ context.Context, id, leaseToken string, result json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.held(id, leaseToken)
	if t == nil {
		return ErrLeaseLost
	}
	t.Status = StatusDone
	t.Result = slices.Clone(result)
	t.LockedBy, t.LeaseToken = "", ""
	t.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *fakeStore) Fail(_ context.Context, id, leaseToken string, taskErr TaskError, retryAt time.Time, terminal bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.held(id, leaseToken)
	if t == nil {
		return ErrLeaseLost
	}
	t.Errors = append(t.Errors, taskErr)
	if terminal {
		t.Status = StatusFailed
	} else {
		t.Status = StatusPending
		t.RunAt = retryAt
	}
	t.LockedBy, t.LeaseToken = "", ""
	t.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *fakeStore) ExtendLease(_ context.Context, id, leaseToken string, lease time.Duration) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.held(id, leaseToken)
	if t == nil {
		return time.Time{}, ErrLeaseLost
	}
	t.LockedUntil = time.Now().UTC().Add(lease)
	return t.LockedUntil, nil
}

func (s *fakeStore) ReapExpired(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var n int64
	for _, t := range s.tasks {
		if t.Status == StatusRunning && t.LockedUntil.Before(now) && t.Attempts >= t.MaxAttempts {
			t.Status = StatusFailed
			t.Errors = append(t.Errors, TaskError{
				At: now, Attempt: t.Attempts, Worker: t.LockedBy,
				Message: "lease expired before completion; attempts exhausted",
			})
			t.LockedBy, t.LeaseToken = "", ""
			n++
		}
	}
	return n, nil
}

func (s *fakeStore) Close(context.Context) error { return nil }
