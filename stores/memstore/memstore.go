// Package memstore is an in-memory gotasks.Store for unit tests and local
// development: run real managers, handlers and enqueue flows with no
// MongoDB, no Docker, in milliseconds, deterministically.
//
// It honors the engine's real semantics — atomic claims, lease-token
// fencing, stale reclaim bounded by attempts, unique keys, requeue,
// reaping, the queues registry and batch finalization — so tests written
// against it exercise genuine library behavior (the gotasks manager test
// suite itself runs on this store).
//
// It is NOT a production store: nothing is persisted, it is single-process
// only, and there is no change-stream support (managers simply poll).
package memstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ameenkh/gotasks"
)

// Store implements gotasks.Store in memory. Create with New.
type Store struct {
	mu       sync.Mutex
	seq      int
	tasks    map[string]*gotasks.Task
	registry map[string]gotasks.QueuePolicy

	metrics   []metric
	gaugeKeys map[string]bool

	forceLeaseLost atomic.Bool
}

type metric struct {
	queue       string
	windowStart time.Time
	managerID   string // "" = gauges
	counters    gotasks.QueueCounters
}

var _ gotasks.Store = (*Store)(nil)

// New returns an empty in-memory store.
func New() *Store {
	return &Store{
		tasks:     map[string]*gotasks.Task{},
		registry:  map[string]gotasks.QueuePolicy{},
		gaugeKeys: map[string]bool{},
	}
}

// ---- test helpers ----------------------------------------------------

// Task returns a copy of one task by id (nil when absent) — for assertions.
func (s *Store) Task(id string) *gotasks.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tasks[id]; ok {
		return clone(t)
	}
	return nil
}

// Tasks returns copies of all tasks — for assertions.
func (s *Store) Tasks() []*gotasks.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*gotasks.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, clone(t))
	}
	return out
}

// ForceLeaseLost makes every ExtendLease fail with ErrLeaseLost while set —
// simulates the task having been reclaimed elsewhere (heartbeat/handshake
// failure paths).
func (s *Store) ForceLeaseLost(v bool) { s.forceLeaseLost.Store(v) }

// CounterTotals sums every recorded counters document for one queue.
func (s *Store) CounterTotals(queue string) gotasks.QueueCounters {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out gotasks.QueueCounters
	for _, m := range s.metrics {
		if m.managerID != "" && m.queue == queue {
			out.Enqueued += m.counters.Enqueued
			out.Claimed += m.counters.Claimed
			out.Done += m.counters.Done
			out.Failed += m.counters.Failed
			out.Dead += m.counters.Dead
		}
	}
	return out
}

// MetricsCount reports how many metric documents (gauges + counters) were
// recorded.
func (s *Store) MetricsCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.metrics)
}

func clone(t *gotasks.Task) *gotasks.Task {
	c := *t
	c.Payload = slices.Clone(t.Payload)
	c.Result = slices.Clone(t.Result)
	c.Errors = slices.Clone(t.Errors)
	return &c
}

// ---- Store implementation --------------------------------------------

func (s *Store) activeKeyHolder(key string) *gotasks.Task {
	for _, t := range s.tasks {
		if t.UniqueKey == key && (t.Status == gotasks.StatusPending || t.Status == gotasks.StatusRunning) {
			return t
		}
	}
	return nil
}

func (s *Store) Enqueue(_ context.Context, tasks []*gotasks.Task) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, len(tasks))
	for i, t := range tasks {
		if t.UniqueKey != "" {
			if holder := s.activeKeyHolder(t.UniqueKey); holder != nil {
				return nil, &gotasks.DuplicateTaskError{Key: t.UniqueKey, ExistingID: holder.ID}
			}
		}
		s.seq++
		c := clone(t)
		c.ID = fmt.Sprintf("task-%d", s.seq)
		s.tasks[c.ID] = c
		ids[i] = c.ID
	}
	return ids, nil
}

// claimOneLocked picks oldest-first — a valid choice for unsorted mode,
// which lets the store take any runnable task.
func (s *Store) claimOneLocked(opts gotasks.ClaimOptions, now time.Time) *gotasks.Task {
	var best *gotasks.Task
	for _, t := range s.tasks {
		if len(opts.Types) > 0 && !slices.Contains(opts.Types, t.Type) {
			continue
		}
		if len(opts.Queues) > 0 && !slices.Contains(opts.Queues, t.Queue) {
			continue
		}
		runnable := (t.Status == gotasks.StatusPending && !t.RunAt.After(now)) ||
			(t.Status == gotasks.StatusRunning && t.LeasedUntil.Before(now) && t.Attempts < t.MaxAttempts)
		if !runnable {
			continue
		}
		if best == nil || t.RunAt.Before(best.RunAt) || (t.RunAt.Equal(best.RunAt) && t.ID < best.ID) {
			best = t
		}
	}
	if best == nil {
		return nil
	}
	s.seq++
	best.Status = gotasks.StatusRunning
	best.Attempts++
	best.LeasedBy = opts.WorkerID
	best.LeaseToken = fmt.Sprintf("lease-%d", s.seq)
	best.LeasedUntil = now.Add(opts.Lease)
	best.UpdatedAt = now
	return clone(best)
}

func (s *Store) Claim(_ context.Context, opts gotasks.ClaimOptions) (*gotasks.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.claimOneLocked(opts, time.Now().UTC())
	if t == nil {
		return nil, gotasks.ErrNoTask
	}
	return t, nil
}

func (s *Store) ClaimBatch(_ context.Context, opts gotasks.ClaimOptions, k int) ([]*gotasks.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var out []*gotasks.Task
	for len(out) < k {
		t := s.claimOneLocked(opts, now)
		if t == nil {
			break
		}
		out = append(out, t)
	}
	if len(out) == 0 {
		return nil, gotasks.ErrNoTask
	}
	return out, nil
}

func (s *Store) held(id, leaseToken string) *gotasks.Task {
	t, ok := s.tasks[id]
	if !ok || t.Status != gotasks.StatusRunning || t.LeaseToken != leaseToken {
		return nil
	}
	return t
}

func (s *Store) Complete(_ context.Context, task *gotasks.Task, result json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.held(task.ID, task.LeaseToken)
	if t == nil {
		return gotasks.ErrLeaseLost
	}
	t.Status = gotasks.StatusDone
	t.Result = slices.Clone(result)
	t.LeasedBy, t.LeaseToken, t.UniqueKey = "", "", ""
	t.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *Store) Fail(_ context.Context, task *gotasks.Task, taskErr gotasks.TaskError, retryAt time.Time, terminal bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.held(task.ID, task.LeaseToken)
	if t == nil {
		return gotasks.ErrLeaseLost
	}
	t.Errors = append(t.Errors, taskErr)
	if terminal {
		t.Status = gotasks.StatusDead
		t.UniqueKey = ""
	} else {
		t.Status = gotasks.StatusPending
		t.RunAt = retryAt
	}
	t.LeasedBy, t.LeaseToken = "", ""
	t.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *Store) FinalizeBatch(ctx context.Context, outcomes []gotasks.Outcome) (int64, error) {
	var lost int64
	for _, o := range outcomes {
		var err error
		if o.Failure != nil {
			err = s.Fail(ctx, o.Task, *o.Failure, o.RetryAt, o.Terminal)
		} else {
			err = s.Complete(ctx, o.Task, o.Result)
		}
		if errors.Is(err, gotasks.ErrLeaseLost) {
			lost++
		} else if err != nil {
			return lost, err
		}
	}
	return lost, nil
}

func (s *Store) ExtendLease(_ context.Context, task *gotasks.Task, lease time.Duration) (time.Time, error) {
	if s.forceLeaseLost.Load() {
		return time.Time{}, gotasks.ErrLeaseLost
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.held(task.ID, task.LeaseToken)
	if t == nil {
		return time.Time{}, gotasks.ErrLeaseLost
	}
	t.LeasedUntil = time.Now().UTC().Add(lease)
	return t.LeasedUntil, nil
}

func (s *Store) requeueLocked(t *gotasks.Task, now time.Time) {
	t.Status = gotasks.StatusPending
	t.Attempts = 0
	t.RunAt = now
	t.Result = nil
	t.LeasedBy, t.LeaseToken = "", ""
	t.UpdatedAt = now
}

func (s *Store) Requeue(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok || t.Status != gotasks.StatusDead {
		return fmt.Errorf("%w: no dead task with id %q", gotasks.ErrNotFound, id)
	}
	s.requeueLocked(t, time.Now().UTC())
	return nil
}

func (s *Store) RequeueDead(_ context.Context, taskType string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var n int64
	for _, t := range s.tasks {
		if t.Status == gotasks.StatusDead && (taskType == "" || t.Type == taskType) {
			s.requeueLocked(t, now)
			n++
		}
	}
	return n, nil
}

func (s *Store) ReapExpired(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var n int64
	for _, t := range s.tasks {
		if t.Status == gotasks.StatusRunning && t.LeasedUntil.Before(now) && t.Attempts >= t.MaxAttempts {
			t.Status = gotasks.StatusDead
			t.Errors = append(t.Errors, gotasks.TaskError{
				At: now, Attempt: t.Attempts, Worker: t.LeasedBy,
				Message: "lease expired before completion; attempts exhausted",
			})
			t.LeasedBy, t.LeaseToken, t.UniqueKey = "", "", ""
			n++
		}
	}
	return n, nil
}

func (s *Store) RegisterQueues(_ context.Context, queues []gotasks.QueuePolicy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range queues {
		if existing, ok := s.registry[q.Name]; ok {
			if existing.TTL != q.TTL || existing.MaxAttempts != q.MaxAttempts {
				return fmt.Errorf("%w: queue %q", gotasks.ErrQueueConflict, q.Name)
			}
			continue
		}
		s.registry[q.Name] = q
	}
	return nil
}

func (s *Store) Queues(context.Context) ([]gotasks.QueuePolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]gotasks.QueuePolicy, 0, len(s.registry))
	for _, q := range s.registry {
		out = append(out, q)
	}
	return out, nil
}

func (s *Store) SetQueuePolicy(_ context.Context, q gotasks.QueuePolicy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registry[q.Name] = q
	return nil
}

func (s *Store) EnsureMetrics(context.Context, time.Duration) error { return nil }

func (s *Store) SnapshotMetrics(_ context.Context, queues []string, windowStart time.Time, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range queues {
		key := q + "|" + windowStart.String()
		if s.gaugeKeys[key] {
			continue // dedup: first snapshot wins
		}
		s.gaugeKeys[key] = true
		s.metrics = append(s.metrics, metric{queue: q, windowStart: windowStart})
	}
	return nil
}

func (s *Store) RecordCounters(_ context.Context, managerID string, windowStart time.Time, _ time.Duration, counters map[string]gotasks.QueueCounters) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for q, c := range counters {
		s.metrics = append(s.metrics, metric{queue: q, windowStart: windowStart, managerID: managerID, counters: c})
	}
	return nil
}

func (s *Store) Close(context.Context) error { return nil }
