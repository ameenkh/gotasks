package gotasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrNoTask is returned by Store.Claim when no runnable task exists.
	ErrNoTask = errors.New("gotasks: no task available")
	// ErrLeaseLost is returned by Complete/Fail/ExtendLease when the task is
	// no longer held under the given lease token (it was reclaimed by another
	// worker after the lease expired, or already finalized).
	ErrLeaseLost = errors.New("gotasks: task lease lost or task already finalized")
	// ErrNotFound is returned when a task id does not exist (or is malformed
	// for the store's id scheme), or is not in the state the call requires
	// (e.g. Requeue on a task that is not dead).
	ErrNotFound = errors.New("gotasks: task not found")
	// ErrDuplicateTask is matched (via errors.Is) by the *DuplicateTaskError
	// returned when enqueueing with a unique key that already has an active
	// (pending/running) task.
	ErrDuplicateTask = errors.New("gotasks: duplicate unique key")
)

// EnqueueFailure is one failed entry of a batch enqueue.
type EnqueueFailure struct {
	Index int // position in the input batch
	Err   error
}

// PartialEnqueueError reports a partially successful batch enqueue: entries
// not listed in Failures were inserted (chunked writes are not atomic).
// The ids returned alongside this error stay aligned with the input, with
// "" at each failed index.
type PartialEnqueueError struct {
	Failures []EnqueueFailure
}

func (e *PartialEnqueueError) Error() string {
	return fmt.Sprintf("gotasks: %d entries of the batch failed to enqueue (first: index %d: %v)",
		len(e.Failures), e.Failures[0].Index, e.Failures[0].Err)
}

// DuplicateTaskError reports a unique-key conflict on enqueue.
// errors.Is(err, ErrDuplicateTask) matches it; ExistingID is the id of the
// active task holding the key ("" if it finished between insert and lookup).
type DuplicateTaskError struct {
	Key        string
	ExistingID string
}

func (e *DuplicateTaskError) Error() string {
	return fmt.Sprintf("gotasks: task with unique key %q already active (id %q)", e.Key, e.ExistingID)
}

func (e *DuplicateTaskError) Is(target error) bool { return target == ErrDuplicateTask }

// ClaimOptions parameterizes Claim and ClaimBatch.
type ClaimOptions struct {
	// WorkerID is recorded on the task as locked_by.
	WorkerID string
	// Types restricts claims to these task types; empty = all types.
	Types []string
	// Queues restricts claims to these queues; empty = all queues.
	Queues []string
	// FIFO requests oldest-first claim order ((run_at, id) ascending).
	// False (the default) lets the store pick whichever runnable task is
	// cheapest to take — faster and less contended, but task staleness is
	// unbounded under sustained overload.
	FIFO bool
	// Lease is how long the claim holds the task before it becomes
	// reclaimable.
	Lease time.Duration
}

// Outcome is one finished attempt, carried between a worker and (batched)
// finalization. Failure nil = success.
type Outcome struct {
	Task     *Task
	Result   json.RawMessage // success only; nil = no result
	Failure  *TaskError      // non-nil = this attempt failed
	RetryAt  time.Time       // failure, non-terminal: the next run_at
	Terminal bool            // failure: attempts exhausted -> dead
}

// Watcher is an optional Store capability: pushing "work may be available"
// wakeup signals so managers don't rely on polling while idle. The manager
// type-asserts for it at Start; stores without it (or whose deployment
// can't support it) are simply polled.
//
// Contract: the returned channel is a coalesced, level-triggered nudge —
// capacity 1, dropped when full; a receive means "claim now, something may
// be runnable". False positives are fine (one empty claim); implementations
// should be conservative the other way. The implementation is responsible
// for reconnection (nudging after any blind window) and for signaling when
// known future run_at times come due. The channel closes when ctx ends.
// An error return means watching is unavailable (e.g. no replica set) and
// the caller should fall back to polling.
type Watcher interface {
	WatchRunnable(ctx context.Context, types, queues []string) (<-chan struct{}, error)
}

// Store is the persistence backend. Implementations must make Claim atomic:
// under concurrent claims, each runnable task is handed to exactly one caller.
//
// Fencing contract: Claim generates a fresh lease token per claim.
// Complete/Fail/ExtendLease must only match a task that is still running
// under the token carried by the passed *Task, and return ErrLeaseLost
// otherwise. Matching must NOT require the lease to still be unexpired —
// finishing after expiry but before anyone reclaimed the task is a success,
// not a conflict.
//
// Unique-key contract: at most one task with a given UniqueKey may be active
// (pending or running) at a time; a conflicting enqueue returns
// *DuplicateTaskError. The key is released when the task reaches done/dead.
type Store interface {
	// Enqueue inserts tasks (batch; callers pass a single-element slice for
	// one task) and returns their assigned ids aligned with the input.
	// Large batches may be written in chunks; on partial failure the ids
	// slice carries "" at failed indexes and the error is a
	// *PartialEnqueueError (single-task unique-key conflicts keep returning
	// *DuplicateTaskError).
	Enqueue(ctx context.Context, tasks []*Task) ([]string, error)

	// Claim atomically takes one runnable task and returns it with
	// Status=running, Attempts incremented, and a fresh LeaseToken/LockedUntil.
	// Runnable means: (pending AND run_at <= now) OR
	// (running AND locked_until < now AND attempts < max_attempts).
	// Returns ErrNoTask when nothing matches.
	Claim(ctx context.Context, opts ClaimOptions) (*Task, error)

	// ClaimBatch atomically takes up to k runnable tasks (same runnable
	// definition and ordering as Claim) under a single lease token, in a
	// constant number of round trips. Concurrent callers may split the
	// candidates: a short (even empty) result with a nil error means others
	// won some or all of them — more work may still exist, so callers should
	// retry promptly. ErrNoTask means no runnable candidates existed at all.
	ClaimBatch(ctx context.Context, opts ClaimOptions, k int) ([]*Task, error)

	// Complete marks t done, stores the handler result (nil = none), and
	// releases t's unique key if it has one.
	Complete(ctx context.Context, t *Task, result json.RawMessage) error

	// Fail records taskErr on t's errors array. If terminal is false the
	// task is rescheduled: status back to pending with run_at = retryAt
	// (unique key kept — the task is still active). If terminal is true the
	// task becomes dead and its unique key is released.
	Fail(ctx context.Context, t *Task, taskErr TaskError, retryAt time.Time, terminal bool) error

	// ExtendLease pushes t's locked_until to now+lease and returns the new
	// deadline. Used by the manager's heartbeat for long-running handlers.
	ExtendLease(ctx context.Context, t *Task, lease time.Duration) (time.Time, error)

	// FinalizeBatch applies many Complete/Fail outcomes in one round trip.
	// Each entry is fenced individually, exactly like Complete/Fail; an
	// entry whose lease was lost is skipped, not an error. Returns how many
	// entries were skipped that way (for observability).
	FinalizeBatch(ctx context.Context, outcomes []Outcome) (lost int64, err error)

	// Requeue resets one dead task to pending: attempts back to 0, runnable
	// now, retention expiry cleared. The errors array is kept as history.
	// Returns ErrNotFound if the id doesn't exist or the task isn't dead.
	Requeue(ctx context.Context, id string) error

	// RequeueDead requeues every dead task (of taskType, or all types when
	// taskType is ""), with the same resets as Requeue. Returns the count.
	RequeueDead(ctx context.Context, taskType string) (int64, error)

	// ReapExpired marks running tasks whose lease expired AND whose attempts
	// are exhausted as dead (with a "lease expired" error entry), so dropped
	// tasks are visible instead of zombie "running" docs. Tasks with
	// attempts remaining are NOT touched — Claim reclaims those naturally.
	// Returns the number of tasks reaped.
	ReapExpired(ctx context.Context) (int64, error)

	// Close releases the store's resources.
	Close(ctx context.Context) error
}
