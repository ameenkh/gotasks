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
	// one task) and returns their assigned ids in order.
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
