package gotasks

import (
	"context"
	"encoding/json"
	"errors"
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
	// for the store's id scheme).
	ErrNotFound = errors.New("gotasks: task not found")
)

// Store is the persistence backend. Implementations must make Claim atomic:
// under concurrent claims, each runnable task is handed to exactly one caller.
//
// Fencing contract: Claim generates a fresh lease token per claim.
// Complete/Fail/ExtendLease must only match a task that is still running
// under that exact token, and return ErrLeaseLost otherwise. Matching must
// NOT require the lease to still be unexpired — finishing after expiry but
// before anyone reclaimed the task is a success, not a conflict.
type Store interface {
	// Enqueue inserts tasks (batch; callers pass a single-element slice for
	// one task) and returns their assigned ids in order.
	Enqueue(ctx context.Context, tasks []*Task) ([]string, error)

	// Claim atomically takes one runnable task and returns it with
	// Status=running, Attempts incremented, and a fresh LeaseToken/LockedUntil.
	// Runnable means: (pending AND run_at <= now) OR
	// (running AND locked_until < now AND attempts < max_attempts).
	// If types is non-empty, only tasks of those types are considered.
	// Selection order is (run_at, id) ascending. Returns ErrNoTask when empty.
	Claim(ctx context.Context, workerID string, types []string, lease time.Duration) (*Task, error)

	// Complete marks the task done and stores the handler result (nil = none).
	Complete(ctx context.Context, id, leaseToken string, result json.RawMessage) error

	// Fail records taskErr on the task's errors array. If terminal is false
	// the task is rescheduled: status back to pending with run_at = retryAt.
	// If terminal is true the task becomes failed.
	Fail(ctx context.Context, id, leaseToken string, taskErr TaskError, retryAt time.Time, terminal bool) error

	// ExtendLease pushes locked_until to now+lease and returns the new
	// deadline. For long-running handlers (heartbeat).
	ExtendLease(ctx context.Context, id, leaseToken string, lease time.Duration) (time.Time, error)

	// ReapExpired marks running tasks whose lease expired AND whose attempts
	// are exhausted as failed (with a "lease expired" error entry), so
	// dropped tasks are visible instead of zombie "running" docs. Tasks with
	// attempts remaining are NOT touched — Claim reclaims those naturally.
	// Returns the number of tasks reaped.
	ReapExpired(ctx context.Context) (int64, error)

	// Close releases the store's resources.
	Close(ctx context.Context) error
}
