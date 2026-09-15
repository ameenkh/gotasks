// Package gotasks is a persistent, atomic, asynchronous task queue for Go.
// Tasks live in a pluggable store (MongoDB first); a pool of workers claims
// them atomically (one claim = one attempt), runs a typed handler per task
// type, and retries failures with backoff. Scheduling is done via run_at;
// every claim takes a lease (locked_until + lease_token) so a task whose
// worker died is reclaimed once the lease expires — never run twice at once.
package gotasks

import (
	"encoding/json"
	"time"
)

// Status is the lifecycle state of a task.
type Status string

const (
	StatusPending Status = "pending" // waiting for a worker (run_at may be in the future)
	StatusRunning Status = "running" // claimed under a live lease
	StatusDone    Status = "done"    // handler succeeded
	// StatusDead is the dead-letter state: attempts exhausted (or lease
	// expired with none left). Dead tasks sit inspectable until requeued
	// (Manager.Requeue / RequeueDead) or pruned by retention.
	StatusDead Status = "dead"
)

// TaskError is one failed attempt, kept on the task in order.
type TaskError struct {
	At      time.Time `bson:"at" json:"at"`
	Attempt int       `bson:"attempt" json:"attempt"`
	Worker  string    `bson:"worker" json:"worker"`
	Message string    `bson:"message" json:"message"`
}

// Task is one unit of work. Payload and Result are JSON; use the typed
// Enqueue / RegisterHandler API instead of touching them directly.
type Task struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	UniqueKey   string          `json:"unique_key,omitempty"` // dedup key: at most one pending/running task per key
	Payload     json.RawMessage `json:"payload,omitempty"`
	Status      Status          `json:"status"`
	Attempts    int             `json:"attempts"`     // claims so far (a claim IS an attempt)
	MaxAttempts int             `json:"max_attempts"` // 1 = at-most-once: never retried, stale never reclaimed
	RunAt       time.Time       `json:"run_at"`       // eligible-to-run time (scheduling / retry backoff)
	LockedBy    string          `json:"locked_by,omitempty"`
	LockedUntil time.Time       `json:"locked_until,omitempty"`
	LeaseToken  string          `json:"-"` // fencing token; regenerated on every claim
	Errors      []TaskError     `json:"errors,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}
