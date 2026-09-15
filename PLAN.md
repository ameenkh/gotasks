# gotasks — Implementation Plan

Persistent, atomic, asynchronous task processing for Go. Pluggable stores
(MongoDB first), a worker pool with a railway fetch loop, retries with backoff,
scheduling via `run_at`, and lease-based fencing so no two workers ever run the
same task.

Module: `github.com/ameenkh/gotasks`

## Design decisions (locked in)

- **IDs**: store-native (Mongo ObjectID hex), exposed as `string`. No counter
  collection — a single counter document is a global write hotspot.
- **Ordering**: claim sorted by `(run_at, _id)` — FIFO-ish for free.
- **Attempts count up** (`attempts` / `max_attempts`), not retries counting
  down. `attempts` is incremented atomically by the claim itself, so a claim
  *is* an attempt — a worker that dies mid-task has still consumed one.
- **Stale tasks**: the claim query reclaims `running` tasks whose
  `locked_until` has passed, **but only while `attempts < max_attempts`**.
  So `max_attempts=1` = at-most-once (a stale task is dropped, never re-run).
  A reaper periodically marks stale+exhausted tasks `failed` with a
  "lease expired" error so drops are visible, not zombies.
- **Fencing**: every claim generates a fresh `lease_token` (UUID).
  Complete/Fail/ExtendLease match on `(id, lease_token, status=running)` — a
  worker whose task was reclaimed can no longer touch it (the token changed).
  Finalizing after lease expiry but *before* anyone reclaimed is allowed: the
  work is done and nobody else took it.
- **Payloads/results**: typed via generics at the API
  (`gotasks.Enqueue[T]`, `gotasks.RegisterHandler[T]`), carried as JSON
  (`json.RawMessage`) in the core, stored as native BSON documents in Mongo
  (Ext-JSON conversion) so tasks stay inspectable with normal Mongo tooling.
  Payloads must be JSON objects; non-object handler results are wrapped as
  `{"value": ...}`.
- **Backoff lives in the manager**, not the store. The store's `Fail` just
  executes a decision (retry at T / terminal).
- **Railway loop**: each worker keeps claiming as long as tasks come back;
  it sleeps `PollInterval` only after an empty claim.
- **No golocks dependency**. Atomic claim *is* the lock. The lease concept
  returns in v0.2 as an internal primitive for scheduler leader election.
- MongoDB ≥ 4.2 required (`$expr` in claim filter, pipeline updates in reaper).

## v0.1 — Core (this milestone)

- [x] Task model: id, type, payload, status (pending/running/done/failed),
      attempts/max_attempts, run_at, locked_by/locked_until, lease_token,
      errors array (`{at, attempt, worker, message}`), result, timestamps
- [x] `Store` interface: Enqueue (batch), Claim, Complete, Fail, ExtendLease,
      ReapExpired, Close
- [x] Mongo store: atomic `findOneAndUpdate` claim with stale reclaim built in,
      indexes ensured on init, optional TTL retention for finished tasks
- [x] Manager: functional options, typed enqueue (`Enqueue`/`EnqueueMany`),
      `WithRunAt`/`WithDelay`/`WithMaxAttempts` per enqueue
- [x] Worker pool: `RegisterHandler[T]` per task type, railway loop,
      per-type timeout, panic recovery (counts as a failed attempt),
      graceful drain on Stop, claim restricted to registered types
- [x] Retries with pluggable backoff (fixed / linear / exponential+jitter)
- [x] Scheduled tasks via `run_at`
- [x] Reaper: stale + attempts-exhausted → `failed` ("lease expired")
- [x] Tests: manager against in-memory fake store; Mongo integration tests
      (skip when no local Mongo)
- [x] Example + README

## v0.2 — Robustness

- [x] Heartbeat: auto ExtendLease for long-running handlers — opt-in via
      `WithHeartbeat` (LeaseTime/3) or `WithHeartbeatInterval`; off by
      default so the library adds zero background writes unless asked. On
      lease loss the handler's context is cancelled — no burning work
      someone else owns.
- [x] Dead-letter status `dead` (replaces terminal `failed`); `Requeue(id)` +
      `RequeueDead(type)` reset attempts and clear retention expiry, keeping
      the errors array as history
- [x] Unique/idempotent tasks: `WithUniqueKey` + partial unique index; key
      held while pending/running (kept across retries), released at done/dead;
      conflict returns existing id + ErrDuplicateTask
- [ ] Recurring/cron tasks + leader election via internal lease (golocks
      idea) — deferred, to discuss
- [ ] `Cancel(id)` for pending; cooperative cancel for running — deferred,
      to discuss
- [x] Per-type retention: `WithRetentionByType` overrides the default
      retention per task type (0 = keep that type forever); reaper applies it
      via a `$switch` pipeline expression

## v0.3 — Big-pipeline scale

- [ ] Batch claim (claim K per round trip; fetcher feeding worker channel)
- [ ] Named queues, per-queue pools
- [ ] Priorities in the claim sort
- [ ] Change-stream wakeup (replica sets), polling fallback
- [ ] Per-type concurrency limits and rate limiting
- [ ] Pause/resume queues
- [ ] Chunked mega-batch enqueue with partial-failure reporting

## v1.0 — Ecosystem

- [ ] Hooks/middleware (OnClaim/OnComplete/OnFail/OnDead)
- [ ] slog integration (done in v0.1), Prometheus metrics, OTel spans
- [ ] Introspection API (counts by status/type, list/filter) + `gotasksctl`
- [ ] Postgres store (`FOR UPDATE SKIP LOCKED`), in-memory store promoted to
      a public package
- [ ] Benchmarks, chaos test (kill workers mid-task; assert no loss/dup)
- [ ] Docs site, CI, semver releases

## v2+ (deferred)

- Workflows/chains (parent-child, fan-out/fan-in)
- Web dashboard
- Multi-tenant namespacing
