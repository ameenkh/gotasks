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
- [x] Per-type retention: `WithRetentionByType` overrides the default
      retention per task type (0 = keep that type forever); reaper applies it
      via a `$switch` pipeline expression
- [x] Examples suite (`examples/`): scheduled, retries/dead-letter/requeue,
      unique keys, heartbeat, at-most-once, batch fan-out, retention
- [x] Scenario soak tests (`scenario_test.go`): 6 scenarios, each producing
      and consuming against real Mongo for 30s (GOTASKS_SCENARIO_DURATION),
      then draining and validating final state — exactly-once counts, retry
      budgets, schedule timing, unique-key overlap, heartbeat no-reclaim
- [x] GitHub Actions CI: build + vet + race tests + scenario soak on every
      push to main and on PRs (Mongo 7 service container)

## v0.3 — Big-pipeline scale

- [x] Batch claim — implemented 2026-09-16. Mode is selected by MaxBatch:
      1 (default) = single mode, workers claim directly exactly as v0.2
      (no fetcher, no channel, no handshake RTT); 2..100 = batch mode via
      WithMaxBatch. Store gains ClaimBatch (find candidate ids → guarded
      updateMany → fetch winners by batch lease token; 3 fixed RTTs, always
      returns exact docs; partial index on lease_token keeps step 3 cheap).
      Spec as designed (two-lease model):
      - Two lease durations: the batch claim takes a QUEUE lease
        (WithQueueLeaseTime, default = LeaseTime) covering time buffered in
        the channel; the TASK lease (LeaseTime) only starts when a worker
        begins execution.
      - Adaptive K: `K = min(MaxBatch, free channel slots)` — never fetch
        more than the pool can absorb soon (MaxBatch default ~32, hard cap
        ~100 to bound crash blast-radius). Keeps channel wait ≈ one handler
        duration, so the default queue lease holds.
      - Continuous top-up, not drain-then-refill: the bounded channel IS the
        backpressure. The fetcher refills whenever free slots ≥ a low-water
        mark (≈ half capacity) so fetch I/O overlaps handler compute and
        workers never bubble on an empty channel; full batch → refetch
        immediately (railway), empty → sleep PollInterval / wait for the
        change-stream nudge (#21). One claim in flight per fetcher —
        parallel batch claims from one process would only contend with
        themselves.
- [x] Unsorted claiming by default + `WithFIFO()` — decided and implemented
      2026-09-16 (revives v0.1's IsFIFO idea). The sort was never needed
      for correctness (run_at is enforced by the claim filter), cost a
      SORT_MERGE across the $or branches, and concentrated all claimers on
      the head doc. WithFIFO restores oldest-first CLAIM order for both
      single and batch claims (parallel completion order is never
      guaranteed by any mode); choose it when task staleness must be
      bounded under overload. Claim/ClaimBatch now take a ClaimOptions
      struct (worker id, types, queues, FIFO, lease).
- [x] Named queues, phase 1 of two ("field only", decided 2026-09-16):
      tasks carry a `queue` field (DefaultQueue unless WithQueue), and a
      manager claims only from its WithQueues set (empty = all). One
      pipeline per manager; per-queue pools today = one manager per queue
      (same process, shared client via FromClient). Phase 2 (below) will
      internalize N pipelines in one manager.
      - Start-of-work handshake at dequeue: if the in-memory locked_until
        already passed → drop locally, no RTT, zero side effects (a
        reclaimer owns it, or the reaper will surface it). Otherwise one
        ExtendLease(task lease): success → we provably own it, run;
        ErrLeaseLost → drop silently; network error → retry once, then
        drop (dropping is always safe). No margin heuristic, no fetcher
        channel-heartbeat, and the in-memory task is immutable after claim.
      - Cost note: the handshake is one uncontended point-write per task on
        the worker's timeline; the contended sorted claim stays amortized at
        ~2 RTTs per batch. A margin-mode opt-out knob can come later for
        sub-ms handler fleets if ever needed.
      - Token fencing on finalize stays the last resort, unchanged.
      - Residual duplicate-execution window = process freeze after the
        handshake, inherent to leases; documented as "effectively
        at-least-once, make handlers idempotent".
- [ ] Named queues phase 2: per-queue pools inside one manager (one
      fetcher/channel/worker-set per configured queue), plus a
      (queue, status, run_at, _id) index when queue-filtered claiming
      becomes the norm
- [ ] Priorities in the claim sort
- [x] Change-stream wakeup — implemented 2026-09-16. Optional Watcher
      capability interface on the store (Store interface unchanged);
      mongostore tails a $match-filtered stream (inserts of matching
      pending tasks; updates touching run_at or status=pending), coalesces
      into level-triggered capacity-1 nudges, tracks the min future run_at
      with a timer (covers retry backoffs + scheduled tasks), reconnects
      with resume tokens and nudges after blind windows. Manager: auto-
      detect at Start with silent poll fallback (WithoutChangeStream to
      force), FallbackPoll safety net (default 30s) while the stream is
      healthy, broadcast wakeup for single mode, direct fetcher wakeup for
      batch mode. Measured: ~11ms median pickup with polling at 10s. CI and
      local Mongo now run as single-node replica sets.
- [x] Batch finalize — implemented 2026-09-17 (WithFinalizeBatch(size,
      interval), off by default; Store gains FinalizeBatch + Outcome;
      workers buffer, a flusher bulkWrites). Measured on the benchmark
      suite: full pipeline (batch claim 64 + finalize 128, 16 workers)
      658 -> 1157 tasks/sec (+76%); single-mode + finalize 64: +36%.
      Design as agreed, lease-aware buffering:
      - Safety net: a missed flush degrades into exactly the died-worker
        path (stale reclaim / reaper) and the flush's fenced entries no-op
        (ErrLeaseLost per op) — correctness is never at risk, only
        duplicate execution.
      - Buffer gate: outcomes with remaining lease < SafetyMargin
        (max(4x flush interval, 2s)) finalize immediately instead of
        buffering — no added cost, no added risk for the risky ones.
      - Deadline trigger: flusher force-flushes when the earliest buffered
        LockedUntil approaches the margin (min-tracking, same shape as the
        change-stream timer), so a buffered outcome cannot outlive its
        lease while the process is alive.
      - max_attempts=1 outcomes NEVER buffer (prevents "succeeded but
        recorded dead" via the reaper; preserves at-most-once semantics).
      - Fenced-out flush entries are logged + counted for observability.
      - Residual risk: crash/freeze with buffered outcomes — widens the
        existing crash-after-handler window by <= one flush interval; same
        at-least-once contract, no new failure class.
      - Flush triggers: N outcomes (~64) / interval (~25-50ms) / lease
        deadline / shutdown drain. Opt-in (immediate finalize stays the
        default), off for at-most-once regardless.
- [x] Benchmark suite (bench_test.go, 2026-09-16): enqueue single/batch +
      end-to-end throughput across worker/MaxBatch configs; reference
      numbers in the README. First finding: throughput plateaus ~600/s on
      dev hardware regardless of claim mode → the per-task finalize write
      is now the bottleneck, which is precisely the batch-finalize case.
- [ ] Chunked mega-batch enqueue with partial-failure reporting

## v1.0 — Ecosystem

- [ ] Hooks/middleware (OnClaim/OnComplete/OnFail/OnDead)
- [ ] slog integration (done in v0.1), Prometheus metrics, OTel spans
- [ ] Introspection API (counts by status/type, list/filter) + `gotasksctl`
- [ ] Transactional enqueue via Mongo sessions (replica sets): Enqueue joins
      the caller's transaction so business writes and their tasks commit
      atomically — closes the gap with Postgres queues' headline feature
- [ ] In-memory store promoted to a public package (users' unit tests)
- [ ] Benchmarks, chaos test (kill workers mid-task; assert no loss/dup)
- [ ] Docs site, CI, semver releases

## Nice to have (not planned — review on demand)

- **Recurring/cron tasks**: discussed 2026-09-15, parked. If revisited, the
  preferred design is leaderless: every node schedules, and a `cron_runs`
  ledger collection with a unique `(name, tick)` index + TTL makes exactly
  one node win each occurrence — no leader election, no failover gap. (Task
  unique keys can't serve as the dedup: they're released at done/dead, so a
  lagging node could re-enqueue a finished occurrence.) Open choices:
  robfig/cron parser vs interval-only syntax; skip vs backfill missed ticks.
- **Pause/resume queues**: parked 2026-09-15 (same shape as cancellation: a
  flag the claim path respects). If revisited: a per-queue/per-type flag
  document that the claim filter checks — paused types simply stop matching.
- **Cancellation**: discussed 2026-09-15, parked. Pending-task cancel is a
  one-line status flip to a new terminal `canceled` status. Running-task
  cancel is cooperative; preferred delivery is piggybacking the heartbeat's
  existing round trip on a `cancel_requested` flag (zero extra load, but
  requires heartbeat enabled). A cancelled handler should finalize as
  `canceled`, not consume a retry attempt (distinguish via context.Cause).

## v2+ (deferred)

- Postgres (or other) stores — decided against 2026-09-16: gotasks is
  Mongo-native by identity. Postgres's FOR UPDATE SKIP LOCKED is genuinely
  the better claim primitive and transactional enqueue is its killer
  feature, but that market is owned by River/Oban and a port would be a
  Mongo-shaped transplant. Instead we lean into Mongo-only mechanisms
  (change streams, pipeline updates, sessions for transactional enqueue,
  shard-aware queues). The Store interface stays as an internal seam and
  test boundary, not a multi-store promise.
- Message/stream semantics (discussed 2026-09-16): pub/sub fan-out and
  consumer groups as a `gotasks/streams` sub-package on change streams +
  per-group offset docs — NOT a pivot of the core. Identity stays "job
  queue, not message broker": tasks have state, history, and exactly one
  owner; broker-scale users are better served by actual brokers. Revisit
  only on real user demand.
- Workflows/chains (parent-child, fan-out/fan-in)
- Web dashboard
- Multi-tenant namespacing
