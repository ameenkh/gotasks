# gotasks

[![CI](https://github.com/ameenkh/gotasks/actions/workflows/ci.yml/badge.svg)](https://github.com/ameenkh/gotasks/actions/workflows/ci.yml)

Persistent, atomic, asynchronous task processing for Go — backed by a
pluggable store (MongoDB first), with typed payloads, retries with backoff,
scheduling, and lease-based fencing so no two workers ever run the same task.

```
go get github.com/ameenkh/gotasks
```

## Why

- **Atomic claiming** — one `findOneAndUpdate` per claim; N workers across N
  machines never take the same task.
- **Lease fencing** — every claim gets a fresh lease token; a worker whose
  lease was reclaimed can no longer complete/fail the task.
- **Railway workers** — workers keep claiming while tasks keep coming, and
  only sleep the poll interval when the queue is empty.
- **Change-stream wakeup** — on replica sets, idle managers are woken by
  Mongo's change stream in milliseconds instead of polling (automatic;
  falls back to polling on standalone servers).
- **Typed API** — payloads and handlers are generic; no `map[string]interface{}`.
- **Inspectable** — tasks are plain BSON documents; open them in mongosh or
  Compass, payloads included.

## Quick start

```go
store, err := mongostore.New(ctx, "mongodb://localhost:27017",
    mongostore.WithDatabase("myapp"),
    mongostore.WithRetention(7*24*time.Hour), // auto-prune finished tasks
)

m, err := gotasks.New(store,
    gotasks.WithWorkers(8),
    gotasks.WithLeaseTime(time.Minute),
)

type EmailPayload struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}

gotasks.RegisterHandler(m, "email",
    func(ctx context.Context, task *gotasks.Task, p EmailPayload) (any, error) {
        return sendEmail(ctx, p) // returned value is stored as the task result
    })

// From anywhere (workers optional in the enqueuing process):
id, err := gotasks.Enqueue(ctx, m, "email", EmailPayload{To: "a@b.c", Subject: "hi"})

// Scheduled / delayed / at-most-once / idempotent:
gotasks.Enqueue(ctx, m, "email", p, gotasks.WithRunAt(tomorrow))
gotasks.Enqueue(ctx, m, "email", p, gotasks.WithDelay(10*time.Minute))
gotasks.Enqueue(ctx, m, "provision", p, gotasks.WithMaxAttempts(1))

// At most one active task per key; a conflict returns the existing id
// and an error matching gotasks.ErrDuplicateTask:
id, err = gotasks.Enqueue(ctx, m, "sync", p, gotasks.WithUniqueKey("tenant-42"))

// Dead-letter management:
m.Requeue(ctx, id)             // reset one dead task (attempts back to 0)
m.RequeueDead(ctx, "email")    // requeue all dead email tasks ("" = all)

// Blocking run (Ctrl-C to drain and exit), or Start()/Stop(ctx):
m.Run(ctx)
```

See [examples/](examples/) for runnable scenarios covering every option:
scheduling, retries + dead-letter + requeue, unique keys, heartbeat,
at-most-once, batch fan-out, and retention.

## Task lifecycle

```
pending ──claim (atomic, +1 attempt, new lease)──▶ running ──ok──▶ done
   ▲                                                  │
   ├────── retry: run_at = now + backoff ──────error──┤
   │            (while attempts < max_attempts)       └──▶ dead
   └───────────── Requeue / RequeueDead ◀─────────────────────┘
```

- A **claim is an attempt**: `attempts` is incremented by the claim itself,
  so work lost to a crashed worker still counts.
- An **opt-in heartbeat** (`WithHeartbeat()`, or `WithHeartbeatInterval(d)`)
  extends the running task's lease every LeaseTime/3, so handlers may run
  longer than LeaseTime. If the heartbeat finds the lease was lost, the
  handler's context is cancelled. **Off by default**: without it, make sure
  LeaseTime exceeds your longest handler run (or call `Manager.ExtendLease`
  from inside the handler).
- A `running` task whose lease (`locked_until`) expired is **reclaimed** by
  the next claim — but only while `attempts < max_attempts`. Setting
  `max_attempts: 1` therefore gives at-most-once execution: never retried on
  error, never re-run after a worker dies.
- A **reaper** periodically marks lease-expired tasks with no attempts left
  as `dead` (error: "lease expired"), so nothing lingers as a zombie.
- **`dead` is the dead-letter state**: inspect, then `Requeue(id)` or
  `RequeueDead(type)` to run again with a fresh attempts budget (the errors
  array is kept as history), or let retention prune them.
- **Unique keys** (`WithUniqueKey`) are held while a task is pending/running
  (including across retries) and released at done/dead.
- Every failed attempt is appended to the task's `errors` array
  (`{at, attempt, worker, message}`).

## Queues and ordering

Tasks carry a queue label (`gotasks.WithQueue("emails")` at enqueue;
`"default"` otherwise), and a manager can be dedicated to specific queues
with `gotasks.WithQueues("emails")` — run one manager per queue (they can
share a process and a Mongo client) to give each queue its own worker pool.

Claims are **unsorted by default**: the store takes whichever runnable task
is cheapest — faster and less contended. Scheduling is unaffected (`run_at`
is enforced by the claim filter, never the ordering). Opt into oldest-first
claiming with `gotasks.WithFIFO()` when task staleness must stay bounded
under overload; note that with parallel workers, *completion* order is
never guaranteed in any mode.

## Pipeline mode (big pipelines)

By default (single mode) each worker claims, handles, and acknowledges its
tasks independently — simple, immediate visibility of every state change.
For high-throughput pipelines, switch on pipeline mode:

```go
m, err := gotasks.New(store,
    gotasks.WithWorkers(16),
    gotasks.WithPipelineMode(gotasks.PipelineConfig{}), // all defaults
    // or tuned:
    // gotasks.WithPipelineMode(gotasks.PipelineConfig{
    //     ClaimBatch:       64,              // tasks per fetch (default 32)
    //     QueueLease:       2 * time.Minute, // channel-wait budget (default LeaseTime)
    //     FinalizeBatch:    128,             // outcomes per bulk ack (default 2x ClaimBatch)
    //     FinalizeInterval: 300 * time.Millisecond,
    //     NoFinalize:       false,           // true = immediate acks, batched claims only
    // }),
)
```

Mega fan-outs are first-class on the enqueue side too: `EnqueueMany` writes
in chunks (default 1000, `mongostore.WithEnqueueChunkSize`), returns ids
aligned with the input, and reports partial failures per index via
`*gotasks.PartialEnqueueError` — a 100k-task broadcast neither builds one
giant wire message nor fails opaquely.

One fetcher per process claims batches under a **queue lease** and feeds a
bounded channel; workers revalidate ownership with a single point-write (the
start-of-work handshake) which starts the normal task lease. This amortizes
the contended claim query across the batch and moves head-of-queue racing
from per-worker to per-process. A task that waits in the channel longer than
the queue lease is dropped locally, unrun, and reclaimed — exactly-once
execution per attempt is preserved (see the scenario tests).

Pipeline mode also enables **batched finalization** by default: finished
tasks' Complete/Fail writes are buffered and flushed as one bulk write (on
size, interval, lease-deadline, or shutdown drain — whichever comes first).
Safety policy: at-most-once tasks and outcomes whose remaining lease is
inside the safety margin bypass the buffer and write immediately, so a
buffered outcome can never outlive its lease while the process is alive. A
crash loses at most one flush interval of finished-but-unrecorded outcomes —
reclaimed and re-run under the standard at-least-once contract. Opt out with
`NoFinalize: true`.

## Change-stream wakeup

On a replica set (any Atlas cluster; a single-node `--replSet` works too),
the manager automatically tails the tasks collection's change stream and
wakes the moment work appears — median enqueue→pickup latency is ~10ms in
the scenario tests, versus half the poll interval otherwise. Ownership is
untouched: the stream only *wakes* claimers; atomic claims still decide who
runs what. Future `run_at`s (scheduled tasks, retry backoffs) are tracked by
a min-timer fed from the same events, and a long safety-net poll
(`WithFallbackPoll`, default 30s) covers stream gaps. On standalone servers
this silently falls back to plain polling; `WithoutChangeStream()` forces
polling explicitly.

## Benchmarks

`go test -bench . -benchtime 2000x -run xxx .` (needs local MongoDB).
Reference numbers from a dev laptop with MongoDB 7 in Docker Desktop
(macOS's container I/O inflates per-op latency several-fold — treat these
as relative, not absolute):

| Benchmark | Result |
|---|---|
| EnqueueSingle | ~4ms/task |
| EnqueueMany (batches of 100) | ~12,800 tasks/sec |
| EnqueueMany (batches of 10,000, chunked) | ~36,000 tasks/sec |
| Throughput, single mode, 4 workers | ~370 tasks/sec |
| Throughput, single mode, 16 workers | ~600 tasks/sec |
| Throughput, pipeline 16×8 workers, NoFinalize | ~560 tasks/sec |
| Throughput, pipeline 16×8 workers (defaults) | ~900 tasks/sec |
| Throughput, pipeline 64×16 workers (defaults) | **~1,355 tasks/sec** |

Reading the numbers: batched claiming removed claiming as the bottleneck,
which made the per-task finalize write the next one; pipeline mode's
batched finalization (compare the NoFinalize row) removes that too. What
remains is per-op latency of the remaining point-writes — far better on
production Linux/Atlas than on this laptop setup.

## Retention

Finished tasks (done/dead) can auto-prune via a TTL index:

```go
mongostore.New(ctx, uri,
    mongostore.WithRetention(7*24*time.Hour), // default for all types
    mongostore.WithRetentionByType(map[string]time.Duration{
        "email":     24 * time.Hour, // noisy, short-lived
        "provision": 0,              // money trail: keep forever
    }),
)
```

## Requirements

- Go 1.23+
- MongoDB 4.2+

## Status / roadmap

v0.2 (current): core queue, Mongo store, worker pool, retries/backoff,
scheduling, reaper, heartbeat, dead-letter + requeue, unique tasks,
per-type retention. See [PLAN.md](PLAN.md) for the full roadmap
(batch claim, named queues, priorities, change-stream wakeup,
Postgres store, metrics...).

## License

MIT
