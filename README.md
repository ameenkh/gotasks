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
)

m, err := gotasks.New(store,
    // Queues are explicit, like Kafka topics — no default queue exists.
    // TTL = task lifetime measured from run_at (0 = keep forever).
    gotasks.WithQueues(gotasks.QueuePolicy{Name: "emails", TTL: 7 * 24 * time.Hour}),
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

// From anywhere (workers optional in the enqueuing process); addressing is
// positional (queue, then type), knobs go in an optional TaskPolicy:
id, err := gotasks.Enqueue(ctx, m, "emails", "email", EmailPayload{To: "a@b.c", Subject: "hi"})

// Scheduled / delayed / at-most-once / idempotent / custom lifetime:
gotasks.Enqueue(ctx, m, "emails", "email", p, gotasks.TaskPolicy{RunAt: tomorrow})
gotasks.Enqueue(ctx, m, "emails", "email", p, gotasks.TaskPolicy{Delay: 10 * time.Minute})
gotasks.Enqueue(ctx, m, "emails", "provision", p, gotasks.TaskPolicy{MaxAttempts: 1})

// At most one active task per key; a conflict returns the existing id
// and an error matching gotasks.ErrDuplicateTask:
id, err = gotasks.Enqueue(ctx, m, "emails", "sync", p, gotasks.TaskPolicy{UniqueKey: "tenant-42"})

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
- A `running` task whose lease (`leased_until`) expired is **reclaimed** by
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

## Dashboard

```go
import "github.com/ameenkh/gotasks/dashboard"

// Behind YOUR auth — the dashboard can requeue and delete tasks:
mux.Handle("/gotasks/", http.StripPrefix("/gotasks", dashboard.Handler(store)))
```

An embeddable, zero-dependency web dashboard (single embedded page, no
JavaScript build chain, no external services — it reads the same Mongo
collections everything else uses): per-queue overview with live counts and
oldest-due age, a task browser with filters, payload/result/error
inspection, requeue and delete, per-queue "requeue dead", and a throughput
chart fed by the metrics collection. Run `go run ./examples/dashboard` for
a live demo with a workload generator.

## Metrics (Mongo-native, opt-in)

```go
gotasks.WithJanitor(gotasks.JanitorConfig{
    // ReapInterval: 60 * time.Second, // stale-zombie sweep (default)
    Metrics: &gotasks.MetricsConfig{Interval: time.Minute, Retention: 7 * 24 * time.Hour},
})
```

The janitor (the reaper's grown-up form) writes two kinds of documents to the
`{namespace}_metrics` collection — no Prometheus, no agents, the Mongo
cluster is the single source of truth:

- **gauges** — one doc per queue per window, cluster-wide (managers race;
  the first insert wins): status counts plus `oldest_due_age_ms`, the
  queue-health number (how long has the oldest DUE task been waiting — the
  same signal as SQS's ApproximateAgeOfOldestMessage).
- **counters** — one doc per manager instance per queue per window with
  exact throughput (`enqueued`, `claimed`, `done`, `failed`, `dead`),
  counted in-process; readers sum across managers. Manager identity is
  dot-separated: `{WithManagerName}.{random}` per instance, workers lease
  as `{managerID}.worker.{n}`.

Metrics prune themselves via their own TTL index (`Retention`, default 7d)
— deliberately independent of any queue's task TTL: task TTL answers "how
long does the work record matter" (per queue), metrics retention answers
"how far back should the dashboard remember" (one ops decision), and the
two often point in opposite directions (short-lived tasks still deserve
weeks of throughput history). Off by default.

Declared queues are recorded in a **queues registry** (a sibling
`<tasks>_queues` collection): the first declaration registers a queue's
policy, an identical redeclaration is a no-op, and a manager declaring
**different** settings for a registered queue fails fast with
`ErrQueueConflict` — policy drift across producer/consumer deployments is
a startup error, never silent behavior. Deliberate changes go through
`Store.SetQueuePolicy`; `Store.Queues(ctx)` lists the registry.

Queues use explicit subscription, like every queue system: tasks carry a
queue label (`gotasks.WithQueue("emails")` at enqueue; `"default"`
otherwise), and a manager consumes **exactly the queues it declares** —
`gotasks.WithQueues("emails")`, default `["default"]`. Nothing is consumed
implicitly: a named queue needs a manager naming it (and `WithQueues`
replaces the set, so include `"default"` if the manager serves it too).
Run one manager per queue — sharing a process and a Mongo client is fine —
to give each queue its own worker pool. Because every claim names its
queues, claims are always index-targeted: measured ~97x fewer documents
examined per claim versus label-only filtering at a 99:1 queue skew.

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

## Task lifetime (TTL)

Every task in a TTL'd queue gets `expires_at = run_at + QueuePolicy.TTL`,
stamped once at creation — the SQS retention model, measured from the
moment the task becomes *eligible*, so scheduled tasks don't burn lifetime
while waiting to come due. When it passes, MongoDB purges the task **in
whatever state it is in**: done, dead, pending under a backlog, mid-retry,
even while a handler runs (that handler's finalize is then fenced off and
logged). Budget it as `queue wait + retries + how long the result should
stay visible` — an unconsumed-but-due task purges too; bounded queue growth
is the feature.

TTL is **queue-scoped by design** (`0` = the queue's tasks never expire):
tasks needing a different lifetime belong in a different queue — the queue
is the policy boundary. `Requeue` does NOT reset the deadline — a revived
task keeps its original lifetime, which doubles as dead-letter retention.
`QueuePolicy` also carries `MaxAttempts` (TaskPolicy → queue →
manager-default inheritance).

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
