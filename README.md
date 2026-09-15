# gotasks

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

See `examples/simple` for a runnable version.

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
(cron + leader election, cancellation, batch claim, priorities,
change-stream wakeup, Postgres store, metrics...).

## License

MIT
