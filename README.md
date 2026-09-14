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

// Scheduled / delayed / at-most-once:
gotasks.Enqueue(ctx, m, "email", p, gotasks.WithRunAt(tomorrow))
gotasks.Enqueue(ctx, m, "email", p, gotasks.WithDelay(10*time.Minute))
gotasks.Enqueue(ctx, m, "provision", p, gotasks.WithMaxAttempts(1))

// Blocking run (Ctrl-C to drain and exit), or Start()/Stop(ctx):
m.Run(ctx)
```

See `examples/simple` for a runnable version.

## Task lifecycle

```
pending ──claim (atomic, +1 attempt, new lease)──▶ running ──ok──▶ done
   ▲                                                  │
   └────── retry: run_at = now + backoff ──────error──┤
                (while attempts < max_attempts)       └──▶ failed
```

- A **claim is an attempt**: `attempts` is incremented by the claim itself,
  so work lost to a crashed worker still counts.
- A `running` task whose lease (`locked_until`) expired is **reclaimed** by
  the next claim — but only while `attempts < max_attempts`. Setting
  `max_attempts: 1` therefore gives at-most-once execution: never retried on
  error, never re-run after a worker dies.
- A **reaper** periodically marks lease-expired tasks with no attempts left
  as `failed` (error: "lease expired"), so nothing lingers as a zombie.
- Every failed attempt is appended to the task's `errors` array
  (`{at, attempt, worker, message}`).

## Requirements

- Go 1.23+
- MongoDB 4.2+

## Status / roadmap

v0.1 (current): core queue, Mongo store, worker pool, retries/backoff,
scheduling, reaper. See [PLAN.md](PLAN.md) for the full roadmap
(heartbeats, cron + leader election, unique tasks, batch claim, priorities,
change-stream wakeup, Postgres store, metrics...).

## License

MIT
