# gotasks examples

Each directory is a self-contained `main.go` targeting a local MongoDB
(`mongodb://localhost:27017`). Run any of them with:

```
go run ./examples/<name>
```

| Example | Shows |
|---|---|
| [simple](simple/) | Basic setup: store, manager, typed handler, enqueue (single/batch/delayed), graceful Ctrl-C drain |
| [scheduled](scheduled/) | `TaskPolicy.RunAt` / `TaskPolicy.Delay`: tasks become runnable only when due |
| [retries](retries/) | Retries with backoff, the errors array, dead-letter state, and `Requeue`/`RequeueDead` |
| [unique](unique/) | `TaskPolicy.UniqueKey`: idempotent enqueue, `ErrDuplicateTask`, key release on completion |
| [heartbeat](heartbeat/) | `WithHeartbeat`: handlers that run far longer than the lease without being reclaimed |
| [atmostonce](atmostonce/) | `TaskPolicy{MaxAttempts: 1}`: at-most-once execution for non-idempotent work |
| [batch](batch/) | `EnqueueMany` fan-out, `WithPipelineMode` consumption, and measuring throughput |
| [queues](queues/) | Named queues: `WithQueue` routing + two managers with dedicated pools via `WithQueues` |
| [dashboard](dashboard/) | The embeddable web dashboard (`dashboard.Handler`) with a live workload generator |
| [retention](retention/) | Task lifetime TTL: `QueuePolicy.TTL`, measured from `run_at`, purges any state |
