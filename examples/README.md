# gotasks examples

Each directory is a self-contained `main.go` targeting a local MongoDB
(`mongodb://localhost:27017`). Run any of them with:

```
go run ./examples/<name>
```

| Example | Shows |
|---|---|
| [simple](simple/) | Basic setup: store, manager, typed handler, enqueue (single/batch/delayed), graceful Ctrl-C drain |
| [scheduled](scheduled/) | `WithRunAt` / `WithDelay`: tasks become runnable only when due |
| [retries](retries/) | Retries with backoff, the errors array, dead-letter state, and `Requeue`/`RequeueDead` |
| [unique](unique/) | `WithUniqueKey`: idempotent enqueue, `ErrDuplicateTask`, key release on completion |
| [heartbeat](heartbeat/) | `WithHeartbeat`: handlers that run far longer than the lease without being reclaimed |
| [atmostonce](atmostonce/) | `WithMaxAttempts(1)`: at-most-once execution for non-idempotent work |
| [batch](batch/) | `EnqueueMany` fan-out, `WithMaxBatch` batch-mode consumption, and measuring throughput |
| [queues](queues/) | Named queues: `WithQueue` routing + two managers with dedicated pools via `WithQueues` |
| [retention](retention/) | `WithRetention` / `WithRetentionByType`: TTL auto-pruning of finished tasks |
