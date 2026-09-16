package gotasks

import (
	"log/slog"
	"time"
)

// Config holds manager settings. Zero values are replaced by defaults in New.
type Config struct {
	// Workers is the number of concurrent worker goroutines. Default 4.
	Workers int
	// PollInterval is how long a worker sleeps after finding no task
	// (railway loop: while tasks keep coming, workers never sleep). Default 2s.
	PollInterval time.Duration
	// LeaseTime is how long a claim holds a task before it becomes
	// reclaimable by other workers. Must exceed your longest handler run
	// (until v0.2's heartbeat lands). Default 60s.
	LeaseTime time.Duration
	// DefaultMaxAttempts for enqueued tasks (per-enqueue override with
	// WithMaxAttempts). 1 = at-most-once. Default 3.
	DefaultMaxAttempts int
	// DefaultTimeout bounds each handler run via context (per-type override
	// with WithHandlerTimeout). 0 disables. Default 60s.
	DefaultTimeout time.Duration
	// Backoff computes retry delays. Default ExponentialBackoff(30s, 1h).
	Backoff Backoff
	// ReapInterval is how often stale+exhausted tasks are swept to dead.
	// 0 disables the reaper. Default 60s.
	ReapInterval time.Duration
	// HeartbeatInterval controls automatic lease extension while a handler
	// runs, letting handlers outlive LeaseTime without being reclaimed.
	// 0 (default) disables the heartbeat — handlers must then finish within
	// LeaseTime, or call Manager.ExtendLease themselves. HeartbeatAuto
	// derives LeaseTime/3; a positive value is used as-is. When the
	// heartbeat discovers the lease was lost (the task was reclaimed), the
	// handler's context is cancelled.
	HeartbeatInterval time.Duration
	// MaxBatch selects the claiming architecture. 1 (default) = single
	// mode: each worker claims tasks directly, one at a time — no fetcher,
	// no channel, no extra round trips (the claim itself starts the task
	// lease). 2..100 = batch mode: one fetcher goroutine claims up to
	// MaxBatch tasks per batch under a QUEUE lease and feeds a bounded
	// channel; a worker taking a task revalidates ownership with one
	// ExtendLease point-write, which starts the task lease (LeaseTime).
	// Batch mode amortizes the contended claim query and is meant for
	// high-throughput pipelines.
	MaxBatch int
	// QueueLeaseTime is the lease applied at batch claim time, budgeting
	// how long a task may wait in the fetcher's channel before it becomes
	// reclaimable by other processes. 0 (default) uses LeaseTime. Only
	// meaningful in batch mode.
	QueueLeaseTime time.Duration
	// FIFO makes claims take the oldest runnable task first ((run_at, id)
	// ascending). Off by default: unsorted claiming is faster and spreads
	// contention, but doesn't bound how stale a task can get under
	// sustained overload. Scheduling (run_at) is enforced by the claim
	// filter in both modes — FIFO only orders already-due tasks.
	FIFO bool
	// Queues restricts this manager to claiming from the named queues.
	// Empty (default) serves all queues. Tasks are assigned a queue at
	// enqueue via WithQueue (DefaultQueue otherwise).
	Queues []string
	// Logger receives worker/reaper diagnostics. Default slog.Default().
	Logger *slog.Logger
}

// Option configures the Manager.
type Option func(*Config)

func WithWorkers(n int) Option                  { return func(c *Config) { c.Workers = n } }
func WithPollInterval(d time.Duration) Option   { return func(c *Config) { c.PollInterval = d } }
func WithLeaseTime(d time.Duration) Option      { return func(c *Config) { c.LeaseTime = d } }
func WithDefaultMaxAttempts(n int) Option       { return func(c *Config) { c.DefaultMaxAttempts = n } }
func WithDefaultTimeout(d time.Duration) Option { return func(c *Config) { c.DefaultTimeout = d } }
func WithBackoff(b Backoff) Option              { return func(c *Config) { c.Backoff = b } }
func WithReapInterval(d time.Duration) Option   { return func(c *Config) { c.ReapInterval = d } }
func WithLogger(l *slog.Logger) Option          { return func(c *Config) { c.Logger = l } }

// HeartbeatAuto selects the automatic heartbeat cadence: LeaseTime/3.
const HeartbeatAuto time.Duration = -1

// WithFIFO makes claims take the oldest runnable task first (see
// Config.FIFO). Off by default.
func WithFIFO() Option {
	return func(c *Config) { c.FIFO = true }
}

// WithQueues restricts this manager to the named queues (see Config.Queues).
func WithQueues(queues ...string) Option {
	return func(c *Config) { c.Queues = queues }
}

// WithMaxBatch enables batch mode with up to n tasks claimed per fetch
// (see Config.MaxBatch). n=1 keeps single mode.
func WithMaxBatch(n int) Option {
	return func(c *Config) { c.MaxBatch = n }
}

// WithQueueLeaseTime sets the channel-time lease budget for batch mode
// (see Config.QueueLeaseTime).
func WithQueueLeaseTime(d time.Duration) Option {
	return func(c *Config) { c.QueueLeaseTime = d }
}

// WithHeartbeat enables automatic lease extension at the auto cadence
// (LeaseTime/3). Off by default.
func WithHeartbeat() Option {
	return func(c *Config) { c.HeartbeatInterval = HeartbeatAuto }
}

// WithHeartbeatInterval enables automatic lease extension at an explicit
// cadence (or HeartbeatAuto for LeaseTime/3; 0 keeps it disabled).
func WithHeartbeatInterval(d time.Duration) Option {
	return func(c *Config) { c.HeartbeatInterval = d }
}

type enqueueOptions struct {
	runAt       time.Time
	delay       time.Duration
	maxAttempts int // 0 = manager default
	uniqueKey   string
	queue       string // "" = DefaultQueue
}

// EnqueueOption configures a single Enqueue/EnqueueMany call.
type EnqueueOption func(*enqueueOptions)

// WithRunAt schedules the task(s) to become runnable at t (wins over WithDelay).
func WithRunAt(t time.Time) EnqueueOption {
	return func(o *enqueueOptions) { o.runAt = t }
}

// WithDelay schedules the task(s) to become runnable after d from now.
func WithDelay(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.delay = d }
}

// WithMaxAttempts overrides the manager's DefaultMaxAttempts for this
// enqueue. Use 1 for at-most-once work (never retried, stale never reclaimed).
func WithMaxAttempts(n int) EnqueueOption {
	return func(o *enqueueOptions) { o.maxAttempts = n }
}

// WithQueue enqueues the task(s) into a named queue (DefaultQueue when not
// used). Managers claim from all queues unless restricted with WithQueues.
func WithQueue(name string) EnqueueOption {
	return func(o *enqueueOptions) { o.queue = name }
}

// WithUniqueKey makes the enqueue idempotent: at most one pending/running
// task per key. A conflicting enqueue returns the existing task's id and an
// error matching ErrDuplicateTask. The key is released when the task reaches
// done or dead. Single-task Enqueue only (a batch sharing one key would just
// dedupe itself to the first element).
func WithUniqueKey(key string) EnqueueOption {
	return func(o *enqueueOptions) { o.uniqueKey = key }
}

type handlerOptions struct {
	timeout time.Duration // 0 = manager default
}

// HandlerOption configures a handler registration.
type HandlerOption func(*handlerOptions)

// WithHandlerTimeout bounds every run of this handler via context deadline.
func WithHandlerTimeout(d time.Duration) HandlerOption {
	return func(o *handlerOptions) { o.timeout = d }
}
