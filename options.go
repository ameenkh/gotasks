package gotasks

import (
	"context"
	"encoding/json"
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
	// Janitor configures the maintenance goroutine's duties (reaping,
	// metrics, future work). The zero value is a working default: reap
	// every 60s, metrics off. See WithJanitor.
	Janitor JanitorConfig
	// HeartbeatInterval controls automatic lease extension while a handler
	// runs, letting handlers outlive LeaseTime without being reclaimed.
	// 0 (default) disables the heartbeat — handlers must then finish within
	// LeaseTime, or call Manager.ExtendLease themselves. HeartbeatAuto
	// derives LeaseTime/3; a positive value is used as-is. When the
	// heartbeat discovers the lease was lost (the task was reclaimed), the
	// handler's context is cancelled.
	HeartbeatInterval time.Duration
	// Pipeline enables pipeline mode when non-nil (set via
	// WithPipelineMode). nil (default) = single mode: each worker claims,
	// handles, and writes its result independently — no fetcher, no
	// channel, no buffering, immediate visibility of every state change.
	// See PipelineConfig for what pipeline mode changes.
	Pipeline *PipelineConfig
	// DisableChangeStream forces polling even when the store supports
	// change-stream wakeup. Default false: the manager tries the stream at
	// Start and falls back to polling silently if unavailable.
	DisableChangeStream bool
	// FallbackPoll is the safety-net poll cadence used while a change
	// stream is active (it catches anything a stream gap missed). Default
	// 30s. Ignored when no stream is active — PollInterval applies then.
	FallbackPoll time.Duration
	// FIFO makes claims take the oldest runnable task first ((run_at, id)
	// ascending). Off by default: unsorted claiming is faster and spreads
	// contention, but doesn't bound how stale a task can get under
	// sustained overload. Scheduling (run_at) is enforced by the claim
	// filter in both modes — FIFO only orders already-due tasks.
	FIFO bool
	// Queues is the explicit set of queues this manager works with — like
	// every queue system (Kafka topics, SQS queues), there is no default
	// queue and nothing is consumed implicitly. Declare each queue with
	// WithQueue; at least one is required. Enqueueing to an undeclared
	// queue is an error (catches typos at the call site), and Start
	// consumes every declared queue (a produce-only manager simply never
	// calls Start; an app producing to A while consuming B uses two
	// managers).
	Queues []QueuePolicy
	// ManagerName is a developer-chosen, non-unique label for this manager
	// instance (default "manager"). The framework derives a unique,
	// dot-separated ManagerID = "{name}.{random}" per instance; workers
	// lease as "{managerID}.worker.{n}" (fetcher: "{managerID}.fetcher"),
	// and metrics counters are attributed to the ManagerID.
	ManagerName string
	// Hooks are observe-only lifecycle callbacks (see Hooks). They run on
	// worker goroutines: keep them fast and non-blocking. A panicking hook
	// is recovered and logged; hooks never affect a task's outcome.
	Hooks Hooks
	// Middleware wraps handler execution (outermost first): tracing spans,
	// timing, log enrichment. Applied identically to every handler at
	// registration time.
	Middleware []Middleware
	// Logger receives worker/reaper diagnostics. Default slog.Default().
	Logger *slog.Logger
}

// HandlerFunc is the raw executable form of a handler that Middleware
// wraps: the payload is still JSON, the result is whatever the (typed)
// handler returned.
type HandlerFunc func(ctx context.Context, t *Task, payload json.RawMessage) (any, error)

// Middleware wraps a HandlerFunc. The first middleware passed to
// WithMiddleware is the outermost.
type Middleware func(next HandlerFunc) HandlerFunc

// Hooks are observe-only lifecycle callbacks. Every field is optional.
// They observe, they never decide: returning is their only effect, and a
// panic inside a hook is recovered and logged without touching the task.
type Hooks struct {
	// OnEnqueue fires once per successfully inserted task.
	OnEnqueue func(t *Task)
	// OnClaim fires when this manager takes ownership of a task.
	OnClaim func(t *Task)
	// OnComplete fires when a handler succeeds; d is the handler duration.
	OnComplete func(t *Task, d time.Duration)
	// OnFail fires on every failed attempt; willRetry=false means the
	// task is going to the dead-letter state (OnDead fires too).
	OnFail func(t *Task, err error, willRetry bool)
	// OnDead fires when a worker's terminal failure sends a task to the
	// dead-letter state. (Reaper-detected zombie deaths don't fire it —
	// no worker context exists there; watch the dead gauge for those.)
	OnDead func(t *Task, err error)
}

// JanitorConfig groups the janitor's duties. The janitor itself is a
// deadline scheduler — every duty keeps its own clock, and the goroutine
// sleeps until the earliest next deadline (no fixed tick, no drift on the
// wall-aligned metrics windows).
type JanitorConfig struct {
	// ReapInterval is how often stale+exhausted tasks are swept to dead.
	// 0 = default 60s; disable with NoReap.
	ReapInterval time.Duration
	// NoReap disables the reap duty (zombies then stay visible as running
	// docs with expired leases).
	NoReap bool
	// Metrics enables the metrics duty when non-nil. nil (default) = off:
	// smaller projects pay nothing.
	Metrics *MetricsConfig
}

// MetricsConfig configures Mongo-native metrics (see JanitorConfig). Zero
// values take defaults.
type MetricsConfig struct {
	// Interval is the snapshot window size (default 60s, min 1s).
	Interval time.Duration
	// Retention is how long metrics documents are kept (TTL index on the
	// metrics collection; default 7 days).
	Retention time.Duration
}

// Option configures the Manager.
type Option func(*Config)

func WithWorkers(n int) Option                  { return func(c *Config) { c.Workers = n } }
func WithPollInterval(d time.Duration) Option   { return func(c *Config) { c.PollInterval = d } }
func WithLeaseTime(d time.Duration) Option      { return func(c *Config) { c.LeaseTime = d } }
func WithDefaultMaxAttempts(n int) Option       { return func(c *Config) { c.DefaultMaxAttempts = n } }
func WithDefaultTimeout(d time.Duration) Option { return func(c *Config) { c.DefaultTimeout = d } }
func WithBackoff(b Backoff) Option              { return func(c *Config) { c.Backoff = b } }
func WithLogger(l *slog.Logger) Option          { return func(c *Config) { c.Logger = l } }

// HeartbeatAuto selects the automatic heartbeat cadence: LeaseTime/3.
const HeartbeatAuto time.Duration = -1

// WithoutChangeStream forces polling even when the store supports
// change-stream wakeup (see Config.DisableChangeStream).
func WithoutChangeStream() Option {
	return func(c *Config) { c.DisableChangeStream = true }
}

// WithFallbackPoll sets the safety-net poll cadence used while a change
// stream is active (see Config.FallbackPoll).
func WithFallbackPoll(d time.Duration) Option {
	return func(c *Config) { c.FallbackPoll = d }
}

// WithHooks sets the lifecycle hooks (see Hooks). Last call wins whole.
func WithHooks(h Hooks) Option {
	return func(c *Config) { c.Hooks = h }
}

// WithMiddleware appends handler middleware; the first registered is the
// outermost.
func WithMiddleware(mw ...Middleware) Option {
	return func(c *Config) { c.Middleware = append(c.Middleware, mw...) }
}

// WithManagerName sets the developer-chosen label for this manager
// instance (see Config.ManagerName).
func WithManagerName(name string) Option {
	return func(c *Config) { c.ManagerName = name }
}

// WithJanitor configures the maintenance duties (last call wins whole).
// Metrics duty: per-window gauge snapshots per queue (status counts +
// oldest due-task age; deduplicated cluster-wide, first manager wins each
// window) and this manager's exact per-queue throughput counters.
func WithJanitor(jc JanitorConfig) Option {
	return func(c *Config) { c.Janitor = jc }
}

// WithFIFO makes claims take the oldest runnable task first (see
// Config.FIFO). Off by default.
func WithFIFO() Option {
	return func(c *Config) { c.FIFO = true }
}

// QueuePolicy declares one queue and its policies. TTL is the queue's task
// lifetime, measured from each task's run_at (its eligible-to-run time):
// expires_at = run_at + TTL is stamped at creation, and MongoDB purges the
// document when it passes — whether or not the task ran (the SQS retention
// model; budget TTL for queue wait + retries + how long you want the result
// visible; scheduled tasks don't burn lifetime while waiting to become
// due). 0 = no expiry. TTL is queue-scoped by design: tasks needing a
// different lifetime belong in a different queue.
type QueuePolicy struct {
	Name string
	// TTL is the queue's total-lifetime default (see above). 0 = no expiry.
	TTL time.Duration
	// MaxAttempts is the queue's default attempts budget. 0 inherits the
	// manager's DefaultMaxAttempts. TaskPolicy.MaxAttempts overrides both.
	MaxAttempts int
}

// WithQueues declares the manager's queues (appendable across calls).
func WithQueues(qs ...QueuePolicy) Option {
	return func(c *Config) { c.Queues = append(c.Queues, qs...) }
}

// TaskPolicy carries the optional per-task knobs for Enqueue/EnqueueMany.
// The zero value means "all defaults": run now, inherited max attempts
// (queue's, else manager's), no unique key.
type TaskPolicy struct {
	// RunAt schedules the task for an absolute time (wins over Delay).
	RunAt time.Time
	// Delay schedules the task for now+Delay.
	Delay time.Duration
	// MaxAttempts overrides the manager's DefaultMaxAttempts; 1 =
	// at-most-once.
	MaxAttempts int
	// UniqueKey makes the enqueue idempotent: at most one active
	// (pending/running) task per key; conflicts return the existing id
	// with an error matching ErrDuplicateTask. Single-task enqueues only.
	UniqueKey string
}

// PipelineConfig configures pipeline mode: the high-throughput
// architecture where one fetcher claims tasks in batches under a QUEUE
// lease and feeds workers through a bounded channel (workers revalidate
// ownership with one point-write that starts the task lease), and finished
// tasks are acknowledged in batched bulk writes.
//
// Zero values take defaults, so WithPipelineMode(PipelineConfig{}) is a
// complete, sensible setup.
//
// Finalize safety policy: outcomes are NEVER buffered when risky —
// at-most-once tasks (max_attempts=1) and tasks whose remaining lease is
// under the safety margin (max(4x FinalizeInterval, 2s)) are written
// immediately, so a buffered outcome cannot outlive its lease while the
// process is alive. The trade-off: a crash loses up to FinalizeInterval of
// finished-but-unflushed outcomes — those tasks are reclaimed and re-run
// (the normal at-least-once contract, slightly widened).
type PipelineConfig struct {
	// ClaimBatch is how many tasks the fetcher claims per round trip
	// (1..100). Default 32.
	ClaimBatch int
	// QueueLease budgets how long a task may wait in the fetcher's channel
	// before becoming reclaimable by other processes. Default: LeaseTime.
	QueueLease time.Duration
	// FinalizeBatch is how many outcomes a finalize flush carries at most
	// (2..1000). Default: 2x ClaimBatch.
	FinalizeBatch int
	// FinalizeInterval is the longest a finished task may wait in the
	// finalize buffer before its outcome is written (default 300ms). A
	// latency bound, not a polling period: the flusher sleeps while the
	// buffer is empty, and on busy queues the size trigger fires first.
	FinalizeInterval time.Duration
	// NoFinalize keeps batched claiming but writes every outcome
	// immediately (single-mode acknowledgment semantics).
	NoFinalize bool
}

// WithPipelineMode switches the manager from single mode to pipeline mode
// (see PipelineConfig). Single mode — the default — keeps every worker
// claiming, handling, and acknowledging independently with immediate
// visibility; pipeline mode trades bounded acknowledgment latency for
// amortized round trips and is meant for high-throughput pipelines.
func WithPipelineMode(pc PipelineConfig) Option {
	return func(c *Config) { c.Pipeline = &pc }
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

type handlerOptions struct {
	timeout time.Duration // 0 = manager default
}

// HandlerOption configures a handler registration.
type HandlerOption func(*handlerOptions)

// WithHandlerTimeout bounds every run of this handler via context deadline.
func WithHandlerTimeout(d time.Duration) HandlerOption {
	return func(o *handlerOptions) { o.timeout = d }
}
