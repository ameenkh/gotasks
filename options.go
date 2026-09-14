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
	// ReapInterval is how often stale+exhausted tasks are swept to failed.
	// 0 disables the reaper. Default 60s.
	ReapInterval time.Duration
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

type enqueueOptions struct {
	runAt       time.Time
	delay       time.Duration
	maxAttempts int // 0 = manager default
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

type handlerOptions struct {
	timeout time.Duration // 0 = manager default
}

// HandlerOption configures a handler registration.
type HandlerOption func(*handlerOptions)

// WithHandlerTimeout bounds every run of this handler via context deadline.
func WithHandlerTimeout(d time.Duration) HandlerOption {
	return func(o *handlerOptions) { o.timeout = d }
}
