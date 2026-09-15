package gotasks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type handlerEntry struct {
	fn      func(ctx context.Context, t *Task) (json.RawMessage, error)
	timeout time.Duration
}

// Manager owns the enqueue API and the worker pool. Create with New, wire
// handlers with RegisterHandler, then Start (or Run). Enqueue works from any
// process, including ones that never start workers.
type Manager struct {
	store    Store
	cfg      Config
	handlers map[string]handlerEntry
	types    []string // registered handler types; claims are restricted to these

	started     atomic.Bool
	runCtx      context.Context    // cancelled only on hard stop; parent of handler contexts
	runCancel   context.CancelFunc
	claimCtx    context.Context    // cancelled first on Stop: stop claiming, drain in-flight
	claimCancel context.CancelFunc
	wg          sync.WaitGroup
}

// New creates a Manager on top of a Store.
func New(store Store, opts ...Option) (*Manager, error) {
	if store == nil {
		return nil, errors.New("gotasks: store is nil")
	}
	cfg := Config{
		Workers:            4,
		PollInterval:       2 * time.Second,
		LeaseTime:          60 * time.Second,
		DefaultMaxAttempts: 3,
		DefaultTimeout:     60 * time.Second,
		Backoff:            ExponentialBackoff(30*time.Second, time.Hour),
		ReapInterval:       60 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.Workers < 1 {
		return nil, errors.New("gotasks: Workers must be >= 1")
	}
	if cfg.PollInterval <= 0 {
		return nil, errors.New("gotasks: PollInterval must be > 0")
	}
	if cfg.LeaseTime <= 0 {
		return nil, errors.New("gotasks: LeaseTime must be > 0")
	}
	if cfg.DefaultMaxAttempts < 1 {
		return nil, errors.New("gotasks: DefaultMaxAttempts must be >= 1")
	}
	if cfg.Backoff == nil {
		return nil, errors.New("gotasks: Backoff must not be nil")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Manager{
		store:    store,
		cfg:      cfg,
		handlers: map[string]handlerEntry{},
	}, nil
}

// Enqueue inserts one task of taskType with a typed payload (any value that
// marshals to a JSON object; use struct{}{} for no payload). Returns the id.
// With WithUniqueKey, a conflict returns the EXISTING active task's id along
// with an error matching ErrDuplicateTask — treat it as idempotent success
// when that suits the caller.
func Enqueue[T any](ctx context.Context, m *Manager, taskType string, payload T, opts ...EnqueueOption) (string, error) {
	ids, err := EnqueueMany(ctx, m, taskType, []T{payload}, opts...)
	if err != nil {
		var dup *DuplicateTaskError
		if errors.As(err, &dup) {
			return dup.ExistingID, err
		}
		return "", err
	}
	return ids[0], nil
}

// EnqueueMany inserts many tasks of one type in a single store round-trip.
// All share the same options (run time, max attempts).
func EnqueueMany[T any](ctx context.Context, m *Manager, taskType string, payloads []T, opts ...EnqueueOption) ([]string, error) {
	if taskType == "" {
		return nil, errors.New("gotasks: task type is empty")
	}
	if len(payloads) == 0 {
		return nil, nil
	}
	eo := enqueueOptions{}
	for _, o := range opts {
		o(&eo)
	}
	if eo.maxAttempts == 0 {
		eo.maxAttempts = m.cfg.DefaultMaxAttempts
	}
	if eo.maxAttempts < 1 {
		return nil, errors.New("gotasks: max attempts must be >= 1")
	}
	if eo.uniqueKey != "" && len(payloads) > 1 {
		return nil, errors.New("gotasks: WithUniqueKey requires a single-task enqueue")
	}
	now := time.Now().UTC()
	runAt := eo.runAt
	if runAt.IsZero() {
		runAt = now.Add(eo.delay)
	}
	tasks := make([]*Task, len(payloads))
	for i, p := range payloads {
		raw, err := marshalPayload(p)
		if err != nil {
			return nil, err
		}
		tasks[i] = &Task{
			Type:        taskType,
			UniqueKey:   eo.uniqueKey,
			Payload:     raw,
			Status:      StatusPending,
			MaxAttempts: eo.maxAttempts,
			RunAt:       runAt.UTC(),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
	}
	return m.store.Enqueue(ctx, tasks)
}

// marshalPayload enforces the object-or-nothing payload contract so every
// store can persist payloads as inspectable documents.
func marshalPayload(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("gotasks: marshal payload: %w", err)
	}
	t := bytes.TrimSpace(b)
	if len(t) == 0 || bytes.Equal(t, []byte("null")) {
		return nil, nil
	}
	if t[0] != '{' {
		return nil, fmt.Errorf("gotasks: payload must marshal to a JSON object, got %.20s", t)
	}
	return b, nil
}

// RegisterHandler wires fn to run tasks of taskType, decoding the payload
// into T. A non-nil returned value is stored as the task's result (wrapped as
// {"value": ...} when it isn't a JSON object). A returned error (or panic)
// consumes an attempt and triggers retry with backoff, or failed when
// attempts are exhausted. Register everything before Start.
func RegisterHandler[T any](m *Manager, taskType string, fn func(ctx context.Context, task *Task, payload T) (any, error), opts ...HandlerOption) error {
	if taskType == "" {
		return errors.New("gotasks: task type is empty")
	}
	if fn == nil {
		return errors.New("gotasks: handler is nil")
	}
	if m.started.Load() {
		return errors.New("gotasks: cannot register handlers after Start")
	}
	if _, dup := m.handlers[taskType]; dup {
		return fmt.Errorf("gotasks: handler already registered for type %q", taskType)
	}
	ho := handlerOptions{}
	for _, o := range opts {
		o(&ho)
	}
	m.handlers[taskType] = handlerEntry{
		timeout: ho.timeout,
		fn: func(ctx context.Context, t *Task) (json.RawMessage, error) {
			var p T
			if len(t.Payload) > 0 {
				if err := json.Unmarshal(t.Payload, &p); err != nil {
					return nil, fmt.Errorf("decode payload: %w", err)
				}
			}
			res, err := fn(ctx, t, p)
			if err != nil || res == nil {
				return nil, err
			}
			b, err := json.Marshal(res)
			if err != nil {
				return nil, fmt.Errorf("encode result: %w", err)
			}
			if tb := bytes.TrimSpace(b); len(tb) > 0 && tb[0] != '{' && !bytes.Equal(tb, []byte("null")) {
				b, _ = json.Marshal(map[string]json.RawMessage{"value": b})
			}
			return b, nil
		},
	}
	return nil
}

// ExtendLease pushes the task's lease forward by the manager's LeaseTime.
// Rarely needed directly — the heartbeat does this automatically unless
// disabled with WithoutHeartbeat.
func (m *Manager) ExtendLease(ctx context.Context, t *Task) error {
	until, err := m.store.ExtendLease(ctx, t, m.cfg.LeaseTime)
	if err != nil {
		return err
	}
	t.LockedUntil = until
	return nil
}

// Requeue resets one dead task to pending (attempts back to 0, runnable
// now); its errors array is kept as history. Returns ErrNotFound if the id
// doesn't exist or the task isn't dead.
func (m *Manager) Requeue(ctx context.Context, id string) error {
	return m.store.Requeue(ctx, id)
}

// RequeueDead requeues every dead task of taskType ("" = all types) with the
// same resets as Requeue, returning how many were requeued.
func (m *Manager) RequeueDead(ctx context.Context, taskType string) (int64, error) {
	return m.store.RequeueDead(ctx, taskType)
}

// Start launches the worker pool and reaper. Non-blocking; pair with Stop.
func (m *Manager) Start() error {
	if len(m.handlers) == 0 {
		return errors.New("gotasks: no handlers registered")
	}
	if !m.started.CompareAndSwap(false, true) {
		return errors.New("gotasks: already started")
	}
	m.types = make([]string, 0, len(m.handlers))
	for t := range m.handlers {
		m.types = append(m.types, t)
	}
	sort.Strings(m.types)

	m.runCtx, m.runCancel = context.WithCancel(context.Background())
	m.claimCtx, m.claimCancel = context.WithCancel(m.runCtx)

	for i := 0; i < m.cfg.Workers; i++ {
		m.wg.Add(1)
		go m.workerLoop(fmt.Sprintf("worker-%d", i))
	}
	if m.cfg.ReapInterval > 0 {
		m.wg.Add(1)
		go m.reaperLoop()
	}
	m.cfg.Logger.Info("gotasks: started", "workers", m.cfg.Workers, "types", m.types)
	return nil
}

// Stop drains the pool: workers stop claiming immediately and in-flight
// handlers get to finish. If ctx expires first, in-flight handler contexts
// are cancelled (each records a failed attempt) and ctx.Err() is returned.
func (m *Manager) Stop(ctx context.Context) error {
	if !m.started.Load() {
		return nil
	}
	m.claimCancel()
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		m.runCancel()
		return nil
	case <-ctx.Done():
		m.runCancel() // hard stop: cancel in-flight handlers
		<-done
		return ctx.Err()
	}
}

// Run is a convenience blocking loop: Start, wait for ctx cancellation, then
// drain with a 30s grace period.
func (m *Manager) Run(ctx context.Context) error {
	if err := m.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return m.Stop(stopCtx)
}

// Close stops the pool (if running) and closes the store.
func (m *Manager) Close(ctx context.Context) error {
	stopErr := m.Stop(ctx)
	if err := m.store.Close(ctx); err != nil {
		return err
	}
	return stopErr
}
