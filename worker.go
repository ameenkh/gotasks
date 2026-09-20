package gotasks

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// finalizeTimeout bounds the Complete/Fail write after a handler returns,
// each heartbeat write, and the start-of-work handshake. These run on a
// context detached from cancellation so results of finished work are
// recorded even during shutdown.
const finalizeTimeout = 15 * time.Second

// finalizeContext returns a context for store writes that must survive
// shutdown cancellation (bounded by finalizeTimeout instead).
func (m *Manager) finalizeContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(m.runCtx), finalizeTimeout)
}

// workerLoop is the railway loop: keep claiming while tasks come back; sleep
// PollInterval only after an empty claim.
func (m *Manager) workerLoop(id string) {
	defer m.wg.Done()
	for {
		select {
		case <-m.claimCtx.Done():
			return
		default:
		}
		t, err := m.store.Claim(m.claimCtx, m.claimOptions(id, m.cfg.LeaseTime))
		switch {
		case err == nil && t != nil:
			m.count(t.Queue, func(c *queueCounters) *atomic.Int64 { return &c.claimed }, 1)
			if h := m.cfg.Hooks.OnClaim; h != nil {
				fireHook(m, "OnClaim", func() { h(t) })
			}
			m.process(id, t)
			continue // railway: go straight for the next task
		case errors.Is(err, ErrNoTask):
			// queue is empty — idle below
		case m.claimCtx.Err() != nil:
			return
		default:
			m.cfg.Logger.Error("gotasks: claim failed", "worker", id, "error", err)
		}
		m.idleWait()
	}
}

func (m *Manager) process(workerID string, t *Task) {
	entry, ok := m.handlers[t.Type]
	if !ok {
		// Claim is restricted to registered types, so this is a bug guard.
		m.cfg.Logger.Error("gotasks: claimed task with no handler", "type", t.Type, "id", t.ID)
		return
	}
	// base is cancelled by the heartbeat when it discovers the lease was
	// lost — no point burning work another worker now owns.
	base, baseCancel := context.WithCancel(m.runCtx)
	defer baseCancel()
	hctx := base
	timeout := entry.timeout
	if timeout == 0 {
		timeout = m.cfg.DefaultTimeout
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		hctx, cancel = context.WithTimeout(base, timeout)
		defer cancel()
	}

	stopHeartbeat := m.startHeartbeat(t, baseCancel)
	started := time.Now()
	result, err := m.invoke(entry, hctx, t)
	duration := time.Since(started)
	stopHeartbeat()

	var o Outcome
	if err != nil {
		now := time.Now().UTC()
		terminal := t.Attempts >= t.MaxAttempts
		if terminal {
			m.count(t.Queue, func(c *queueCounters) *atomic.Int64 { return &c.dead }, 1)
		} else {
			m.count(t.Queue, func(c *queueCounters) *atomic.Int64 { return &c.failed }, 1)
		}
		if h := m.cfg.Hooks.OnFail; h != nil {
			fireHook(m, "OnFail", func() { h(t, err, !terminal) })
		}
		if terminal {
			if h := m.cfg.Hooks.OnDead; h != nil {
				fireHook(m, "OnDead", func() { h(t, err) })
			}
		}
		taskErr := TaskError{At: now, Attempt: t.Attempts, Worker: workerID, Message: err.Error()}
		o = Outcome{Task: t, Failure: &taskErr, RetryAt: now.Add(m.cfg.Backoff.Next(t.Attempts)), Terminal: terminal}
		m.cfg.Logger.Warn("gotasks: task attempt failed",
			"id", t.ID, "type", t.Type, "attempt", t.Attempts, "max_attempts", t.MaxAttempts,
			"terminal", terminal, "error", err)
	} else {
		m.count(t.Queue, func(c *queueCounters) *atomic.Int64 { return &c.done }, 1)
		if h := m.cfg.Hooks.OnComplete; h != nil {
			fireHook(m, "OnComplete", func() { h(t, duration) })
		}
		o = Outcome{Task: t, Result: result}
	}
	if m.shouldBufferOutcome(t) {
		m.bufferOutcome(o)
		return
	}
	m.finalizeNow(o)
}

// shouldBufferOutcome applies the batch-finalize safety gate: buffer only
// when batching is on, the task is not at-most-once (its semantics must
// never wait in a buffer), and the lease comfortably outlives the buffer
// window. t.LeasedUntil may be stale-early if the heartbeat extended the
// lease meanwhile — that errs toward the immediate path, which is safe.
func (m *Manager) shouldBufferOutcome(t *Task) bool {
	if !m.finalizeOn() || t.MaxAttempts == 1 {
		return false
	}
	return time.Until(t.LeasedUntil) >= m.finMargin
}

// finalizeNow writes one outcome immediately (the only path when batching
// is off; the bypass path otherwise).
func (m *Manager) finalizeNow(o Outcome) {
	fctx, cancel := m.finalizeContext()
	defer cancel()
	t := o.Task
	var err error
	if o.Failure != nil {
		err = m.store.Fail(fctx, t, *o.Failure, o.RetryAt, o.Terminal)
	} else {
		err = m.store.Complete(fctx, t, o.Result)
	}
	if err != nil {
		if errors.Is(err, ErrLeaseLost) {
			m.cfg.Logger.Warn("gotasks: task was reclaimed before its outcome could be recorded",
				"id", t.ID, "type", t.Type)
		} else {
			m.cfg.Logger.Error("gotasks: finalize failed", "id", t.ID, "type", t.Type, "error", err)
		}
	}
}

// bufferOutcome adds an outcome to the finalize buffer and wakes the
// flusher (which flushes on size, interval, lease deadline, or drain).
func (m *Manager) bufferOutcome(o Outcome) {
	m.finMu.Lock()
	if len(m.finBuf) == 0 {
		m.finOldest = time.Now()
		m.finMinLease = o.Task.LeasedUntil
	} else if o.Task.LeasedUntil.Before(m.finMinLease) {
		m.finMinLease = o.Task.LeasedUntil
	}
	m.finBuf = append(m.finBuf, o)
	m.finMu.Unlock()
	select {
	case m.finWake <- struct{}{}:
	default:
	}
}

// flusherLoop drains the finalize buffer as bulk writes. Flush triggers:
// size reached, oldest entry aged past FinalizeInterval, earliest buffered
// lease approaching the safety margin, or all workers done (drain).
func (m *Manager) flusherLoop() {
	defer m.wg.Done()
	for {
		m.finMu.Lock()
		n := len(m.finBuf)
		var deadline time.Time
		if n > 0 {
			deadline = m.finOldest.Add(m.cfg.Pipeline.FinalizeInterval)
			if leaseEdge := m.finMinLease.Add(-m.finMargin / 2); leaseEdge.Before(deadline) {
				deadline = leaseEdge
			}
		}
		m.finMu.Unlock()

		if n >= m.cfg.Pipeline.FinalizeBatch || (n > 0 && !time.Now().Before(deadline)) {
			m.flushOutcomes()
			continue
		}
		var timerC <-chan time.Time
		if n > 0 {
			timerC = time.After(time.Until(deadline))
		}
		select {
		case <-m.workersDone:
			m.flushOutcomes() // drain: workers can add nothing more
			return
		case <-m.finWake:
		case <-timerC:
		}
	}
}

func (m *Manager) flushOutcomes() {
	m.finMu.Lock()
	buf := m.finBuf
	m.finBuf = nil
	m.finMu.Unlock()
	if len(buf) == 0 {
		return
	}
	fctx, cancel := m.finalizeContext()
	defer cancel()
	lost, err := m.store.FinalizeBatch(fctx, buf)
	if err != nil {
		// Outcomes are lost; the tasks recover via lease expiry (reclaim /
		// reaper) exactly as if this process had died holding them.
		m.cfg.Logger.Error("gotasks: finalize flush failed; tasks will be reclaimed",
			"count", len(buf), "error", err)
		return
	}
	if lost > 0 {
		m.cfg.Logger.Warn("gotasks: finalize entries lost their lease before the flush",
			"lost", lost, "flushed", len(buf))
	}
}

// startHeartbeat extends t's lease every interval until stopped, so handlers
// may outlive LeaseTime. Disabled by default (interval 0); HeartbeatAuto
// derives LeaseTime/3. On ErrLeaseLost it calls onLost (cancelling the
// handler's context) and exits. Returns an idempotent stop func.
func (m *Manager) startHeartbeat(t *Task, onLost context.CancelFunc) (stop func()) {
	interval := m.cfg.HeartbeatInterval
	if interval == 0 {
		return func() {}
	}
	if interval < 0 {
		interval = m.cfg.LeaseTime / 3
	}
	if interval <= 0 {
		return func() {}
	}
	stopCh := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				ctx, cancel := m.finalizeContext()
				_, err := m.store.ExtendLease(ctx, t, m.cfg.LeaseTime)
				cancel()
				if errors.Is(err, ErrLeaseLost) {
					m.cfg.Logger.Warn("gotasks: lease lost mid-run; cancelling handler",
						"id", t.ID, "type", t.Type)
					onLost()
					return
				}
				if err != nil {
					m.cfg.Logger.Error("gotasks: heartbeat lease extension failed",
						"id", t.ID, "type", t.Type, "error", err)
				}
			}
		}
	}()
	return func() { once.Do(func() { close(stopCh) }) }
}

// invoke runs the handler with panic recovery — a panic consumes an attempt
// like any other failure instead of killing the worker.
func (m *Manager) invoke(entry handlerEntry, ctx context.Context, t *Task) (result []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return entry.fn(ctx, t)
}

// janitorLoop runs the manager's maintenance duties on their own clocks:
// reaping (ReapInterval) and, when enabled, metrics (Metrics.Interval,
// aligned to wall-clock window boundaries so all managers agree on window
// identity for the cluster-wide gauge dedup).
func (m *Manager) janitorLoop() {
	defer m.wg.Done()

	jc := m.cfg.Janitor
	now := time.Now()
	var nextReap, nextMetrics time.Time
	if !jc.NoReap {
		nextReap = now.Add(jc.ReapInterval)
	}
	metricsOn := jc.Metrics != nil
	if metricsOn {
		if err := m.store.EnsureMetrics(m.claimCtx, jc.Metrics.Retention); err != nil {
			m.cfg.Logger.Error("gotasks: metrics storage setup failed; metrics disabled", "error", err)
			metricsOn = false
		} else {
			nextMetrics = now.Truncate(jc.Metrics.Interval).Add(jc.Metrics.Interval)
		}
	}
	if nextReap.IsZero() && !metricsOn {
		return
	}

	for {
		next := nextReap
		if metricsOn && (next.IsZero() || nextMetrics.Before(next)) {
			next = nextMetrics
		}
		select {
		case <-m.claimCtx.Done():
			if metricsOn {
				m.flushMetrics(nextMetrics.Add(-jc.Metrics.Interval), true)
			}
			return
		case <-time.After(time.Until(next)):
		}
		if !nextReap.IsZero() && !time.Now().Before(nextReap) {
			m.reap()
			nextReap = time.Now().Add(jc.ReapInterval)
		}
		if metricsOn && !time.Now().Before(nextMetrics) {
			m.flushMetrics(nextMetrics.Add(-jc.Metrics.Interval), false)
			nextMetrics = nextMetrics.Add(jc.Metrics.Interval)
		}
	}
}

func (m *Manager) reap() {
	n, err := m.store.ReapExpired(m.claimCtx)
	if err != nil {
		if m.claimCtx.Err() == nil {
			m.cfg.Logger.Error("gotasks: reap failed", "error", err)
		}
	} else if n > 0 {
		m.cfg.Logger.Warn("gotasks: reaped stale tasks with exhausted attempts", "count", n)
	}
}

// flushMetrics closes the window that STARTED at windowStart: snapshots
// cluster-wide gauges (store dedups; first manager wins) and writes this
// manager's exact counters, swap-reset atomically. Runs on a detached
// context so the shutdown drain flush still lands.
func (m *Manager) flushMetrics(windowStart time.Time, drain bool) {
	interval := m.cfg.Janitor.Metrics.Interval
	ctx, cancel := m.finalizeContext()
	defer cancel()

	if !drain { // gauges are point-in-time; skip during teardown
		if err := m.store.SnapshotMetrics(ctx, m.queueNames, windowStart.UTC(), interval); err != nil {
			m.cfg.Logger.Error("gotasks: metrics snapshot failed", "error", err)
		}
	}
	counters := make(map[string]QueueCounters, len(m.counters))
	for q, c := range m.counters {
		qc := QueueCounters{
			Enqueued: c.enqueued.Swap(0),
			Claimed:  c.claimed.Swap(0),
			Done:     c.done.Swap(0),
			Failed:   c.failed.Swap(0),
			Dead:     c.dead.Swap(0),
		}
		if qc != (QueueCounters{}) {
			counters[q] = qc
		}
	}
	if len(counters) == 0 {
		return
	}
	if err := m.store.RecordCounters(ctx, m.managerID, windowStart.UTC(), interval, counters); err != nil {
		m.cfg.Logger.Error("gotasks: metrics counters write failed", "error", err)
	}
}
