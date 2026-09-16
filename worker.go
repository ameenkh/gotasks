package gotasks

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
			m.process(id, t)
			continue // railway: go straight for the next task
		case errors.Is(err, ErrNoTask):
			// queue is empty — idle below
		case m.claimCtx.Err() != nil:
			return
		default:
			m.cfg.Logger.Error("gotasks: claim failed", "worker", id, "error", err)
		}
		select {
		case <-m.claimCtx.Done():
			return
		case <-time.After(m.cfg.PollInterval):
		}
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
	result, err := m.invoke(entry, hctx, t)
	stopHeartbeat()

	fctx, cancel := m.finalizeContext()
	defer cancel()

	if err != nil {
		terminal := t.Attempts >= t.MaxAttempts
		now := time.Now().UTC()
		taskErr := TaskError{At: now, Attempt: t.Attempts, Worker: workerID, Message: err.Error()}
		retryAt := now.Add(m.cfg.Backoff.Next(t.Attempts))
		if ferr := m.store.Fail(fctx, t, taskErr, retryAt, terminal); ferr != nil {
			if errors.Is(ferr, ErrLeaseLost) {
				m.cfg.Logger.Warn("gotasks: task was reclaimed before failure could be recorded",
					"id", t.ID, "type", t.Type)
			} else {
				m.cfg.Logger.Error("gotasks: recording failure failed", "id", t.ID, "type", t.Type, "error", ferr)
			}
		}
		m.cfg.Logger.Warn("gotasks: task attempt failed",
			"id", t.ID, "type", t.Type, "attempt", t.Attempts, "max_attempts", t.MaxAttempts,
			"terminal", terminal, "error", err)
		return
	}
	if cerr := m.store.Complete(fctx, t, result); cerr != nil {
		if errors.Is(cerr, ErrLeaseLost) {
			m.cfg.Logger.Warn("gotasks: task was reclaimed before completion could be recorded",
				"id", t.ID, "type", t.Type)
		} else {
			m.cfg.Logger.Error("gotasks: completing task failed", "id", t.ID, "type", t.Type, "error", cerr)
		}
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

func (m *Manager) reaperLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.ReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.claimCtx.Done():
			return
		case <-ticker.C:
			n, err := m.store.ReapExpired(m.claimCtx)
			if err != nil {
				if m.claimCtx.Err() == nil {
					m.cfg.Logger.Error("gotasks: reap failed", "error", err)
				}
			} else if n > 0 {
				m.cfg.Logger.Warn("gotasks: reaped stale tasks with exhausted attempts", "count", n)
			}
		}
	}
}
