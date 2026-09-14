package gotasks

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// finalizeTimeout bounds the Complete/Fail write after a handler returns.
// It runs on a context detached from cancellation so results of finished
// work are recorded even during shutdown.
const finalizeTimeout = 15 * time.Second

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
		t, err := m.store.Claim(m.claimCtx, id, m.types, m.cfg.LeaseTime)
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
	timeout := entry.timeout
	if timeout == 0 {
		timeout = m.cfg.DefaultTimeout
	}
	hctx := m.runCtx
	if timeout > 0 {
		var cancel context.CancelFunc
		hctx, cancel = context.WithTimeout(hctx, timeout)
		defer cancel()
	}

	result, err := m.invoke(entry, hctx, t)

	fctx, cancel := context.WithTimeout(context.WithoutCancel(m.runCtx), finalizeTimeout)
	defer cancel()

	if err != nil {
		terminal := t.Attempts >= t.MaxAttempts
		now := time.Now().UTC()
		taskErr := TaskError{At: now, Attempt: t.Attempts, Worker: workerID, Message: err.Error()}
		retryAt := now.Add(m.cfg.Backoff.Next(t.Attempts))
		if ferr := m.store.Fail(fctx, t.ID, t.LeaseToken, taskErr, retryAt, terminal); ferr != nil {
			m.cfg.Logger.Error("gotasks: recording failure failed", "id", t.ID, "type", t.Type, "error", ferr)
		}
		m.cfg.Logger.Warn("gotasks: task attempt failed",
			"id", t.ID, "type", t.Type, "attempt", t.Attempts, "max_attempts", t.MaxAttempts,
			"terminal", terminal, "error", err)
		return
	}
	if cerr := m.store.Complete(fctx, t.ID, t.LeaseToken, result); cerr != nil {
		m.cfg.Logger.Error("gotasks: completing task failed", "id", t.ID, "type", t.Type, "error", cerr)
	}
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
