package gotasks

import (
	"errors"
	"sync/atomic"
	"time"
)

// fetcherLoop is pipeline mode's single claimer: it fills taskCh with batches
// claimed under the QUEUE lease, keeping fetch I/O overlapped with handler
// compute. The bounded channel is the backpressure; the low-water mark
// avoids RTT-wasteful tiny top-ups when the channel is nearly full.
func (m *Manager) fetcherLoop() {
	defer m.wg.Done()
	lowWater := max(1, m.cfg.Pipeline.ClaimBatch/2)
	queueLease := m.cfg.Pipeline.QueueLease
	for {
		select {
		case <-m.claimCtx.Done():
			return
		default:
		}

		free := cap(m.taskCh) - len(m.taskCh)
		if free < lowWater {
			// Channel comfortably full — wait for a worker to take a task
			// instead of polling.
			select {
			case <-m.claimCtx.Done():
				return
			case <-m.fetchNudge:
			}
			continue
		}

		k := min(m.cfg.Pipeline.ClaimBatch, free)
		tasks, err := m.store.ClaimBatch(m.claimCtx, m.claimOptions(m.managerID+".fetcher", queueLease), k)
		switch {
		case err == nil:
			for _, t := range tasks {
				m.count(t.Queue, func(c *queueCounters) *atomic.Int64 { return &c.claimed }, 1)
				if h := m.cfg.Hooks.OnClaim; h != nil {
					t := t
					fireHook(m, "OnClaim", func() { h(t) })
				}
				select {
				case m.taskCh <- t:
				case <-m.claimCtx.Done():
					return // buffered leases lapse and the tasks get reclaimed
				}
			}
			if len(tasks) == 0 {
				// Candidates existed but another process won them all;
				// more work likely remains — brief pause, then retry.
				select {
				case <-m.claimCtx.Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
			// Full or short batch: loop immediately (railway) — a short
			// batch's next iteration hits ErrNoTask and idles below.
		case errors.Is(err, ErrNoTask):
			m.idleWait()
		case m.claimCtx.Err() != nil:
			return
		default:
			m.cfg.Logger.Error("gotasks: batch claim failed", "error", err)
			m.idleWait()
		}
	}
}

// batchWorkerLoop consumes the fetcher's channel. Before running a task it
// performs the start-of-work handshake: a task whose queue lease visibly
// expired while buffered is dropped locally (someone else owns it now);
// otherwise one ExtendLease point-write both revalidates ownership and
// starts the task lease (LeaseTime).
func (m *Manager) batchWorkerLoop(id string) {
	defer m.wg.Done()
	for {
		select {
		case <-m.claimCtx.Done():
			return
		case t := <-m.taskCh:
			select {
			case m.fetchNudge <- struct{}{}:
			default:
			}
			if m.startOfWork(t) {
				m.process(id, t)
			}
		}
	}
}

// startOfWork reports whether this worker still owns t and may run it.
func (m *Manager) startOfWork(t *Task) bool {
	if !time.Now().Before(t.LeasedUntil) {
		m.cfg.Logger.Warn("gotasks: dropping task whose queue lease expired in channel",
			"id", t.ID, "type", t.Type)
		return false
	}
	for attempt := 0; attempt < 2; attempt++ {
		fctx, cancel := m.finalizeContext()
		until, err := m.store.ExtendLease(fctx, t, m.cfg.LeaseTime)
		cancel()
		if err == nil {
			t.LeasedUntil = until
			return true
		}
		if errors.Is(err, ErrLeaseLost) {
			m.cfg.Logger.Warn("gotasks: task reclaimed before start of work; dropping",
				"id", t.ID, "type", t.Type)
			return false
		}
		m.cfg.Logger.Error("gotasks: start-of-work lease extension failed",
			"id", t.ID, "type", t.Type, "attempt", attempt+1, "error", err)
	}
	// Persistent store trouble: dropping is always safe — the lease lapses
	// and the task is reclaimed.
	return false
}
