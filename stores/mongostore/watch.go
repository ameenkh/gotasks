package mongostore

// Change-stream wakeup: the store implements gotasks.Watcher by tailing the
// tasks collection's change stream and coalescing "something may be
// runnable" into level-triggered nudges. Ownership/claiming is untouched —
// this only replaces idle polling. Requires a replica set; WatchRunnable
// returns an error on standalone servers and the manager falls back to
// polling.

import (
	"container/heap"
	"context"
	"time"

	"github.com/ameenkh/gotasks"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ gotasks.Watcher = (*Store)(nil)

// WatchRunnable implements gotasks.Watcher.
func (s *Store) WatchRunnable(ctx context.Context, types, queues []string) (<-chan struct{}, error) {
	cs, err := s.openStream(ctx, types, queues, nil)
	if err != nil {
		return nil, err
	}
	ch := make(chan struct{}, 1)
	nudge(ch) // pre-existing backlog may be claimable right now
	go s.watchLoop(ctx, cs, types, queues, ch)
	return ch, nil
}

// nudge is level-triggered: capacity-1 send, dropped when one is already
// pending. A burst of 10k events collapses to "there is work".
func nudge(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// openStream opens the filtered change stream. The $match keeps only events
// that could make a task runnable for this manager: inserts of matching
// pending tasks, and updates that set run_at (retry/requeue) or flip status
// back to pending. Updates can't be type/queue-filtered without the
// fullDocument lookup cost — false positives are just one empty claim.
func (s *Store) openStream(ctx context.Context, types, queues []string, resumeAfter bson.Raw) (*mongo.ChangeStream, error) {
	insertMatch := bson.M{
		"operationType":       "insert",
		"fullDocument.status": string(gotasks.StatusPending),
	}
	if len(types) > 0 {
		insertMatch["fullDocument.type"] = bson.M{"$in": types}
	}
	if len(queues) > 0 {
		insertMatch["fullDocument.queue"] = bson.M{"$in": queues}
	}
	updateMatch := bson.M{
		"operationType": "update",
		"$or": bson.A{
			bson.M{"updateDescription.updatedFields.run_at": bson.M{"$exists": true}},
			bson.M{"updateDescription.updatedFields.status": string(gotasks.StatusPending)},
		},
	}
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"$or": bson.A{insertMatch, updateMatch}}}},
	}
	opts := options.ChangeStream().SetMaxAwaitTime(10 * time.Second)
	if len(resumeAfter) > 0 {
		opts = opts.SetResumeAfter(resumeAfter)
	}
	return s.tasksCol.Watch(ctx, pipeline, opts)
}

type streamEvent struct {
	OperationType string `bson:"operationType"`
	FullDocument  struct {
		RunAt time.Time `bson:"run_at"`
	} `bson:"fullDocument"`
	UpdateDescription struct {
		UpdatedFields struct {
			RunAt *time.Time `bson:"run_at"`
		} `bson:"updatedFields"`
	} `bson:"updateDescription"`
}

// runAt extracts when the event's task becomes runnable; zero means "now or
// unknown" (unknown nudges immediately — conservative direction).
func (e *streamEvent) runAt() time.Time {
	if e.OperationType == "insert" {
		return e.FullDocument.RunAt
	}
	if e.UpdateDescription.UpdatedFields.RunAt != nil {
		return *e.UpdateDescription.UpdatedFields.RunAt
	}
	return time.Time{}
}

func (s *Store) watchLoop(ctx context.Context, cs *mongo.ChangeStream, types, queues []string, ch chan struct{}) {
	defer close(ch)

	// The stream is silent when a FUTURE run_at comes due (nothing changes
	// in the DB at that moment), so future run_ats seen in events feed a
	// min-timer that nudges on time — this is what keeps retry backoffs
	// and scheduled tasks low-latency. Anything dropped (full buffer,
	// restart) is covered by the manager's fallback poll.
	future := make(chan time.Time, 64)
	go timerLoop(ctx, future, ch)

	for {
		for cs.Next(ctx) {
			var ev streamEvent
			if err := cs.Decode(&ev); err != nil {
				nudge(ch) // can't tell — assume runnable
				continue
			}
			if at := ev.runAt(); at.After(time.Now()) {
				select {
				case future <- at:
				default: // buffer full; fallback poll covers it
				}
			} else {
				nudge(ch)
			}
		}
		token := cs.ResumeToken()
		_ = cs.Close(context.WithoutCancel(ctx))
		if ctx.Err() != nil {
			return
		}

		// Reconnect: resume from the token when possible, fresh otherwise;
		// always nudge after reconnecting to cover the blind window.
		for {
			var err error
			if cs, err = s.openStream(ctx, types, queues, token); err == nil {
				break
			}
			token = nil // resume token may itself be the problem
			if cs, err = s.openStream(ctx, types, queues, nil); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
		nudge(ch)
	}
}

// timerLoop keeps a bounded min-heap of known future run_ats and nudges as
// each earliest deadline comes due — tracking only the minimum would forget
// every later deadline once it fired (scheduled tasks behind the first one,
// staggered retry backoffs) and leave them to the slow fallback poll.
// Heap overflow drops the extra deadlines; the fallback poll covers those.
func timerLoop(ctx context.Context, future <-chan time.Time, ch chan struct{}) {
	const idle = 24 * time.Hour
	const maxDeadlines = 4096
	timer := time.NewTimer(idle)
	defer timer.Stop()
	h := &timeHeap{}
	rearm := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if h.Len() == 0 {
			timer.Reset(idle)
			return
		}
		timer.Reset(max(time.Until((*h)[0]), time.Millisecond))
	}
	for {
		select {
		case <-ctx.Done():
			return
		case at := <-future:
			if h.Len() >= maxDeadlines {
				continue
			}
			heap.Push(h, at)
			rearm()
		case <-timer.C:
			now := time.Now()
			fired := false
			for h.Len() > 0 && !(*h)[0].After(now) {
				heap.Pop(h)
				fired = true
			}
			if fired {
				nudge(ch)
			}
			rearm()
		}
	}
}

type timeHeap []time.Time

func (h timeHeap) Len() int            { return len(h) }
func (h timeHeap) Less(i, j int) bool  { return h[i].Before(h[j]) }
func (h timeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *timeHeap) Push(x any)         { *h = append(*h, x.(time.Time)) }
func (h *timeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
