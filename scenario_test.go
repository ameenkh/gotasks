package gotasks_test

// Scenario soak tests: each one runs producers and the worker pool against
// real MongoDB for GOTASKS_SCENARIO_DURATION (default 30s), then drains the
// queue and validates that the final state matches the scenario's contract
// (counts by status, exactly-once execution, retry budgets, timing...).
//
// They skip under -short and when no MongoDB is reachable. Run just these:
//
//	go test -run TestScenario -v .

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/ameenkh/gotasks/stores/mongostore"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func scenarioDuration() time.Duration {
	if s := os.Getenv("GOTASKS_SCENARIO_DURATION"); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d
		}
	}
	return 30 * time.Second
}

type scenario struct {
	t   *testing.T
	m   *gotasks.Manager
	st  *mongostore.Store
	col *mongo.Collection
}

// newScenario builds a manager on a throwaway Mongo collection plus a raw
// collection handle for validation queries. Register handlers on sc.m, then
// call sc.start().
func newScenario(t *testing.T, name string, opts ...gotasks.Option) *scenario {
	t.Helper()
	if testing.Short() {
		t.Skip("scenario soak tests skipped in -short mode")
	}
	uri := os.Getenv("GOTASKS_TEST_MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	collName := fmt.Sprintf("scenario_%s_%d", name, time.Now().UnixNano())
	st, err := mongostore.New(ctx, uri,
		mongostore.WithDatabase("gotasks_test"),
		mongostore.WithCollection(collName),
	)
	if err != nil {
		t.Skipf("no MongoDB at %s: %v", uri, err)
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("validation client: %v", err)
	}

	base := []gotasks.Option{
		gotasks.WithWorkers(4),
		gotasks.WithPollInterval(50 * time.Millisecond),
		gotasks.WithLeaseTime(30 * time.Second),
		gotasks.WithReapInterval(500 * time.Millisecond),
		gotasks.WithBackoff(gotasks.FixedBackoff(100 * time.Millisecond)),
	}
	m, err := gotasks.New(st, append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	col := client.Database("gotasks_test").Collection(collName)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = m.Close(ctx) // stops the pool and closes the store
		_ = col.Drop(ctx)
		_ = client.Disconnect(ctx)
	})
	return &scenario{t: t, m: m, st: st, col: col}
}

func (s *scenario) start() {
	s.t.Helper()
	if err := s.m.Start(); err != nil {
		s.t.Fatalf("Start: %v", err)
	}
}

// produce calls fn on a fixed cadence until the scenario duration elapses,
// returning how many times it ran.
func (s *scenario) produce(every time.Duration, fn func(i int64)) int64 {
	s.t.Helper()
	deadline := time.Now().Add(scenarioDuration())
	var i int64
	for time.Now().Before(deadline) {
		fn(i)
		i++
		time.Sleep(every)
	}
	return i
}

func (s *scenario) count(filter bson.M) int64 {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, err := s.col.CountDocuments(ctx, filter)
	if err != nil {
		s.t.Fatalf("count %v: %v", filter, err)
	}
	return n
}

func (s *scenario) statusCounts() map[gotasks.Status]int64 {
	out := map[gotasks.Status]int64{}
	for _, st := range []gotasks.Status{gotasks.StatusPending, gotasks.StatusRunning, gotasks.StatusDone, gotasks.StatusDead} {
		out[st] = s.count(bson.M{"status": string(st)})
	}
	return out
}

// drain waits until nothing is pending or running (two consecutive quiet
// checks), failing the test if the queue hasn't settled within timeout.
func (s *scenario) drain(timeout time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	quiet := 0
	for time.Now().Before(deadline) {
		if s.count(bson.M{"status": bson.M{"$in": bson.A{
			string(gotasks.StatusPending), string(gotasks.StatusRunning)}}}) == 0 {
			quiet++
			if quiet >= 2 {
				return
			}
		} else {
			quiet = 0
		}
		time.Sleep(200 * time.Millisecond)
	}
	s.t.Fatalf("queue did not drain within %s: %+v", timeout, s.statusCounts())
}

// callCounter tracks how many times each task id was handled.
type callCounter struct{ m sync.Map }

func (c *callCounter) hit(id string) int32 {
	v, _ := c.m.LoadOrStore(id, new(int32))
	return atomic.AddInt32(v.(*int32), 1)
}

func (c *callCounter) total() (tasks int64, calls int64) {
	c.m.Range(func(_, v any) bool {
		tasks++
		calls += int64(atomic.LoadInt32(v.(*int32)))
		return true
	})
	return
}

// Scenario: continuous stream of well-behaved tasks. Contract: every
// enqueued task is executed exactly once and ends done; nothing dies.
func TestScenarioSteadyThroughput(t *testing.T) {
	t.Parallel()
	sc := newScenario(t, "steady")
	ctx := context.Background()

	var calls callCounter
	err := gotasks.RegisterHandler(sc.m, "work",
		func(ctx context.Context, task *gotasks.Task, p struct{ N int64 }) (any, error) {
			calls.hit(task.ID)
			return map[string]int64{"n": p.N}, nil
		})
	if err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	sc.start()

	enqueued := sc.produce(10*time.Millisecond, func(i int64) {
		if _, err := gotasks.Enqueue(ctx, sc.m, "work", struct{ N int64 }{N: i}); err != nil {
			t.Errorf("enqueue %d: %v", i, err)
		}
	})
	sc.drain(30 * time.Second)

	counts := sc.statusCounts()
	if counts[gotasks.StatusDone] != enqueued || counts[gotasks.StatusDead] != 0 {
		t.Errorf("counts = %+v, want done=%d dead=0", counts, enqueued)
	}
	tasks, total := calls.total()
	if tasks != enqueued || total != enqueued {
		t.Errorf("executed %d tasks / %d calls, want exactly-once for all %d", tasks, total, enqueued)
	}
	if n := sc.count(bson.M{"status": "done", "attempts": bson.M{"$ne": 1}}); n != 0 {
		t.Errorf("%d done tasks have attempts != 1", n)
	}
}

// Scenario: flaky tasks. Payload says how many attempts fail first:
// 0 -> done on attempt 1, 1 -> done on attempt 2, 3 -> exhausts the
// 3-attempt budget and dies. Contract: statuses, per-task call counts, and
// error histories all match the failure plan.
func TestScenarioFlakyRetries(t *testing.T) {
	t.Parallel()
	sc := newScenario(t, "flaky", gotasks.WithDefaultMaxAttempts(3))
	ctx := context.Background()

	type flaky struct {
		FailTimes int `json:"fail_times"`
	}
	var calls callCounter
	err := gotasks.RegisterHandler(sc.m, "flaky",
		func(ctx context.Context, task *gotasks.Task, p flaky) (any, error) {
			calls.hit(task.ID)
			if task.Attempts <= p.FailTimes {
				return nil, errors.New("planned failure")
			}
			return nil, nil
		})
	if err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	sc.start()

	plan := []int{0, 1, 3} // cycled; 3 >= maxAttempts -> dead
	perPlan := map[int]int64{}
	enqueued := sc.produce(15*time.Millisecond, func(i int64) {
		fails := plan[i%int64(len(plan))]
		perPlan[fails]++
		if _, err := gotasks.Enqueue(ctx, sc.m, "flaky", flaky{FailTimes: fails}); err != nil {
			t.Errorf("enqueue %d: %v", i, err)
		}
	})
	sc.drain(30 * time.Second)

	counts := sc.statusCounts()
	wantDone, wantDead := perPlan[0]+perPlan[1], perPlan[3]
	if counts[gotasks.StatusDone] != wantDone || counts[gotasks.StatusDead] != wantDead {
		t.Errorf("counts = %+v, want done=%d dead=%d", counts, wantDone, wantDead)
	}
	// Call totals: plan 0 -> 1 call, plan 1 -> 2 calls, plan 3 -> 3 calls.
	wantCalls := perPlan[0]*1 + perPlan[1]*2 + perPlan[3]*3
	if tasks, total := calls.total(); tasks != enqueued || total != wantCalls {
		t.Errorf("executed %d tasks / %d calls, want %d / %d", tasks, total, enqueued, wantCalls)
	}
	// Every dead task carries its full error history.
	if n := sc.count(bson.M{"status": "dead", "errors.2": bson.M{"$exists": false}}); n != 0 {
		t.Errorf("%d dead tasks have fewer than 3 recorded errors", n)
	}
	if n := sc.count(bson.M{"status": "done", "attempts": bson.M{"$gt": 2}}); n != 0 {
		t.Errorf("%d done tasks took more than 2 attempts", n)
	}
}

// Scenario: scheduled tasks with 1-8s delays enqueued continuously.
// Contract: no task ever executes before its run_at, and all complete.
func TestScenarioScheduled(t *testing.T) {
	t.Parallel()
	sc := newScenario(t, "scheduled")
	ctx := context.Background()

	type timed struct {
		RunAt time.Time `json:"run_at"`
	}
	var early atomic.Int64
	var calls callCounter
	err := gotasks.RegisterHandler(sc.m, "timed",
		func(ctx context.Context, task *gotasks.Task, p timed) (any, error) {
			calls.hit(task.ID)
			// 50ms tolerance for Mongo's millisecond time truncation.
			if time.Since(p.RunAt) < -50*time.Millisecond {
				early.Add(1)
			}
			return nil, nil
		})
	if err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	sc.start()

	enqueued := sc.produce(100*time.Millisecond, func(i int64) {
		delay := time.Duration(1+i%8) * time.Second
		runAt := time.Now().Add(delay)
		if _, err := gotasks.Enqueue(ctx, sc.m, "timed", timed{RunAt: runAt},
			gotasks.WithRunAt(runAt)); err != nil {
			t.Errorf("enqueue %d: %v", i, err)
		}
	})
	sc.drain(30 * time.Second) // covers the tail of up-to-8s delays

	if n := early.Load(); n != 0 {
		t.Errorf("%d tasks executed before their run_at", n)
	}
	counts := sc.statusCounts()
	if counts[gotasks.StatusDone] != enqueued || counts[gotasks.StatusDead] != 0 {
		t.Errorf("counts = %+v, want done=%d dead=0", counts, enqueued)
	}
	if tasks, total := calls.total(); tasks != enqueued || total != enqueued {
		t.Errorf("executed %d tasks / %d calls, want exactly-once for all %d", tasks, total, enqueued)
	}
}

// Scenario: producers hammer 20 unique keys while slow handlers hold them.
// Contract: per key, tasks never run concurrently; every accepted enqueue
// completes; dedup actually triggered (sanity).
func TestScenarioUniqueKeys(t *testing.T) {
	t.Parallel()
	sc := newScenario(t, "unique")
	ctx := context.Background()

	type keyed struct {
		Key string `json:"key"`
	}
	// Each key is re-enqueued every keys*5ms = 50ms while its handler holds
	// it for 100ms — contention (and therefore dedup) is guaranteed.
	const keys = 10
	var active [keys]atomic.Int32
	var overlaps atomic.Int64
	err := gotasks.RegisterHandler(sc.m, "keyed",
		func(ctx context.Context, task *gotasks.Task, p keyed) (any, error) {
			var idx int
			fmt.Sscanf(p.Key, "key-%d", &idx)
			if active[idx].Add(1) > 1 {
				overlaps.Add(1)
			}
			defer active[idx].Add(-1)
			time.Sleep(100 * time.Millisecond)
			return nil, nil
		})
	if err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	sc.start()

	var accepted, deduped atomic.Int64
	sc.produce(5*time.Millisecond, func(i int64) {
		key := fmt.Sprintf("key-%d", i%keys)
		_, err := gotasks.Enqueue(ctx, sc.m, "keyed", keyed{Key: key},
			gotasks.WithUniqueKey(key))
		switch {
		case errors.Is(err, gotasks.ErrDuplicateTask):
			deduped.Add(1)
		case err != nil:
			t.Errorf("enqueue: %v", err)
		default:
			accepted.Add(1)
		}
	})
	sc.drain(30 * time.Second)

	if n := overlaps.Load(); n != 0 {
		t.Errorf("%d overlapping executions of the same unique key", n)
	}
	if deduped.Load() == 0 {
		t.Error("dedup never triggered — scenario produced no contention")
	}
	counts := sc.statusCounts()
	if counts[gotasks.StatusDone] != accepted.Load() || counts[gotasks.StatusDead] != 0 {
		t.Errorf("counts = %+v, want done=%d dead=0 (deduped %d)", counts, accepted.Load(), deduped.Load())
	}
}

// Scenario: handlers run 4x longer than the lease, kept alive by the
// heartbeat. Contract: exactly-once — no reclaims, no dead tasks, every
// task done with attempts=1.
func TestScenarioHeartbeatLongTasks(t *testing.T) {
	t.Parallel()
	sc := newScenario(t, "heartbeat",
		gotasks.WithWorkers(8),
		gotasks.WithLeaseTime(500*time.Millisecond),
		gotasks.WithHeartbeat(),
		gotasks.WithDefaultTimeout(time.Minute),
	)
	ctx := context.Background()

	var calls callCounter
	err := gotasks.RegisterHandler(sc.m, "long",
		func(ctx context.Context, task *gotasks.Task, p struct{ N int64 }) (any, error) {
			calls.hit(task.ID)
			time.Sleep(2 * time.Second) // 4x the lease
			return nil, nil
		})
	if err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	sc.start()

	// 8 workers x 2s tasks = 4 tasks/sec capacity; enqueue at ~2.5/sec.
	enqueued := sc.produce(400*time.Millisecond, func(i int64) {
		if _, err := gotasks.Enqueue(ctx, sc.m, "long", struct{ N int64 }{N: i}); err != nil {
			t.Errorf("enqueue %d: %v", i, err)
		}
	})
	sc.drain(60 * time.Second) // tail: queued long tasks finish after producing stops

	counts := sc.statusCounts()
	if counts[gotasks.StatusDone] != enqueued || counts[gotasks.StatusDead] != 0 {
		t.Errorf("counts = %+v, want done=%d dead=0", counts, enqueued)
	}
	if tasks, total := calls.total(); tasks != enqueued || total != enqueued {
		t.Errorf("executed %d tasks / %d calls, want exactly-once for all %d", tasks, total, enqueued)
	}
	if n := sc.count(bson.M{"attempts": bson.M{"$ne": 1}}); n != 0 {
		t.Errorf("%d tasks have attempts != 1 (reclaim happened despite heartbeat)", n)
	}
}

// Scenario: at-most-once tasks (max_attempts=1), half of which fail.
// Contract: every task executes exactly once — failures go straight to
// dead with a single recorded error, never retried.
func TestScenarioAtMostOnce(t *testing.T) {
	t.Parallel()
	sc := newScenario(t, "atmostonce")
	ctx := context.Background()

	type once struct {
		ShouldFail bool `json:"should_fail"`
	}
	var calls callCounter
	err := gotasks.RegisterHandler(sc.m, "once",
		func(ctx context.Context, task *gotasks.Task, p once) (any, error) {
			calls.hit(task.ID)
			if p.ShouldFail {
				return nil, errors.New("planned failure")
			}
			return nil, nil
		})
	if err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	sc.start()

	var wantDead int64
	enqueued := sc.produce(10*time.Millisecond, func(i int64) {
		fail := i%2 == 1
		if fail {
			wantDead++
		}
		if _, err := gotasks.Enqueue(ctx, sc.m, "once", once{ShouldFail: fail},
			gotasks.WithMaxAttempts(1)); err != nil {
			t.Errorf("enqueue %d: %v", i, err)
		}
	})
	sc.drain(30 * time.Second)

	counts := sc.statusCounts()
	if counts[gotasks.StatusDead] != wantDead || counts[gotasks.StatusDone] != enqueued-wantDead {
		t.Errorf("counts = %+v, want done=%d dead=%d", counts, enqueued-wantDead, wantDead)
	}
	if tasks, total := calls.total(); tasks != enqueued || total != enqueued {
		t.Errorf("executed %d tasks / %d calls, want exactly-once for all %d", tasks, total, enqueued)
	}
	if n := sc.count(bson.M{"attempts": bson.M{"$gt": 1}}); n != 0 {
		t.Errorf("%d at-most-once tasks have more than 1 attempt", n)
	}
}

// Scenario: batch mode under a continuous stream. Contract identical to
// steady-throughput — exactly-once, everything done — but through the
// fetcher/channel pipeline with the start-of-work handshake.
func TestScenarioBatchMode(t *testing.T) {
	t.Parallel()
	sc := newScenario(t, "batchmode",
		gotasks.WithMaxBatch(16),
		gotasks.WithWorkers(8),
	)
	ctx := context.Background()

	var calls callCounter
	err := gotasks.RegisterHandler(sc.m, "work",
		func(ctx context.Context, task *gotasks.Task, p struct{ N int64 }) (any, error) {
			calls.hit(task.ID)
			time.Sleep(5 * time.Millisecond) // small but real work
			return nil, nil
		})
	if err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	sc.start()

	enqueued := sc.produce(5*time.Millisecond, func(i int64) {
		if _, err := gotasks.Enqueue(ctx, sc.m, "work", struct{ N int64 }{N: i}); err != nil {
			t.Errorf("enqueue %d: %v", i, err)
		}
	})
	sc.drain(30 * time.Second)

	counts := sc.statusCounts()
	if counts[gotasks.StatusDone] != enqueued || counts[gotasks.StatusDead] != 0 {
		t.Errorf("counts = %+v, want done=%d dead=0", counts, enqueued)
	}
	tasks, total := calls.total()
	if tasks != enqueued || total != enqueued {
		t.Errorf("executed %d tasks / %d calls, want exactly-once for all %d", tasks, total, enqueued)
	}
	if n := sc.count(bson.M{"status": "done", "attempts": bson.M{"$ne": 1}}); n != 0 {
		t.Errorf("%d done tasks have attempts != 1", n)
	}
}

// Scenario: change-stream wakeup latency. Polling is stretched to 10s so
// only the stream can explain fast pickup. Contract: median enqueue->start
// latency well under the poll interval; everything completes.
func TestScenarioChangeStreamLatency(t *testing.T) {
	t.Parallel()
	sc := newScenario(t, "cslatency",
		gotasks.WithPollInterval(10*time.Second),
		gotasks.WithFallbackPoll(10*time.Second),
	)
	ctx := context.Background()

	// Probe: skip when the deployment can't do change streams.
	probeCtx, probeCancel := context.WithCancel(context.Background())
	if _, err := probeStore(sc).WatchRunnable(probeCtx, nil, nil); err != nil {
		probeCancel()
		t.Skipf("change streams unavailable: %v", err)
	}
	probeCancel()

	type timed struct {
		SentAt time.Time `json:"sent_at"`
	}
	var latencies syncDurations
	err := gotasks.RegisterHandler(sc.m, "ping",
		func(ctx context.Context, task *gotasks.Task, p timed) (any, error) {
			latencies.add(time.Since(p.SentAt))
			return nil, nil
		})
	if err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	sc.start()
	time.Sleep(300 * time.Millisecond) // let workers reach their idle wait

	enqueued := sc.produce(500*time.Millisecond, func(i int64) {
		if _, err := gotasks.Enqueue(ctx, sc.m, "ping", timed{SentAt: time.Now()}); err != nil {
			t.Errorf("enqueue %d: %v", i, err)
		}
	})
	sc.drain(30 * time.Second)

	counts := sc.statusCounts()
	if counts[gotasks.StatusDone] != enqueued {
		t.Errorf("counts = %+v, want done=%d", counts, enqueued)
	}
	med := latencies.median()
	t.Logf("change-stream pickup latency: median %s over %d tasks", med, enqueued)
	// Generous bound: far below the 10s poll, so pickup came from the stream.
	if med > time.Second {
		t.Errorf("median latency %s — stream wakeup not working (poll is 10s)", med)
	}
}

func probeStore(sc *scenario) *mongostore.Store { return sc.st }

type syncDurations struct {
	mu sync.Mutex
	ds []time.Duration
}

func (s *syncDurations) add(d time.Duration) {
	s.mu.Lock()
	s.ds = append(s.ds, d)
	s.mu.Unlock()
}

func (s *syncDurations) median() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ds) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), s.ds...)
	slices.Sort(sorted)
	return sorted[len(sorted)/2]
}

// Scenario: the full throughput pipeline — batch claim + batch finalize —
// under a continuous stream. Contract: exactly-once, everything done.
func TestScenarioBatchFinalize(t *testing.T) {
	t.Parallel()
	sc := newScenario(t, "batchfinalize",
		gotasks.WithMaxBatch(16),
		gotasks.WithWorkers(8),
		gotasks.WithFinalizeBatch(64, 25*time.Millisecond),
	)
	ctx := context.Background()

	var calls callCounter
	err := gotasks.RegisterHandler(sc.m, "work",
		func(ctx context.Context, task *gotasks.Task, p struct{ N int64 }) (any, error) {
			calls.hit(task.ID)
			return nil, nil
		})
	if err != nil {
		t.Fatalf("RegisterHandler: %v", err)
	}
	sc.start()

	enqueued := sc.produce(5*time.Millisecond, func(i int64) {
		if _, err := gotasks.Enqueue(ctx, sc.m, "work", struct{ N int64 }{N: i}); err != nil {
			t.Errorf("enqueue %d: %v", i, err)
		}
	})
	sc.drain(30 * time.Second)

	counts := sc.statusCounts()
	if counts[gotasks.StatusDone] != enqueued || counts[gotasks.StatusDead] != 0 {
		t.Errorf("counts = %+v, want done=%d dead=0", counts, enqueued)
	}
	tasks, total := calls.total()
	if tasks != enqueued || total != enqueued {
		t.Errorf("executed %d tasks / %d calls, want exactly-once for all %d", tasks, total, enqueued)
	}
	if n := sc.count(bson.M{"status": "done", "attempts": bson.M{"$ne": 1}}); n != 0 {
		t.Errorf("%d done tasks have attempts != 1", n)
	}
}
