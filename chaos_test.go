package gotasks_test

// The chaos test: real worker processes are SIGKILLed randomly under a
// continuous task stream, then the survivors drain and the test audits the
// crash-recovery invariants against the database AND an external execution
// ledger written by the handlers themselves:
//
//   1. Nothing lost: every enqueued task terminates done or dead.
//   2. At-most-once is absolute: "once" tasks have <= 1 execution, period.
//   3. At-least-once is bounded: executions <= attempts <= max_attempts.
//   4. Done implies executed: no task completes without a ledger record.
//
// Profiles (edit/add in chaosProfiles below): "realistic" runs library
// defaults — single mode, 60s lease, 60s reap — with occasional kills;
// "aggressive" runs pipeline mode with tiny leases and near-constant kills.
//
// Gated (minutes-long, kills processes):
//
//	GOTASKS_CHAOS=1 go test -run TestChaos .
//
// GOTASKS_CHAOS_DURATION overrides the storm length per profile;
// GOTASKS_CHAOS_PROFILE runs a single profile by name.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/ameenkh/gotasks/stores/mongostore"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// chaosProfile is one storm configuration. Fixture fields are passed to
// internal/chaosworker via env; storm fields drive the killer/producer.
type chaosProfile struct {
	name string

	// storm shape
	storm        time.Duration // default; GOTASKS_CHAOS_DURATION overrides
	procs        int           // concurrent worker processes
	killEveryMin time.Duration
	killEveryMax time.Duration
	enqueueEvery time.Duration
	drainGrace   time.Duration // post-storm budget for full recovery

	// fixture (worker process) tuning; zero values = library-ish defaults
	lease, queueLease, reap, finalize, poll time.Duration
	claimBatch                              int // 0 = single mode

	// disruptMongo restarts the database mid-storm (twice) using the shell
	// command in GOTASKS_CHAOS_MONGO_DISRUPT (e.g. "docker restart mongo").
	// The profile skips itself when that env is unset.
	disruptMongo bool
}

var chaosProfiles = []chaosProfile{
	{
		// Library defaults, gentle failures: proves recovery at production
		// cadence (60s lease, 60s reap, single mode, 2s poll) — a corpse's
		// work genuinely waits a full lease before rescue.
		name:  "realistic",
		storm: 90 * time.Second, procs: 3,
		killEveryMin: 5 * time.Second, killEveryMax: 10 * time.Second,
		enqueueEvery: 10 * time.Millisecond,
		drainGrace:   3 * time.Minute, // lease(60s) + reap(60s) + slack
		// fixture: all zero -> defaults (single mode)
	},
	{
		// Everything cranked: pipeline mode (widest ack crash windows),
		// tiny leases, a kill roughly every 700ms.
		name:  "aggressive",
		storm: 90 * time.Second, procs: 4,
		killEveryMin: 400 * time.Millisecond, killEveryMax: time.Second,
		enqueueEvery: 4 * time.Millisecond,
		drainGrace:   90 * time.Second,
		lease:        2 * time.Second, queueLease: 2 * time.Second,
		reap: 2 * time.Second, finalize: 100 * time.Millisecond,
		poll: 100 * time.Millisecond, claimBatch: 16,
	},
	{
		// Kills workers AND restarts MongoDB itself mid-storm: exercises
		// driver reconnects, change-stream resume, claim/finalize failure
		// paths, and — with the store's pinned w:majority — proves no
		// acked work is lost across a database outage.
		name:  "failover",
		storm: 90 * time.Second, procs: 3,
		killEveryMin: 8 * time.Second, killEveryMax: 15 * time.Second,
		enqueueEvery: 10 * time.Millisecond,
		drainGrace:   3 * time.Minute,
		lease:        5 * time.Second, queueLease: 5 * time.Second,
		reap: 5 * time.Second, finalize: 200 * time.Millisecond,
		poll: 500 * time.Millisecond, claimBatch: 8,
		disruptMongo: true,
	},
}

func TestChaos(t *testing.T) {
	if os.Getenv("GOTASKS_CHAOS") == "" {
		t.Skip("chaos test skipped: set GOTASKS_CHAOS=1 (kills processes, runs minutes)")
	}
	uri := os.Getenv("GOTASKS_TEST_MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	ctx := context.Background()
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	client, err := mongo.Connect(cctx, options.Client().ApplyURI(uri))
	cancel()
	if err != nil || client.Ping(ctx, nil) != nil {
		t.Skipf("no MongoDB at %s", uri)
	}
	t.Cleanup(func() { _ = client.Disconnect(ctx) })

	bin := filepath.Join(t.TempDir(), "chaosworker")
	if out, err := exec.Command("go", "build", "-o", bin, "./internal/chaosworker").CombinedOutput(); err != nil {
		t.Fatalf("build fixture: %v\n%s", err, out)
	}

	only := os.Getenv("GOTASKS_CHAOS_PROFILE")
	for _, p := range chaosProfiles {
		if only != "" && p.name != only {
			continue
		}
		t.Run(p.name, func(t *testing.T) { runChaos(t, client, bin, uri, p) })
	}
}

func runChaos(t *testing.T, client *mongo.Client, bin, uri string, p chaosProfile) {
	ctx := context.Background()
	storm := p.storm
	if s := os.Getenv("GOTASKS_CHAOS_DURATION"); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			storm = d
		}
	}
	db := "gotasks_test"
	ns := fmt.Sprintf("chaos_%s_%d", p.name, time.Now().UnixNano())
	t.Cleanup(func() {
		for _, c := range []string{"_tasks", "_queues", "_metrics", "_ledger"} {
			_ = client.Database(db).Collection(ns + c).Drop(ctx)
		}
	})

	fixtureEnv := append(os.Environ(),
		"GOTASKS_CHAOS_URI="+uri, "GOTASKS_CHAOS_DB="+db, "GOTASKS_CHAOS_NS="+ns)
	addDur := func(key string, d time.Duration) {
		if d > 0 {
			fixtureEnv = append(fixtureEnv, fmt.Sprintf("%s=%s", key, d))
		}
	}
	addDur("GOTASKS_CHAOS_LEASE", p.lease)
	addDur("GOTASKS_CHAOS_QLEASE", p.queueLease)
	addDur("GOTASKS_CHAOS_REAP", p.reap)
	addDur("GOTASKS_CHAOS_FINALIZE", p.finalize)
	addDur("GOTASKS_CHAOS_POLL", p.poll)
	if p.claimBatch > 0 {
		fixtureEnv = append(fixtureEnv, fmt.Sprintf("GOTASKS_CHAOS_BATCH=%d", p.claimBatch))
	}

	var mu sync.Mutex
	var kills, spawned int
	procs := make([]*exec.Cmd, p.procs)
	spawn := func() *exec.Cmd {
		mu.Lock()
		spawned++
		n := spawned
		mu.Unlock()
		cmd := exec.Command(bin)
		cmd.Env = append(fixtureEnv, fmt.Sprintf("GOTASKS_CHAOS_NAME=%s-w%d", p.name, n))
		if err := cmd.Start(); err != nil {
			t.Fatalf("spawn worker: %v", err)
		}
		return cmd
	}
	killProc := func(cmd *exec.Cmd) {
		_ = cmd.Process.Kill() // SIGKILL: no drain, no goodbye
		_ = cmd.Wait()
	}
	for i := range procs {
		procs[i] = spawn()
	}
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range procs {
			if c != nil {
				killProc(c)
			}
		}
	})

	// Producer: enqueue-only manager, same declared policies as workers.
	pst, err := mongostore.New(ctx, uri,
		mongostore.WithDatabase(db), mongostore.WithNamespace(ns))
	if err != nil {
		t.Fatalf("producer store: %v", err)
	}
	defer pst.Close(ctx)
	prod, err := gotasks.New(pst, gotasks.WithQueues(
		gotasks.QueuePolicy{Name: "jobs", MaxAttempts: 5},
		gotasks.QueuePolicy{Name: "once", MaxAttempts: 1},
	))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}

	stormEnd := time.Now().Add(storm)
	var jobsN, onceN atomic.Int64
	var disruptions int
	var wg sync.WaitGroup

	if p.disruptMongo {
		disruptCmd := os.Getenv("GOTASKS_CHAOS_MONGO_DISRUPT")
		if disruptCmd == "" {
			t.Skip("failover profile needs GOTASKS_CHAOS_MONGO_DISRUPT (e.g. \"docker restart mongo\")")
		}
		wg.Add(1)
		go func() { // the database disruptor: two outages per storm
			defer wg.Done()
			for _, at := range []time.Duration{storm / 3, 2 * storm / 3} {
				wait := time.Until(stormEnd.Add(at - storm))
				if wait > 0 {
					time.Sleep(wait)
				}
				out, err := exec.Command("sh", "-c", disruptCmd).CombinedOutput()
				if err != nil {
					t.Errorf("mongo disrupt failed: %v\n%s", err, out)
					return
				}
				disruptions++
				t.Logf("[%s] mongo disrupted (%d)", p.name, disruptions)
			}
		}()
	}

	wg.Add(1)
	go func() { // the killer
		defer wg.Done()
		spread := int(p.killEveryMax - p.killEveryMin)
		for time.Now().Before(stormEnd) {
			time.Sleep(p.killEveryMin + time.Duration(rand.Intn(spread+1)))
			i := rand.Intn(len(procs))
			mu.Lock()
			victim := procs[i]
			mu.Unlock()
			killProc(victim)
			replacement := spawn()
			mu.Lock()
			procs[i] = replacement
			kills++
			mu.Unlock()
		}
	}()

	wg.Add(1)
	go func() { // the producer
		defer wg.Done()
		for i := 0; time.Now().Before(stormEnd); i++ {
			if _, err := gotasks.Enqueue(ctx, prod, "jobs", "work", struct{ N int }{N: i}); err == nil {
				jobsN.Add(1)
			}
			if i%10 == 0 {
				if _, err := gotasks.Enqueue(ctx, prod, "once", "work", struct{ N int }{N: i}); err == nil {
					onceN.Add(1)
				}
			}
			time.Sleep(p.enqueueEvery)
		}
	}()
	wg.Wait()
	total := jobsN.Load() + onceN.Load()
	t.Logf("[%s] storm over: %d tasks enqueued (%d jobs, %d once), %d workers SIGKILLed, %d db outages",
		p.name, total, jobsN.Load(), onceN.Load(), kills, disruptions)

	// Drain: survivors finish everything; stale claims recover via lease
	// expiry + reap at the PROFILE's cadence.
	tasksCol := client.Database(db).Collection(ns + "_tasks")
	deadline := time.Now().Add(p.drainGrace)
	quiet := 0
	for {
		n, err := tasksCol.CountDocuments(ctx, bson.M{"status": bson.M{"$in": bson.A{"pending", "running"}}})
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if n == 0 {
			if quiet++; quiet >= 3 {
				break
			}
		} else {
			quiet = 0
		}
		if time.Now().After(deadline) {
			t.Fatalf("INVARIANT 1 VIOLATED: %d tasks still pending/running %s after the storm", n, p.drainGrace)
		}
		time.Sleep(300 * time.Millisecond)
	}
	mu.Lock()
	for i, c := range procs { // stop all writers before auditing
		killProc(c)
		procs[i] = nil
	}
	mu.Unlock()

	// ---- audit ----
	count := func(filter bson.M) int64 {
		n, err := tasksCol.CountDocuments(ctx, filter)
		if err != nil {
			t.Fatalf("count %v: %v", filter, err)
		}
		return n
	}
	done, dead := count(bson.M{"status": "done"}), count(bson.M{"status": "dead"})
	if done+dead != total {
		t.Errorf("INVARIANT 1: done(%d)+dead(%d) = %d, want %d — tasks lost", done, dead, done+dead, total)
	}

	ledger := client.Database(db).Collection(ns + "_ledger")
	cur, err := ledger.Aggregate(ctx, mongo.Pipeline{
		{{Key: "$group", Value: bson.M{"_id": "$task_id", "n": bson.M{"$sum": 1}}}},
	})
	if err != nil {
		t.Fatalf("ledger aggregate: %v", err)
	}
	var rows []struct {
		ID string `bson:"_id"`
		N  int64  `bson:"n"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		t.Fatalf("ledger decode: %v", err)
	}
	execs := make(map[string]int64, len(rows))
	for _, r := range rows {
		execs[r.ID] = r.N
	}

	tcur, err := tasksCol.Find(ctx, bson.M{})
	if err != nil {
		t.Fatalf("find tasks: %v", err)
	}
	var duplicates, onceDuplicates, onceDead int64
	for tcur.Next(ctx) {
		var task struct {
			ID          primitive.ObjectID `bson:"_id"`
			Queue       string             `bson:"queue"`
			Status      string             `bson:"status"`
			Attempts    int64              `bson:"attempts"`
			MaxAttempts int64              `bson:"max_attempts"`
		}
		if err := tcur.Decode(&task); err != nil {
			t.Fatalf("decode: %v", err)
		}
		id := task.ID.Hex()
		e := execs[id]
		if task.Attempts > task.MaxAttempts {
			t.Errorf("INVARIANT 3: task %s attempts %d > max %d", id, task.Attempts, task.MaxAttempts)
		}
		if e > task.Attempts {
			t.Errorf("INVARIANT 3: task %s executed %d times with only %d attempts", id, e, task.Attempts)
		}
		if task.Queue == "once" {
			if e > 1 {
				onceDuplicates++
				t.Errorf("INVARIANT 2 VIOLATED: at-most-once task %s executed %d times", id, e)
			}
			if task.Status == "dead" {
				onceDead++
			}
		}
		if task.Status == "done" && e == 0 {
			t.Errorf("INVARIANT 4: task %s is done but never executed", id)
		}
		if e > 1 {
			duplicates++
		}
	}
	if err := tcur.Err(); err != nil {
		t.Fatalf("cursor: %v", err)
	}

	t.Logf("[%s] audit: %d done, %d dead (%d once-tasks killed before running); "+
		"duplicate executions: %d retryable (documented at-least-once window), "+
		"%d at-most-once (MUST be 0)",
		p.name, done, dead, onceDead, duplicates-onceDuplicates, onceDuplicates)
}
