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
// Gated (minutes-long, kills processes): GOTASKS_CHAOS=1 go test -run TestChaos .
// Storm length: GOTASKS_CHAOS_DURATION (default 90s — long enough for the
// fixture's realistic 60s reap interval to fire mid-storm; override the
// cadence with GOTASKS_CHAOS_REAP).

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

func TestChaos(t *testing.T) {
	if os.Getenv("GOTASKS_CHAOS") == "" {
		t.Skip("chaos test skipped: set GOTASKS_CHAOS=1 (kills processes, runs minutes)")
	}
	storm := 90 * time.Second
	if s := os.Getenv("GOTASKS_CHAOS_DURATION"); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			storm = d
		}
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
	db := "gotasks_test"
	ns := fmt.Sprintf("chaos_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		for _, c := range []string{"_tasks", "_queues", "_metrics", "_ledger"} {
			_ = client.Database(db).Collection(ns + c).Drop(ctx)
		}
		_ = client.Disconnect(ctx)
	})

	// Build the fixture worker.
	bin := filepath.Join(t.TempDir(), "chaosworker")
	if out, err := exec.Command("go", "build", "-o", bin, "./internal/chaosworker").CombinedOutput(); err != nil {
		t.Fatalf("build fixture: %v\n%s", err, out)
	}

	// Worker process management.
	var mu sync.Mutex
	var kills, spawned int
	procs := make([]*exec.Cmd, 3)
	spawn := func(i int) *exec.Cmd {
		mu.Lock()
		spawned++
		n := spawned
		mu.Unlock()
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(),
			"GOTASKS_CHAOS_URI="+uri, "GOTASKS_CHAOS_DB="+db,
			"GOTASKS_CHAOS_NS="+ns, fmt.Sprintf("GOTASKS_CHAOS_NAME=w%d", n))
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
		procs[i] = spawn(i)
	}
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, p := range procs {
			if p != nil {
				killProc(p)
			}
		}
	})

	// Producer: an enqueue-only manager (never Started) with the same
	// declared policies as the workers (registry drift guard must agree).
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
	var wg sync.WaitGroup

	wg.Add(1)
	go func() { // the killer
		defer wg.Done()
		for time.Now().Before(stormEnd) {
			time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)
			i := rand.Intn(len(procs))
			mu.Lock()
			victim := procs[i]
			mu.Unlock()
			killProc(victim)
			replacement := spawn(i)
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
			time.Sleep(5 * time.Millisecond)
		}
	}()
	wg.Wait()
	total := jobsN.Load() + onceN.Load()
	t.Logf("storm over: %d tasks enqueued (%d jobs, %d once), %d workers SIGKILLed",
		total, jobsN.Load(), onceN.Load(), kills)

	// Drain: the current (post-storm) workers finish everything. Stale
	// claims from corpses recover via the 3s lease + reaper.
	tasksCol := client.Database(db).Collection(ns + "_tasks")
	deadline := time.Now().Add(3 * time.Minute)
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
			t.Fatalf("INVARIANT 1 VIOLATED: %d tasks still pending/running 2m after the storm", n)
		}
		time.Sleep(300 * time.Millisecond)
	}
	// Stop all writers before auditing.
	mu.Lock()
	for i, p := range procs {
		killProc(p)
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

	// Execution counts per task from the handlers' own ledger.
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

	t.Logf("audit: %d done, %d dead (%d once-tasks killed before running); "+
		"duplicate executions: %d retryable (documented at-least-once window), "+
		"%d at-most-once (MUST be 0)", done, dead, onceDead, duplicates-onceDuplicates, onceDuplicates)
}
