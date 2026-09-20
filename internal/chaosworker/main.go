// chaosworker is the chaos test's fixture: a real gotasks worker process
// meant to be SIGKILLed mid-flight. It runs pipeline mode with batch
// finalization (the configuration with the widest crash windows) and
// records every handler execution in a ledger collection — the external
// ground truth the chaos test audits after the storm.
//
// It deliberately installs no signal handling: death is always ungraceful.
package main

import (
	"context"
	"log"
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/ameenkh/gotasks/stores/mongostore"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	uri := env("GOTASKS_CHAOS_URI", "mongodb://localhost:27017")
	db := env("GOTASKS_CHAOS_DB", "gotasks_test")
	ns := os.Getenv("GOTASKS_CHAOS_NS")
	name := env("GOTASKS_CHAOS_NAME", "chaos")
	if ns == "" {
		log.Fatal("GOTASKS_CHAOS_NS required")
	}
	// Every knob env-tunable so chaos profiles configure the fixture:
	dur := func(key string, def time.Duration) time.Duration {
		if d, err := time.ParseDuration(os.Getenv(key)); err == nil && d > 0 {
			return d
		}
		return def
	}
	lease := dur("GOTASKS_CHAOS_LEASE", 60*time.Second)
	qlease := dur("GOTASKS_CHAOS_QLEASE", lease)
	reap := dur("GOTASKS_CHAOS_REAP", 60*time.Second)
	finalize := dur("GOTASKS_CHAOS_FINALIZE", 300*time.Millisecond)
	poll := dur("GOTASKS_CHAOS_POLL", 2*time.Second)
	claimBatch := 0 // 0 = single mode
	if v, err := strconv.Atoi(os.Getenv("GOTASKS_CHAOS_BATCH")); err == nil && v > 0 {
		claimBatch = v
	}

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal(err)
	}
	st, err := mongostore.FromClient(ctx, client,
		mongostore.WithDatabase(db), mongostore.WithNamespace(ns))
	if err != nil {
		log.Fatal(err)
	}
	ledger := client.Database(db).Collection(ns + "_ledger")

	opts := []gotasks.Option{
		gotasks.WithQueues(
			gotasks.QueuePolicy{Name: "jobs", MaxAttempts: 5},
			gotasks.QueuePolicy{Name: "once", MaxAttempts: 1},
		),
		gotasks.WithWorkers(4),
		gotasks.WithManagerName(name),
		gotasks.WithPollInterval(poll),
		gotasks.WithLeaseTime(lease),
		gotasks.WithBackoff(gotasks.FixedBackoff(100 * time.Millisecond)),
		gotasks.WithJanitor(gotasks.JanitorConfig{ReapInterval: reap}),
	}
	if claimBatch > 0 {
		opts = append(opts, gotasks.WithPipelineMode(gotasks.PipelineConfig{
			ClaimBatch:       claimBatch,
			QueueLease:       qlease,
			FinalizeInterval: finalize, // the ack crash window
		}))
	}
	m, err := gotasks.New(st, opts...)
	if err != nil {
		log.Fatal(err)
	}

	err = gotasks.RegisterHandler(m, "work",
		func(ctx context.Context, t *gotasks.Task, p struct{ N int }) (any, error) {
			// Ledger insert FIRST: an execution counts from the moment side
			// effects could have started, even if we are killed mid-sleep.
			if _, err := ledger.InsertOne(ctx, bson.M{
				"task_id": t.ID, "queue": t.Queue, "attempt": t.Attempts,
				"worker": name, "at": time.Now().UTC(),
			}); err != nil {
				return nil, err
			}
			time.Sleep(time.Duration(5+rand.IntN(35)) * time.Millisecond)
			return map[string]int{"n": p.N}, nil
		})
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Start(); err != nil {
		log.Fatal(err)
	}
	log.Printf("chaosworker %s up", name)
	select {} // no graceful shutdown, ever — SIGKILL is the only exit
}
