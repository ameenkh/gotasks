// Task lifetime (TTL): expires_at = run_at + QueuePolicy.TTL, stamped at
// creation; MongoDB purges the document when it passes — done, dead, or
// never consumed (the SQS retention model). Measured from run_at, so
// scheduled tasks don't burn lifetime while waiting to become due. Budget
// a TTL for: queue wait + retries + how long the result should stay
// visible. TTL is queue-scoped by design (0 = keep forever) — tasks
// needing a different lifetime belong in a different queue.
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/ameenkh/gotasks/stores/mongostore"
)

type Payload struct {
	N int `json:"n"`
}

func main() {
	ctx := context.Background()

	store, err := mongostore.New(ctx, "mongodb://localhost:27017",
		mongostore.WithDatabase("gotasks_examples"),
		mongostore.WithCollection("ttl"),
	)
	if err != nil {
		log.Fatal(err)
	}

	m, err := gotasks.New(store,
		gotasks.WithQueues(gotasks.QueuePolicy{Name: "emails", TTL: time.Minute}), // gone a minute after creation
		gotasks.WithQueues(gotasks.QueuePolicy{Name: "audit"}),                    // TTL 0 = kept forever
		gotasks.WithWorkers(2),
		gotasks.WithPollInterval(100*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close(ctx)

	var done atomic.Int32
	handler := func(ctx context.Context, task *gotasks.Task, p Payload) (any, error) {
		done.Add(1)
		return nil, nil
	}
	for _, typ := range []string{"send", "record"} {
		if err := gotasks.RegisterHandler(m, typ, handler); err != nil {
			log.Fatal(err)
		}
	}

	// Queue TTL from now, queue TTL measured from a future run_at, forever.
	if _, err := gotasks.Enqueue(ctx, m, "emails", "send", Payload{N: 1}); err != nil {
		log.Fatal(err)
	}
	if _, err := gotasks.Enqueue(ctx, m, "emails", "send", Payload{N: 2},
		gotasks.TaskPolicy{Delay: 30 * time.Second}); err != nil { // expires at run_at+1m, not created+1m
		log.Fatal(err)
	}
	if _, err := gotasks.Enqueue(ctx, m, "audit", "record", Payload{N: 3}); err != nil {
		log.Fatal(err)
	}
	if err := m.Start(); err != nil {
		log.Fatal(err)
	}
	for done.Load() < 2 { // #2 runs after its 30s delay; don't wait for it here
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("all three tasks done; their lifetimes:")
	fmt.Println("  emails #1 -> purged ~1 minute after run_at (= enqueue time)")
	fmt.Println("  emails #2 -> purged ~1m30s after enqueue (run_at was +30s)")
	fmt.Println("  audit  #3 -> never (queue TTL 0)")
	fmt.Println()
	fmt.Println("inspect with: mongosh gotasks_examples --eval \\")
	fmt.Println("  'db.ttl.find({},{queue:1,status:1,expires_at:1})'")
	fmt.Println("(MongoDB's TTL monitor runs about once a minute — and it will")
	fmt.Println(" purge a task whose TTL passes even if it was never consumed)")

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}
