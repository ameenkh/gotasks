// Retention: finished tasks (done/dead) can auto-prune via MongoDB's TTL
// index. WithRetention sets the default; WithRetentionByType overrides it
// per task type (0 = keep that type's finished tasks forever). Pending and
// running tasks are never pruned — expires_at is only set when a task
// finishes.
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
		mongostore.WithCollection("retention"),
		// Default: finished tasks prune after 7 days...
		mongostore.WithRetention(7*24*time.Hour),
		// ...except these types:
		mongostore.WithRetentionByType(map[string]time.Duration{
			"email": time.Minute,      // noisy: gone a minute after finishing
			"audit": 0,                 // money trail: kept forever
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	m, err := gotasks.New(store,
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
	for _, typ := range []string{"email", "audit", "report"} {
		if err := gotasks.RegisterHandler(m, typ, handler); err != nil {
			log.Fatal(err)
		}
	}

	for i, typ := range []string{"email", "audit", "report"} {
		if _, err := gotasks.Enqueue(ctx, m, typ, Payload{N: i}); err != nil {
			log.Fatal(err)
		}
	}
	if err := m.Start(); err != nil {
		log.Fatal(err)
	}
	for done.Load() < 3 {
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("all three tasks are done; their expires_at is now set as:")
	fmt.Println("  email  -> ~1 minute from now  (per-type override)")
	fmt.Println("  report -> ~7 days from now    (default retention)")
	fmt.Println("  audit  -> never               (per-type 0 = keep forever)")
	fmt.Println()
	fmt.Println("inspect with: mongosh gotasks_examples --eval \\")
	fmt.Println("  'db.retention.find({},{type:1,status:1,expires_at:1})'")
	fmt.Println("(MongoDB's TTL monitor runs about once a minute)")

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}
