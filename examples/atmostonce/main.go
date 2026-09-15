// At-most-once: WithMaxAttempts(1) is for non-idempotent, credit-spending
// work (provisioning, charging a card). The task is never retried on error,
// and — because the claim query only reclaims stale tasks while
// attempts < max_attempts — never re-run if its worker dies mid-task either.
// A failure goes straight to the dead-letter state for a human to inspect
// and (deliberately) requeue.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/ameenkh/gotasks/stores/mongostore"
)

type Provision struct {
	OrderID string `json:"order_id"`
	OK      bool   `json:"ok"` // simulate provider success/failure
}

func main() {
	ctx := context.Background()

	store, err := mongostore.New(ctx, "mongodb://localhost:27017",
		mongostore.WithDatabase("gotasks_examples"),
		mongostore.WithCollection("atmostonce"),
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

	var settled atomic.Int32
	err = gotasks.RegisterHandler(m, "provision",
		func(ctx context.Context, task *gotasks.Task, p Provision) (any, error) {
			defer settled.Add(1)
			fmt.Printf("order %s: attempt %d of max %d (never more)\n",
				p.OrderID, task.Attempts, task.MaxAttempts)
			if !p.OK {
				// This will NOT retry: the task goes dead immediately.
				return nil, errors.New("provider rejected the order")
			}
			return map[string]string{"line": "provisioned-" + p.OrderID}, nil
		})
	if err != nil {
		log.Fatal(err)
	}

	orders := []Provision{
		{OrderID: "ord-1001", OK: true},
		{OrderID: "ord-1002", OK: false}, // -> dead letter, exactly one attempt
	}
	for _, o := range orders {
		if _, err := gotasks.Enqueue(ctx, m, "provision", o,
			gotasks.WithMaxAttempts(1)); err != nil {
			log.Fatal(err)
		}
	}

	if err := m.Start(); err != nil {
		log.Fatal(err)
	}
	for settled.Load() < int32(len(orders)) {
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Println("\nord-1002 is now in the dead-letter state; after fixing the")
	fmt.Println("root cause, a human (or ops tooling) can run m.Requeue(id).")

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}
