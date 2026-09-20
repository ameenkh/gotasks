// Batch fan-out and throughput: EnqueueMany inserts a whole batch in one
// store round-trip (this is how broadcast-style workloads fan one message
// out to thousands of recipients), and WithPipelineMode enables pipeline
// consumption: one fetcher claims up to 16 tasks per ~3 round trips and
// feeds the workers through a bounded channel, instead of each worker
// paying a contended claim query per task.
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

type Notify struct {
	UserID int `json:"user_id"`
}

func main() {
	ctx := context.Background()

	store, err := mongostore.New(ctx, "mongodb://localhost:27017",
		mongostore.WithDatabase("gotasks_examples"),
		mongostore.WithNamespace("batch"),
	)
	if err != nil {
		log.Fatal(err)
	}

	m, err := gotasks.New(store,
		gotasks.WithQueues(gotasks.QueuePolicy{Name: "notifications"}),
		gotasks.WithWorkers(8),
		gotasks.WithPipelineMode(gotasks.PipelineConfig{ClaimBatch: 16}), // pipeline mode
		gotasks.WithPollInterval(time.Second), // irrelevant while the queue is busy
	)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close(ctx)

	var done atomic.Int64
	err = gotasks.RegisterHandler(m, "notify",
		func(ctx context.Context, task *gotasks.Task, p Notify) (any, error) {
			done.Add(1) // real work would go here
			return nil, nil
		})
	if err != nil {
		log.Fatal(err)
	}

	const total = 2000
	payloads := make([]Notify, total)
	for i := range payloads {
		payloads[i] = Notify{UserID: i}
	}

	t0 := time.Now()
	ids, err := gotasks.EnqueueMany(ctx, m, "notifications", "notify", payloads)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("enqueued %d tasks in one round-trip (%s)\n", len(ids), time.Since(t0).Round(time.Millisecond))

	t1 := time.Now()
	if err := m.Start(); err != nil {
		log.Fatal(err)
	}
	for done.Load() < total {
		time.Sleep(50 * time.Millisecond)
	}
	elapsed := time.Since(t1)
	fmt.Printf("processed %d tasks in %s (%.0f tasks/sec, 8 workers)\n",
		total, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}
