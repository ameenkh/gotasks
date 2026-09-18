// Heartbeat: with WithHeartbeat, a worker extends its task's lease every
// LeaseTime/3 while the handler runs, so handlers may run far longer than
// LeaseTime without another worker reclaiming the task. Here the lease is
// 2s and the handler takes 8s — without the heartbeat this task would be
// reclaimed and run twice; with it, attempts stays 1.
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

type Encode struct {
	File string `json:"file"`
}

func main() {
	ctx := context.Background()

	store, err := mongostore.New(ctx, "mongodb://localhost:27017",
		mongostore.WithDatabase("gotasks_examples"),
		mongostore.WithNamespace("heartbeat"),
	)
	if err != nil {
		log.Fatal(err)
	}

	m, err := gotasks.New(store,
		gotasks.WithQueues(gotasks.QueuePolicy{Name: "encodes"}),
		gotasks.WithWorkers(4), // several workers: any of them COULD reclaim a stale task
		gotasks.WithPollInterval(200*time.Millisecond),
		gotasks.WithLeaseTime(2*time.Second),
		gotasks.WithHeartbeat(), // extends the lease every LeaseTime/3 (off by default)
		gotasks.WithDefaultTimeout(time.Minute),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close(ctx)

	var done atomic.Int32
	err = gotasks.RegisterHandler(m, "encode",
		func(ctx context.Context, task *gotasks.Task, p Encode) (any, error) {
			fmt.Printf("encoding %s (attempt %d, lease %s)...\n",
				p.File, task.Attempts, 2*time.Second)
			for i := 1; i <= 8; i++ {
				time.Sleep(time.Second) // simulate work in 1s chunks, 4x the lease
				fmt.Printf("  %s: %d/8 (lease kept alive by heartbeat)\n", p.File, i)
			}
			done.Add(1)
			return map[string]any{"attempts": task.Attempts}, nil
		})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := gotasks.Enqueue(ctx, m, "encodes", "encode", Encode{File: "movie.mkv"}); err != nil {
		log.Fatal(err)
	}
	if err := m.Start(); err != nil {
		log.Fatal(err)
	}
	for done.Load() < 1 {
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Println("done — ran exactly once despite outliving its lease 4x")

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}
