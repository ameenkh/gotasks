// Retries, backoff, dead-letter, and requeue: a flaky handler fails a
// configurable number of times per task. Tasks that stay within their
// attempts budget retry with backoff and succeed; one exhausts its budget,
// goes dead (with its full error history), and is then requeued for a fresh
// attempts budget.
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

type Job struct {
	Name      string `json:"name"`
	FailTimes int    `json:"fail_times"` // fail this many attempts before succeeding
}

func main() {
	ctx := context.Background()

	store, err := mongostore.New(ctx, "mongodb://localhost:27017",
		mongostore.WithDatabase("gotasks_examples"),
		mongostore.WithCollection("retries"),
	)
	if err != nil {
		log.Fatal(err)
	}

	m, err := gotasks.New(store,
		gotasks.WithQueues(gotasks.QueuePolicy{Name: "jobs"}),
		gotasks.WithWorkers(2),
		gotasks.WithPollInterval(200*time.Millisecond),
		gotasks.WithDefaultMaxAttempts(3),
		// Real deployments want ExponentialBackoff(30*time.Second, time.Hour);
		// this is short so the demo finishes quickly.
		gotasks.WithBackoff(gotasks.FixedBackoff(500*time.Millisecond)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close(ctx)

	// A task is "settled" when it succeeds or goes dead.
	var settled atomic.Int32
	err = gotasks.RegisterHandler(m, "flaky",
		func(ctx context.Context, task *gotasks.Task, p Job) (any, error) {
			if task.Attempts <= p.FailTimes {
				fmt.Printf("%-12s attempt %d/%d -> failing\n", p.Name, task.Attempts, task.MaxAttempts)
				if task.Attempts == task.MaxAttempts {
					settled.Add(1) // this failure sends it to the dead-letter state
				}
				return nil, errors.New("simulated failure")
			}
			fmt.Printf("%-12s attempt %d/%d -> success (previous errors: %d)\n",
				p.Name, task.Attempts, task.MaxAttempts, len(task.Errors))
			settled.Add(1)
			return nil, nil
		})
	if err != nil {
		log.Fatal(err)
	}

	jobs := []Job{
		{Name: "clean", FailTimes: 0},   // succeeds first try
		{Name: "flaky-once", FailTimes: 1}, // fails once, then succeeds
		{Name: "hopeless", FailTimes: 99},  // exhausts 3 attempts -> dead
	}
	for _, j := range jobs {
		if _, err := gotasks.Enqueue(ctx, m, "jobs", "flaky", j); err != nil {
			log.Fatal(err)
		}
	}

	if err := m.Start(); err != nil {
		log.Fatal(err)
	}
	waitFor(&settled, int32(len(jobs)))

	// "hopeless" is now dead. Requeue every dead "flaky" task: it gets a
	// fresh attempts budget (its error history is kept) and dies again.
	n, err := m.RequeueDead(ctx, "flaky")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nrequeued %d dead task(s) for another round\n\n", n)
	waitFor(&settled, int32(len(jobs))+1)

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}

func waitFor(counter *atomic.Int32, want int32) {
	for counter.Load() < want {
		time.Sleep(100 * time.Millisecond)
	}
}
