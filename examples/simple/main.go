// A minimal end-to-end example: enqueue a few email tasks (one scheduled),
// run a worker pool against local MongoDB, stop cleanly on Ctrl-C.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/ameenkh/gotasks/stores/mongostore"
)

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func main() {
	ctx := context.Background()

	store, err := mongostore.New(ctx, "mongodb://localhost:27017",
		mongostore.WithDatabase("gotasks_example"),
	)
	if err != nil {
		log.Fatal(err)
	}

	m, err := gotasks.New(store,
		// Every queue is declared explicitly; TTL is the tasks' total
		// lifetime (purged 24h after creation, done or not).
		gotasks.WithQueues(gotasks.QueuePolicy{Name: "emails", TTL: 24 * time.Hour}),
		gotasks.WithWorkers(4),
		gotasks.WithPollInterval(2*time.Second),
		gotasks.WithLeaseTime(time.Minute),
		gotasks.WithBackoff(gotasks.ExponentialBackoff(5*time.Second, 5*time.Minute)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close(ctx)

	err = gotasks.RegisterHandler(m, "email",
		func(ctx context.Context, task *gotasks.Task, p EmailPayload) (any, error) {
			fmt.Printf("sending email to %s: %q (attempt %d)\n", p.To, p.Subject, task.Attempts)
			return map[string]string{"message_id": "msg-" + task.ID}, nil
		},
		gotasks.WithHandlerTimeout(30*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Immediate, batch, and scheduled enqueues.
	if _, err := gotasks.Enqueue(ctx, m, "emails", "email", EmailPayload{To: "a@example.com", Subject: "hello"}); err != nil {
		log.Fatal(err)
	}
	if _, err := gotasks.EnqueueMany(ctx, m, "emails", "email", []EmailPayload{
		{To: "b@example.com", Subject: "batch 1"},
		{To: "c@example.com", Subject: "batch 2"},
	}); err != nil {
		log.Fatal(err)
	}
	if _, err := gotasks.Enqueue(ctx, m, "emails", "email",
		EmailPayload{To: "d@example.com", Subject: "10s later"},
		gotasks.TaskPolicy{Delay: 10 * time.Second},
	); err != nil {
		log.Fatal(err)
	}

	// Run until Ctrl-C, then drain in-flight tasks.
	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := m.Run(runCtx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("drained, bye")
}
