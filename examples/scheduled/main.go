// Scheduled tasks: WithDelay and WithRunAt defer when a task becomes
// runnable. Workers only claim tasks whose run_at has passed, so nothing
// executes early — watch the timestamps in the output.
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

type Reminder struct {
	Text    string    `json:"text"`
	DueAt   time.Time `json:"due_at"`
}

func main() {
	ctx := context.Background()

	store, err := mongostore.New(ctx, "mongodb://localhost:27017",
		mongostore.WithDatabase("gotasks_examples"),
		mongostore.WithCollection("scheduled"),
	)
	if err != nil {
		log.Fatal(err)
	}

	m, err := gotasks.New(store,
		gotasks.WithQueues(gotasks.QueuePolicy{Name: "reminders"}),
		gotasks.WithWorkers(2),
		gotasks.WithPollInterval(200*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close(ctx)

	const total = 5
	var done atomic.Int32
	err = gotasks.RegisterHandler(m, "reminder",
		func(ctx context.Context, task *gotasks.Task, p Reminder) (any, error) {
			late := time.Since(p.DueAt).Round(10 * time.Millisecond)
			fmt.Printf("%s  fired %q (due %s, late by %s)\n",
				time.Now().Format("15:04:05.000"), p.Text, p.DueAt.Format("15:04:05.000"), late)
			done.Add(1)
			return nil, nil
		})
	if err != nil {
		log.Fatal(err)
	}

	// Delays of 1..4 seconds via WithDelay...
	for i := 1; i <= total-1; i++ {
		delay := time.Duration(i) * time.Second
		due := time.Now().Add(delay)
		_, err := gotasks.Enqueue(ctx, m, "reminders", "reminder",
			Reminder{Text: fmt.Sprintf("after %s", delay), DueAt: due},
			gotasks.TaskPolicy{Delay: delay})
		if err != nil {
			log.Fatal(err)
		}
	}
	// ...and an absolute time via WithRunAt.
	at := time.Now().Add(5 * time.Second)
	if _, err := gotasks.Enqueue(ctx, m, "reminders", "reminder",
		Reminder{Text: "at an absolute time", DueAt: at},
		gotasks.TaskPolicy{RunAt: at}); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s  enqueued %d reminders, waiting...\n", time.Now().Format("15:04:05.000"), total)
	if err := m.Start(); err != nil {
		log.Fatal(err)
	}
	for done.Load() < total {
		time.Sleep(100 * time.Millisecond)
	}
	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}
