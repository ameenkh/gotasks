// Named queues: tasks carry a queue label (WithQueue at enqueue; "default"
// otherwise), and each manager can be pointed at specific queues with
// WithQueues. Here two managers share one process and one Mongo client:
// a big pool for the noisy "emails" queue and a small dedicated pool for
// "reports" — so report generation is never starved by email volume.
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/ameenkh/gotasks/stores/mongostore"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Job struct {
	N int `json:"n"`
}

func main() {
	ctx := context.Background()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	newStore := func() *mongostore.Store {
		s, err := mongostore.FromClient(ctx, client,
			mongostore.WithDatabase("gotasks_examples"),
			mongostore.WithCollection("queues"),
		)
		if err != nil {
			log.Fatal(err)
		}
		return s
	}

	var emailsDone, reportsDone atomic.Int32

	// Manager 1: eight workers dedicated to the "emails" queue.
	emailMgr, err := gotasks.New(newStore(),
		gotasks.WithWorkers(8),
		gotasks.WithQueues("emails"),
		gotasks.WithPollInterval(200*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer emailMgr.Close(ctx)
	_ = gotasks.RegisterHandler(emailMgr, "send",
		func(ctx context.Context, task *gotasks.Task, p Job) (any, error) {
			time.Sleep(20 * time.Millisecond)
			emailsDone.Add(1)
			return nil, nil
		})

	// Manager 2: two workers dedicated to the "reports" queue.
	reportMgr, err := gotasks.New(newStore(),
		gotasks.WithWorkers(2),
		gotasks.WithQueues("reports"),
		gotasks.WithPollInterval(200*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer reportMgr.Close(ctx)
	_ = gotasks.RegisterHandler(reportMgr, "generate",
		func(ctx context.Context, task *gotasks.Task, p Job) (any, error) {
			time.Sleep(300 * time.Millisecond) // reports are heavy
			reportsDone.Add(1)
			return nil, nil
		})

	// Fan in work: lots of emails, a few reports.
	emails := make([]Job, 200)
	for i := range emails {
		emails[i] = Job{N: i}
	}
	if _, err := gotasks.EnqueueMany(ctx, emailMgr, "send", emails,
		gotasks.WithQueue("emails")); err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := gotasks.Enqueue(ctx, reportMgr, "generate", Job{N: i},
			gotasks.WithQueue("reports")); err != nil {
			log.Fatal(err)
		}
	}

	t0 := time.Now()
	if err := emailMgr.Start(); err != nil {
		log.Fatal(err)
	}
	if err := reportMgr.Start(); err != nil {
		log.Fatal(err)
	}
	for emailsDone.Load() < 200 || reportsDone.Load() < 5 {
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("emails: %d done, reports: %d done in %s — separate pools, no starvation\n",
		emailsDone.Load(), reportsDone.Load(), time.Since(t0).Round(time.Millisecond))

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = emailMgr.Stop(stopCtx)
	_ = reportMgr.Stop(stopCtx)
}
