// Unique keys: TaskPolicy.UniqueKey guarantees at most one active (pending or
// running) task per key. Duplicate enqueues return the existing task's id
// with an error matching ErrDuplicateTask — treat that as idempotent
// success. Once the task finishes, the key is released and can be reused.
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

type SyncJob struct {
	Tenant string `json:"tenant"`
}

func main() {
	ctx := context.Background()

	store, err := mongostore.New(ctx, "mongodb://localhost:27017",
		mongostore.WithDatabase("gotasks_examples"),
		mongostore.WithNamespace("unique"),
	)
	if err != nil {
		log.Fatal(err)
	}

	m, err := gotasks.New(store,
		gotasks.WithQueues(gotasks.QueuePolicy{Name: "syncs"}),
		gotasks.WithWorkers(2),
		gotasks.WithPollInterval(100*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close(ctx)

	var done atomic.Int32
	err = gotasks.RegisterHandler(m, "tenant-sync",
		func(ctx context.Context, task *gotasks.Task, p SyncJob) (any, error) {
			fmt.Printf("syncing tenant %s (task %s)...\n", p.Tenant, task.ID)
			time.Sleep(2 * time.Second) // pretend this is a heavy sync
			done.Add(1)
			return nil, nil
		})
	if err != nil {
		log.Fatal(err)
	}

	enqueue := func() {
		id, err := gotasks.Enqueue(ctx, m, "syncs", "tenant-sync",
			SyncJob{Tenant: "tenant-42"},
			gotasks.TaskPolicy{UniqueKey: "sync:tenant-42"})
		switch {
		case errors.Is(err, gotasks.ErrDuplicateTask):
			fmt.Printf("enqueue deduped -> already active as task %s\n", id)
		case err != nil:
			log.Fatal(err)
		default:
			fmt.Printf("enqueue accepted -> task %s\n", id)
		}
	}

	// Three rapid enqueues for the same key: one accepted, two deduped.
	enqueue()
	enqueue()
	enqueue()

	if err := m.Start(); err != nil {
		log.Fatal(err)
	}
	for done.Load() < 1 {
		time.Sleep(100 * time.Millisecond)
	}

	// The sync finished, so the key was released — this one is accepted.
	fmt.Println("\nfirst sync done; key released:")
	enqueue()
	for done.Load() < 2 {
		time.Sleep(100 * time.Millisecond)
	}

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}
