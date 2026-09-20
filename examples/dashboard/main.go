// The gotasks dashboard, live: mounts dashboard.Handler on :8080 and runs a small
// workload generator (steady emails, occasional failures, scheduled
// reports) so there is something to look at. Open http://localhost:8080.
//
// In a real service, mount the handler behind YOUR auth:
//
//	mux.Handle("/gotasks/", http.StripPrefix("/gotasks", dashboard.Handler(store)))
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/ameenkh/gotasks/stores/mongostore"
	"github.com/ameenkh/gotasks/dashboard"
)

type Job struct {
	N int `json:"n"`
}

func main() {
	ctx := context.Background()

	store, err := mongostore.New(ctx, "mongodb://localhost:27017",
		mongostore.WithDatabase("gotasks_examples"),
		mongostore.WithNamespace("dashboard"),
	)
	if err != nil {
		log.Fatal(err)
	}

	m, err := gotasks.New(store,
		gotasks.WithQueues(
			gotasks.QueuePolicy{Name: "emails", TTL: time.Hour},
			gotasks.QueuePolicy{Name: "reports"},
		),
		gotasks.WithWorkers(4),
		gotasks.WithManagerName("demo"),
		gotasks.WithJanitor(gotasks.JanitorConfig{
			ReapInterval: 30 * time.Second,
			Metrics:      &gotasks.MetricsConfig{Interval: 10 * time.Second},
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close(ctx)

	_ = gotasks.RegisterHandler(m, "send", func(ctx context.Context, t *gotasks.Task, p Job) (any, error) {
		time.Sleep(time.Duration(20+rand.IntN(80)) * time.Millisecond)
		if rand.IntN(10) == 0 {
			return nil, errors.New("smtp: connection reset (simulated)")
		}
		return map[string]string{"message_id": fmt.Sprintf("msg-%d", p.N)}, nil
	})
	_ = gotasks.RegisterHandler(m, "generate", func(ctx context.Context, t *gotasks.Task, p Job) (any, error) {
		time.Sleep(time.Duration(300+rand.IntN(500)) * time.Millisecond)
		return map[string]int{"rows": 1000 + rand.IntN(9000)}, nil
	})
	if err := m.Start(); err != nil {
		log.Fatal(err)
	}

	// Workload generator.
	go func() {
		for n := 0; ; n++ {
			_, _ = gotasks.Enqueue(ctx, m, "emails", "send", Job{N: n})
			if n%20 == 0 {
				_, _ = gotasks.Enqueue(ctx, m, "reports", "generate", Job{N: n},
					gotasks.TaskPolicy{Delay: time.Duration(rand.IntN(30)) * time.Second})
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	fmt.Println("gotasks dashboard: http://localhost:" + port)
	log.Fatal(http.ListenAndServe(":"+port, dashboard.Handler(store)))
}
