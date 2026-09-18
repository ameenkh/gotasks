package gotasks_test

// Benchmarks against local MongoDB (skip when unreachable). Run:
//
//	go test -bench . -benchtime 2000x -run xxx .
//
// Throughput benchmarks report tasks/sec end-to-end: enqueue b.N tasks,
// then time the pool draining them (claim + handshake + handler + finalize).

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/ameenkh/gotasks/stores/mongostore"
)

func benchStore(b *testing.B) *mongostore.Store {
	b.Helper()
	uri := os.Getenv("GOTASKS_TEST_MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	coll := fmt.Sprintf("bench_%d", time.Now().UnixNano())
	st, err := mongostore.New(ctx, uri,
		mongostore.WithDatabase("gotasks_bench"),
		mongostore.WithNamespace(coll),
	)
	if err != nil {
		b.Skipf("no MongoDB at %s: %v", uri, err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = st.DropCollection(ctx)
		_ = st.Close(ctx)
	})
	return st
}

func BenchmarkEnqueueSingle(b *testing.B) {
	st := benchStore(b)
	m, err := gotasks.New(st, gotasks.WithQueues(gotasks.QueuePolicy{Name: "bench"}))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := gotasks.Enqueue(ctx, m, "bench", "bench", struct{ N int }{N: i}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnqueueMany100(b *testing.B) {
	st := benchStore(b)
	m, err := gotasks.New(st, gotasks.WithQueues(gotasks.QueuePolicy{Name: "bench"}))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	batch := make([]struct{ N int }, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := gotasks.EnqueueMany(ctx, m, "bench", "bench", batch); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "tasks/sec")
}

func benchThroughput(b *testing.B, opts ...gotasks.Option) {
	st := benchStore(b)
	m, err := gotasks.New(st, append([]gotasks.Option{
		gotasks.WithQueues(gotasks.QueuePolicy{Name: "bench"}),
		gotasks.WithPollInterval(50 * time.Millisecond),
		gotasks.WithJanitor(gotasks.JanitorConfig{NoReap: true}),
		gotasks.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	}, opts...)...)
	if err != nil {
		b.Fatal(err)
	}
	var done atomic.Int64
	err = gotasks.RegisterHandler(m, "bench",
		func(ctx context.Context, task *gotasks.Task, _ struct{ N int }) (any, error) {
			done.Add(1)
			return nil, nil
		})
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	const chunk = 1000
	payloads := make([]struct{ N int }, chunk)
	for enqueued := 0; enqueued < b.N; enqueued += chunk {
		n := min(chunk, b.N-enqueued)
		if _, err := gotasks.EnqueueMany(ctx, m, "bench", "bench", payloads[:n]); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	if err := m.Start(); err != nil {
		b.Fatal(err)
	}
	for done.Load() < int64(b.N) {
		time.Sleep(5 * time.Millisecond)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/sec")

	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_ = m.Stop(stopCtx)
}

func BenchmarkThroughputSingleMode4Workers(b *testing.B) {
	benchThroughput(b, gotasks.WithWorkers(4))
}

func BenchmarkThroughputSingleMode16Workers(b *testing.B) {
	benchThroughput(b, gotasks.WithWorkers(16))
}

func BenchmarkThroughputPipeline16x8NoFinalize(b *testing.B) {
	benchThroughput(b, gotasks.WithWorkers(8),
		gotasks.WithPipelineMode(gotasks.PipelineConfig{ClaimBatch: 16, NoFinalize: true}))
}

func BenchmarkThroughputPipeline16x8Workers(b *testing.B) {
	benchThroughput(b, gotasks.WithWorkers(8),
		gotasks.WithPipelineMode(gotasks.PipelineConfig{ClaimBatch: 16, FinalizeInterval: 25 * time.Millisecond}))
}

func BenchmarkThroughputPipeline64x16Workers(b *testing.B) {
	benchThroughput(b, gotasks.WithWorkers(16),
		gotasks.WithPipelineMode(gotasks.PipelineConfig{ClaimBatch: 64, FinalizeInterval: 25 * time.Millisecond}))
}



func BenchmarkEnqueueMany10k(b *testing.B) {
	st := benchStore(b)
	m, err := gotasks.New(st, gotasks.WithQueues(gotasks.QueuePolicy{Name: "bench"}))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	batch := make([]struct{ N int }, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := gotasks.EnqueueMany(ctx, m, "bench", "bench", batch); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*10000)/b.Elapsed().Seconds(), "tasks/sec")
}
