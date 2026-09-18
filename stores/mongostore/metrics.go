package mongostore

// Mongo-native metrics: one "<tasks collection>_metrics" sibling collection
// (regular, NOT time-series — time-series collections don't support the
// unique index the cluster-wide gauge dedup relies on). Two document kinds:
//   gauges   — cluster-wide state snapshot per queue per window; managers
//              race, unique {queue, window_start, kind, manager_id:""}
//              makes the first insert win;
//   counters — one doc per manager instance per queue per window with
//              exact throughput counts (no dedup; readers sum).
// A TTL index on window_start prunes metrics after the configured
// retention (updated in place via collMod when retention changes).

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ameenkh/gotasks"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type metricDoc struct {
	Queue       string    `bson:"queue"`
	WindowStart time.Time `bson:"window_start"`
	WindowMS    int64     `bson:"window_ms"`
	Kind        string    `bson:"kind"`       // "gauges" | "counters"
	ManagerID   string    `bson:"manager_id"` // "" for gauges

	// gauges
	Pending        int64 `bson:"pending,omitempty"`
	Running        int64 `bson:"running,omitempty"`
	Done           int64 `bson:"done,omitempty"`
	Dead           int64 `bson:"dead,omitempty"`
	OldestDueAgeMS int64 `bson:"oldest_due_age_ms,omitempty"`

	// counters
	Enqueued int64 `bson:"enqueued,omitempty"`
	Claimed  int64 `bson:"claimed,omitempty"`
	CDone    int64 `bson:"c_done,omitempty"`
	CFailed  int64 `bson:"c_failed,omitempty"`
	CDead    int64 `bson:"c_dead,omitempty"`
}

func (s *Store) EnsureMetrics(ctx context.Context, retention time.Duration) error {
	_, err := s.metricsCol.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "queue", Value: 1}, {Key: "window_start", Value: 1},
				{Key: "kind", Value: 1}, {Key: "manager_id", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("metric_identity"),
		},
		{
			Keys: bson.D{{Key: "window_start", Value: 1}},
			Options: options.Index().SetName("metrics_ttl").
				SetExpireAfterSeconds(int32(retention.Seconds())),
		},
	})
	if err == nil {
		return nil
	}
	// Retention changed since the TTL index was created: update in place.
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) && (cmdErr.Name == "IndexOptionsConflict" || cmdErr.Code == 85) {
		res := s.metricsCol.Database().RunCommand(ctx, bson.D{
			{Key: "collMod", Value: s.metricsCol.Name()},
			{Key: "index", Value: bson.M{"name": "metrics_ttl", "expireAfterSeconds": int32(retention.Seconds())}},
		})
		if res.Err() != nil {
			return fmt.Errorf("mongostore: update metrics retention: %w", res.Err())
		}
		return nil
	}
	return fmt.Errorf("mongostore: ensure metrics: %w", err)
}

func (s *Store) SnapshotMetrics(ctx context.Context, queues []string, windowStart time.Time, window time.Duration) error {
	now := time.Now().UTC()
	for _, q := range queues {
		doc := metricDoc{
			Queue: q, WindowStart: windowStart.UTC(), WindowMS: window.Milliseconds(),
			Kind: "gauges", ManagerID: "",
		}
		var err error
		count := func(status gotasks.Status) (int64, error) {
			return s.tasksCol.CountDocuments(ctx, bson.M{"queue": q, "status": string(status)})
		}
		if doc.Pending, err = count(gotasks.StatusPending); err != nil {
			return fmt.Errorf("mongostore: snapshot %q: %w", q, err)
		}
		if doc.Running, err = count(gotasks.StatusRunning); err != nil {
			return fmt.Errorf("mongostore: snapshot %q: %w", q, err)
		}
		if doc.Done, err = count(gotasks.StatusDone); err != nil {
			return fmt.Errorf("mongostore: snapshot %q: %w", q, err)
		}
		if doc.Dead, err = count(gotasks.StatusDead); err != nil {
			return fmt.Errorf("mongostore: snapshot %q: %w", q, err)
		}
		// Oldest DUE task only (run_at <= now): a task scheduled for next
		// week must not read as a week of staleness.
		var oldest struct {
			RunAt time.Time `bson:"run_at"`
		}
		ferr := s.tasksCol.FindOne(ctx,
			bson.M{"queue": q, "status": string(gotasks.StatusPending), "run_at": bson.M{"$lte": now}},
			options.FindOne().SetSort(bson.D{{Key: "run_at", Value: 1}}).SetProjection(bson.M{"run_at": 1}),
		).Decode(&oldest)
		switch {
		case ferr == nil:
			doc.OldestDueAgeMS = now.Sub(oldest.RunAt).Milliseconds()
		case errors.Is(ferr, mongo.ErrNoDocuments):
		default:
			return fmt.Errorf("mongostore: snapshot %q: %w", q, ferr)
		}

		if _, err := s.metricsCol.InsertOne(ctx, doc); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				continue // another manager won this window
			}
			return fmt.Errorf("mongostore: snapshot %q: %w", q, err)
		}
	}
	return nil
}

func (s *Store) RecordCounters(ctx context.Context, managerID string, windowStart time.Time, window time.Duration, counters map[string]gotasks.QueueCounters) error {
	if len(counters) == 0 {
		return nil
	}
	docs := make([]any, 0, len(counters))
	for q, c := range counters {
		docs = append(docs, metricDoc{
			Queue: q, WindowStart: windowStart.UTC(), WindowMS: window.Milliseconds(),
			Kind: "counters", ManagerID: managerID,
			Enqueued: c.Enqueued, Claimed: c.Claimed,
			CDone: c.Done, CFailed: c.Failed, CDead: c.Dead,
		})
	}
	if _, err := s.metricsCol.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false)); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil // same manager re-flushing a window (restart edge); keep first
		}
		return fmt.Errorf("mongostore: record counters: %w", err)
	}
	return nil
}
