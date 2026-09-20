package mongostore

// Introspection: the read/admin surface consumed by the ui package (and
// usable programmatically). Mongo-specific by design — this is not part of
// the gotasks.Store engine interface.

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

// TaskQuery filters and paginates ListTasks. Pagination is cursor-based:
// pass the last task's id as Before to get the next (older) page.
type TaskQuery struct {
	Queue  string `json:"queue"`
	Status string `json:"status"`
	Type   string `json:"type"`
	Limit  int    `json:"limit"` // default 50, max 200
	Before string `json:"before"`
}

// ListTasks returns tasks newest-first, plus the total number of tasks
// matching the filter (ignoring the pagination cursor).
func (s *Store) ListTasks(ctx context.Context, q TaskQuery) ([]*gotasks.Task, int64, error) {
	filter := bson.M{}
	if q.Queue != "" {
		filter["queue"] = q.Queue
	}
	if q.Status != "" {
		filter["status"] = q.Status
	}
	if q.Type != "" {
		filter["type"] = q.Type
	}
	total, err := s.tasksCol.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("mongostore: list tasks: %w", err)
	}
	if q.Before != "" {
		o, err := oid(q.Before)
		if err != nil {
			return nil, 0, err
		}
		filter["_id"] = bson.M{"$lt": o}
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	cur, err := s.tasksCol.Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, 0, fmt.Errorf("mongostore: list tasks: %w", err)
	}
	var docs []taskDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, 0, fmt.Errorf("mongostore: list tasks: %w", err)
	}
	out := make([]*gotasks.Task, 0, len(docs))
	for i := range docs {
		t, err := docs[i].toTask()
		if err != nil {
			return nil, 0, err
		}
		out = append(out, t)
	}
	return out, total, nil
}

// GetTask returns one task by id (ErrNotFound when absent).
func (s *Store) GetTask(ctx context.Context, id string) (*gotasks.Task, error) {
	o, err := oid(id)
	if err != nil {
		return nil, err
	}
	var doc taskDoc
	err = s.tasksCol.FindOne(ctx, bson.M{"_id": o}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("%w: %s", gotasks.ErrNotFound, id)
	}
	if err != nil {
		return nil, fmt.Errorf("mongostore: get task: %w", err)
	}
	return doc.toTask()
}

// DeleteTask removes one task in any state (an operator action: it bypasses
// lease fencing — a running task's worker will log a lease-lost finalize).
func (s *Store) DeleteTask(ctx context.Context, id string) error {
	o, err := oid(id)
	if err != nil {
		return err
	}
	res, err := s.tasksCol.DeleteOne(ctx, bson.M{"_id": o})
	if err != nil {
		return fmt.Errorf("mongostore: delete task: %w", err)
	}
	if res.DeletedCount == 0 {
		return fmt.Errorf("%w: %s", gotasks.ErrNotFound, id)
	}
	return nil
}

// RequeueDeadByQueue requeues every dead task in one queue (fresh attempts
// budget, errors kept as history). Returns the count.
func (s *Store) RequeueDeadByQueue(ctx context.Context, queue string) (int64, error) {
	res, err := s.tasksCol.UpdateMany(ctx,
		bson.M{"status": string(gotasks.StatusDead), "queue": queue},
		requeueUpdate(time.Now().UTC()))
	if err != nil {
		return 0, fmt.Errorf("mongostore: requeue dead by queue: %w", err)
	}
	return res.ModifiedCount, nil
}

// QueueOverview is one registered queue's policy plus live state.
type QueueOverview struct {
	Name           string        `json:"name"`
	TTL            time.Duration `json:"ttl"`
	MaxAttempts    int           `json:"max_attempts"`
	Pending        int64         `json:"pending"`
	Running        int64         `json:"running"`
	Done           int64         `json:"done"`
	Dead           int64         `json:"dead"`
	OldestDueAgeMS int64         `json:"oldest_due_age_ms"`
}

// Overview returns one page of registered queues (name-ordered) with live
// state, plus the total number of registered queues. Pass the previous
// page's next value as after; next is "" on the last page.
func (s *Store) Overview(ctx context.Context, after string, limit int) ([]QueueOverview, string, int64, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	total, err := s.queuesCol.CountDocuments(ctx, bson.M{})
	if err != nil {
		return nil, "", 0, fmt.Errorf("mongostore: overview: %w", err)
	}
	filter := bson.M{}
	if after != "" {
		filter["_id"] = bson.M{"$gt": after}
	}
	cur, err := s.queuesCol.Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, "", 0, fmt.Errorf("mongostore: overview: %w", err)
	}
	var docs []queueDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, "", 0, fmt.Errorf("mongostore: overview: %w", err)
	}
	out := make([]QueueOverview, 0, len(docs))
	for _, d := range docs {
		row, err := s.overviewRow(ctx, d.toPolicy())
		if err != nil {
			return nil, "", 0, err
		}
		out = append(out, row)
	}
	next := ""
	if len(docs) == limit {
		next = docs[len(docs)-1].Name
	}
	return out, next, total, nil
}

// QueueDetail returns one registered queue's policy and live state
// (ErrNotFound for unregistered names).
func (s *Store) QueueDetail(ctx context.Context, name string) (QueueOverview, error) {
	var doc queueDoc
	err := s.queuesCol.FindOne(ctx, bson.M{"_id": name}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return QueueOverview{}, fmt.Errorf("%w: queue %q", gotasks.ErrNotFound, name)
	}
	if err != nil {
		return QueueOverview{}, fmt.Errorf("mongostore: queue detail: %w", err)
	}
	return s.overviewRow(ctx, doc.toPolicy())
}

func (s *Store) overviewRow(ctx context.Context, p gotasks.QueuePolicy) (QueueOverview, error) {
	now := time.Now().UTC()
	row := QueueOverview{Name: p.Name, TTL: p.TTL, MaxAttempts: p.MaxAttempts}
	for status, dst := range map[gotasks.Status]*int64{
		gotasks.StatusPending: &row.Pending, gotasks.StatusRunning: &row.Running,
		gotasks.StatusDone: &row.Done, gotasks.StatusDead: &row.Dead,
	} {
		n, err := s.tasksCol.CountDocuments(ctx, bson.M{"queue": p.Name, "status": string(status)})
		if err != nil {
			return row, fmt.Errorf("mongostore: overview %q: %w", p.Name, err)
		}
		*dst = n
	}
	var oldest struct {
		RunAt time.Time `bson:"run_at"`
	}
	ferr := s.tasksCol.FindOne(ctx,
		bson.M{"queue": p.Name, "status": string(gotasks.StatusPending), "run_at": bson.M{"$lte": now}},
		options.FindOne().SetSort(bson.D{{Key: "run_at", Value: 1}}).SetProjection(bson.M{"run_at": 1}),
	).Decode(&oldest)
	if ferr == nil {
		row.OldestDueAgeMS = now.Sub(oldest.RunAt).Milliseconds()
	} else if !errors.Is(ferr, mongo.ErrNoDocuments) {
		return row, fmt.Errorf("mongostore: overview %q: %w", p.Name, ferr)
	}
	return row, nil
}

// MetricPoint is one metrics window for one queue: the cluster-wide gauges
// plus counters summed across manager instances.
type MetricPoint struct {
	WindowStart    time.Time `json:"window_start"`
	Pending        int64     `json:"pending"`
	Running        int64     `json:"running"`
	Done           int64     `json:"done"`
	Dead           int64     `json:"dead"`
	OldestDueAgeMS int64     `json:"oldest_due_age_ms"`
	Enqueued       int64     `json:"enqueued"`
	Claimed        int64     `json:"claimed"`
	CDone          int64     `json:"c_done"`
	CFailed        int64     `json:"c_failed"`
	CDead          int64     `json:"c_dead"`
}

// MetricsSeries returns one queue's metric points since from, oldest first.
func (s *Store) MetricsSeries(ctx context.Context, queue string, from time.Time) ([]MetricPoint, error) {
	byWindow := map[time.Time]*MetricPoint{}
	point := func(w time.Time) *MetricPoint {
		if p, ok := byWindow[w]; ok {
			return p
		}
		p := &MetricPoint{WindowStart: w}
		byWindow[w] = p
		return p
	}

	cur, err := s.metricsCol.Find(ctx, bson.M{
		"queue": queue, "kind": "gauges", "window_start": bson.M{"$gte": from.UTC()},
	})
	if err != nil {
		return nil, fmt.Errorf("mongostore: metrics series: %w", err)
	}
	var gauges []metricDoc
	if err := cur.All(ctx, &gauges); err != nil {
		return nil, fmt.Errorf("mongostore: metrics series: %w", err)
	}
	for _, g := range gauges {
		p := point(g.WindowStart)
		p.Pending, p.Running, p.Done, p.Dead, p.OldestDueAgeMS =
			g.Pending, g.Running, g.Done, g.Dead, g.OldestDueAgeMS
	}

	// Counters summed across manager instances per window.
	agg, err := s.metricsCol.Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"queue": queue, "kind": "counters", "window_start": bson.M{"$gte": from.UTC()},
		}}},
		{{Key: "$group", Value: bson.M{
			"_id":      "$window_start",
			"enqueued": bson.M{"$sum": "$enqueued"},
			"claimed":  bson.M{"$sum": "$claimed"},
			"c_done":   bson.M{"$sum": "$c_done"},
			"c_failed": bson.M{"$sum": "$c_failed"},
			"c_dead":   bson.M{"$sum": "$c_dead"},
		}}},
	})
	if err != nil {
		return nil, fmt.Errorf("mongostore: metrics series: %w", err)
	}
	var sums []struct {
		Window   time.Time `bson:"_id"`
		Enqueued int64     `bson:"enqueued"`
		Claimed  int64     `bson:"claimed"`
		CDone    int64     `bson:"c_done"`
		CFailed  int64     `bson:"c_failed"`
		CDead    int64     `bson:"c_dead"`
	}
	if err := agg.All(ctx, &sums); err != nil {
		return nil, fmt.Errorf("mongostore: metrics series: %w", err)
	}
	for _, c := range sums {
		p := point(c.Window)
		p.Enqueued, p.Claimed, p.CDone, p.CFailed, p.CDead =
			c.Enqueued, c.Claimed, c.CDone, c.CFailed, c.CDead
	}

	out := make([]MetricPoint, 0, len(byWindow))
	for _, p := range byWindow {
		out = append(out, *p)
	}
	sortMetricPoints(out)
	return out, nil
}

func sortMetricPoints(ps []MetricPoint) {
	for i := 1; i < len(ps); i++ {
		for j := i; j > 0 && ps[j].WindowStart.Before(ps[j-1].WindowStart); j-- {
			ps[j], ps[j-1] = ps[j-1], ps[j]
		}
	}
}
