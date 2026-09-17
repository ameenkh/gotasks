// Package mongostore implements the gotasks Store on MongoDB (>= 4.2).
//
// Claiming uses a single findOneAndUpdate, so N workers across N machines
// never take the same task. Payloads and results are stored as native BSON
// documents (not opaque blobs) so tasks stay inspectable with mongosh/Compass.
// Finished tasks can be auto-pruned by a TTL index via WithRetention /
// WithRetentionByType, and unique keys are enforced with a partial unique
// index (unique only while a task is active).
package mongostore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ameenkh/gotasks"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type config struct {
	database        string
	collection      string
	retention       time.Duration
	retentionByType map[string]time.Duration
	enqueueChunk    int
	ping            bool
}

// Option configures the store.
type Option func(*config)

// WithDatabase sets the database name (default "gotasks").
func WithDatabase(name string) Option { return func(c *config) { c.database = name } }

// WithCollection sets the tasks collection name (default "tasks").
func WithCollection(name string) Option { return func(c *config) { c.collection = name } }

// WithRetention auto-deletes done/dead tasks d after they finish, via a TTL
// index on expires_at. 0 (default) keeps finished tasks forever. Task types
// listed in WithRetentionByType override this default.
func WithRetention(d time.Duration) Option { return func(c *config) { c.retention = d } }

// WithRetentionByType gives specific task types their own retention,
// overriding WithRetention. A 0 value keeps that type's finished tasks
// forever even when a default retention is set.
func WithRetentionByType(byType map[string]time.Duration) Option {
	return func(c *config) { c.retentionByType = byType }
}

// WithEnqueueChunkSize sets how many tasks a single batch-enqueue insert
// carries (default 1000). Bigger batches are written chunk by chunk, so a
// 100k fan-out neither builds one giant wire message nor fails opaquely.
func WithEnqueueChunkSize(n int) Option { return func(c *config) { c.enqueueChunk = n } }

// WithPing controls the connectivity check in New (default true).
func WithPing(ping bool) Option { return func(c *config) { c.ping = ping } }

// Store is the MongoDB-backed gotasks.Store.
type Store struct {
	client          *mongo.Client
	col             *mongo.Collection
	retention       time.Duration
	retentionByType map[string]time.Duration
	enqueueChunk    int
	ownsClient      bool
}

var _ gotasks.Store = (*Store)(nil)

// New connects to MongoDB and prepares the store (indexes included).
func New(ctx context.Context, uri string, opts ...Option) (*Store, error) {
	cfg := defaults(opts)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("mongostore: connect: %w", err)
	}
	if cfg.ping {
		if err := client.Ping(ctx, readpref.Primary()); err != nil {
			_ = client.Disconnect(ctx)
			return nil, fmt.Errorf("mongostore: ping: %w", err)
		}
	}
	s, err := build(ctx, client, cfg)
	if err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	s.ownsClient = true
	return s, nil
}

// FromClient builds the store on an existing client (not disconnected by
// Close). Indexes are still ensured.
func FromClient(ctx context.Context, client *mongo.Client, opts ...Option) (*Store, error) {
	return build(ctx, client, defaults(opts))
}

func defaults(opts []Option) config {
	cfg := config{database: "gotasks", collection: "tasks", enqueueChunk: 1000, ping: true}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func build(ctx context.Context, client *mongo.Client, cfg config) (*Store, error) {
	if cfg.enqueueChunk < 1 || cfg.enqueueChunk > 10000 {
		return nil, fmt.Errorf("mongostore: enqueue chunk size must be 1..10000, got %d", cfg.enqueueChunk)
	}
	s := &Store{
		client:          client,
		col:             client.Database(cfg.database).Collection(cfg.collection),
		retention:       cfg.retention,
		retentionByType: cfg.retentionByType,
		enqueueChunk:    cfg.enqueueChunk,
	}
	if err := s.ensureIndexes(ctx); err != nil {
		return nil, fmt.Errorf("mongostore: ensure indexes: %w", err)
	}
	return s, nil
}

func (s *Store) ensureIndexes(ctx context.Context) error {
	_, err := s.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		// Claim, pending branch + (run_at, _id) sort.
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "run_at", Value: 1}, {Key: "_id", Value: 1}}},
		// Claim stale-reclaim branch + reaper sweep.
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "locked_until", Value: 1}}},
		// TTL retention: docs are deleted once expires_at passes; docs
		// without the field (pending/running) are never touched.
		{
			Keys:    bson.D{{Key: "expires_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0),
		},
		// Unique keys: unique only while the field exists — it is unset when
		// a task reaches done/dead, releasing the key.
		{
			Keys: bson.D{{Key: "unique_key", Value: 1}},
			Options: options.Index().SetUnique(true).
				SetPartialFilterExpression(bson.M{"unique_key": bson.M{"$exists": true}}),
		},
		// ClaimBatch's winners-fetch looks up by the batch lease token; the
		// partial index keeps it cheap and only covers in-flight tasks.
		{
			Keys: bson.D{{Key: "lease_token", Value: 1}},
			Options: options.Index().
				SetPartialFilterExpression(bson.M{"lease_token": bson.M{"$exists": true}}),
		},
	})
	return err
}

// retentionFor returns the retention for a task type (per-type override,
// else the default). <= 0 means keep forever.
func (s *Store) retentionFor(taskType string) time.Duration {
	if d, ok := s.retentionByType[taskType]; ok {
		return d
	}
	return s.retention
}

type taskDoc struct {
	ID          primitive.ObjectID  `bson:"_id,omitempty"`
	Queue       string              `bson:"queue"`
	Type        string              `bson:"type"`
	UniqueKey   string              `bson:"unique_key,omitempty"`
	Payload     bson.Raw            `bson:"payload,omitempty"`
	Status      string              `bson:"status"`
	Attempts    int                 `bson:"attempts"`
	MaxAttempts int                 `bson:"max_attempts"`
	RunAt       time.Time           `bson:"run_at"`
	LockedBy    string              `bson:"locked_by,omitempty"`
	LockedUntil time.Time           `bson:"locked_until,omitempty"`
	LeaseToken  string              `bson:"lease_token,omitempty"`
	Errors      []gotasks.TaskError `bson:"errors,omitempty"`
	Result      bson.Raw            `bson:"result,omitempty"`
	ExpiresAt   *time.Time          `bson:"expires_at,omitempty"`
	CreatedAt   time.Time           `bson:"created_at"`
	UpdatedAt   time.Time           `bson:"updated_at"`
}

// jsonToRaw converts the core's JSON payload/result to a BSON document so it
// is stored inspectable. gotasks guarantees payloads are objects or empty.
func jsonToRaw(j json.RawMessage) (bson.Raw, error) {
	t := bytes.TrimSpace(j)
	if len(t) == 0 || bytes.Equal(t, []byte("null")) {
		return nil, nil
	}
	var d bson.D
	if err := bson.UnmarshalExtJSON(t, false, &d); err != nil {
		return nil, fmt.Errorf("mongostore: payload to BSON: %w", err)
	}
	b, err := bson.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("mongostore: payload to BSON: %w", err)
	}
	return b, nil
}

func rawToJSON(r bson.Raw) (json.RawMessage, error) {
	if len(r) == 0 {
		return nil, nil
	}
	b, err := bson.MarshalExtJSON(r, false, false)
	if err != nil {
		return nil, fmt.Errorf("mongostore: BSON to JSON: %w", err)
	}
	return b, nil
}

func (d *taskDoc) toTask() (*gotasks.Task, error) {
	payload, err := rawToJSON(d.Payload)
	if err != nil {
		return nil, err
	}
	result, err := rawToJSON(d.Result)
	if err != nil {
		return nil, err
	}
	return &gotasks.Task{
		ID:          d.ID.Hex(),
		Queue:       d.Queue,
		Type:        d.Type,
		UniqueKey:   d.UniqueKey,
		Payload:     payload,
		Status:      gotasks.Status(d.Status),
		Attempts:    d.Attempts,
		MaxAttempts: d.MaxAttempts,
		RunAt:       d.RunAt,
		LockedBy:    d.LockedBy,
		LockedUntil: d.LockedUntil,
		LeaseToken:  d.LeaseToken,
		Errors:      d.Errors,
		Result:      result,
		CreatedAt:   d.CreatedAt,
		UpdatedAt:   d.UpdatedAt,
	}, nil
}

func oid(id string) (primitive.ObjectID, error) {
	o, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return primitive.NilObjectID, fmt.Errorf("%w: bad id %q", gotasks.ErrNotFound, id)
	}
	return o, nil
}

// Enqueue inserts tasks in chunks of enqueueChunk. Ids are generated
// client-side upfront, so the returned slice is always aligned with the
// input; failed entries are reported per index via *PartialEnqueueError
// with "" left at their positions (a single-task unique-key conflict keeps
// the *DuplicateTaskError contract).
func (s *Store) Enqueue(ctx context.Context, tasks []*gotasks.Task) ([]string, error) {
	if len(tasks) == 0 {
		return nil, nil
	}
	docs := make([]any, len(tasks))
	ids := make([]string, len(tasks))
	for i, t := range tasks {
		payload, err := jsonToRaw(t.Payload)
		if err != nil {
			return nil, err
		}
		queue := t.Queue
		if queue == "" {
			queue = gotasks.DefaultQueue
		}
		id := primitive.NewObjectID()
		ids[i] = id.Hex()
		docs[i] = taskDoc{
			ID:          id,
			Queue:       queue,
			Type:        t.Type,
			UniqueKey:   t.UniqueKey,
			Payload:     payload,
			Status:      string(t.Status),
			Attempts:    t.Attempts,
			MaxAttempts: t.MaxAttempts,
			RunAt:       t.RunAt.UTC(),
			CreatedAt:   t.CreatedAt.UTC(),
			UpdatedAt:   t.UpdatedAt.UTC(),
		}
	}

	var failures []gotasks.EnqueueFailure
	failChunk := func(start, end int, cause error) {
		for i := start; i < end; i++ {
			ids[i] = ""
			failures = append(failures, gotasks.EnqueueFailure{Index: i, Err: cause})
		}
	}
	for start := 0; start < len(docs); start += s.enqueueChunk {
		end := min(start+s.enqueueChunk, len(docs))
		_, err := s.col.InsertMany(ctx, docs[start:end], options.InsertMany().SetOrdered(false))
		if err == nil {
			continue
		}
		var bwe mongo.BulkWriteException
		if errors.As(err, &bwe) && len(bwe.WriteErrors) > 0 {
			// Per-entry failures (e.g. unique-key conflicts); the rest of
			// the unordered chunk was inserted.
			for _, we := range bwe.WriteErrors {
				gi := start + we.Index
				ids[gi] = ""
				cause := fmt.Errorf("mongostore: enqueue: %s", we.Message)
				if we.Code == 11000 { // duplicate key
					cause = fmt.Errorf("%w: %s", gotasks.ErrDuplicateTask, we.Message)
				}
				failures = append(failures, gotasks.EnqueueFailure{Index: gi, Err: cause})
			}
			continue
		}
		// Hard error (network, context): this chunk's fate is unknown and
		// later chunks were never attempted — report them all and stop.
		cause := fmt.Errorf("mongostore: enqueue: %w", err)
		failChunk(start, len(docs), cause)
		break
	}

	if len(failures) == 0 {
		return ids, nil
	}
	// Single-task unique-key conflict keeps the richer DuplicateTaskError
	// contract (existing holder's id looked up).
	if len(tasks) == 1 && errors.Is(failures[0].Err, gotasks.ErrDuplicateTask) {
		return nil, s.duplicateError(ctx, tasks)
	}
	return ids, &gotasks.PartialEnqueueError{Failures: failures}
}

// duplicateError builds a *gotasks.DuplicateTaskError for a unique-key
// conflict, looking up which active task holds the key. ExistingID may be
// "" if the holder finished between the insert attempt and the lookup.
func (s *Store) duplicateError(ctx context.Context, tasks []*gotasks.Task) error {
	key := ""
	for _, t := range tasks {
		if t.UniqueKey != "" {
			key = t.UniqueKey
			break
		}
	}
	dup := &gotasks.DuplicateTaskError{Key: key}
	var doc taskDoc
	if err := s.col.FindOne(ctx, bson.M{"unique_key": key}).Decode(&doc); err == nil {
		dup.ExistingID = doc.ID.Hex()
	}
	return dup
}

// runnableFilter matches tasks a claim may take: due pending tasks, or
// stale running ones with attempts remaining. With max_attempts=1 a stale
// task is never reclaimed (at-most-once); the reaper marks it dead instead.
func runnableFilter(now time.Time, types, queues []string) bson.M {
	filter := bson.M{"$or": bson.A{
		bson.M{"status": string(gotasks.StatusPending), "run_at": bson.M{"$lte": now}},
		bson.M{
			"status":       string(gotasks.StatusRunning),
			"locked_until": bson.M{"$lt": now},
			"$expr":        bson.M{"$lt": bson.A{"$attempts", "$max_attempts"}},
		},
	}}
	if len(types) > 0 {
		filter["type"] = bson.M{"$in": types}
	}
	if len(queues) > 0 {
		filter["queue"] = bson.M{"$in": queues}
	}
	return filter
}

func claimUpdate(now time.Time, workerID, leaseToken string, lease time.Duration) bson.M {
	return bson.M{
		"$set": bson.M{
			"status":       string(gotasks.StatusRunning),
			"locked_by":    workerID,
			"lease_token":  leaseToken,
			"locked_until": now.Add(lease),
			"updated_at":   now,
		},
		"$inc": bson.M{"attempts": 1},
	}
}

var claimSort = bson.D{{Key: "run_at", Value: 1}, {Key: "_id", Value: 1}}

func (s *Store) Claim(ctx context.Context, claim gotasks.ClaimOptions) (*gotasks.Task, error) {
	now := time.Now().UTC()
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	if claim.FIFO {
		opts = opts.SetSort(claimSort)
	}

	var doc taskDoc
	err := s.col.FindOneAndUpdate(ctx,
		runnableFilter(now, claim.Types, claim.Queues),
		claimUpdate(now, claim.WorkerID, uuid.NewString(), claim.Lease),
		opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, gotasks.ErrNoTask
	}
	if err != nil {
		return nil, fmt.Errorf("mongostore: claim: %w", err)
	}
	return doc.toTask()
}

// ClaimBatch takes up to k runnable tasks in three fixed round trips:
// find candidate ids -> guarded updateMany (the runnable filter is
// re-applied, so ids stolen by a concurrent claimer in between are simply
// not matched) -> fetch the docs we won by their fresh lease token. The
// last step also makes the returned tasks exact (attempts, errors) even if
// a candidate was claimed-failed-retried between steps.
func (s *Store) ClaimBatch(ctx context.Context, claim gotasks.ClaimOptions, k int) ([]*gotasks.Task, error) {
	if k < 1 {
		return nil, fmt.Errorf("mongostore: claim batch: k must be >= 1, got %d", k)
	}
	now := time.Now().UTC()

	findOpts := options.Find().SetLimit(int64(k)).SetProjection(bson.M{"_id": 1})
	if claim.FIFO {
		findOpts = findOpts.SetSort(claimSort)
	}
	cur, err := s.col.Find(ctx, runnableFilter(now, claim.Types, claim.Queues), findOpts)
	if err != nil {
		return nil, fmt.Errorf("mongostore: claim batch find: %w", err)
	}
	var candidates []struct {
		ID primitive.ObjectID `bson:"_id"`
	}
	if err := cur.All(ctx, &candidates); err != nil {
		return nil, fmt.Errorf("mongostore: claim batch decode: %w", err)
	}
	if len(candidates) == 0 {
		return nil, gotasks.ErrNoTask
	}
	ids := make(bson.A, len(candidates))
	for i, c := range candidates {
		ids[i] = c.ID
	}

	token := uuid.NewString()
	guard := runnableFilter(now, nil, nil) // ids are already type/queue-filtered
	guard["_id"] = bson.M{"$in": ids}
	res, err := s.col.UpdateMany(ctx, guard, claimUpdate(now, claim.WorkerID, token, claim.Lease))
	if err != nil {
		return nil, fmt.Errorf("mongostore: claim batch update: %w", err)
	}
	if res.ModifiedCount == 0 {
		return nil, nil // all candidates stolen; more work may exist — retry
	}

	won, err := s.col.Find(ctx, bson.M{"lease_token": token})
	if err != nil {
		return nil, fmt.Errorf("mongostore: claim batch fetch: %w", err)
	}
	var docs []taskDoc
	if err := won.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("mongostore: claim batch fetch decode: %w", err)
	}
	tasks := make([]*gotasks.Task, 0, len(docs))
	for i := range docs {
		t, err := docs[i].toTask()
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

// leaseFilter fences finalizing writes: only the current lease token may
// touch a running task. Deliberately no locked_until check — finishing after
// expiry but before reclaim is a success (reclaim rotates the token anyway).
func leaseFilter(id primitive.ObjectID, leaseToken string) bson.M {
	return bson.M{
		"_id":         id,
		"lease_token": leaseToken,
		"status":      string(gotasks.StatusRunning),
	}
}

// completeWrite builds the fenced filter+update marking t done.
func (s *Store) completeWrite(t *gotasks.Task, result json.RawMessage, now time.Time) (bson.M, bson.M, error) {
	o, err := oid(t.ID)
	if err != nil {
		return nil, nil, err
	}
	set := bson.M{
		"status":     string(gotasks.StatusDone),
		"updated_at": now,
	}
	if raw, err := jsonToRaw(result); err != nil {
		return nil, nil, err
	} else if raw != nil {
		set["result"] = raw
	}
	if d := s.retentionFor(t.Type); d > 0 {
		set["expires_at"] = now.Add(d)
	}
	update := bson.M{
		"$set":   set,
		"$unset": bson.M{"locked_by": "", "lease_token": "", "unique_key": ""},
	}
	return leaseFilter(o, t.LeaseToken), update, nil
}

func (s *Store) Complete(ctx context.Context, t *gotasks.Task, result json.RawMessage) error {
	filter, update, err := s.completeWrite(t, result, time.Now().UTC())
	if err != nil {
		return err
	}
	res, err := s.col.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("mongostore: complete: %w", err)
	}
	if res.MatchedCount == 0 {
		return gotasks.ErrLeaseLost
	}
	return nil
}

// failWrite builds the fenced filter+update recording a failed attempt.
func (s *Store) failWrite(t *gotasks.Task, taskErr gotasks.TaskError, retryAt time.Time, terminal bool, now time.Time) (bson.M, bson.M, error) {
	o, err := oid(t.ID)
	if err != nil {
		return nil, nil, err
	}
	set := bson.M{"updated_at": now}
	unset := bson.M{"locked_by": "", "lease_token": ""}
	if terminal {
		set["status"] = string(gotasks.StatusDead)
		unset["unique_key"] = "" // dead releases the unique key
		if d := s.retentionFor(t.Type); d > 0 {
			set["expires_at"] = now.Add(d)
		}
	} else {
		// Still active (will retry): the unique key stays held.
		set["status"] = string(gotasks.StatusPending)
		set["run_at"] = retryAt.UTC()
	}
	update := bson.M{
		"$set":   set,
		"$push":  bson.M{"errors": taskErr},
		"$unset": unset,
	}
	return leaseFilter(o, t.LeaseToken), update, nil
}

func (s *Store) Fail(ctx context.Context, t *gotasks.Task, taskErr gotasks.TaskError, retryAt time.Time, terminal bool) error {
	filter, update, err := s.failWrite(t, taskErr, retryAt, terminal, time.Now().UTC())
	if err != nil {
		return err
	}
	res, err := s.col.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("mongostore: fail: %w", err)
	}
	if res.MatchedCount == 0 {
		return gotasks.ErrLeaseLost
	}
	return nil
}

// FinalizeBatch applies many outcomes as one unordered bulkWrite; each
// entry keeps the exact fencing of Complete/Fail. Lost = entries whose
// lease was gone by flush time (reclaimed) — skipped, reported, not errors.
func (s *Store) FinalizeBatch(ctx context.Context, outcomes []gotasks.Outcome) (int64, error) {
	if len(outcomes) == 0 {
		return 0, nil
	}
	now := time.Now().UTC()
	models := make([]mongo.WriteModel, 0, len(outcomes))
	for _, o := range outcomes {
		var filter, update bson.M
		var err error
		if o.Failure != nil {
			filter, update, err = s.failWrite(o.Task, *o.Failure, o.RetryAt, o.Terminal, now)
		} else {
			filter, update, err = s.completeWrite(o.Task, o.Result, now)
		}
		if err != nil {
			return 0, err
		}
		models = append(models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update))
	}
	res, err := s.col.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, fmt.Errorf("mongostore: finalize batch: %w", err)
	}
	return int64(len(outcomes)) - res.MatchedCount, nil
}

func (s *Store) ExtendLease(ctx context.Context, t *gotasks.Task, lease time.Duration) (time.Time, error) {
	o, err := oid(t.ID)
	if err != nil {
		return time.Time{}, err
	}
	now := time.Now().UTC()
	until := now.Add(lease)
	res, err := s.col.UpdateOne(ctx, leaseFilter(o, t.LeaseToken),
		bson.M{"$set": bson.M{"locked_until": until, "updated_at": now}})
	if err != nil {
		return time.Time{}, fmt.Errorf("mongostore: extend lease: %w", err)
	}
	if res.MatchedCount == 0 {
		return time.Time{}, gotasks.ErrLeaseLost
	}
	return until, nil
}

// requeueUpdate resets a dead task to a freshly-enqueued state: pending,
// attempts 0, runnable now, no retention expiry or stale result. Errors are
// kept as history. The unique key was released when the task died and is NOT
// restored (a new task may legitimately hold it by now).
func requeueUpdate(now time.Time) bson.M {
	return bson.M{
		"$set":   bson.M{"status": string(gotasks.StatusPending), "attempts": 0, "run_at": now, "updated_at": now},
		"$unset": bson.M{"expires_at": "", "result": "", "locked_by": "", "locked_until": "", "lease_token": ""},
	}
}

func (s *Store) Requeue(ctx context.Context, id string) error {
	o, err := oid(id)
	if err != nil {
		return err
	}
	res, err := s.col.UpdateOne(ctx,
		bson.M{"_id": o, "status": string(gotasks.StatusDead)},
		requeueUpdate(time.Now().UTC()))
	if err != nil {
		return fmt.Errorf("mongostore: requeue: %w", err)
	}
	if res.MatchedCount == 0 {
		return fmt.Errorf("%w: no dead task with id %q", gotasks.ErrNotFound, id)
	}
	return nil
}

func (s *Store) RequeueDead(ctx context.Context, taskType string) (int64, error) {
	filter := bson.M{"status": string(gotasks.StatusDead)}
	if taskType != "" {
		filter["type"] = taskType
	}
	res, err := s.col.UpdateMany(ctx, filter, requeueUpdate(time.Now().UTC()))
	if err != nil {
		return 0, fmt.Errorf("mongostore: requeue dead: %w", err)
	}
	return res.ModifiedCount, nil
}

// retentionExpr builds the expires_at value for the reaper's pipeline
// update. With per-type retention it is a $switch on the task's own type
// (0-retention types resolve to $$REMOVE = keep forever); with only a
// default it is a plain date; with no retention at all it returns nil.
func (s *Store) retentionExpr(now time.Time) any {
	expiry := func(d time.Duration) any {
		if d <= 0 {
			return "$$REMOVE"
		}
		return now.Add(d)
	}
	if len(s.retentionByType) == 0 {
		if s.retention <= 0 {
			return nil
		}
		return now.Add(s.retention)
	}
	branches := bson.A{}
	for taskType, d := range s.retentionByType {
		branches = append(branches, bson.M{
			"case": bson.M{"$eq": bson.A{"$type", taskType}},
			"then": expiry(d),
		})
	}
	return bson.M{"$switch": bson.M{"branches": branches, "default": expiry(s.retention)}}
}

func (s *Store) ReapExpired(ctx context.Context) (int64, error) {
	now := time.Now().UTC()
	filter := bson.M{
		"status":       string(gotasks.StatusRunning),
		"locked_until": bson.M{"$lt": now},
		"$expr":        bson.M{"$gte": bson.A{"$attempts", "$max_attempts"}},
	}
	set := bson.M{
		"status":     string(gotasks.StatusDead),
		"updated_at": now,
		"errors": bson.M{"$concatArrays": bson.A{
			bson.M{"$ifNull": bson.A{"$errors", bson.A{}}},
			bson.A{bson.M{
				"at":      now,
				"attempt": "$attempts",
				"worker":  "$locked_by",
				"message": "lease expired before completion; attempts exhausted",
			}},
		}},
	}
	if expr := s.retentionExpr(now); expr != nil {
		set["expires_at"] = expr
	}
	// Pipeline update so each reaped task's error entry records its own
	// attempts/worker fields (and its own type's retention).
	res, err := s.col.UpdateMany(ctx, filter, mongo.Pipeline{
		{{Key: "$set", Value: set}},
		{{Key: "$unset", Value: bson.A{"locked_by", "lease_token", "unique_key"}}},
	})
	if err != nil {
		return 0, fmt.Errorf("mongostore: reap: %w", err)
	}
	return res.ModifiedCount, nil
}

// DropCollection drops the underlying tasks collection — a helper for
// tests and benchmarks; production cleanup should use retention TTLs.
func (s *Store) DropCollection(ctx context.Context) error {
	return s.col.Drop(ctx)
}

func (s *Store) Close(ctx context.Context) error {
	if s.ownsClient {
		return s.client.Disconnect(ctx)
	}
	return nil
}
