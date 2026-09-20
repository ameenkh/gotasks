// Package mongostore implements the gotasks Store on MongoDB (>= 4.2).
//
// Claiming uses a single findOneAndUpdate, so N workers across N machines
// never take the same task. Payloads and results are stored as native BSON
// documents (not opaque blobs) so tasks stay inspectable with mongosh/Compass.
// Task lifetime is enforced by a TTL index on expires_at (stamped at
// enqueue from the queue's TTL policy), and unique keys are enforced with
// a partial unique index (unique only while a task is active).
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
	database     string
	namespace    string
	enqueueChunk int
	ping         bool
}

// Option configures the store.
type Option func(*config)

// WithDatabase sets the database name (default "gotasks").
func WithDatabase(name string) Option { return func(c *config) { c.database = name } }

// WithNamespace sets the collection namespace (default "gotasks"): the
// store uses "{namespace}_tasks", "{namespace}_queues" and
// "{namespace}_metrics".
func WithNamespace(name string) Option { return func(c *config) { c.namespace = name } }

// WithEnqueueChunkSize sets how many tasks a single batch-enqueue insert
// carries (default 1000). Bigger batches are written chunk by chunk, so a
// 100k fan-out neither builds one giant wire message nor fails opaquely.
func WithEnqueueChunkSize(n int) Option { return func(c *config) { c.enqueueChunk = n } }

// WithPing controls the connectivity check in New (default true).
func WithPing(ping bool) Option { return func(c *config) { c.ping = ping } }

// Store is the MongoDB-backed gotasks.Store.
type Store struct {
	client       *mongo.Client
	tasksCol     *mongo.Collection // "{namespace}_tasks"
	queuesCol    *mongo.Collection // "{namespace}_queues"
	metricsCol   *mongo.Collection // "{namespace}_metrics"
	enqueueChunk int
	ownsClient   bool
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
	cfg := config{database: "gotasks", namespace: "gotasks", enqueueChunk: 1000, ping: true}
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
		client:       client,
		tasksCol:     client.Database(cfg.database).Collection(cfg.namespace + "_tasks"),
		queuesCol:    client.Database(cfg.database).Collection(cfg.namespace + "_queues"),
		metricsCol:   client.Database(cfg.database).Collection(cfg.namespace + "_metrics"),
		enqueueChunk: cfg.enqueueChunk,
	}
	if err := s.ensureIndexes(ctx); err != nil {
		return nil, fmt.Errorf("mongostore: ensure indexes: %w", err)
	}
	return s, nil
}

func (s *Store) ensureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		// THE claim index: managers always subscribe to explicit queues, so
		// every claim carries a queue clause and hits the leading equality.
		// (run_at, _id) after (queue, status) also serves the FIFO sort.
		// Pre-v0.5 deployments can drop the old "status_1_run_at_1__id_1".
		{
			Keys: bson.D{{Key: "queue", Value: 1}, {Key: "status", Value: 1},
				{Key: "run_at", Value: 1}, {Key: "_id", Value: 1}},
			Options: options.Index().SetName("queue_claim"),
		},
		// Claim stale-reclaim branch + reaper sweep.
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "leased_until", Value: 1}}},
		// Total-lifetime TTL: expires_at is stamped at CREATION (when a TTL
		// applies) and Mongo purges the doc when it passes, in any state.
		// Sparse: no-TTL tasks carry no field and pay no index write.
		// Named explicitly so it never conflicts with a pre-v0.4 non-sparse
		// "expires_at_1" (drop that one manually after upgrading).
		{
			Keys: bson.D{{Key: "expires_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0).
				SetSparse(true).SetName("expires_at_ttl"),
		},
		// Unique keys: unique only while the field exists — it is unset when
		// a task reaches done/dead, releasing the key.
		{
			Keys: bson.D{{Key: "unique_key", Value: 1}},
			Options: options.Index().SetUnique(true).
				SetPartialFilterExpression(bson.M{"unique_key": bson.M{"$exists": true}}),
		},
		// No lease_token index: ClaimBatch's winners-fetch targets the known
		// candidate _ids instead, so token lookups never need one (pre-v0.4
		// deployments can drop the leftover "lease_token_1" index).
	}
	_, err := s.tasksCol.Indexes().CreateMany(ctx, indexes)
	return err
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
	LeasedBy    string              `bson:"leased_by,omitempty"`
	LeasedUntil time.Time           `bson:"leased_until,omitempty"`
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
		ExpiresAt:   d.ExpiresAt,
		LeasedBy:    d.LeasedBy,
		LeasedUntil: d.LeasedUntil,
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
		id := primitive.NewObjectID()
		ids[i] = id.Hex()
		docs[i] = taskDoc{
			ID:          id,
			Queue:       t.Queue,
			Type:        t.Type,
			UniqueKey:   t.UniqueKey,
			Payload:     payload,
			Status:      string(t.Status),
			Attempts:    t.Attempts,
			MaxAttempts: t.MaxAttempts,
			RunAt:       t.RunAt.UTC(),
			ExpiresAt:   t.ExpiresAt,
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
		_, err := s.tasksCol.InsertMany(ctx, docs[start:end], options.InsertMany().SetOrdered(false))
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
	if err := s.tasksCol.FindOne(ctx, bson.M{"unique_key": key}).Decode(&doc); err == nil {
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
			"leased_until": bson.M{"$lt": now},
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
			"leased_by":    workerID,
			"lease_token":  leaseToken,
			"leased_until": now.Add(lease),
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
	err := s.tasksCol.FindOneAndUpdate(ctx,
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
	cur, err := s.tasksCol.Find(ctx, runnableFilter(now, claim.Types, claim.Queues), findOpts)
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
	res, err := s.tasksCol.UpdateMany(ctx, guard, claimUpdate(now, claim.WorkerID, token, claim.Lease))
	if err != nil {
		return nil, fmt.Errorf("mongostore: claim batch update: %w", err)
	}
	if res.ModifiedCount == 0 {
		return nil, nil // all candidates stolen; more work may exist — retry
	}

	// Winners are a subset of the candidates, so target their _ids (point
	// lookups) and filter by token — no lease_token index needed.
	won, err := s.tasksCol.Find(ctx, bson.M{"_id": bson.M{"$in": ids}, "lease_token": token})
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
// touch a running task. Deliberately no leased_until check — finishing after
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
	update := bson.M{
		"$set":   set,
		"$unset": bson.M{"leased_by": "", "lease_token": "", "unique_key": ""},
	}
	return leaseFilter(o, t.LeaseToken), update, nil
}

func (s *Store) Complete(ctx context.Context, t *gotasks.Task, result json.RawMessage) error {
	filter, update, err := s.completeWrite(t, result, time.Now().UTC())
	if err != nil {
		return err
	}
	res, err := s.tasksCol.UpdateOne(ctx, filter, update)
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
	unset := bson.M{"leased_by": "", "lease_token": ""}
	if terminal {
		set["status"] = string(gotasks.StatusDead)
		unset["unique_key"] = "" // dead releases the unique key
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
	res, err := s.tasksCol.UpdateOne(ctx, filter, update)
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
	res, err := s.tasksCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
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
	res, err := s.tasksCol.UpdateOne(ctx, leaseFilter(o, t.LeaseToken),
		bson.M{"$set": bson.M{"leased_until": until, "updated_at": now}})
	if err != nil {
		return time.Time{}, fmt.Errorf("mongostore: extend lease: %w", err)
	}
	if res.MatchedCount == 0 {
		return time.Time{}, gotasks.ErrLeaseLost
	}
	return until, nil
}

// requeueUpdate resets a dead task to a freshly-enqueued state: pending,
// attempts 0, runnable now, stale result cleared. Errors are kept as
// history, the unique key is NOT restored (a new task may hold it by now),
// and expires_at is UNTOUCHED: it is the task's total lifetime, stamped at
// creation — a revived task keeps its original purge deadline.
func requeueUpdate(now time.Time) bson.M {
	return bson.M{
		"$set":   bson.M{"status": string(gotasks.StatusPending), "attempts": 0, "run_at": now, "updated_at": now},
		"$unset": bson.M{"result": "", "leased_by": "", "leased_until": "", "lease_token": ""},
	}
}

func (s *Store) Requeue(ctx context.Context, id string) error {
	o, err := oid(id)
	if err != nil {
		return err
	}
	res, err := s.tasksCol.UpdateOne(ctx,
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
	res, err := s.tasksCol.UpdateMany(ctx, filter, requeueUpdate(time.Now().UTC()))
	if err != nil {
		return 0, fmt.Errorf("mongostore: requeue dead: %w", err)
	}
	return res.ModifiedCount, nil
}

func (s *Store) ReapExpired(ctx context.Context) (int64, error) {
	now := time.Now().UTC()
	filter := bson.M{
		"status":       string(gotasks.StatusRunning),
		"leased_until": bson.M{"$lt": now},
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
				"worker":  "$leased_by",
				"message": "lease expired before completion; attempts exhausted",
			}},
		}},
	}
	// Pipeline update so each reaped task's error entry records its own
	// attempts/worker fields.
	res, err := s.tasksCol.UpdateMany(ctx, filter, mongo.Pipeline{
		{{Key: "$set", Value: set}},
		{{Key: "$unset", Value: bson.A{"leased_by", "lease_token", "unique_key"}}},
	})
	if err != nil {
		return 0, fmt.Errorf("mongostore: reap: %w", err)
	}
	return res.ModifiedCount, nil
}

// DropCollection drops the underlying tasks collection and its sibling
// queues registry — a helper for tests and benchmarks; production cleanup
// should use queue TTLs.
func (s *Store) DropCollection(ctx context.Context) error {
	if err := s.tasksCol.Drop(ctx); err != nil {
		return err
	}
	if err := s.queuesCol.Drop(ctx); err != nil {
		return err
	}
	return s.metricsCol.Drop(ctx)
}

func (s *Store) Close(ctx context.Context) error {
	if s.ownsClient {
		return s.client.Disconnect(ctx)
	}
	return nil
}
