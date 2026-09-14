// Package mongostore implements the gotasks Store on MongoDB (>= 4.2).
//
// Claiming uses a single findOneAndUpdate, so N workers across N machines
// never take the same task. Payloads and results are stored as native BSON
// documents (not opaque blobs) so tasks stay inspectable with mongosh/Compass.
// Finished tasks can be auto-pruned by a TTL index via WithRetention.
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
	database   string
	collection string
	retention  time.Duration
	ping       bool
}

// Option configures the store.
type Option func(*config)

// WithDatabase sets the database name (default "gotasks").
func WithDatabase(name string) Option { return func(c *config) { c.database = name } }

// WithCollection sets the tasks collection name (default "tasks").
func WithCollection(name string) Option { return func(c *config) { c.collection = name } }

// WithRetention auto-deletes done/failed tasks d after they finish, via a TTL
// index on expires_at. 0 (default) keeps finished tasks forever.
func WithRetention(d time.Duration) Option { return func(c *config) { c.retention = d } }

// WithPing controls the connectivity check in New (default true).
func WithPing(ping bool) Option { return func(c *config) { c.ping = ping } }

// Store is the MongoDB-backed gotasks.Store.
type Store struct {
	client     *mongo.Client
	col        *mongo.Collection
	retention  time.Duration
	ownsClient bool
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
	cfg := config{database: "gotasks", collection: "tasks", ping: true}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func build(ctx context.Context, client *mongo.Client, cfg config) (*Store, error) {
	s := &Store{
		client:    client,
		col:       client.Database(cfg.database).Collection(cfg.collection),
		retention: cfg.retention,
	}
	if err := s.ensureIndexes(ctx); err != nil {
		return nil, fmt.Errorf("mongostore: ensure indexes: %w", err)
	}
	return s, nil
}

func (s *Store) ensureIndexes(ctx context.Context) error {
	_, err := s.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "run_at", Value: 1}, {Key: "_id", Value: 1}}},
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "locked_until", Value: 1}}},
		{
			Keys:    bson.D{{Key: "expires_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0),
		},
	})
	return err
}

type taskDoc struct {
	ID          primitive.ObjectID  `bson:"_id,omitempty"`
	Type        string              `bson:"type"`
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
		Type:        d.Type,
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

func (s *Store) Enqueue(ctx context.Context, tasks []*gotasks.Task) ([]string, error) {
	if len(tasks) == 0 {
		return nil, nil
	}
	docs := make([]any, len(tasks))
	for i, t := range tasks {
		payload, err := jsonToRaw(t.Payload)
		if err != nil {
			return nil, err
		}
		docs[i] = taskDoc{
			Type:        t.Type,
			Payload:     payload,
			Status:      string(t.Status),
			Attempts:    t.Attempts,
			MaxAttempts: t.MaxAttempts,
			RunAt:       t.RunAt.UTC(),
			CreatedAt:   t.CreatedAt.UTC(),
			UpdatedAt:   t.UpdatedAt.UTC(),
		}
	}
	res, err := s.col.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil {
		return nil, fmt.Errorf("mongostore: enqueue: %w", err)
	}
	ids := make([]string, len(res.InsertedIDs))
	for i, id := range res.InsertedIDs {
		ids[i] = id.(primitive.ObjectID).Hex()
	}
	return ids, nil
}

func (s *Store) Claim(ctx context.Context, workerID string, types []string, lease time.Duration) (*gotasks.Task, error) {
	now := time.Now().UTC()
	filter := bson.M{"$or": bson.A{
		bson.M{"status": string(gotasks.StatusPending), "run_at": bson.M{"$lte": now}},
		// Stale reclaim: expired lease AND attempts remaining. With
		// max_attempts=1 a stale task is never reclaimed (at-most-once);
		// the reaper marks it failed instead.
		bson.M{
			"status":       string(gotasks.StatusRunning),
			"locked_until": bson.M{"$lt": now},
			"$expr":        bson.M{"$lt": bson.A{"$attempts", "$max_attempts"}},
		},
	}}
	if len(types) > 0 {
		filter["type"] = bson.M{"$in": types}
	}
	update := bson.M{
		"$set": bson.M{
			"status":       string(gotasks.StatusRunning),
			"locked_by":    workerID,
			"lease_token":  uuid.NewString(),
			"locked_until": now.Add(lease),
			"updated_at":   now,
		},
		"$inc": bson.M{"attempts": 1},
	}
	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: "run_at", Value: 1}, {Key: "_id", Value: 1}}).
		SetReturnDocument(options.After)

	var doc taskDoc
	err := s.col.FindOneAndUpdate(ctx, filter, update, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, gotasks.ErrNoTask
	}
	if err != nil {
		return nil, fmt.Errorf("mongostore: claim: %w", err)
	}
	return doc.toTask()
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

func (s *Store) Complete(ctx context.Context, id, leaseToken string, result json.RawMessage) error {
	o, err := oid(id)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	set := bson.M{
		"status":     string(gotasks.StatusDone),
		"updated_at": now,
	}
	if raw, err := jsonToRaw(result); err != nil {
		return err
	} else if raw != nil {
		set["result"] = raw
	}
	if s.retention > 0 {
		set["expires_at"] = now.Add(s.retention)
	}
	update := bson.M{"$set": set, "$unset": bson.M{"locked_by": "", "lease_token": ""}}
	res, err := s.col.UpdateOne(ctx, leaseFilter(o, leaseToken), update)
	if err != nil {
		return fmt.Errorf("mongostore: complete: %w", err)
	}
	if res.MatchedCount == 0 {
		return gotasks.ErrLeaseLost
	}
	return nil
}

func (s *Store) Fail(ctx context.Context, id, leaseToken string, taskErr gotasks.TaskError, retryAt time.Time, terminal bool) error {
	o, err := oid(id)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	set := bson.M{"updated_at": now}
	if terminal {
		set["status"] = string(gotasks.StatusFailed)
		if s.retention > 0 {
			set["expires_at"] = now.Add(s.retention)
		}
	} else {
		set["status"] = string(gotasks.StatusPending)
		set["run_at"] = retryAt.UTC()
	}
	update := bson.M{
		"$set":   set,
		"$push":  bson.M{"errors": taskErr},
		"$unset": bson.M{"locked_by": "", "lease_token": ""},
	}
	res, err := s.col.UpdateOne(ctx, leaseFilter(o, leaseToken), update)
	if err != nil {
		return fmt.Errorf("mongostore: fail: %w", err)
	}
	if res.MatchedCount == 0 {
		return gotasks.ErrLeaseLost
	}
	return nil
}

func (s *Store) ExtendLease(ctx context.Context, id, leaseToken string, lease time.Duration) (time.Time, error) {
	o, err := oid(id)
	if err != nil {
		return time.Time{}, err
	}
	now := time.Now().UTC()
	until := now.Add(lease)
	res, err := s.col.UpdateOne(ctx, leaseFilter(o, leaseToken),
		bson.M{"$set": bson.M{"locked_until": until, "updated_at": now}})
	if err != nil {
		return time.Time{}, fmt.Errorf("mongostore: extend lease: %w", err)
	}
	if res.MatchedCount == 0 {
		return time.Time{}, gotasks.ErrLeaseLost
	}
	return until, nil
}

func (s *Store) ReapExpired(ctx context.Context) (int64, error) {
	now := time.Now().UTC()
	filter := bson.M{
		"status":       string(gotasks.StatusRunning),
		"locked_until": bson.M{"$lt": now},
		"$expr":        bson.M{"$gte": bson.A{"$attempts", "$max_attempts"}},
	}
	set := bson.M{
		"status":     string(gotasks.StatusFailed),
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
	if s.retention > 0 {
		set["expires_at"] = now.Add(s.retention)
	}
	// Pipeline update so each reaped task's error entry records its own
	// attempts/worker fields.
	res, err := s.col.UpdateMany(ctx, filter, mongo.Pipeline{
		{{Key: "$set", Value: set}},
		{{Key: "$unset", Value: bson.A{"locked_by", "lease_token"}}},
	})
	if err != nil {
		return 0, fmt.Errorf("mongostore: reap: %w", err)
	}
	return res.ModifiedCount, nil
}

func (s *Store) Close(ctx context.Context) error {
	if s.ownsClient {
		return s.client.Disconnect(ctx)
	}
	return nil
}
