package mongostore

// The queues registry: one document per declared queue, keyed by name, in
// the "<tasks collection>_queues" sibling collection (so isolation follows
// the tasks collection automatically). Managers register their declared
// policies at startup / first enqueue; a redeclaration with different
// settings fails — policy drift across deployments is an error, never
// silent. Deliberate changes go through SetQueuePolicy.

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

type queueDoc struct {
	Name        string    `bson:"_id"`
	TTLMillis   int64     `bson:"ttl_ms"`
	MaxAttempts int       `bson:"max_attempts"`
	CreatedAt   time.Time `bson:"created_at"`
	UpdatedAt   time.Time `bson:"updated_at"`
}

func toQueueDoc(q gotasks.QueuePolicy, now time.Time) queueDoc {
	return queueDoc{
		Name:        q.Name,
		TTLMillis:   q.TTL.Milliseconds(),
		MaxAttempts: q.MaxAttempts,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func (d queueDoc) toPolicy() gotasks.QueuePolicy {
	return gotasks.QueuePolicy{
		Name:        d.Name,
		TTL:         time.Duration(d.TTLMillis) * time.Millisecond,
		MaxAttempts: d.MaxAttempts,
	}
}

func (d queueDoc) matches(q gotasks.QueuePolicy) bool {
	return d.TTLMillis == q.TTL.Milliseconds() && d.MaxAttempts == q.MaxAttempts
}

// RegisterQueues implements the drift guard: insert-if-absent, no-op if
// identical, ErrQueueConflict if the registered policy differs.
func (s *Store) RegisterQueues(ctx context.Context, queues []gotasks.QueuePolicy) error {
	now := time.Now().UTC()
	for _, q := range queues {
		var existing queueDoc
		err := s.queuesCol.FindOne(ctx, bson.M{"_id": q.Name}).Decode(&existing)
		switch {
		case errors.Is(err, mongo.ErrNoDocuments):
			if _, err := s.queuesCol.InsertOne(ctx, toQueueDoc(q, now)); err != nil {
				if mongo.IsDuplicateKeyError(err) {
					// Raced another manager registering the same queue:
					// re-read and fall through to the comparison.
					if err := s.queuesCol.FindOne(ctx, bson.M{"_id": q.Name}).Decode(&existing); err != nil {
						return fmt.Errorf("mongostore: register queue %q: %w", q.Name, err)
					}
					break
				}
				return fmt.Errorf("mongostore: register queue %q: %w", q.Name, err)
			}
			continue
		case err != nil:
			return fmt.Errorf("mongostore: register queue %q: %w", q.Name, err)
		}
		if !existing.matches(q) {
			return fmt.Errorf("%w: queue %q is registered with TTL=%s MaxAttempts=%d, this manager declares TTL=%s MaxAttempts=%d (change deliberately via SetQueuePolicy)",
				gotasks.ErrQueueConflict, q.Name,
				time.Duration(existing.TTLMillis)*time.Millisecond, existing.MaxAttempts,
				q.TTL, q.MaxAttempts)
		}
	}
	return nil
}

// Queues lists every registered queue policy, name-ordered.
func (s *Store) Queues(ctx context.Context) ([]gotasks.QueuePolicy, error) {
	cur, err := s.queuesCol.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("mongostore: list queues: %w", err)
	}
	var docs []queueDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("mongostore: list queues: %w", err)
	}
	out := make([]gotasks.QueuePolicy, len(docs))
	for i, d := range docs {
		out[i] = d.toPolicy()
	}
	return out, nil
}

// SetQueuePolicy deliberately overwrites (or creates) one registry entry.
func (s *Store) SetQueuePolicy(ctx context.Context, q gotasks.QueuePolicy) error {
	if q.Name == "" {
		return errors.New("mongostore: queue name must not be empty")
	}
	now := time.Now().UTC()
	_, err := s.queuesCol.UpdateOne(ctx,
		bson.M{"_id": q.Name},
		bson.M{
			"$set":         bson.M{"ttl_ms": q.TTL.Milliseconds(), "max_attempts": q.MaxAttempts, "updated_at": now},
			"$setOnInsert": bson.M{"created_at": now},
		},
		options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("mongostore: set queue policy %q: %w", q.Name, err)
	}
	return nil
}
