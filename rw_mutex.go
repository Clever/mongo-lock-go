package lock

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	// ErrNotOwner is returnedd when an unlocking client did not hold the lock
	ErrNotOwner = errors.New("client does not hold lock")
)

// RWMutex implements a reader/writer lock. The interfaces matches that of sync.RWMutex
type RWMutex struct {
	collection *mongo.Collection
	lockID     string
	clientID   string
	SleepTime  time.Duration
}

// mongoLock is the resource stored in mongo to represent the lock
type mongoLock struct {
	LockID  string   `bson:"lockID"`
	Writer  string   `bson:"writer"`
	Readers []string `bson:"readers"`
}

var emptyWriterQuery = bson.M{
	"$or": []bson.M{
		{
			"writer": bson.M{"$exists": false},
		},
		{
			"writer": "",
		},
		{
			"writer": nil,
		},
	},
}

var emptyReaderQuery = bson.M{
	"$or": []bson.M{
		{
			"readers": bson.M{"$exists": false},
		},
		{
			"readers": bson.M{"$size": 0},
		},
		{
			"readers": nil,
		},
	},
}

func deepCloneMap(originalMap bson.M) bson.M {
	clonedMap := make(bson.M)
	for k, v := range originalMap {
		switch v := v.(type) {
		case bson.M:
			clonedMap[k] = deepCloneMap(v)
		case []bson.M:
			clonedSlice := make([]bson.M, len(v))
			for i, item := range v {
				clonedSlice[i] = deepCloneMap(item)
			}
			clonedMap[k] = clonedSlice
		default:
			clonedMap[k] = v
		}
	}
	return clonedMap
}

// NewRWMutex returns a new RWMutex
func NewRWMutex(
	collection *mongo.Collection,
	lockID, clientID string) *RWMutex {
	return &RWMutex{
		collection: collection,
		lockID:     lockID,
		clientID:   clientID,
		SleepTime:  time.Duration(5) * time.Second,
	}
}

func (m *RWMutex) TryLock() error {
	writerQuery := deepCloneMap(emptyWriterQuery)
	writerQuery["$or"] = append(writerQuery["$or"].([]bson.M), bson.M{"writer": m.clientID})
	writeLockQuery := bson.M{
		"lockID": m.lockID,
		"$and":   []bson.M{emptyReaderQuery, writerQuery},
	}
	res, err := m.collection.UpdateOne(
		context.TODO(),
		writeLockQuery,
		bson.M{
			"$set": bson.M{
				"writer":  m.clientID,
				"readers": []string{},
			},
		},
		options.Update().SetUpsert(true))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return ErrNotOwner
		}
		return err
	}
	if res.MatchedCount > 0 || res.UpsertedCount > 0 {
		return nil
	}

	return ErrNotOwner
}

// Lock acquires the write lock
func (m *RWMutex) Lock() error {
	for {
		err := m.TryLock()
		if err != nil && err == ErrNotOwner {
			jitter := time.Duration(rand.Int63n(1000)) * time.Millisecond
			time.Sleep(m.SleepTime + jitter)
			continue // keep looping
		}
		return err
	}
}

// Unlock releases the write lock
func (m *RWMutex) Unlock() error {

	deleteRes, err := m.collection.DeleteOne(
		context.TODO(),
		bson.M{
			"lockID": m.lockID,
			"writer": m.clientID,
		},
	)
	if err != nil {
		return err
	}

	if deleteRes.DeletedCount > 0 {
		return nil
	}

	return ErrNotOwner
}

// RLock acquires the read lock
func (m *RWMutex) RLock() error {
	for {
		err := m.TryRLock()
		if err != nil && err == ErrNotOwner {
			jitter := time.Duration(rand.Int63n(1000)) * time.Millisecond
			time.Sleep(m.SleepTime + jitter)
			continue
		}
		return err
	}
}

// TryRLock tries to acquires the read lock
func (m *RWMutex) TryRLock() error {
	updateRes, err := m.collection.UpdateOne(
		context.TODO(),
		bson.M{
			"lockID": m.lockID,
			"$or":    emptyWriterQuery["$or"],
		},
		bson.M{
			"$set": bson.M{
				"writer": "",
			},
			"$addToSet": bson.M{
				"readers": m.clientID,
			},
		},
		options.Update().SetUpsert(true))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return ErrNotOwner
		}
		return err
	}

	if updateRes.MatchedCount > 0 || updateRes.UpsertedCount > 0 {
		return nil
	}

	return ErrNotOwner
}

// RUnlock releases the read lock
func (m *RWMutex) RUnlock() error {
	deleteRes, err := m.collection.DeleteOne(
		context.TODO(),
		bson.M{
			"lockID": m.lockID,
			"$or":    emptyWriterQuery["$or"],
			"readers": bson.M{
				"$size": 1,
				"$all":  []string{m.clientID},
			},
		},
	)
	if err != nil {
		return err
	}
	if deleteRes.DeletedCount > 0 {
		return nil
	}

	updateRes, err := m.collection.UpdateOne(
		context.TODO(),
		bson.M{
			"lockID":  m.lockID,
			"readers": m.clientID,
		},
		bson.M{
			"$pull": bson.M{
				"readers": m.clientID,
			},
		},
	)
	if err != nil {
		return err
	}
	if updateRes.MatchedCount <= 0 {
		return ErrNotOwner

	}

	return nil
}
