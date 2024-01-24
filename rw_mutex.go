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
			"readers": nil,
		},
		{
			"readers": []string{},
		},
	},
}

// NewRWMutex returns a new RWMutex
func NewRWMutex(collection *mongo.Collection, lockID, clientID string) *RWMutex {
	return &RWMutex{
		collection: collection,
		lockID:     lockID,
		clientID:   clientID,
		SleepTime:  time.Duration(5) * time.Second,
	}
}

func (m *RWMutex) tryToGetWriteLock() error {
	res := m.collection.FindOneAndUpdate(context.TODO(), bson.M{
		"lockID": m.lockID,
		"$and":   []bson.M{emptyReaderQuery, emptyWriterQuery},
	}, bson.M{
		"$set": bson.M{
			"writer": m.clientID,
		},
	})
	if res.Err() == nil {
		return nil // we got the lock!
	} else if res.Err() != mongo.ErrNoDocuments {
		// This only works if the mgo.Session object has Safe mode enabled.
		// Safe is the default but something for which we should maintain
		// external documentation
		return res.Err()
	}

	return ErrNotOwner
}

// TryLock tries to acquire the write lock
func (m *RWMutex) TryLock() error {
	lock, err := m.findOrCreateLock()
	if err != nil {
		return err
	}

	// if this clientID already has the lock, re-enter the lock and return
	if lock.Writer == m.clientID {
		return nil
	}

	return m.tryToGetWriteLock()
}

// Lock acquires the write lock
func (m *RWMutex) Lock() error {
	lock, err := m.findOrCreateLock()
	if err != nil {
		return err
	}

	// if this clientID already has the lock, re-enter the lock and return
	if lock.Writer == m.clientID {
		return nil
	}

	for {
		err := m.tryToGetWriteLock()
		if err == ErrNotOwner {
			jitter := time.Duration(rand.Int63n(1000)) * time.Millisecond
			time.Sleep(m.SleepTime + jitter)
			continue // keep looping
		}
		return err
	}
}

// Unlock releases the write lock
func (m *RWMutex) Unlock() error {
	res := m.collection.FindOneAndUpdate(context.TODO(), bson.M{
		"lockID": m.lockID,
		"writer": m.clientID,
	}, bson.M{
		"$set": bson.M{
			"writer": "",
		},
	})
	if res.Err() == mongo.ErrNoDocuments {
		return ErrNotOwner
	}
	return res.Err()
}

// RLock acquires the read lock
func (m *RWMutex) RLock() error {
	lock, err := m.findOrCreateLock()
	if err != nil {
		return err
	}

	for {
		err := m.tryToGetReadLock(lock)
		if err == ErrNotOwner {
			jitter := time.Duration(rand.Int63n(1000)) * time.Millisecond
			time.Sleep(m.SleepTime + jitter)
			continue
		}
		return err
	}
}

// TryRLock tries to acquires the read lock
func (m *RWMutex) TryRLock() error {
	lock, err := m.findOrCreateLock()
	if err != nil {
		return err
	}

	return m.tryToGetReadLock(lock)
}

// tryToGetReadLock makes an attempt to acquire a read lock given an existing lock.
// It will return:
// - `nil` if the lock is acquired.
// - ErrNotOwner if a writer has acquired the lock
// - a non-nil error in a failure case
func (m *RWMutex) tryToGetReadLock(lock *mongoLock) error {
	for _, reader := range lock.Readers {
		// if this clientID already has a read lock, re-enter the lock and return
		if reader == m.clientID {
			return nil
		}
	}

	res := m.collection.FindOneAndUpdate(context.TODO(), bson.M{
		"lockID": m.lockID,
		"$and":   []bson.M{emptyWriterQuery},
	}, bson.M{
		"$addToSet": bson.M{
			"readers": m.clientID,
		},
	})
	if res.Err() == nil {
		return nil
	} else if res.Err() != mongo.ErrNoDocuments {
		// This only works if the mgo.Session object has Safe mode enabled. Safe is the default but
		// something for which we should maintain external documentation
		return res.Err()
	}

	return ErrNotOwner
}

// RUnlock releases the read lock
func (m *RWMutex) RUnlock() error {
	res := m.collection.FindOneAndUpdate(context.TODO(), bson.M{
		"lockID":  m.lockID,
		"readers": m.clientID,
	}, bson.M{
		"$pull": bson.M{
			"readers": m.clientID,
		},
	})
	if res.Err() == mongo.ErrNoDocuments {
		return ErrNotOwner
	}
	return res.Err()
}

// findOrCreateLock will attempt to see if an existing lock ID exists
// if it does not, we will create the lock. afterwards we just return the lock.
func (m *RWMutex) findOrCreateLock() (*mongoLock, error) {
	var lock mongoLock

	res := m.collection.FindOneAndUpdate(context.TODO(),
		bson.M{"lockID": m.lockID}, bson.M{"$set": bson.M{"lockID": m.lockID}},
		options.FindOneAndUpdate().SetReturnDocument(options.After).SetUpsert(true),
	)
	if res.Err() != nil {
		if !mongo.IsDuplicateKeyError(res.Err()) {
			return nil, res.Err()
		}
	}
	if err := res.Decode(&lock); err != nil {
		return nil, err
	}
	return &lock, nil
}
