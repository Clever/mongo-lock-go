package lock

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	// ErrNotOwner is returnedd when an unlocking client did not hold the lock
	ErrNotOwner = errors.New("client does not hold lock")
)

// RWMutex implements a reader/writer lock. The interfaces matches that of sync.RWMutex
type RWMutex struct {
	collection         *mongo.Collection
	lockID             string
	districtID         string
	clientID           string
	SleepTime          time.Duration
	checkBothLockTypes bool
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
func NewRWMutex(
	collection *mongo.Collection,
	lockID, clientID, districtID string,
	checkBothLockTypes bool) *RWMutex {
	return &RWMutex{
		collection:         collection,
		lockID:             lockID,
		clientID:           clientID,
		districtID:         districtID,
		SleepTime:          time.Duration(5) * time.Second,
		checkBothLockTypes: checkBothLockTypes,
	}
}

func (m *RWMutex) tryToGetWriteLock() error {
	writeLockQuery := bson.M{
		"lockID": m.lockID,
		"$and":   []bson.M{emptyReaderQuery, emptyWriterQuery},
	}
	if m.checkBothLockTypes {
		writeLockQuery = bson.M{
			"$or": []bson.M{
				{"lockID": m.lockID},
				{"lockID": bson.M{"$regex": fmt.Sprintf("^%s", m.districtID)}},
			},
			"$and": []bson.M{emptyReaderQuery, emptyWriterQuery},
		}
	}

	res := m.collection.FindOneAndUpdate(context.TODO(), writeLockQuery, bson.M{
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
	unlockQuery := bson.M{
		"lockID": m.lockID,
		"writer": m.clientID,
	}
	if m.checkBothLockTypes {
		unlockQuery = bson.M{
			"$or": []bson.M{
				{"lockID": m.lockID},
				{"lockID": bson.M{"$regex": fmt.Sprintf("^%s", m.districtID)}},
			},
			"writer": m.clientID,
		}
	}

	res := m.collection.FindOneAndUpdate(context.TODO(), unlockQuery, bson.M{
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

	readLockQuery := bson.M{
		"lockID": m.lockID,
		"$and":   []bson.M{emptyWriterQuery},
	}
	if m.checkBothLockTypes {
		readLockQuery = bson.M{
			"$or": []bson.M{
				{"lockID": m.lockID},
				{"lockID": bson.M{"$regex": fmt.Sprintf("^%s", m.districtID)}},
			},
			"$and": []bson.M{emptyWriterQuery},
		}
	}

	res := m.collection.FindOneAndUpdate(context.TODO(), readLockQuery, bson.M{
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
	unlockQuery := bson.M{
		"lockID":  m.lockID,
		"readers": m.clientID,
	}
	if m.checkBothLockTypes {
		unlockQuery = bson.M{
			"$or": []bson.M{
				{"lockID": m.lockID},
				{"lockID": bson.M{"$regex": fmt.Sprintf("^%s", m.districtID)}},
			},
			"readers": m.clientID,
		}
	}

	res := m.collection.FindOneAndUpdate(context.TODO(), unlockQuery, bson.M{
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

	lockIDQuery := bson.M{"lockID": m.lockID}
	if m.checkBothLockTypes {
		lockIDQuery = bson.M{"$or": []bson.M{
			{"lockID": m.lockID},
			{"lockID": bson.M{"$regex": fmt.Sprintf("^%s", m.districtID)}},
		}}
	}

	var foundLock mongoLock
	err := m.collection.FindOne(context.TODO(), lockIDQuery).Decode(&foundLock)
	if err != nil {
		if err != mongo.ErrNoDocuments {
			return nil, err
		}

		_, err = m.collection.InsertOne(context.TODO(), mongoLock{
			LockID:  m.lockID,
			Writer:  "",
			Readers: []string{},
		})
		if err != nil {
			return nil, err
		}

		err = m.collection.FindOne(context.TODO(), lockIDQuery).Decode(&lock)
		if err != nil {
			return nil, err
		}
		return &lock, nil
	}
	if foundLock.LockID != m.lockID &&
		foundLock.LockID == m.districtID &&
		strings.HasPrefix(m.lockID, m.districtID) {
		_, err = m.collection.UpdateOne(
			context.TODO(),
			bson.M{"lockID": foundLock.LockID},
			bson.M{
				"$set": bson.M{
					"lockID": m.lockID,
				},
			},
		)
		if err != nil {
			return nil, err
		}
	}

	return &foundLock, nil
}
