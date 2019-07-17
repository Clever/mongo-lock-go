package lock

import (
	"errors"
	"math/rand"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	// ErrNotOwner is returnedd when an unlocking client did not hold the lock
	ErrNotOwner = errors.New("client does not hold lock")
)

// RWMutex implements a reader/writer lock. The interfaces matches that of sync.RWMutex
type RWMutex struct {
	collection *mgo.Collection
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

// NewRWMutex returns a new RWMutex
func NewRWMutex(collection *mgo.Collection, lockID, clientID string) *RWMutex {
	return &RWMutex{
		collection: collection,
		lockID:     lockID,
		clientID:   clientID,
		SleepTime:  time.Duration(5) * time.Second,
	}
}

func (m *RWMutex) tryToGetWriteLock() error {
	err := m.collection.Update(bson.M{
		"lockID":  m.lockID,
		"readers": []string{},
		"writer":  "",
	}, bson.M{
		"$set": bson.M{
			"writer": m.clientID,
		},
	})
	if err == nil {
		return nil // we got the lock!
	} else if err != mgo.ErrNotFound {
		// This only works if the mgo.Session object has Safe mode enabled.
		// Safe is the default but something for which we should maintain
		// external documentation
		return err
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
	err := m.collection.Update(bson.M{
		"lockID": m.lockID,
		"writer": m.clientID,
	}, bson.M{
		"$set": bson.M{
			"writer": "",
		},
	})
	if err == mgo.ErrNotFound {
		return ErrNotOwner
	}
	return err
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

	err := m.collection.Update(bson.M{
		"lockID": m.lockID,
		"writer": "",
	}, bson.M{
		"$addToSet": bson.M{
			"readers": m.clientID,
		},
	})
	if err == nil {
		return nil
	} else if err != mgo.ErrNotFound {
		// This only works if the mgo.Session object has Safe mode enabled. Safe is the default but
		// something for which we should maintain external documentation
		return err
	}

	return ErrNotOwner
}

// RUnlock releases the read lock
func (m *RWMutex) RUnlock() error {
	err := m.collection.Update(bson.M{
		"lockID":  m.lockID,
		"readers": m.clientID,
	}, bson.M{
		"$pull": bson.M{
			"readers": m.clientID,
		},
	})
	if err == mgo.ErrNotFound {
		return ErrNotOwner
	}
	return err
}

func (m *RWMutex) findOrCreateLock() (*mongoLock, error) {
	var lock mongoLock
	err := m.collection.
		Find(bson.M{"lockID": m.lockID}).
		One(&lock)
	if err == mgo.ErrNotFound {
		// If the lock doesn't exist, we should create it
		err := m.collection.Insert(&mongoLock{
			LockID: m.lockID,
		})
		if mgo.IsDup(err) {
			// Someone else has already inserted the lock
			err := m.collection.
				Find(bson.M{"lockID": m.lockID}).
				One(&lock)
			return &lock, err
		} else if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return &lock, nil
}
