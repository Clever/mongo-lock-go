package lock

import (
	"fmt"
	"math/rand"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	pingLockSeconds   = 5
	staleLockPings    = 5
	staleLockDuration = time.Duration(-pingLockSeconds*staleLockPings) * time.Second
)

// RWMutex implements a reader/writer lock. The interfaces matches that of sync.RWMutex
type RWMutex struct {
	collection *mgo.Collection
	lockID     string
	clientID   string
	SleepTime  time.Duration
	lastPing   time.Time
}

// mongoLock is the resource stored in mongo to represent the lock
type mongoLock struct {
	LockID      string    `bson:"lockID"`
	Writer      string    `bson:"writer"`
	Readers     []string  `bson:"readers"`
	LastUpdated time.Time `bson:"lastUpdated"`
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
		err := m.collection.Update(bson.M{
			"lockID":  m.lockID,
			"readers": []string{},
			"writer":  "",
		}, bson.M{
			"$set": bson.M{
				"writer":      m.clientID,
				"lastUpdated": time.Now(),
			},
		})
		if err == nil {
			return nil
		} else if err != mgo.ErrNotFound {
			// This only works if the mgo.Session object has Safe mode enabled. Safe is the default
			// but something for which we should maintain external documentation
			return err
		}
		jitter := time.Duration(rand.Int63n(1000)) * time.Millisecond
		time.Sleep(m.SleepTime + jitter)
	}
}

// Unlock releases the write lock
func (m *RWMutex) Unlock() error {
	err := m.collection.Update(bson.M{
		"lockID": m.lockID,
		"writer": m.clientID,
	}, bson.M{
		"$set": bson.M{
			"writer":      "",
			"lastUpdated": time.Now(),
		},
	})
	if err == mgo.ErrNotFound {
		return fmt.Errorf("lock %s not currently held by client: %s", m.lockID, m.clientID)
	}
	return err
}

// RLock acquires the write lock
func (m *RWMutex) RLock() error {
	lock, err := m.findOrCreateLock()
	if err != nil {
		return err
	}

	for _, reader := range lock.Readers {
		// if this clientID already has a read lock, re-enter the lock and return
		if reader == m.clientID {
			return nil
		}
	}

	for {
		err := m.collection.Update(bson.M{
			"lockID": m.lockID,
			"writer": "",
		}, bson.M{
			"$addToSet": bson.M{
				"readers": m.clientID,
			},
			"$set": bson.M{
				"lastUpdated": time.Now(),
			},
		})
		if err == nil {
			return nil
		} else if err != mgo.ErrNotFound {
			// This only works if the mgo.Session object has Safe mode enabled. Safe is the default but
			// something for which we should maintain external documentation
			return err
		}
		jitter := time.Duration(rand.Int63n(1000)) * time.Millisecond
		time.Sleep(m.SleepTime + jitter)
	}
}

// RUnlock releases the write lock
func (m *RWMutex) RUnlock() error {
	err := m.collection.Update(bson.M{
		"lockID":  m.lockID,
		"readers": m.clientID,
	}, bson.M{
		"$pull": bson.M{
			"readers": m.clientID,
		},
		"$set": bson.M{
			"lastUpdated": time.Now(),
		},
	})
	if err == mgo.ErrNotFound {
		return fmt.Errorf("lock %s not currently held by client: %s", m.lockID, m.clientID)
	}
	return err
}

// Ping updates the lastUpdated field of the lock
func (m *RWMutex) Ping() error {
	pingTime = time.Now()
	err := m.collection.Update(bson.M{
		"lockID": m.lockID,
		"lastUpdated": bson.M{
			"$gte": m.staleLockThresholdTime(),
		},
	}, bson.M{
		"$set": bson.M{
			"lastUpdated": pingTime,
		},
	})
	if err == mgo.ErrNotFound {
		return fmt.Errorf("cannot ping lock %s, does not exist!", m.lockID)
	} else if err == nil {
		m.lastPing = pingTime
	}
	return err
}

func (m *RWMutex) staleLockThresholdTime() *time.Time {
	return time.Now().Add(staleLockDuration)
}

func (m *RWMutex) findOrCreateLock() (*mongoLock, error) {
	var lock mongoLock
	err := m.collection.Find(bson.M{
		"lockID": m.lockID,
		"lastUpdated": bson.M{
			"$gte": m.staleLockThresholdTime(),
		},
	}).One(&lock)
	if err == mgo.ErrNotFound {
		// If the lock doesn't exist, we should create it with an upsert
		lock = &mongoLock{
			LockID:      m.lockID,
			LastUpdated: time.Now(),
		}
		_, err := m.collection.Upsert(
			bson.M{
				"lockID": m.lockID,
			},
			lock,
		)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return &lock, nil
}
