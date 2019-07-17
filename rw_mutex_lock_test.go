package lock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	clientID     = "clientID"
	clientID2    = "clientID-2"
	lockID       = "lockID"
	testMongoURL = "mongodb://127.0.0.1:27017/test"
)

func setupRWMutexTest(t *testing.T) *mgo.Collection {
	session, err := mgo.Dial(testMongoURL)
	require.NoError(t, err)
	collection := session.DB("").C("testlocks")
	collection.EnsureIndex(mgo.Index{Key: []string{"lockID"}, Unique: true})
	_, err = collection.RemoveAll(nil)
	require.NoError(t, err)
	return collection
}

// TestLockSuccess checks that we successfully acquire an existing lock not in contention
func TestLockSuccess(t *testing.T) {
	c := setupRWMutexTest(t)
	// Insert the base lock
	err := c.Insert(&mongoLock{
		LockID: lockID,
	})
	require.NoError(t, err)

	lock := NewRWMutex(c, lockID, clientID)
	err = lock.Lock()
	require.NoError(t, err)

	var mLock mongoLock
	err = c.Find(bson.M{"lockID": lockID}).One(&mLock)
	require.NoError(t, err)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}

// TestLockNewSuccess checks that we successfully acquire a new lock not in contention
func TestLockNewSuccess(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c, lockID, clientID)
	err := lock.Lock()
	require.NoError(t, err)

	var mLock mongoLock
	err = c.Find(bson.M{"lockID": lockID}).One(&mLock)
	require.NoError(t, err)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}

// TestLockWaitsForWriter checks that we wait until an existing writer has released the lock
func TestLockWaitsForWriter(t *testing.T) {
	c := setupRWMutexTest(t)

	firstLock := NewRWMutex(c, lockID, "client_2")
	err := firstLock.Lock()
	require.NoError(t, err)
	// check the lock after 10 milliseconds
	go func() {
		time.Sleep(time.Duration(10) * time.Millisecond)
		var mLock mongoLock
		err := c.Find(bson.M{"lockID": lockID}).One(&mLock)
		require.NoError(t, err)
		assert.Equal(t, mongoLock{
			LockID:  lockID,
			Writer:  "client_2",
			Readers: []string{},
		}, mLock)
	}()
	// clear the lock after 100 milliseconds
	go func() {
		time.Sleep(time.Duration(100) * time.Millisecond)
		err := firstLock.Unlock()
		require.NoError(t, err)
	}()

	secondLock := NewRWMutex(c, lockID, clientID)
	secondLock.SleepTime = time.Duration(5) * time.Millisecond
	err = secondLock.Lock()
	require.NoError(t, err)

	var mLock mongoLock
	err = c.Find(bson.M{"lockID": lockID}).One(&mLock)
	require.NoError(t, err)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}

// TestLockWaitsForReader checks that we wait until an existing reader has released the lock
func TestLockWaitsForReader(t *testing.T) {
	c := setupRWMutexTest(t)

	firstLock := NewRWMutex(c, lockID, "client_2")
	err := firstLock.RLock()
	require.NoError(t, err)
	// check the lock after 10 milliseconds
	go func() {
		time.Sleep(time.Duration(10) * time.Millisecond)
		var mLock mongoLock
		err := c.Find(bson.M{"lockID": lockID}).One(&mLock)
		require.NoError(t, err)
		assert.Equal(t, mongoLock{
			LockID:  lockID,
			Writer:  "",
			Readers: []string{"client_2"},
		}, mLock)
	}()
	// clear the lock after 100 milliseconds
	go func() {
		time.Sleep(time.Duration(100) * time.Millisecond)
		err := firstLock.RUnlock()
		require.NoError(t, err)
	}()

	secondLock := NewRWMutex(c, lockID, clientID)
	secondLock.SleepTime = time.Duration(5) * time.Millisecond
	err = secondLock.Lock()
	require.NoError(t, err)

	var mLock mongoLock
	err = c.Find(bson.M{"lockID": lockID}).One(&mLock)
	require.NoError(t, err)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}

// TestLockReenter checks that RWMutex.lock reenters a write lock with the same client id
func TestLockReenter(t *testing.T) {
	c := setupRWMutexTest(t)
	// Insert the base lock
	err := c.Insert(&mongoLock{
		LockID: lockID,
		Writer: clientID,
	})
	require.NoError(t, err)

	lock := NewRWMutex(c, lockID, clientID)
	err = lock.Lock()
	require.NoError(t, err)

	var mLock mongoLock
	err = c.Find(bson.M{"lockID": lockID}).One(&mLock)
	require.NoError(t, err)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}

// TestLockReenter checks that RWMutex.lock reenters a write lock with the same client id
func TestTryLock(t *testing.T) {
	c := setupRWMutexTest(t)
	// Insert the base lock
	err := c.Insert(&mongoLock{
		LockID: lockID,
		Writer: clientID,
	})
	require.NoError(t, err)

	// client2 fails
	lock := NewRWMutex(c, lockID, clientID2)
	require.Equal(t, lock.TryLock(), ErrNotOwner)

	// client1 succeeds
	lock = NewRWMutex(c, lockID, clientID)
	require.NoError(t, lock.TryLock())

	var mLock mongoLock
	err = c.Find(bson.M{"lockID": lockID}).One(&mLock)
	require.NoError(t, err)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}
