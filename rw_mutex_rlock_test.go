package lock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TestRLockSuccess checks that RWMutex successfully acquires an existing lock not in contention
func TestRLockSuccess(t *testing.T) {
	c := setupRWMutexTest(t)
	// Insert the base lock
	c.InsertWithLockID(t, lockID)
	lock := NewRWMutex(c.collection, lockID, clientID, districtID, false)
	err := lock.RLock()

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	require.NoError(t, err)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Readers: []string{clientID},
	}, mLock)
}

// TestDistrictIDRLockSuccess checks that RWMutex successfully acquires an existing lock not in contention
func TestRDistrictIDLockSuccess(t *testing.T) {
	c := setupRWMutexTest(t)
	// Insert the base lock
	c.InsertWithLockID(t, districtID)
	lock := NewRWMutex(c.collection, lockID, clientID, districtID, true)
	err := lock.RLock()

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": districtID}, options.FindOne(), &mLock)
	require.NoError(t, err)
	assert.Equal(t, mongoLock{
		LockID:  districtID,
		Readers: []string{clientID},
	}, mLock)
}

// TestRLockNewSuccess checks that RWMutex successfully acquires a new lock not in contention
func TestRLockNewSuccess(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c.collection, lockID, clientID, districtID, false)
	err := lock.RLock()
	require.NoError(t, err)

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Readers: []string{clientID},
	}, mLock)
}

// TestRLockNewSuccess checks that RWMutex successfully acquires a new lock not in contention
func TestRDistrictIDLockNewSuccess(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c.collection, lockID, clientID, districtID, true)
	err := lock.RLock()
	require.NoError(t, err)

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": districtID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  districtID,
		Readers: []string{clientID},
	}, mLock)
}

// TestRLockWaitsForWriter checks that RWMutex waitss until an existing writer has released the lock
func TestRLockWaitsForWriter(t *testing.T) {
	c := setupRWMutexTest(t)

	firstLock := NewRWMutex(c.collection, lockID, "client_2", districtID, true)
	err := firstLock.Lock()
	require.NoError(t, err)
	// check the lock after 10 milliseconds
	go func() {
		time.Sleep(time.Duration(10) * time.Millisecond)
		var mLock mongoLock
		c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
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

	secondLock := NewRWMutex(c.collection, lockID, clientID, districtID, true)
	secondLock.SleepTime = time.Duration(5) * time.Millisecond
	err = secondLock.RLock()
	require.NoError(t, err)

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  "",
		Readers: []string{clientID},
	}, mLock)
}

// TestRDistrictIDLockWaitsForWriter checks that RWMutex waitss until an existing writer has released the lock
func TestRDistrictIDLockWaitsForWriter(t *testing.T) {
	c := setupRWMutexTest(t)

	firstLock := NewRWMutex(c.collection, districtID, "client_2", districtID, false)
	err := firstLock.Lock()
	require.NoError(t, err)
	// check the lock after 10 milliseconds
	go func() {
		time.Sleep(time.Duration(10) * time.Millisecond)
		var mLock mongoLock
		c.FindOne(t, bson.M{"lockID": districtID}, options.FindOne(), &mLock)
		assert.Equal(t, mongoLock{
			LockID:  districtID,
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

	secondLock := NewRWMutex(c.collection, lockID, clientID, districtID, true)
	secondLock.SleepTime = time.Duration(5) * time.Millisecond
	err = secondLock.RLock()
	require.NoError(t, err)

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": districtID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  districtID,
		Writer:  "",
		Readers: []string{clientID},
	}, mLock)
}

// // TestRLockMultipleReaders checks that RWMutex.RLock acquires the lock if another client has the
// // read lock
func TestRLockMultipleReaders(t *testing.T) {
	c := setupRWMutexTest(t)

	firstLock := NewRWMutex(c.collection, lockID, "client_2", districtID, false)
	err := firstLock.RLock()
	require.NoError(t, err)

	var firstMLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &firstMLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  "",
		Readers: []string{"client_2"},
	}, firstMLock)

	secondLock := NewRWMutex(c.collection, lockID, clientID, districtID, false)
	secondLock.SleepTime = time.Duration(5) * time.Millisecond
	err = secondLock.RLock()
	require.NoError(t, err)

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Readers: []string{"client_2", clientID},
	}, mLock)
}

// // TestRDistrictIDLockMultipleReaders checks that RWMutex.RLock acquires the lock if another client has the
// // read lock
func TestRDistrictIDLockMultipleReaders(t *testing.T) {
	c := setupRWMutexTest(t)

	firstLock := NewRWMutex(c.collection, districtID, "client_2", districtID, false)
	err := firstLock.RLock()
	require.NoError(t, err)

	var firstMLock mongoLock
	c.FindOne(t, bson.M{"lockID": districtID}, options.FindOne(), &firstMLock)
	assert.Equal(t, mongoLock{
		LockID:  districtID,
		Writer:  "",
		Readers: []string{"client_2"},
	}, firstMLock)

	secondLock := NewRWMutex(c.collection, lockID, clientID, districtID, true)
	secondLock.SleepTime = time.Duration(5) * time.Millisecond
	err = secondLock.RLock()
	require.NoError(t, err)

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": districtID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  districtID,
		Readers: []string{"client_2", clientID},
	}, mLock)
}

// TestRLockReenter checks that RWMutex.lock reenters a write lock with the same client id
func TestRLockReenter(t *testing.T) {
	c := setupRWMutexTest(t)
	// Insert the base lock
	c.InsertWithLockID(t, lockID)

	lock := NewRWMutex(c.collection, lockID, clientID, districtID, true)

	// get the read lock for the first time
	require.NoError(t, lock.RLock())
	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  "",
		Readers: []string{clientID},
	}, mLock)

	// check that grabbing it again yields same results
	require.NoError(t, lock.RLock())
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  "",
		Readers: []string{clientID},
	}, mLock)
}

// TestRDistrictIDLockReenter checks that RWMutex.lock reenters a write lock with the same client id
func TestRDistrictIDLockReenter(t *testing.T) {
	c := setupRWMutexTest(t)
	// Insert the base lock
	c.InsertWithLockID(t, districtID)

	lock := NewRWMutex(c.collection, lockID, clientID, districtID, true)

	// get the read lock for the first time
	require.NoError(t, lock.RLock())
	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": districtID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  districtID,
		Writer:  "",
		Readers: []string{clientID},
	}, mLock)

	// check that grabbing it again yields same results
	require.NoError(t, lock.RLock())
	c.FindOne(t, bson.M{"lockID": districtID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  districtID,
		Writer:  "",
		Readers: []string{clientID},
	}, mLock)
}
