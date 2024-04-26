package lock

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TestRUnlockSuccess - RWMutex.RUnlock releases the lock correctly
func TestRUnlockSuccess(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c.collection, lockID, clientID)
	require.NoError(t, lock.RLock())

	// add a second lock to check RWMutex only removes the read lock for its client
	secondLock := NewRWMutex(c.collection, lockID, "client_2")
	require.NoError(t, secondLock.RLock())

	err := lock.RUnlock()
	assert.NoError(t, err)
	var mLock mongoLock
	c.FindOne(t, bson.M{
		"lockID": lockID,
	}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Readers: []string{"client_2"},
		Writer:  "",
	}, mLock)
}

// TestRUnlockNotHeld - RWMutex.RUnlock returns an error if the client did not hold the lock
func TestRUnlockNotHeld(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c.collection, lockID, clientID)

	err := lock.RUnlock()
	assert.Error(t, err)
}
