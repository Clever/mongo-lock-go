package lock

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TestUnlockSuccess - RWMutex.Unlock releases the lock correctly
func TestUnlockSuccess(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c.collection, lockID, clientID, districtID, true)
	require.NoError(t, lock.Lock())

	err := lock.Unlock()
	assert.NoError(t, err)
	var mLock mongoLock
	c.FindOne(t, bson.M{
		"lockID": lockID,
	}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  "",
		Readers: []string{},
	}, mLock)
}

// TestUnlockNotHeld - RWMutex.Unlock returns an error if the client did not hold the lock
func TestUnlockNotHeld(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c.collection, lockID, clientID, districtID, true)

	err := lock.Unlock()
	assert.Error(t, err)
}
