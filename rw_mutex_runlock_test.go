package lock

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/mgo.v2/bson"
)

// TestUnlockSuccess - RWMutex.Unlock releases the lock correctly
func TestUnlockSuccess(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c, lockID, clientID)
	require.NoError(t, lock.Lock())

	err := lock.Unlock()
	assert.NoError(t, err)
	var mLock mongoLock
	require.NoError(t, c.Find(bson.M{
		"lockID": lockID,
	}).One(&mLock))
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Readers: []string{},
		Writer:  "",
	}, mLock)
}

// TestUnlockNotHeld - RWMutex.Unlock returns an error if the client did not hold the lock
func TestUnlockNotHeld(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c, lockID, clientID)

	err := lock.Unlock()
	assert.Error(t, err)
}
