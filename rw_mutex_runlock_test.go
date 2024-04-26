package lock

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// TestUnlockSuccess - RWMutex.Unlock releases the lock correctly
func TestUnlockSuccess(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c.collection, lockID, clientID)
	require.NoError(t, lock.Lock())

	findRes := c.collection.FindOne(
		context.TODO(),
		bson.M{"lockID": lockID},
	)
	foundLock := mongoLock{}
	err := findRes.Decode(&foundLock)
	require.NoError(t, err)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, foundLock)

	require.NoError(t, lock.Unlock())
	findRes = c.collection.FindOne(
		context.TODO(),
		bson.M{"lockID": lockID},
	)

	assert.Equal(t, mongo.ErrNoDocuments, findRes.Err())

}

// TestUnlockNotHeld - RWMutex.Unlock returns an error if the client did not hold the lock
func TestUnlockNotHeld(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c.collection, lockID, clientID)

	err := lock.Unlock()
	assert.Error(t, err)
}
