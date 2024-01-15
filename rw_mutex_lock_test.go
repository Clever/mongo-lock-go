package lock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/mgocompat"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	clientID       = "clientID"
	clientID2      = "clientID-2"
	lockID         = "this be a lock id"
	testMongoURL   = "mongodb://127.0.0.1"
	testDatabase   = "test"
	testCollection = "testLocks"
)

type TestCollection struct {
	collection *mongo.Collection
}

func (tc *TestCollection) Insert(t *testing.T, doc interface{}) {
	_, err := tc.collection.InsertOne(context.TODO(), doc)
	require.NoError(t, err)
}

func (tc *TestCollection) FindOne(t *testing.T, filter interface{}, opts *options.FindOneOptions, result interface{}) {
	res := tc.collection.FindOne(context.TODO(), filter, opts)
	require.NoError(t, res.Err())
	require.NoError(t, res.Decode(result))
}

func setupRWMutexTest(t *testing.T) *TestCollection {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(testMongoURL).SetRegistry(mgocompat.Registry))
	require.NoError(t, err)
	require.NoError(t, client.Database(testDatabase).Drop(context.TODO()))
	require.NoError(t, client.Database(testDatabase).CreateCollection(context.TODO(), "test"))
	collection := client.Database(testDatabase).Collection(testDatabase)
	_, err = collection.Indexes().CreateOne(context.TODO(), mongo.IndexModel{
		Keys:    bson.M{"lockID": 1},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)
	return &TestCollection{
		collection: collection,
	}
}

// TestLockSuccess checks that we successfully acquire an existing lock not in contention
func TestLockSuccess(t *testing.T) {
	tc := setupRWMutexTest(t)
	// Insert the base lock
	tc.Insert(t, &mongoLock{
		LockID: lockID,
	})

	lock := NewRWMutex(tc.collection, lockID, clientID)
	require.NoError(t, lock.Lock())

	var mLock mongoLock
	tc.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}

// TestLockNewSuccess checks that we successfully acquire a new lock not in contention
func TestLockNewSuccess(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c.collection, lockID, clientID)
	err := lock.Lock()
	require.NoError(t, err)

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID: lockID,
		Writer: clientID,
		// Readers: []string{},
	}, mLock)
}

// TestLockWaitsForWriter checks that we wait until an existing writer has released the lock
func TestLockWaitsForWriter(t *testing.T) {
	c := setupRWMutexTest(t)

	firstLock := NewRWMutex(c.collection, lockID, "client_2")
	err := firstLock.Lock()
	require.NoError(t, err)
	// check the lock after 10 milliseconds
	go func() {
		time.Sleep(time.Duration(10) * time.Millisecond)
		var mLock mongoLock
		c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
		assert.Equal(t, mongoLock{
			LockID: lockID,
			Writer: "client_2",
			// Readers: []string{},
		}, mLock)
	}()
	// clear the lock after 100 milliseconds
	go func() {
		time.Sleep(time.Duration(100) * time.Millisecond)
		err := firstLock.Unlock()
		require.NoError(t, err)
	}()

	secondLock := NewRWMutex(c.collection, lockID, clientID)
	secondLock.SleepTime = time.Duration(5) * time.Millisecond
	err = secondLock.Lock()
	require.NoError(t, err)

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID: lockID,
		Writer: clientID,
		// Readers: []string{},
	}, mLock)
}

// TestLockWaitsForReader checks that we wait until an existing reader has released the lock
func TestLockWaitsForReader(t *testing.T) {
	c := setupRWMutexTest(t)

	firstLock := NewRWMutex(c.collection, lockID, "client_2")
	err := firstLock.RLock()
	require.NoError(t, err)
	// check the lock after 10 milliseconds
	go func() {
		time.Sleep(time.Duration(10) * time.Millisecond)
		var mLock mongoLock
		c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
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

	secondLock := NewRWMutex(c.collection, lockID, clientID)
	secondLock.SleepTime = time.Duration(5) * time.Millisecond
	err = secondLock.Lock()
	require.NoError(t, err)

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
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
	c.Insert(t, &mongoLock{
		LockID: lockID,
		Writer: clientID,
	})

	lock := NewRWMutex(c.collection, lockID, clientID)
	err := lock.Lock()
	require.NoError(t, err)

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
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
	c.Insert(t, &mongoLock{
		LockID: lockID,
		Writer: clientID,
	})

	// client2 fails
	lock := NewRWMutex(c.collection, lockID, clientID2)
	require.Equal(t, lock.TryLock(), ErrNotOwner)

	// client1 succeeds
	lock = NewRWMutex(c.collection, lockID, clientID)
	require.NoError(t, lock.TryLock())

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}
