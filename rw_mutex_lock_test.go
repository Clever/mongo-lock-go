package lock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	clientID       = "clientID"
	clientID2      = "clientID-2"
	lockID         = "districtID:this be a lock id"
	districtID     = "districtID"
	testMongoURL   = "mongodb://127.0.0.1"
	testDatabase   = "test"
	testCollection = "testLocks"
)

type TestCollection struct {
	collection *mongo.Collection
}

func (tc *TestCollection) InsertWithLockID(t *testing.T, lockID string) {
	mutex := NewRWMutex(tc.collection, lockID, "doesntmatter", districtID, false)
	_, err := mutex.findOrCreateLock()
	require.NoError(t, err)
}

func (tc *TestCollection) FindOne(t *testing.T, filter interface{}, opts *options.FindOneOptions, result interface{}) {
	res := tc.collection.FindOne(context.TODO(), filter, opts)
	require.NoError(t, res.Err())
	require.NoError(t, res.Decode(result))
}

func setupRWMutexTest(t *testing.T) *TestCollection {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(testMongoURL))
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
	tc.InsertWithLockID(t, lockID)
	lock := NewRWMutex(tc.collection, lockID, clientID, districtID, false)
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
	lock := NewRWMutex(c.collection, lockID, clientID, districtID, false)
	err := lock.Lock()
	require.NoError(t, err)

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}

// TestLockNewChangesIncorrectLockIDSuccess checks that we successfully acquire a new lock and change its ID
// to the correct one
func TestLockNewChangesIncorrectLockIDSuccess(t *testing.T) {
	tc := setupRWMutexTest(t)
	// Insert the base lock
	tc.InsertWithLockID(t, districtID)
	lock := NewRWMutex(tc.collection, lockID, clientID, districtID, true)
	require.NoError(t, lock.Lock())

	var mLock mongoLock
	tc.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
	totalLocks, err := tc.collection.CountDocuments(context.TODO(), bson.M{}, options.Count())
	require.NoError(t, err)
	assert.Equal(t, int64(1), totalLocks)
}

// TestChangedLockIDFindable checks that we successfully acquire a new lock and change its ID
// then can still find that lock when trying to find it with the old ID
func TestChangedLockIDFindable(t *testing.T) {
	tc := setupRWMutexTest(t)
	// Insert the base lock
	tc.InsertWithLockID(t, districtID)
	lock := NewRWMutex(tc.collection, lockID, clientID, districtID, true)
	require.NoError(t, lock.Lock())

	var mLock mongoLock
	tc.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
	totalLocks, err := tc.collection.CountDocuments(context.TODO(), bson.M{}, options.Count())
	require.NoError(t, err)
	assert.Equal(t, int64(1), totalLocks)
	require.NoError(t, lock.Unlock())

	lock2 := NewRWMutex(tc.collection, districtID, clientID, districtID, true)
	require.NoError(t, lock2.Lock())
	var mLock2 mongoLock
	tc.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock2)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock2)

	totalLocks, err = tc.collection.CountDocuments(context.TODO(), bson.M{}, options.Count())
	require.NoError(t, err)
	assert.Equal(t, int64(1), totalLocks)
}

// TestDistrictTypeLockNewSuccess checks that we successfully acquire a new lock not in contention
func TestDistrictIDLockNewSuccess(t *testing.T) {
	c := setupRWMutexTest(t)
	lock := NewRWMutex(c.collection, districtID, clientID, districtID, true)
	err := lock.Lock()
	require.NoError(t, err)

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": districtID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  districtID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}

// TestLockWaitsForWriter checks that we wait until an existing writer has released the lock
func TestLockWaitsForWriter(t *testing.T) {
	c := setupRWMutexTest(t)

	firstLock := NewRWMutex(c.collection, lockID, "client_2", districtID, false)
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

	secondLock := NewRWMutex(c.collection, lockID, clientID, districtID, false)
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

// TestDistrictIDLockWaitsForWriter checks that we wait until an existing writer has released the lock
func TestDistrictIDLockWaitsForWriter(t *testing.T) {
	c := setupRWMutexTest(t)

	firstLock := NewRWMutex(c.collection, districtID, "client_2", districtID, true)
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

	// fetch the lock with districtID since lockID is different here than what is in mongo
	secondLock := NewRWMutex(c.collection, lockID, clientID, districtID, true)
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

// TestLockWaitsForReader checks that we wait until an existing reader has released the lock
func TestLockWaitsForReader(t *testing.T) {
	c := setupRWMutexTest(t)

	firstLock := NewRWMutex(c.collection, lockID, "client_2", districtID, false)
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

	secondLock := NewRWMutex(c.collection, lockID, clientID, districtID, false)
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

// TestDistrictIDLockWaitsForReader checks that we wait until an existing reader has released the lock
func TestDistrictIDLockWaitsForReader(t *testing.T) {
	c := setupRWMutexTest(t)

	firstLock := NewRWMutex(c.collection, districtID, "client_2", districtID, true)
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

	secondLock := NewRWMutex(c.collection, lockID, clientID, districtID, true)
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
	c.InsertWithLockID(t, lockID)

	lock := NewRWMutex(c.collection, lockID, clientID, districtID, false)

	// enter first time
	require.NoError(t, lock.Lock())
	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)

	// re-enter
	require.NoError(t, lock.Lock())
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}

// TestDistrictIDLockReenter checks that RWMutex.lock reenters a write lock with the same client id
func TestDistrictIDLockReenter(t *testing.T) {
	c := setupRWMutexTest(t)
	// Insert the base lock
	c.InsertWithLockID(t, districtID)

	lock := NewRWMutex(c.collection, lockID, clientID, districtID, true)

	// enter first time
	require.NoError(t, lock.Lock())
	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)

	// re-enter
	require.NoError(t, lock.Lock())
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
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
	c.InsertWithLockID(t, lockID)

	// client 1 grabs the lock first
	lock := NewRWMutex(c.collection, lockID, clientID, districtID, false)
	require.NoError(t, lock.TryLock())

	// client2 fails
	lock = NewRWMutex(c.collection, lockID, clientID2, districtID, false)
	require.Equal(t, lock.TryLock(), ErrNotOwner)

	// client1 succeeds
	lock = NewRWMutex(c.collection, lockID, clientID, districtID, false)
	require.NoError(t, lock.TryLock())

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}

// TestTryDistrictIDLock checks that RWMutex.lock reenters a write lock with the same client id
func TestTryDistrictIDLock(t *testing.T) {
	c := setupRWMutexTest(t)
	// Insert the base lock
	c.InsertWithLockID(t, districtID)

	// client 1 grabs the lock first
	lock := NewRWMutex(c.collection, lockID, clientID, districtID, true)
	require.NoError(t, lock.TryLock())

	// client2 fails
	lock = NewRWMutex(c.collection, lockID, clientID2, districtID, true)
	require.Equal(t, lock.TryLock(), ErrNotOwner)

	// client1 succeeds
	lock = NewRWMutex(c.collection, lockID, clientID, districtID, true)
	require.NoError(t, lock.TryLock())

	var mLock mongoLock
	c.FindOne(t, bson.M{"lockID": lockID}, options.FindOne(), &mLock)
	assert.Equal(t, mongoLock{
		LockID:  lockID,
		Writer:  clientID,
		Readers: []string{},
	}, mLock)
}
