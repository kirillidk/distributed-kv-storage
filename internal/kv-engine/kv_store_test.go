package kvengine

import (
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKVStorage(t *testing.T) {
	time := &TestTimeSource{t: 1_000}
	st := MemoryStorage{values: make(map[string]StorageEntry), timeSource: time}

	err := st.Set("", "asdf", 0)
	assert.ErrorIs(t, err, ErrEmptyKey)
	value, expiresAt, err := st.Get("")
	assert.Empty(t, value)
	assert.Zero(t, expiresAt)
	assert.ErrorIs(t, err, ErrEmptyKey)

	err = st.Set("empty-value", "", 0)
	assert.NoError(t, err)
	value, expiresAt, err = st.Get("empty-value")
	assert.NoError(t, err)
	assert.Empty(t, value)
	assert.Zero(t, expiresAt)

	err = st.Set("key", "value", 0)
	assert.NoError(t, err)
	value, expiresAt, err = st.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "value", value)
	assert.Zero(t, expiresAt)

	err = st.Set("key", "updated", 0)
	assert.NoError(t, err)
	value, _, err = st.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "updated", value)

	value, expiresAt, err = st.Get("missing")
	assert.Empty(t, value)
	assert.Zero(t, expiresAt)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	time.AdvanceTime(10_000_000)
	value, expiresAt, err = st.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "updated", value)
	assert.Zero(t, expiresAt)

	time.SetTime(2_000)
	err = st.Set("expiring", "value", 2_060)
	assert.NoError(t, err)
	value, expiresAt, err = st.Get("expiring")
	assert.NoError(t, err)
	assert.Equal(t, "value", value)
	assert.Equal(t, uint64(2_060), expiresAt)

	time.AdvanceTime(59)
	value, _, err = st.Get("expiring")
	assert.NoError(t, err)
	assert.Equal(t, "value", value)

	time.AdvanceTime(1)
	value, expiresAt, err = st.Get("expiring")
	assert.Empty(t, value)
	assert.Zero(t, expiresAt)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	err = st.Set("already-expired", "value", 2_000)
	assert.NoError(t, err)
	value, expiresAt, err = st.Get("already-expired")
	assert.Empty(t, value)
	assert.Zero(t, expiresAt)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestKVStorageAdd(t *testing.T) {
	timeSource := &TestTimeSource{t: 1_000}
	st := &MemoryStorage{values: make(map[string]StorageEntry), timeSource: timeSource}

	_, err := st.Add("", "value", 0)
	assert.ErrorIs(t, err, ErrEmptyKey)

	success, err := st.Add("missing", "created", 0)
	assert.NoError(t, err)
	assert.True(t, success)

	value, expiresAt, err := st.Get("missing")
	assert.NoError(t, err)
	assert.Equal(t, "created", value)
	assert.Zero(t, expiresAt)

	// Adding an existing key must not overwrite it or change its TTL.
	success, err = st.Add("missing", "overwritten", 2_060)
	assert.NoError(t, err)
	assert.False(t, success)

	value, expiresAt, err = st.Get("missing")
	assert.NoError(t, err)
	assert.Equal(t, "created", value)
	assert.Zero(t, expiresAt)

	// An expired entry is treated as missing, so Add succeeds.
	err = st.Set("expired", "old", 1_000)
	assert.NoError(t, err)
	success, err = st.Add("expired", "new", 0)
	assert.NoError(t, err)
	assert.True(t, success)

	value, _, err = st.Get("expired")
	assert.NoError(t, err)
	assert.Equal(t, "new", value)

	success, err = st.Add("with-ttl", "value", 1_060)
	assert.NoError(t, err)
	assert.True(t, success)

	_, expiresAt, err = st.Get("with-ttl")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1_060), expiresAt)
}

func TestKVStorageReplace(t *testing.T) {
	timeSource := &TestTimeSource{t: 1_000}
	st := &MemoryStorage{values: make(map[string]StorageEntry), timeSource: timeSource}

	_, err := st.Replace("", "value", 0)
	assert.ErrorIs(t, err, ErrEmptyKey)

	success, err := st.Replace("missing", "value", 0)
	assert.NoError(t, err)
	assert.False(t, success)

	_, _, err = st.Get("missing")
	assert.ErrorIs(t, err, ErrKeyNotFound)

	err = st.Set("key", "original", 1_060)
	assert.NoError(t, err)

	success, err = st.Replace("key", "replaced", 0)
	assert.NoError(t, err)
	assert.True(t, success)

	value, expiresAt, err := st.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "replaced", value)
	assert.Zero(t, expiresAt)

	// Replace can also set a new expiration.
	success, err = st.Replace("key", "replaced-again", 2_060)
	assert.NoError(t, err)
	assert.True(t, success)

	value, expiresAt, err = st.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "replaced-again", value)
	assert.Equal(t, uint64(2_060), expiresAt)

	// Expired entries count as missing and must not be replaced.
	err = st.Set("expired", "old", 1_000)
	assert.NoError(t, err)
	success, err = st.Replace("expired", "new", 0)
	assert.NoError(t, err)
	assert.False(t, success)

	_, _, err = st.Get("expired")
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestKVStorageDelete(t *testing.T) {
	timeSource := &TestTimeSource{t: 1_000}
	st := &MemoryStorage{values: make(map[string]StorageEntry), timeSource: timeSource}

	_, err := st.Delete("")
	assert.ErrorIs(t, err, ErrEmptyKey)

	success, err := st.Delete("missing")
	assert.NoError(t, err)
	assert.False(t, success)

	err = st.Set("key", "value", 0)
	assert.NoError(t, err)

	success, err = st.Delete("key")
	assert.NoError(t, err)
	assert.True(t, success)

	_, _, err = st.Get("key")
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// Deleting again reports that nothing was removed.
	success, err = st.Delete("key")
	assert.NoError(t, err)
	assert.False(t, success)

	// Expired entries count as missing.
	err = st.Set("expired", "value", 1_000)
	assert.NoError(t, err)
	success, err = st.Delete("expired")
	assert.NoError(t, err)
	assert.False(t, success)
}

func TestKVStorageIncr(t *testing.T) {
	timeSource := &TestTimeSource{t: 1_000}
	st := &MemoryStorage{values: make(map[string]StorageEntry), timeSource: timeSource}

	_, err := st.Incr("", 1)
	assert.ErrorIs(t, err, ErrEmptyKey)

	_, err = st.Incr("missing", 1)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	err = st.Set("text", "not-a-number", 0)
	assert.NoError(t, err)
	_, err = st.Incr("text", 1)
	assert.ErrorIs(t, err, ErrNotANumber)

	err = st.Set("key", "10", 1_060)
	assert.NoError(t, err)

	value, err := st.Incr("key", 5)
	assert.NoError(t, err)
	assert.Equal(t, uint64(15), value)

	stored, expiresAt, err := st.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "15", stored)
	assert.Equal(t, uint64(1_060), expiresAt, "Incr must preserve the expiration")

	// Incrementing by zero is a no-op.
	value, err = st.Incr("key", 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(15), value)

	err = st.Set("max", "18446744073709551615", 0)
	assert.NoError(t, err)
	_, err = st.Incr("max", 1)
	assert.ErrorIs(t, err, ErrOverflow)

	stored, _, err = st.Get("max")
	assert.NoError(t, err)
	assert.Equal(t, "18446744073709551615", stored, "an overflowing Incr must not change the value")
}

func TestKVStorageDecr(t *testing.T) {
	timeSource := &TestTimeSource{t: 1_000}
	st := &MemoryStorage{values: make(map[string]StorageEntry), timeSource: timeSource}

	_, err := st.Decr("", 1)
	assert.ErrorIs(t, err, ErrEmptyKey)

	_, err = st.Decr("missing", 1)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	err = st.Set("text", "not-a-number", 0)
	assert.NoError(t, err)
	_, err = st.Decr("text", 1)
	assert.ErrorIs(t, err, ErrNotANumber)

	err = st.Set("key", "10", 1_060)
	assert.NoError(t, err)

	value, err := st.Decr("key", 4)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), value)

	stored, expiresAt, err := st.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "6", stored)
	assert.Equal(t, uint64(1_060), expiresAt, "Decr must preserve the expiration")

	// Decrementing by zero is a no-op.
	value, err = st.Decr("key", 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), value)

	// Decrementing below zero must fail and leave the value untouched.
	_, err = st.Decr("key", 7)
	assert.ErrorIs(t, err, ErrOverflow)

	stored, _, err = st.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "6", stored, "an underflowing Decr must not change the value")
}

func TestRaces(t *testing.T) {
	st := CreateMemoryStorage()
	wg := new(sync.WaitGroup)
	wg.Add(10)

	for range 10 {
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			for range 10000 {
				key := string(rune(rand.IntN(26) + 97))
				value := string(rune(rand.IntN(26) + 97))
				if rand.IntN(2) == 0 {
					st.Set(key, value, 0)
				} else {
					st.Get(key)
				}
			}
		}(wg)
	}

	wg.Wait()
}
