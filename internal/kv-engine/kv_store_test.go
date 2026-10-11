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
