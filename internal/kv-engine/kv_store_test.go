package kvengine

import (
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKVStorage(t *testing.T) {
	time := &TestTimeSource{}
	st := MemoryStorage{make(map[string]StorageEntry), &sync.RWMutex{}, time}

	// empty key
	err := st.Set("", "asdf", 0)
	assert.ErrorIs(t, err, ErrEmptyKey)
	res, err := st.Get("")
	assert.Empty(t, res)
	assert.ErrorIs(t, err, ErrEmptyKey)

	// empty value
	err = st.Set("empty_value", "", 0)
	assert.Nil(t, err)
	res, err = st.Get("empty_value")
	assert.Equal(t, "", res)
	assert.Nil(t, err)

	// normal operations
	err = st.Set("key1", "value1", 0)
	assert.Nil(t, err)
	res, err = st.Get("key1")
	assert.Equal(t, "value1", res)
	assert.Nil(t, err)
	err = st.Set("key1", "value2", 0)
	assert.Nil(t, err)
	res, err = st.Get("key1")
	assert.Equal(t, "value2", res)
	assert.Nil(t, err)
	err = st.Set("key2", "value3", 0)
	assert.Nil(t, err)
	res, err = st.Get("key1")
	assert.Equal(t, "value2", res)
	assert.Nil(t, err)
	res, err = st.Get("key3")
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// ttl behavior: ttl = 0
	err = st.Set("not_expiring", "value4", 0)
	assert.Nil(t, err)
	res, err = st.Get("not_expiring")
	assert.Equal(t, "value4", res)
	assert.Nil(t, err)
	time.AdvanceTime(10_000_000)
	res, err = st.Get("not_expiring")
	assert.Equal(t, "value4", res)
	assert.Nil(t, err)

	// ttl behavior: 0 < ttl <= 2_502_000
	err = st.Set("expires_after_1_minute", "value5", 60)
	assert.Nil(t, err)
	res, err = st.Get("expires_after_1_minute")
	assert.Equal(t, "value5", res)
	assert.Nil(t, err)
	time.AdvanceTime(59)
	res, err = st.Get("expires_after_1_minute")
	assert.Equal(t, "value5", res)
	assert.Nil(t, err)
	time.AdvanceTime(1)
	res, err = st.Get("expires_after_1_minute")
	assert.ErrorIs(t, err, ErrKeyNotFound)

	err = st.Set("expires_after_30_days", "value6", SECONDS_IN_MONTH)
	assert.Nil(t, err)
	time.AdvanceTime(SECONDS_IN_MONTH - 1)
	res, err = st.Get("expires_after_30_days")
	assert.Equal(t, "value6", res)
	assert.Nil(t, err)
	time.AdvanceTime(1)
	res, err = st.Get("expires_after_30_days")
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// ttl behavior: ttl > 2_520_000
	err = st.Set("expires_absolute", "value7", SECONDS_IN_MONTH+1)
	assert.Nil(t, err)
	time.SetTime(SECONDS_IN_MONTH)
	res, err = st.Get("expires_absolute")
	assert.Equal(t, "value7", res)
	assert.Nil(t, err)
	time.AdvanceTime(1)
	res, err = st.Get("expires_absolute")
	assert.ErrorIs(t, err, ErrKeyNotFound)

	time.SetTime(3_000_000)
	err = st.Set("expires_absolute_past", "value8", 2_700_000)
	assert.Nil(t, err)
	res, err = st.Get("expires_absolute_past")
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// ttl behavior - overwriting ttl
	err = st.Set("changing_ttl", "value9", 0)
	assert.Nil(t, err)
	res, err = st.Get("changing_ttl")
	assert.Equal(t, "value9", res)
	assert.Nil(t, err)
	time.AdvanceTime(10_000_000)
	res, err = st.Get("changing_ttl")
	assert.Equal(t, "value9", res)
	assert.Nil(t, err)

	err = st.Set("changing_ttl", "value9", 60)
	assert.Nil(t, err)
	res, err = st.Get("changing_ttl")
	assert.Equal(t, "value9", res)
	assert.Nil(t, err)
	time.AdvanceTime(59)
	res, err = st.Get("changing_ttl")
	assert.Equal(t, "value9", res)
	assert.Nil(t, err)
	time.AdvanceTime(1)
	res, err = st.Get("changing_ttl")
	assert.ErrorIs(t, err, ErrKeyNotFound)

	err = st.Set("changing_ttl", "value9", 10_000_000)
	assert.Nil(t, err)
	time.SetTime(9_999_999)
	res, err = st.Get("changing_ttl")
	assert.Equal(t, "value9", res)
	assert.Nil(t, err)
	time.AdvanceTime(1)
	res, err = st.Get("changing_ttl")
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
