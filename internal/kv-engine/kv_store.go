package kvengine

import (
	"errors"
	"math"
	"strconv"
	"sync"
)

var (
	ErrEmptyKey    = errors.New("Key can't be empty")
	ErrKeyNotFound = errors.New("Key not found")
	ErrNotANumber  = errors.New("Value is not a number")
	ErrOverflow    = errors.New("Value overflow")
)

const (
	SECONDS_IN_MONTH = uint64(30 * 24 * 60 * 60)
)

type Storage interface {
	Set(key string, value string, expiresAt uint64) error
	Get(key string) (string, uint64, error)
	// Conditional mutations use the time recorded in the command, so replay
	// does not change its outcome when a key has since expired.
	Add(key string, value string, expiresAt, now uint64) (bool, error)
	Replace(key string, value string, expiresAt, now uint64) (bool, error)
	Delete(key string, now uint64) (bool, error)
	Incr(key string, delta int64, now uint64) (int64, error)
	Decr(key string, delta int64, now uint64) (int64, error)
}

type StorageEntry struct {
	value     string
	expiresAt uint64
}

type MemoryStorage struct {
	values     map[string]StorageEntry
	lock       sync.RWMutex
	timeSource TimeSource
}

func (m *MemoryStorage) getAt(key string, now uint64) *StorageEntry {
	v, ok := m.values[key]
	if !ok || (v.expiresAt != 0 && now >= v.expiresAt) {
		return nil
	}
	return &v
}

func (m *MemoryStorage) _Set(key string, value string, expiresAt uint64) {
	m.values[key] = StorageEntry{value: value, expiresAt: expiresAt}
}

func (m *MemoryStorage) Set(key string, value string, expiresAt uint64) error {
	if key == "" {
		return ErrEmptyKey
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m._Set(key, value, expiresAt)
	return nil
}

func (m *MemoryStorage) Get(key string) (string, uint64, error) {
	if key == "" {
		return "", 0, ErrEmptyKey
	}

	m.lock.RLock()
	defer m.lock.RUnlock()
	value := m.getAt(key, m.timeSource.Now())
	if value == nil {
		return "", 0, ErrKeyNotFound
	}
	return value.value, value.expiresAt, nil
}

func (m *MemoryStorage) Add(key string, value string, expiresAt, now uint64) (bool, error) {
	if key == "" {
		return false, ErrEmptyKey
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	v := m.getAt(key, now)
	if v != nil {
		return false, nil
	}
	m._Set(key, value, expiresAt)
	return true, nil
}

func (m *MemoryStorage) Replace(key string, value string, expiresAt, now uint64) (bool, error) {
	if key == "" {
		return false, ErrEmptyKey
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	v := m.getAt(key, now)
	if v == nil {
		return false, nil
	}
	m._Set(key, value, expiresAt)
	return true, nil
}

func (m *MemoryStorage) Delete(key string, now uint64) (bool, error) {
	if key == "" {
		return false, ErrEmptyKey
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	v := m.getAt(key, now)
	if v == nil {
		return false, nil
	}
	delete(m.values, key)
	return true, nil
}

func (m *MemoryStorage) Incr(key string, delta int64, now uint64) (int64, error) {
	if key == "" {
		return 0, ErrEmptyKey
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	v := m.getAt(key, now)
	if v == nil {
		return 0, ErrKeyNotFound
	}
	intValue, err := strconv.ParseInt(v.value, 10, 64)
	if err != nil {
		return 0, ErrNotANumber
	}
	if (delta > 0 && intValue > math.MaxInt64-delta) || (delta < 0 && intValue < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	newValue := intValue + delta
	m._Set(key, strconv.FormatInt(newValue, 10), v.expiresAt)
	return newValue, nil
}

func (m *MemoryStorage) Decr(key string, delta int64, now uint64) (int64, error) {
	if key == "" {
		return 0, ErrEmptyKey
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	v := m.getAt(key, now)
	if v == nil {
		return 0, ErrKeyNotFound
	}
	intValue, err := strconv.ParseInt(v.value, 10, 64)
	if err != nil {
		return 0, ErrNotANumber
	}
	if (delta > 0 && intValue < math.MinInt64+delta) || (delta < 0 && intValue > math.MaxInt64+delta) {
		return 0, ErrOverflow
	}
	newValue := intValue - delta
	m._Set(key, strconv.FormatInt(newValue, 10), v.expiresAt)
	return newValue, nil
}

func CreateMemoryStorage() Storage {
	return &MemoryStorage{values: make(map[string]StorageEntry), timeSource: SystemTimeSource{}}
}
