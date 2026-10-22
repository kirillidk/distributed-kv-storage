package kvengine

import (
	"errors"
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
	Add(key string, value string, expiresAt uint64) (bool, error)
	Replace(key string, value string, expiresAt uint64) (bool, error)
	Delete(key string) (bool, error)
	Incr(key string, delta uint64) (uint64, error)
	Decr(key string, delta uint64) (uint64, error)
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

func (m *MemoryStorage) _Get(key string) *StorageEntry {
	v, ok := m.values[key]
	if !ok || (v.expiresAt != 0 && m.timeSource.Now() >= v.expiresAt) {
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
	value := m._Get(key)
	if value == nil {
		return "", 0, ErrKeyNotFound
	}
	return value.value, value.expiresAt, nil
}

func (m *MemoryStorage) Add(key string, value string, expiresAt uint64) (bool, error) {
	if key == "" {
		return false, ErrEmptyKey
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	v := m._Get(key)
	if v != nil {
		return false, nil
	}
	m._Set(key, value, expiresAt)
	return true, nil
}

func (m *MemoryStorage) Replace(key string, value string, expiresAt uint64) (bool, error) {
	if key == "" {
		return false, ErrEmptyKey
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	v := m._Get(key)
	if v == nil {
		return false, nil
	}
	m._Set(key, value, expiresAt)
	return true, nil
}

func (m *MemoryStorage) Delete(key string) (bool, error) {
	if key == "" {
		return false, ErrEmptyKey
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	v := m._Get(key)
	if v == nil {
		return false, nil
	}
	delete(m.values, key)
	return true, nil
}

func (m *MemoryStorage) Incr(key string, delta uint64) (uint64, error) {
	if key == "" {
		return 0, ErrEmptyKey
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	v := m._Get(key)
	if v == nil {
		return 0, ErrKeyNotFound
	}
	intValue, err := strconv.ParseUint(v.value, 10, 64)
	if err != nil {
		return 0, ErrNotANumber
	}
	newValue := intValue + delta
	if newValue < intValue {
		return 0, ErrOverflow
	}
	m._Set(key, strconv.FormatUint(newValue, 10), v.expiresAt)
	return newValue, nil
}

func (m *MemoryStorage) Decr(key string, delta uint64) (uint64, error) {
	if key == "" {
		return 0, ErrEmptyKey
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	v := m._Get(key)
	if v == nil {
		return 0, ErrKeyNotFound
	}
	intValue, err := strconv.ParseUint(v.value, 10, 64)
	if err != nil {
		return 0, ErrNotANumber
	}
	newValue := intValue - delta
	if newValue > intValue {
		return 0, ErrOverflow
	}
	m._Set(key, strconv.FormatUint(newValue, 10), v.expiresAt)
	return newValue, nil
}

func CreateMemoryStorage() Storage {
	return &MemoryStorage{values: make(map[string]StorageEntry), timeSource: SystemTimeSource{}}
}
