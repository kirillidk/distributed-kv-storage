package kvengine

import (
	"errors"
	"sync"
)

var (
	ErrEmptyKey    = errors.New("Key can't be empty")
	ErrKeyNotFound = errors.New("Key not found")
)

const (
	SECONDS_IN_MONTH = uint64(30 * 24 * 60 * 60)
)

type Storage interface {
	Set(key string, value string, expiresAt uint64) error
	Get(key string) (string, uint64, error)
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

func (m *MemoryStorage) Set(key string, value string, expiresAt uint64) error {
	if key == "" {
		return ErrEmptyKey
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.values[key] = StorageEntry{value: value, expiresAt: expiresAt}
	return nil
}

func (m *MemoryStorage) Get(key string) (string, uint64, error) {
	if key == "" {
		return "", 0, ErrEmptyKey
	}

	m.lock.RLock()
	defer m.lock.RUnlock()
	value, ok := m.values[key]
	if !ok {
		return "", 0, ErrKeyNotFound
	}

	if value.expiresAt != 0 && m.timeSource.Now() >= value.expiresAt {
		return "", 0, ErrKeyNotFound
	}
	return value.value, value.expiresAt, nil
}

func CreateMemoryStorage() Storage {
	return &MemoryStorage{values: make(map[string]StorageEntry), timeSource: SystemTimeSource{}}
}
