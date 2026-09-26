package kvengine

import (
	"errors"
	"sync"
)

var (
	ErrEmptyKey      = errors.New("Key can't be empty")
	ErrKeyNotFound   = errors.New("Key not found")
	SECONDS_IN_MONTH = uint64(2_502_000)
)

type Storage interface {
	Set(key string, value string, expires uint64) error
	Get(key string) (string, error)
}

type StorageEntry struct {
	value   string
	expires uint64
}

type MemoryStorage struct {
	values     map[string]StorageEntry
	lock       *sync.RWMutex
	timeSource TimeSource
}

func (m MemoryStorage) Set(key string, value string, expires uint64) error {
	if key == "" {
		return ErrEmptyKey
	}
	if expires == 0 {
		expires = ^uint64(0) // maximum value
	}
	if expires <= SECONDS_IN_MONTH {
		expires += m.timeSource.Now()
	}
	m.lock.Lock()
	m.values[key] = StorageEntry{value, expires}
	m.lock.Unlock()
	return nil
}

func (m MemoryStorage) Get(key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}

	m.lock.RLock()
	value, ok := m.values[key]
	m.lock.RUnlock()
	if !ok {
		return "", ErrKeyNotFound
	}

	if m.timeSource.Now() >= value.expires {
		return "", ErrKeyNotFound
	} else {
		return value.value, nil
	}
}

func CreateMemoryStorage() Storage {
	return MemoryStorage{make(map[string]StorageEntry), &sync.RWMutex{}, SystemTimeSource{}}
}
