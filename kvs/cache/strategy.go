package cache

import (
	"sync"
	"time"
)

// CacheEntry represents a versioned cache entry with metadata
type CacheEntry struct {
	Key       string
	Value     string
	Version   uint64
	Timestamp time.Time
}

// CacheStrategy defines the interface for different cache management strategies
type CacheStrategy interface {
	// GetName returns the name of the strategy
	GetName() string

	// OnRead is called when a transaction reads a key from the server
	// Returns the cache entry and whether it should be used (true) or fetched from server (false)
	OnRead(key string) (*CacheEntry, bool)

	// OnServerRead is called after fetching a value from the server
	// Updates the cache with the new data
	OnServerRead(key string, value string, version uint64)

	// OnCommit is called when a transaction commits successfully
	// Updates cache state based on committed read and write sets
	OnCommit(readSet map[string]*CacheEntry, writeSet map[string]*CacheEntry)

	// OnAbort is called when a transaction aborts
	// Updates cache state based on aborted transaction
	OnAbort(readSet map[string]*CacheEntry, writeSet map[string]*CacheEntry)

	// OnInvalidate is called by server for proactive invalidation
	// Only used by ProactiveInvalidation strategy (pushes new value and version)
	OnInvalidate(key string, value string, version uint64)

	// GetCacheEntry retrieves a cache entry if present
	GetCacheEntry(key string) (*CacheEntry, bool)

	// Clear removes all entries from the cache
	Clear()
}

// BaseCache provides common cache functionality
type BaseCache struct {
	cache sync.Map // map[string]*CacheEntry
}

// NewBaseCache creates a new base cache
func NewBaseCache() *BaseCache {
	return &BaseCache{}
}

// Get retrieves a cache entry
func (bc *BaseCache) Get(key string) (*CacheEntry, bool) {
	if val, ok := bc.cache.Load(key); ok {
		entry := val.(*CacheEntry)
		return entry, true
	}
	return nil, false
}

// Set stores a cache entry
func (bc *BaseCache) Set(key string, entry *CacheEntry) {
	bc.cache.Store(key, entry)
}

// Delete removes a cache entry
func (bc *BaseCache) Delete(key string) {
	bc.cache.Delete(key)
}

// Clear removes all cache entries
func (bc *BaseCache) Clear() {
	bc.cache.Range(func(key, value interface{}) bool {
		bc.cache.Delete(key)
		return true
	})
}

// Range iterates over all cache entries
func (bc *BaseCache) Range(f func(key string, entry *CacheEntry) bool) {
	bc.cache.Range(func(k, v interface{}) bool {
		return f(k.(string), v.(*CacheEntry))
	})
}
