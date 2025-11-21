package cache

import (
	"time"
)

// DiscardOnAbortStrategy discards cached items when a transaction aborts
// This is the most conservative strategy - only removes offending items
type DiscardOnAbortStrategy struct {
	*BaseCache
}

// NewDiscardOnAbortStrategy creates a new discard-on-abort cache strategy
func NewDiscardOnAbortStrategy() *DiscardOnAbortStrategy {
	return &DiscardOnAbortStrategy{
		BaseCache: NewBaseCache(),
	}
}

// GetName returns the strategy name
func (s *DiscardOnAbortStrategy) GetName() string {
	return "discard-on-abort"
}

// OnRead checks if we can reuse a cached value
// For discard-on-abort, we reuse all cached values optimistically
func (s *DiscardOnAbortStrategy) OnRead(key string) (*CacheEntry, bool) {
	if entry, found := s.Get(key); found {
		// Return cached entry - will be validated at commit time
		return entry, true
	}
	return nil, false
}

// OnServerRead updates cache after fetching from server
func (s *DiscardOnAbortStrategy) OnServerRead(key string, value string, version uint64) {
	entry := &CacheEntry{
		Key:       key,
		Value:     value,
		Version:   version,
		Timestamp: time.Now(),
	}
	s.Set(key, entry)
}

// OnCommit updates cache with committed writes
func (s *DiscardOnAbortStrategy) OnCommit(readSet map[string]*CacheEntry, writeSet map[string]*CacheEntry) {
	// Update cache with all writes from the committed transaction
	for key, entry := range writeSet {
		// Increment version for writes (server will have incremented it)
		newEntry := &CacheEntry{
			Key:       key,
			Value:     entry.Value,
			Version:   entry.Version + 1, // Server incremented version
			Timestamp: time.Now(),
		}
		s.Set(key, newEntry)
	}
	// Read set entries remain valid (no action needed)
}

// OnAbort removes entries that caused the abort
// Strategy: Discard all entries in the read set to avoid future conflicts
func (s *DiscardOnAbortStrategy) OnAbort(readSet map[string]*CacheEntry, writeSet map[string]*CacheEntry) {
	// Discard all read set entries - one of them likely caused the abort
	for key := range readSet {
		s.Delete(key)
	}
	// Also discard write set entries
	for key := range writeSet {
		s.Delete(key)
	}
}

// OnInvalidate is not used by this strategy
func (s *DiscardOnAbortStrategy) OnInvalidate(key string, version uint64) {
	// No-op: This strategy doesn't support proactive invalidation
}

// GetCacheEntry retrieves a cache entry if present
func (s *DiscardOnAbortStrategy) GetCacheEntry(key string) (*CacheEntry, bool) {
	return s.Get(key)
}
