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
	for key, entry := range writeSet {
		// Only update if we read the key (so we know the base version)
		if _, read := readSet[key]; read {
			newVersion := entry.Version + 1
			// Check if we already have a newer version
			if current, found := s.Get(key); found {
				if current.Version >= newVersion {
					continue
				}
			}

			newEntry := &CacheEntry{
				Key:       key,
				Value:     entry.Value,
				Version:   newVersion, // Server incremented version
				Timestamp: time.Now(),
			}
			s.Set(key, newEntry)
		} else {
			// Blind write: we don't know the server version, so invalidate
			s.Delete(key)
		}
	}
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
func (s *DiscardOnAbortStrategy) OnInvalidate(key string, value string, version uint64) {
	// No-op: This strategy doesn't support proactive invalidation
}

// GetCacheEntry retrieves a cache entry if present
func (s *DiscardOnAbortStrategy) GetCacheEntry(key string) (*CacheEntry, bool) {
	return s.Get(key)
}
