package cache

import (
	"sync"
	"time"
)

// ProactiveInvalidationStrategy receives invalidation messages from servers
// Servers track which clients have cached each key and push invalidations
type ProactiveInvalidationStrategy struct {
	*BaseCache
	invalidationMu sync.RWMutex
}

// NewProactiveInvalidationStrategy creates a new proactive invalidation strategy
func NewProactiveInvalidationStrategy() *ProactiveInvalidationStrategy {
	return &ProactiveInvalidationStrategy{
		BaseCache: NewBaseCache(),
	}
}

// GetName returns the strategy name
func (s *ProactiveInvalidationStrategy) GetName() string {
	return "proactive-invalidation"
}

// OnRead checks if we can reuse a cached value
// For proactive invalidation, we trust the cache until we receive an invalidation
func (s *ProactiveInvalidationStrategy) OnRead(key string) (*CacheEntry, bool) {
	if entry, found := s.Get(key); found {
		// Return cached entry - server will invalidate if stale
		return entry, true
	}
	return nil, false
}

// OnServerRead updates cache after fetching from server
func (s *ProactiveInvalidationStrategy) OnServerRead(key string, value string, version uint64) {
	entry := &CacheEntry{
		Key:       key,
		Value:     value,
		Version:   version,
		Timestamp: time.Now(),
	}
	s.Set(key, entry)
}

// OnCommit updates cache with committed writes
func (s *ProactiveInvalidationStrategy) OnCommit(readSet map[string]*CacheEntry, writeSet map[string]*CacheEntry) {
	// Update cache with all writes from the committed transaction
	for key, entry := range writeSet {
		newEntry := &CacheEntry{
			Key:       key,
			Value:     entry.Value,
			Version:   entry.Version + 1, // Server incremented version
			Timestamp: time.Now(),
		}
		s.Set(key, newEntry)
	}
	// Read set entries remain valid (server will send invalidations if needed)
}

// OnAbort keeps cached entries - they may still be valid
// Server will send invalidations if needed
func (s *ProactiveInvalidationStrategy) OnAbort(readSet map[string]*CacheEntry, writeSet map[string]*CacheEntry) {
	// Keep all read set entries - rely on server invalidations
	// Discard write set entries (never committed)
	for key := range writeSet {
		s.Delete(key)
	}
}

// OnInvalidate is called when server sends an invalidation message
// This is the key feature of this strategy
func (s *ProactiveInvalidationStrategy) OnInvalidate(key string, version uint64) {
	s.invalidationMu.Lock()
	defer s.invalidationMu.Unlock()

	if entry, found := s.Get(key); found {
		// If our cached version is older than invalidation version, remove it
		if entry.Version < version {
			s.Delete(key)
		}
	}
}

// GetCacheEntry retrieves a cache entry if present
func (s *ProactiveInvalidationStrategy) GetCacheEntry(key string) (*CacheEntry, bool) {
	return s.Get(key)
}
