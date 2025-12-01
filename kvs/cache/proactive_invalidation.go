package cache

import (
	"time"
)

// ProactiveInvalidationStrategy receives invalidation messages from servers
// Servers track which clients have cached each key and push invalidations
type ProactiveInvalidationStrategy struct {
	*BaseCache
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

// OnAbort invalidates cached entries that caused validation failure
// This forces fresh reads on retry, allowing invalidations to take effect
func (s *ProactiveInvalidationStrategy) OnAbort(readSet map[string]*CacheEntry, writeSet map[string]*CacheEntry) {
	// Invalidate read set entries - they likely caused the abort
	// This ensures next retry will either use pushed updates or fetch fresh data
	for key := range readSet {
		s.Delete(key)
	}
	// Discard write set entries (never committed)
	for key := range writeSet {
		s.Delete(key)
	}
}

// OnInvalidate is called when server sends an invalidation message
// This is the key feature of this strategy
func (s *ProactiveInvalidationStrategy) OnInvalidate(key string, value string, version uint64) {
	// Check if this is an invalidation signal (version=0, empty value)
	// This means "delete your cache, fresh value coming later"
	if version == 0 && value == "" {
		s.Delete(key)
		return
	}

	if entry, found := s.Get(key); found {
		// If our cached version is older than invalidation version, update it
		if entry.Version < version {
			// Proactively UPDATE cache with the new value (not just delete)
			newEntry := &CacheEntry{
				Key:       key,
				Value:     value,
				Version:   version,
				Timestamp: time.Now(),
			}
			s.Set(key, newEntry)
		}
	} else {
		// Even if not cached before, proactively cache the new value
		newEntry := &CacheEntry{
			Key:       key,
			Value:     value,
			Version:   version,
			Timestamp: time.Now(),
		}
		s.Set(key, newEntry)
	}
}

// GetCacheEntry retrieves a cache entry if present
func (s *ProactiveInvalidationStrategy) GetCacheEntry(key string) (*CacheEntry, bool) {
	return s.Get(key)
}
