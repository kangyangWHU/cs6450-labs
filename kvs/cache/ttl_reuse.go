package cache

import (
	"time"
)

// TTLReuseStrategy only reuses cached values if they are younger than a TTL
// This provides a time-based freshness guarantee
type TTLReuseStrategy struct {
	*BaseCache
	ttl time.Duration
}

// NewTTLReuseStrategy creates a new TTL-based reuse strategy
func NewTTLReuseStrategy(ttl time.Duration) *TTLReuseStrategy {
	return &TTLReuseStrategy{
		BaseCache: NewBaseCache(),
		ttl:       ttl,
	}
}

// GetName returns the strategy name
func (s *TTLReuseStrategy) GetName() string {
	return "ttl-reuse"
}

// OnRead checks if we can reuse a cached value based on TTL
// Only returns cached entry if it's younger than TTL
func (s *TTLReuseStrategy) OnRead(key string) (*CacheEntry, bool) {
	if entry, found := s.Get(key); found {
		age := time.Since(entry.Timestamp)
		if age < s.ttl {
			// Entry is fresh enough - reuse it
			return entry, true
		}
		// Entry is too old - remove it from cache
		s.Delete(key)
	}
	return nil, false
}

// OnServerRead updates cache after fetching from server
func (s *TTLReuseStrategy) OnServerRead(key string, value string, version uint64) {
	entry := &CacheEntry{
		Key:       key,
		Value:     value,
		Version:   version,
		Timestamp: time.Now(),
	}
	s.Set(key, entry)
}

// OnCommit updates cache with committed writes
func (s *TTLReuseStrategy) OnCommit(readSet map[string]*CacheEntry, writeSet map[string]*CacheEntry) {
	// Update cache with all writes from the committed transaction
	for key, entry := range writeSet {
		newEntry := &CacheEntry{
			Key:       key,
			Value:     entry.Value,
			Version:   entry.Version + 1, // Server incremented version
			Timestamp: time.Now(),        // Reset timestamp on write
		}
		s.Set(key, newEntry)
	}
	// Read set entries remain valid until TTL expires
}

// OnAbort removes aborted entries to avoid immediate reuse
func (s *TTLReuseStrategy) OnAbort(readSet map[string]*CacheEntry, writeSet map[string]*CacheEntry) {
	// Remove all entries involved in aborted transaction
	// This is more aggressive than necessary, but conservative
	for key := range readSet {
		s.Delete(key)
	}
	for key := range writeSet {
		s.Delete(key)
	}
}

// OnInvalidate is not used by this strategy (relies on TTL)
func (s *TTLReuseStrategy) OnInvalidate(key string, version uint64) {
	// No-op: This strategy relies on TTL, not invalidations
}

// GetCacheEntry retrieves a cache entry if present and not expired
func (s *TTLReuseStrategy) GetCacheEntry(key string) (*CacheEntry, bool) {
	if entry, found := s.Get(key); found {
		age := time.Since(entry.Timestamp)
		if age < s.ttl {
			return entry, true
		}
		s.Delete(key)
	}
	return nil, false
}

// SetTTL updates the TTL duration (useful for experiments)
func (s *TTLReuseStrategy) SetTTL(ttl time.Duration) {
	s.ttl = ttl
}

// GetTTL returns the current TTL duration
func (s *TTLReuseStrategy) GetTTL() time.Duration {
	return s.ttl
}
