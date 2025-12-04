package cache

import (
	"sync"
	"sync/atomic"
	"time"
)

// TTLReuseStrategy only reuses cached values if they are younger than a TTL
// This provides a time-based freshness guarantee with adaptive TTL adjustment
type TTLReuseStrategy struct {
	*BaseCache
	ttl      time.Duration
	minTTL   time.Duration
	maxTTL   time.Duration
	fixedTTL bool // If true, disable adaptive adjustment

	// Metrics for adaptive TTL
	cacheHits   atomic.Uint64
	cacheMisses atomic.Uint64
	aborts      atomic.Uint64
	commits     atomic.Uint64
	reads       atomic.Uint64
	writes      atomic.Uint64

	// Adaptive adjustment
	lastAdjustment time.Time
	adjustInterval time.Duration
	mu             sync.RWMutex
}

// NewTTLReuseStrategy creates a new TTL-based reuse strategy with adaptive adjustment
func NewTTLReuseStrategy(ttl time.Duration, fixedTTL bool) *TTLReuseStrategy {
	// Set reasonable min/max bounds for TTL
	minTTL := ttl / 10
	if minTTL < 1*time.Millisecond {
		minTTL = 1 * time.Millisecond
	}
	maxTTL := ttl * 10
	if maxTTL > 10*time.Second {
		maxTTL = 10 * time.Second
	}

	return &TTLReuseStrategy{
		BaseCache:      NewBaseCache(),
		ttl:            ttl,
		minTTL:         minTTL,
		maxTTL:         maxTTL,
		fixedTTL:       fixedTTL,
		lastAdjustment: time.Now(),
		adjustInterval: 50 * time.Millisecond, // Adjust every 50 milliseconds
	}
}

// GetName returns the strategy name
func (s *TTLReuseStrategy) GetName() string {
	return "ttl-reuse"
}

// OnRead checks if we can reuse a cached value based on TTL
// Only returns cached entry if it's younger than TTL
func (s *TTLReuseStrategy) OnRead(key string) (*CacheEntry, bool) {
	s.reads.Add(1)

	// Periodically adjust TTL based on metrics (only if not fixed)
	if !s.fixedTTL {
		s.adjustTTL()
	}

	s.mu.RLock()
	currentTTL := s.ttl
	s.mu.RUnlock()

	if entry, found := s.Get(key); found {
		age := time.Since(entry.Timestamp)
		if age < currentTTL {
			// Entry is fresh enough - reuse it
			s.cacheHits.Add(1)
			return entry, true
		}
		// Entry is too old - remove it from cache
		s.Delete(key)
	}
	s.cacheMisses.Add(1)
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
	s.commits.Add(1)

	for key, entry := range writeSet {
		s.writes.Add(1)
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

// OnAbort removes aborted entries to avoid immediate reuse
func (s *TTLReuseStrategy) OnAbort(readSet map[string]*CacheEntry, writeSet map[string]*CacheEntry) {
	s.aborts.Add(1)

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
func (s *TTLReuseStrategy) OnInvalidate(key string, value string, version uint64) {
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ttl
}

// adjustTTL dynamically adjusts TTL based on cache performance and workload characteristics
func (s *TTLReuseStrategy) adjustTTL() {
	now := time.Now()

	// Only adjust periodically
	s.mu.RLock()
	shouldAdjust := now.Sub(s.lastAdjustment) >= s.adjustInterval
	s.mu.RUnlock()

	if !shouldAdjust {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring lock
	if now.Sub(s.lastAdjustment) < s.adjustInterval {
		return
	}

	s.lastAdjustment = now

	// Get current metrics
	hits := s.cacheHits.Load()
	misses := s.cacheMisses.Load()
	aborts := s.aborts.Load()
	commits := s.commits.Load()
	reads := s.reads.Load()
	writes := s.writes.Load()

	totalAccess := hits + misses
	totalTxns := aborts + commits

	// Need minimum data points to make adjustment
	if totalAccess < 10 || totalTxns < 5 {
		return
	}

	// Calculate key metrics
	cacheHitRate := float64(hits) / float64(totalAccess)
	abortRate := float64(aborts) / float64(totalTxns)

	// Calculate read/write ratio
	var readWriteRatio float64
	if writes > 0 {
		readWriteRatio = float64(reads) / float64(writes)
	} else {
		readWriteRatio = 100.0 // Very read-heavy
	}

	// Adaptive TTL adjustment logic
	oldTTL := s.ttl
	newTTL := oldTTL

	// Strategy 1: High abort rate → decrease TTL (cache is stale)
	if abortRate > 0.3 {
		// High contention, aggressive decrease
		newTTL = time.Duration(float64(oldTTL) * 0.7)
	} else if abortRate > 0.1 {
		// Moderate contention, moderate decrease
		newTTL = time.Duration(float64(oldTTL) * 0.85)
	} else if abortRate < 0.05 {
		// Low abort rate, can increase TTL
		// Strategy 2: High cache hit rate → increase TTL (cache is useful)
		if cacheHitRate > 0.5 {
			newTTL = time.Duration(float64(oldTTL) * 1.15)
		} else if cacheHitRate > 0.3 {
			newTTL = time.Duration(float64(oldTTL) * 1.05)
		}
	}

	// Strategy 3: Read/write ratio affects TTL
	// More reads relative to writes → can keep data longer
	if readWriteRatio > 20.0 {
		// Very read-heavy (e.g., YCSB-C), increase TTL
		newTTL = time.Duration(float64(newTTL) * 1.2)
	} else if readWriteRatio < 2.0 {
		// Write-heavy (e.g., YCSB-A), decrease TTL
		newTTL = time.Duration(float64(newTTL) * 0.8)
	}

	// Strategy 4: Low cache hit rate + low abort rate → increase TTL
	// (entries are expiring too quickly, not due to staleness)
	if cacheHitRate < 0.2 && abortRate < 0.05 {
		newTTL = time.Duration(float64(oldTTL) * 1.3)
	}

	// Enforce min/max bounds
	if newTTL < s.minTTL {
		newTTL = s.minTTL
	}
	if newTTL > s.maxTTL {
		newTTL = s.maxTTL
	}

	// Only update if change is significant (>5%)
	if float64(newTTL) < float64(oldTTL)*0.95 || float64(newTTL) > float64(oldTTL)*1.05 {
		s.ttl = newTTL
		// Reset metrics after adjustment to measure new TTL effectiveness
		s.cacheHits.Store(0)
		s.cacheMisses.Store(0)
		s.aborts.Store(0)
		s.commits.Store(0)
		s.reads.Store(0)
		s.writes.Store(0)
	}
}

// GetMetrics returns current cache metrics (useful for monitoring)
func (s *TTLReuseStrategy) GetMetrics() (hits, misses, aborts, commits uint64, hitRate, abortRate float64) {
	hits = s.cacheHits.Load()
	misses = s.cacheMisses.Load()
	aborts = s.aborts.Load()
	commits = s.commits.Load()

	totalAccess := hits + misses
	if totalAccess > 0 {
		hitRate = float64(hits) / float64(totalAccess)
	}

	totalTxns := aborts + commits
	if totalTxns > 0 {
		abortRate = float64(aborts) / float64(totalTxns)
	}

	return
}
