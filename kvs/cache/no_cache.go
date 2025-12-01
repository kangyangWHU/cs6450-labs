package cache

// NoCacheStrategy never caches - always fetches from server
// This provides a baseline for measuring cache effectiveness
type NoCacheStrategy struct {
	*BaseCache
}

// NewNoCacheStrategy creates a new no-cache strategy
func NewNoCacheStrategy() *NoCacheStrategy {
	return &NoCacheStrategy{
		BaseCache: NewBaseCache(),
	}
}

// GetName returns the strategy name
func (s *NoCacheStrategy) GetName() string {
	return "no-cache"
}

// OnRead never returns cached values - always returns cache miss
func (s *NoCacheStrategy) OnRead(key string) (*CacheEntry, bool) {
	// Always return cache miss to force server fetch
	return nil, false
}

// OnServerRead does not cache the fetched value
func (s *NoCacheStrategy) OnServerRead(key string, value string, version uint64) {
	// No-op: don't cache anything
}

// OnCommit does not cache committed writes
func (s *NoCacheStrategy) OnCommit(readSet map[string]*CacheEntry, writeSet map[string]*CacheEntry) {
	// No-op: don't cache anything
}

// OnAbort does nothing (no cache to clean)
func (s *NoCacheStrategy) OnAbort(readSet map[string]*CacheEntry, writeSet map[string]*CacheEntry) {
	// No-op: nothing to clean up
}

// OnInvalidate is not used by this strategy
func (s *NoCacheStrategy) OnInvalidate(key string, value string, version uint64) {
	// No-op: no cache to invalidate
}

// GetCacheEntry always returns not found
func (s *NoCacheStrategy) GetCacheEntry(key string) (*CacheEntry, bool) {
	return nil, false
}
