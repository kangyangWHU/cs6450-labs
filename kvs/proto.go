package kvs

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct {
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value string
}

// Transaction support
type BeginRequest struct {
}

type BeginResponse struct {
	TxnId string
}

type CommitRequest struct {
	TxnId string
	Lead  bool // true if this participant is the leader/coordinator
}

type CommitResponse struct {
	Success bool
}

type AbortRequest struct {
	TxnId string
}

type AbortResponse struct {
}

// Transactional operations
type TxnGetRequest struct {
	TxnId string
	Key   string
}

type TxnGetResponse struct {
	Value string
	Found bool
}

type TxnPutRequest struct {
	TxnId string
	Key   string
	Value string
}

type TxnPutResponse struct {
}

// 2PC Protocol messages
type PrepareRequest struct {
	TxnId string
}

type PrepareResponse struct {
	TxnId string
	Vote  bool // true = YES (ready to commit), false = NO (abort)
}

type GlobalCommitRequest struct {
	TxnId string
	Lead  bool // true if this participant is the leader/coordinator
}

type GlobalCommitResponse struct {
}

type GlobalAbortRequest struct {
	TxnId string
}

type GlobalAbortResponse struct {
}

// =============================================================================
// OCC Protocol messages
// =============================================================================

// VersionedValue represents a key-value pair with version information
type VersionedValue struct {
	Key     string
	Value   string
	Version uint64
}

// OCC Transactional Get - returns value with version
type OCCTxnGetRequest struct {
	TxnId    string
	Key      string
	ClientId string // For proactive invalidation tracking
}

type OCCTxnGetResponse struct {
	Value   string
	Version uint64
	Found   bool
}

// OCC Transactional Put - stores in local write set
type OCCTxnPutRequest struct {
	TxnId string
	Key   string
	Value string
}

type OCCTxnPutResponse struct {
}

// OCC Validation Request - sent at commit time
type OCCValidateRequest struct {
	TxnId    string
	ReadSet  []VersionedValue // Keys read with expected versions
	WriteSet []VersionedValue // Keys to write with new values
	Lead     bool             // true if this participant is the coordinator
	ClientId string           // For proactive invalidation tracking
}

type OCCValidateResponse struct {
	TxnId   string
	Success bool // true if validation passed, false if conflict detected
}

// Proactive Invalidation - server notifies client of stale cache
type InvalidateRequest struct {
	Key     string
	Value   string // Push the new value to client
	Version uint64
}

type InvalidateResponse struct {
}

// Alias for RPC compatibility
type InvalidationRequest = InvalidateRequest
type InvalidationResponse = InvalidateResponse

// OCC Begin transaction
type OCCBeginRequest struct {
	ClientId           string // For proactive invalidation tracking
	ClientCallbackHost string // Client's host:port for receiving invalidations
}

type OCCBeginResponse struct {
	TxnId string
}
