package main

import (
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

// OCCTransaction represents an OCC transaction
type OCCTransaction struct {
	id        string
	readSet   map[string]uint64 // key -> version read
	writeSet  map[string]string // key -> value to write
	timestamp time.Time
}

// VersionedEntry stores value with its version
type VersionedEntry struct {
	Value   string
	Version uint64
}

func NewOCCTransaction(id string) *OCCTransaction {
	return &OCCTransaction{
		id:        id,
		readSet:   make(map[string]uint64),
		writeSet:  make(map[string]string),
		timestamp: time.Now(),
	}
}

// OCCKVService provides OCC-based transaction support
type OCCKVService struct {
	// Versioned key-value store: key -> VersionedEntry
	store sync.Map // map[string]*VersionedEntry

	// Active transactions (for server-side tracking if needed)
	transactions sync.Map // map[string]*OCCTransaction

	// Global read and write sets for conflict detection
	globalReadSet  map[string]map[string]bool // key -> set of txnIds
	globalWriteSet map[string]string          // key -> txnId

	// For proactive invalidation: track which clients have cached each key
	cacheTracking sync.Map // map[string]*sync.Map - key -> sync.Map of clientIds

	// Client connections for sending invalidations (clientId -> host address)
	clientConnections sync.Map // map[string]string - clientId -> host:port

	// RPC clients for sending invalidations (host:port -> *rpc.Client)
	rpcClients sync.Map

	// Single global lock for validation and write phase (OCC critical section)
	globalMu sync.RWMutex

	txnCounter atomic.Uint64
	stats      Stats
	prevStats  Stats
	lastPrint  time.Time
	printMutex sync.Mutex
}

func NewOCCKVServiceWithPartitioning(serverId, numServers int) *OCCKVService {
	kvs := &OCCKVService{
		globalReadSet:  make(map[string]map[string]bool),
		globalWriteSet: make(map[string]string),
	}
	kvs.lastPrint = time.Now()

	// Initialize accounts with version 0
	accountsInitialized := []int{}
	for i := 0; i < 10; i++ {
		if i%numServers == serverId {
			kvs.store.Store(fmt.Sprintf("%d", i), &VersionedEntry{
				Value:   "1000000",
				Version: 0,
			})
			accountsInitialized = append(accountsInitialized, i)
		}
	}
	log.Printf("OCC Server initialized accounts: %v\n", accountsInitialized)

	return kvs
}

// OCCBegin starts a new OCC transaction
func (kv *OCCKVService) OCCBegin(request *kvs.OCCBeginRequest, response *kvs.OCCBeginResponse) error {
	txnCounter := kv.txnCounter.Add(1)
	txnId := fmt.Sprintf("occ-txn-%d", txnCounter)
	kv.transactions.Store(txnId, NewOCCTransaction(txnId))

	// Register client connection for proactive invalidation
	if request.ClientId != "" && request.ClientCallbackHost != "" {
		kv.clientConnections.Store(request.ClientId, request.ClientCallbackHost)
	}

	response.TxnId = txnId
	return nil
}

// OCCTxnGet returns value with version (may be served from cache on client side)
func (kv *OCCKVService) OCCTxnGet(request *kvs.OCCTxnGetRequest, response *kvs.OCCTxnGetResponse) error {
	kv.stats.gets.Add(1)

	// Track that this client has cached this key (for proactive invalidation)
	if request.ClientId != "" {
		clientSetVal, _ := kv.cacheTracking.LoadOrStore(request.Key, &sync.Map{})
		clientSet := clientSetVal.(*sync.Map)
		clientSet.Store(request.ClientId, true)
	}

	// Read current value and version (no lock needed for reads)
	if valueVal, found := kv.store.Load(request.Key); found {
		entry := valueVal.(*VersionedEntry)
		response.Value = entry.Value
		response.Version = entry.Version
		response.Found = true
	} else {
		response.Value = ""
		response.Version = 0
		response.Found = true // Assume empty string for missing keys
	}

	return nil
}

// OCCTxnPut stores write in transaction's local write set (no server-side write yet)
// In OCC, this is primarily for compatibility - client maintains write set
func (kv *OCCKVService) OCCTxnPut(request *kvs.OCCTxnPutRequest, response *kvs.OCCTxnPutResponse) error {
	kv.stats.puts.Add(1)
	// In pure OCC, client maintains write set locally
	// Server doesn't need to track puts until validation
	return nil
}

// OCCPrepare validates transaction using OCC forward validation
// This is Phase 1 of 2PC - validate and prepare to commit
func (kv *OCCKVService) OCCPrepare(request *kvs.OCCValidateRequest, response *kvs.OCCValidateResponse) error {
	response.TxnId = request.TxnId
	response.Success = false

	// Build local write set map for efficient lookup (outside lock)
	localWriteSetKeys := make(map[string]bool)
	for _, writeItem := range request.WriteSet {
		localWriteSetKeys[writeItem.Key] = true
	}

	// TRUE OCC: Pre-validate WITHOUT holding global lock
	// Phase 1: Optimistic validation (no locks held)

	// Pre-validate read set: check versions match (optimistically - may change before commit)
	for _, readItem := range request.ReadSet {
		currentVal, found := kv.store.Load(readItem.Key)
		if readItem.Version == 0 && readItem.Value == "" {
			// Expected empty/missing key
			if found {
				currentEntry := currentVal.(*VersionedEntry)
				if currentEntry.Value != "" || currentEntry.Version != 0 {
					log.Printf("OCC Pre-validation failed for txn %s: read key %s changed (expected empty, got v%d)\n",
						request.TxnId, readItem.Key, currentEntry.Version)
					return nil
				}
			}
		} else {
			// Expected specific version
			if !found {
				log.Printf("OCC Pre-validation failed for txn %s: read key %s not found (expected v%d)\n",
					request.TxnId, readItem.Key, readItem.Version)
				return nil
			}
			currentEntry := currentVal.(*VersionedEntry)
			if currentEntry.Version != readItem.Version {
				log.Printf("OCC Pre-validation failed for txn %s: read key %s version mismatch (expected v%d, got v%d)\n",
					request.TxnId, readItem.Key, readItem.Version, currentEntry.Version)
				return nil
			}
		}
	}

	// Create transaction object to store
	txn := NewOCCTransaction(request.TxnId)
	for _, readItem := range request.ReadSet {
		if localWriteSetKeys[readItem.Key] {
			continue // Skip keys we're also writing
		}
		txn.readSet[readItem.Key] = readItem.Version
	}
	for _, writeItem := range request.WriteSet {
		txn.writeSet[writeItem.Key] = writeItem.Value
	}

	// Phase 2: CRITICAL SECTION - Atomic validation and lock acquisition
	// This is the ONLY place we hold the global lock, and we hold it BRIEFLY
	isReadOnly := len(request.WriteSet) == 0
	if isReadOnly {
		kv.globalMu.RLock()
	} else {
		kv.globalMu.Lock()
	}

	// Re-validate read set while holding lock (catch races from pre-validation to now)
	for _, readItem := range request.ReadSet {
		// Check if key is in global write set (another txn is writing to it)
		if writerTxn, found := kv.globalWriteSet[readItem.Key]; found {
			if isReadOnly {
				kv.globalMu.RUnlock()
			} else {
				kv.globalMu.Unlock()
			}
			log.Printf("OCC Validation failed for txn %s: read key %s is in global write set (writer: %s)\n",
				request.TxnId, readItem.Key, writerTxn)
			return nil
		}

		// Re-check version matches current committed value
		currentVal, found := kv.store.Load(readItem.Key)
		if readItem.Version == 0 && readItem.Value == "" {
			if found {
				currentEntry := currentVal.(*VersionedEntry)
				if currentEntry.Value != "" || currentEntry.Version != 0 {
					if isReadOnly {
						kv.globalMu.RUnlock()
					} else {
						kv.globalMu.Unlock()
					}
					log.Printf("OCC Validation failed for txn %s: read key %s changed (expected empty, got v%d)\n",
						request.TxnId, readItem.Key, currentEntry.Version)
					return nil
				}
			}
		} else {
			if !found {
				if isReadOnly {
					kv.globalMu.RUnlock()
				} else {
					kv.globalMu.Unlock()
				}
				log.Printf("OCC Validation failed for txn %s: read key %s not found (expected v%d)\n",
					request.TxnId, readItem.Key, readItem.Version)
				return nil
			}
			currentEntry := currentVal.(*VersionedEntry)
			if currentEntry.Version != readItem.Version {
				if isReadOnly {
					kv.globalMu.RUnlock()
				} else {
					kv.globalMu.Unlock()
				}
				log.Printf("OCC Validation failed for txn %s: read key %s version mismatch (expected v%d, got v%d)\n",
					request.TxnId, readItem.Key, readItem.Version, currentEntry.Version)
				return nil
			}
		}
	}

	// Validate and lock write set
	for _, writeItem := range request.WriteSet {
		// Check if key is in global write set (write-write conflict)
		if writerTxn, found := kv.globalWriteSet[writeItem.Key]; found {
			if isReadOnly {
				kv.globalMu.RUnlock()
			} else {
				kv.globalMu.Unlock()
			}
			log.Printf("OCC Validation failed for txn %s: write key %s is in global write set (writer: %s)\n",
				request.TxnId, writeItem.Key, writerTxn)
			return nil
		}

		// Check if key is in global read set (write-read conflict)
		if readerSet, found := kv.globalReadSet[writeItem.Key]; found {
			if len(readerSet) > 0 {
				if isReadOnly {
					kv.globalMu.RUnlock()
				} else {
					kv.globalMu.Unlock()
				}
				log.Printf("OCC Validation failed for txn %s: write key %s is in global read set (%d readers)\n",
					request.TxnId, writeItem.Key, len(readerSet))
				return nil
			}
		}
	}

	// Validation passed - atomically add to global read/write sets

	// Optimization: For Read-Only transactions (empty WriteSet), we don't need to add to globalReadSet.
	// Since we've already validated the reads, and we're not writing anything,
	// we don't need to block future writers. The transaction is effectively serialized at this point.

	if !isReadOnly {
		// Add reads to global read set
		for _, readItem := range request.ReadSet {
			if localWriteSetKeys[readItem.Key] {
				continue // Skip keys we're also writing
			}
			readerSet, found := kv.globalReadSet[readItem.Key]
			if !found {
				readerSet = make(map[string]bool)
				kv.globalReadSet[readItem.Key] = readerSet
			}
			readerSet[request.TxnId] = true
		}

		// Add writes to global write set (lock the keys)
		for _, writeItem := range request.WriteSet {
			kv.globalWriteSet[writeItem.Key] = request.TxnId
		}
	}

	kv.transactions.Store(request.TxnId, txn)

	// CRITICAL SECTION END - lock held only for atomic validation + lock acquisition
	if isReadOnly {
		kv.globalMu.RUnlock()
	} else {
		kv.globalMu.Unlock()
	}

	response.Success = true
	return nil
}

// OCCCommit commits a prepared transaction (Phase 2 of 2PC)
// Apply writes atomically and clean up global read/write sets
func (kv *OCCKVService) OCCCommit(request *kvs.OCCValidateRequest, response *kvs.OCCValidateResponse) error {
	response.TxnId = request.TxnId
	response.Success = false

	txnVal, exists := kv.transactions.Load(request.TxnId)
	if !exists {
		// Transaction not found or already committed/aborted
		return nil
	}

	txn := txnVal.(*OCCTransaction)

	// TRUE OCC: Prepare new entries outside the lock
	newEntries := make(map[string]*VersionedEntry)
	for key, value := range txn.writeSet {
		currentVal, found := kv.store.Load(key)
		var newVersion uint64
		if found {
			currentEntry := currentVal.(*VersionedEntry)
			newVersion = currentEntry.Version + 1
		} else {
			newVersion = 1
		}
		newEntries[key] = &VersionedEntry{
			Value:   value,
			Version: newVersion,
		}
	}

	// CRITICAL SECTION START: Minimal lock just for atomic write
	// For Read-Only transactions, we don't need to acquire the lock at all
	// because we don't modify store or global sets.
	if len(txn.writeSet) > 0 {
		kv.globalMu.Lock()

		// Atomically apply all writes
		for key, entry := range newEntries {
			kv.store.Store(key, entry)
			// Remove from global write set
			delete(kv.globalWriteSet, key)
		}

		// Remove from global read set (only if we added it - i.e., not read-only)
		for key := range txn.readSet {
			if readerSet, found := kv.globalReadSet[key]; found {
				delete(readerSet, request.TxnId)
				if len(readerSet) == 0 {
					delete(kv.globalReadSet, key)
				}
			}
		}

		// CRITICAL SECTION END - lock held only for atomic writes
		kv.globalMu.Unlock()
	}

	response.Success = true

	// Count commits only on coordinator to avoid double counting
	if request.Lead {
		kv.stats.commits.Add(1)
	}

	kv.transactions.Delete(request.TxnId)

	// Proactive invalidation: push new value to clients (outside lock - async network I/O)
	for key := range txn.writeSet {
		if entry, ok := newEntries[key]; ok {
			kv.sendInvalidations(key, entry.Value, entry.Version)
		}
	}

	return nil
}

// OCCAbortPrepared aborts a prepared transaction
func (kv *OCCKVService) OCCAbortPrepared(request *kvs.OCCValidateRequest, response *kvs.OCCValidateResponse) error {
	response.TxnId = request.TxnId
	response.Success = true

	txnVal, exists := kv.transactions.Load(request.TxnId)
	if !exists {
		return nil
	}

	txn := txnVal.(*OCCTransaction)

	// CRITICAL SECTION START: Only lock during global set cleanup
	// For Read-Only transactions, we don't need to acquire the lock at all
	if len(txn.writeSet) > 0 {
		kv.globalMu.Lock()

		// Remove from global write set
		for key := range txn.writeSet {
			delete(kv.globalWriteSet, key)
		}

		// Remove from global read set (only if we added it - i.e., not read-only)
		for key := range txn.readSet {
			if readerSet, found := kv.globalReadSet[key]; found {
				delete(readerSet, request.TxnId)
				if len(readerSet) == 0 {
					delete(kv.globalReadSet, key)
				}
			}
		}

		// CRITICAL SECTION END
		kv.globalMu.Unlock()
	}

	// Remove transaction from tracking (outside lock)
	kv.transactions.Delete(request.TxnId)

	// If we sent speculative invalidations in Prepare phase, we need to "undo" them
	// Send the CURRENT (correct) values back to clients (outside lock - async network I/O)
	for key := range txn.writeSet {
		// Get the actual current value (not the aborted write)
		if currentVal, found := kv.store.Load(key); found {
			entry := currentVal.(*VersionedEntry)
			// Push the correct current value to fix any speculative updates
			kv.sendInvalidations(key, entry.Value, entry.Version)
		} else {
			// Key doesn't exist - send invalidation signal to clear any cached data
			kv.sendInvalidationSignal(key)
		}
	}

	return nil
}

// sendInvalidations proactively pushes new value to clients (true proactive invalidation)
func (kv *OCCKVService) sendInvalidations(key string, value string, newVersion uint64) {
	if clientSetVal, found := kv.cacheTracking.Load(key); found {
		clientSet := clientSetVal.(*sync.Map)

		// Push new value to each client that cached this key
		clientSet.Range(func(clientIdVal, _ interface{}) bool {
			clientId := clientIdVal.(string)

			// Get client connection info
			if hostVal, found := kv.clientConnections.Load(clientId); found {
				host := hostVal.(string)

				// Get or create RPC client
				var client *rpc.Client
				if clientVal, ok := kv.rpcClients.Load(host); ok {
					client = clientVal.(*rpc.Client)
				} else {
					var err error
					client, err = rpc.DialHTTP("tcp", host)
					if err != nil {
						log.Printf("Failed to dial client %s: %v", host, err)
						return true
					}
					kv.rpcClients.Store(host, client)
				}

				// Send invalidation RPC with new value to client (best-effort, don't block on failure)
				go func(c *rpc.Client, cid, h, v string, ver uint64) {
					// log.Printf("Sending invalidation to client %s at %s for key %s (value=%s, version=%d)\n", cid, h, key, v, ver)
					request := &kvs.InvalidationRequest{
						Key:     key,
						Value:   value,
						Version: ver,
					}
					response := &kvs.InvalidationResponse{}

					if err := c.Call("OCCClientInvalidationService.ReceiveInvalidation", request, response); err != nil {
						log.Printf("Failed to send invalidation to client %s: %v\n", cid, err)
						// If connection failed, remove from cache so we reconnect next time
						if err == rpc.ErrShutdown {
							kv.rpcClients.Delete(h)
						}
					}
				}(client, clientId, host, value, newVersion)
			} else {
				log.Printf("No connection info found for client %s\n", clientId)
			}

			return true
		})

		// Clear tracking for this key after sending invalidations
		// kv.cacheTracking.Delete(key)
	}
}

// sendInvalidationSignal tells clients to invalidate cache without pushing new value
// Safer than sendInvalidations for Prepare phase - clients delete stale data but
// must fetch fresh data from server (which will have the new value after Commit)
func (kv *OCCKVService) sendInvalidationSignal(key string) {
	if clientSetVal, found := kv.cacheTracking.Load(key); found {
		clientSet := clientSetVal.(*sync.Map)

		// Tell clients to invalidate (delete) cached entry
		clientSet.Range(func(clientIdVal, _ interface{}) bool {
			clientId := clientIdVal.(string)

			if hostVal, found := kv.clientConnections.Load(clientId); found {
				host := hostVal.(string)

				// Get or create RPC client
				var client *rpc.Client
				if clientVal, ok := kv.rpcClients.Load(host); ok {
					client = clientVal.(*rpc.Client)
				} else {
					var err error
					client, err = rpc.DialHTTP("tcp", host)
					if err != nil {
						log.Printf("Failed to dial client %s: %v", host, err)
						return true
					}
					kv.rpcClients.Store(host, client)
				}

				// Send invalidation signal (version=0, empty value means "delete cache")
				go func(c *rpc.Client, cid, h string) {
					request := &kvs.InvalidationRequest{
						Key:     key,
						Value:   "",
						Version: 0,
					}
					response := &kvs.InvalidationResponse{}

					if err := c.Call("OCCClientInvalidationService.ReceiveInvalidation", request, response); err != nil {
						log.Printf("Failed to send invalidation signal to client %s: %v\n", cid, err)
						if err == rpc.ErrShutdown {
							kv.rpcClients.Delete(h)
						}
					}
				}(client, clientId, host)
			}

			return true
		})

		// Keep tracking - will send full update in Commit phase
		// Don't delete: kv.cacheTracking.Delete(key)
	}
}

// // OCCAbort aborts an OCC transaction (called before prepare phase)
// func (kv *OCCKVService) OCCAbort(request *kvs.AbortRequest, response *kvs.AbortResponse) error {
// 	// Simply remove transaction from tracking
// 	kv.transactions.Delete(request.TxnId)
// 	return nil
// }

func (kv *OCCKVService) printStats() {
	kv.printMutex.Lock()

	currentPuts := kv.stats.puts.Load()
	currentGets := kv.stats.gets.Load()
	currentCommits := kv.stats.commits.Load()

	prevPuts := kv.prevStats.puts.Load()
	prevGets := kv.prevStats.gets.Load()
	prevCommits := kv.prevStats.commits.Load()

	kv.prevStats.puts.Store(currentPuts)
	kv.prevStats.gets.Store(currentGets)
	kv.prevStats.commits.Store(currentCommits)

	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.printMutex.Unlock()

	diffPuts := currentPuts - prevPuts
	diffGets := currentGets - prevGets
	diffCommits := currentCommits - prevCommits
	deltaS := now.Sub(lastPrint).Seconds()

	// Always print stats regardless of verbose setting
	statsLogger.Printf("get/s %0.2f\nput/s %0.2f\ncommit/s %0.2f\nops/s %0.2f\n\n",
		float64(diffGets)/deltaS,
		float64(diffPuts)/deltaS,
		float64(diffCommits)/deltaS,
		float64(diffGets+diffPuts)/deltaS)
}

// Compatibility methods for 2PL-style interface (used by verification)
func (kv *OCCKVService) Begin(request *kvs.BeginRequest, response *kvs.BeginResponse) error {
	occReq := &kvs.OCCBeginRequest{}
	occResp := &kvs.OCCBeginResponse{}
	err := kv.OCCBegin(occReq, occResp)
	response.TxnId = occResp.TxnId
	return err
}

func (kv *OCCKVService) TxnGet(request *kvs.TxnGetRequest, response *kvs.TxnGetResponse) error {
	occReq := &kvs.OCCTxnGetRequest{
		TxnId: request.TxnId,
		Key:   request.Key,
	}
	occResp := &kvs.OCCTxnGetResponse{}
	err := kv.OCCTxnGet(occReq, occResp)
	response.Value = occResp.Value
	response.Found = occResp.Found
	return err
}

func (kv *OCCKVService) TxnPut(request *kvs.TxnPutRequest, response *kvs.TxnPutResponse) error {
	occReq := &kvs.OCCTxnPutRequest{
		TxnId: request.TxnId,
		Key:   request.Key,
		Value: request.Value,
	}
	occResp := &kvs.OCCTxnPutResponse{}
	return kv.OCCTxnPut(occReq, occResp)
}

func (kv *OCCKVService) Prepare(request *kvs.PrepareRequest, response *kvs.PrepareResponse) error {
	occReq := &kvs.OCCValidateRequest{TxnId: request.TxnId}
	occResp := &kvs.OCCValidateResponse{}
	err := kv.OCCPrepare(occReq, occResp)
	response.Vote = occResp.Success
	return err
}

func (kv *OCCKVService) GlobalCommit(request *kvs.GlobalCommitRequest, response *kvs.GlobalCommitResponse) error {
	occReq := &kvs.OCCValidateRequest{TxnId: request.TxnId}
	occResp := &kvs.OCCValidateResponse{}
	return kv.OCCCommit(occReq, occResp)
}

func (kv *OCCKVService) GlobalAbort(request *kvs.GlobalAbortRequest, response *kvs.GlobalAbortResponse) error {
	occReq := &kvs.OCCValidateRequest{TxnId: request.TxnId}
	occResp := &kvs.OCCValidateResponse{}
	return kv.OCCAbortPrepared(occReq, occResp)
}
