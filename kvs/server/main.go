package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Stats struct {
	puts    atomic.Uint64
	gets    atomic.Uint64
	commits atomic.Uint64
}

// Lock types for 2PL
type LockType int

const (
	SharedLock    LockType = 0
	ExclusiveLock LockType = 1
)

type Lock struct {
	lockType LockType
	txnId    string
}

// Transaction state with 2PL support
type Transaction struct {
	id        string
	readSet   sync.Map // map[string]string
	writeSet  sync.Map // map[string]string
	locksHeld sync.Map // map[string]LockType - keys locked by this transaction
	committed atomic.Bool
	aborted   atomic.Bool
	prepared  atomic.Bool // for 2PC
	timestamp time.Time
}

func NewTransaction(id string) *Transaction {
	return &Transaction{
		id:        id,
		timestamp: time.Now(),
	}
}

type KVService struct {
	// sync.Maps for lock-free access to individual keys
	mp           sync.Map // map[string]string - the actual key-value store
	transactions sync.Map // map[string]*Transaction
	locks        sync.Map // map[string][]Lock - key -> list of locks

	// Still need mutex for compound operations on locks
	// This protects the critical section where we check and acquire locks atomically
	lockMutex sync.Mutex

	txnCounter atomic.Uint64
	stats      Stats
	prevStats  Stats
	lastPrint  time.Time
	printMutex sync.Mutex
}

func NewKVServiceWithPartitioning(serverId, numServers int) *KVService {
	kvs := &KVService{}
	kvs.lastPrint = time.Now()

	// Initialize only the bank accounts that this server is responsible for
	// Accounts are partitioned by account_id % num_servers == server_id
	accountsInitialized := []int{}
	for i := 0; i < 10; i++ {
		if i%numServers == serverId {
			kvs.mp.Store(strconv.Itoa(i), "1000000")
			accountsInitialized = append(accountsInitialized, i)
		}
	}
	log.Printf("%v\n", accountsInitialized)

	return kvs
}

// 2PL Lock management
// NOTE: Caller must hold lockMutex when calling
func (kv *KVService) canAcquireLock(key string, lockType LockType, txnId string) bool {
	locksVal, exists := kv.locks.Load(key)
	if !exists {
		return true // no locks on this key
	}
	locks := locksVal.([]Lock)

	for _, lock := range locks {
		if lock.txnId == txnId {
			// Same transaction already has a lock
			if lockType == ExclusiveLock && lock.lockType == SharedLock {
				// Need to upgrade from shared to exclusive
				return len(locks) == 1 // can only upgrade if this is the only lock
			}
			return true // already has compatible or stronger lock
		}
		if lock.lockType == ExclusiveLock || lockType == ExclusiveLock {
			return false // exclusive lock conflicts with any other lock
		}
	}
	return true // all existing locks are shared and we want shared
}

func (kv *KVService) acquireLock(key string, lockType LockType, txnId string) bool {
	if !kv.canAcquireLock(key, lockType, txnId) {
		return false
	}

	locksVal, _ := kv.locks.Load(key)
	var oldLocks []Lock
	if locksVal != nil {
		oldLocks = locksVal.([]Lock)
	}

	// Always create a new slice to avoid data races
	newLocks := make([]Lock, 0, len(oldLocks)+1)

	// Check if we already have a lock and need to upgrade
	upgraded := false
	for _, lock := range oldLocks {
		if lock.txnId == txnId {
			if lockType == ExclusiveLock && lock.lockType == SharedLock {
				// Upgrade to exclusive - create new lock with upgraded type
				newLocks = append(newLocks, Lock{lockType: ExclusiveLock, txnId: txnId})
				upgraded = true
				if txnVal, exists := kv.transactions.Load(txnId); exists {
					txn := txnVal.(*Transaction)
					txn.locksHeld.Store(key, ExclusiveLock)
				}
			} else {
				// Already have compatible or stronger lock
				return true
			}
		} else {
			newLocks = append(newLocks, lock)
		}
	}

	if upgraded {
		kv.locks.Store(key, newLocks)
		return true
	}

	// Add new lock
	newLock := Lock{lockType: lockType, txnId: txnId}
	newLocks = append(newLocks, newLock)
	kv.locks.Store(key, newLocks)

	// Record lock in transaction
	if txnVal, exists := kv.transactions.Load(txnId); exists {
		txn := txnVal.(*Transaction)
		txn.locksHeld.Store(key, lockType)
	}

	return true
}

func (kv *KVService) releaseLocks(txnId string) {
	// NOTE: Caller must hold lockMutex
	txnVal, exists := kv.transactions.Load(txnId)
	if !exists {
		return
	}
	txn := txnVal.(*Transaction)

	// Iterate over keys this transaction holds
	txn.locksHeld.Range(func(keyVal, _ interface{}) bool {
		key := keyVal.(string)
		locksVal, exists := kv.locks.Load(key)
		if !exists {
			return true
		}
		locks := locksVal.([]Lock)

		newLocks := make([]Lock, 0, len(locks))
		for _, lock := range locks {
			if lock.txnId != txnId {
				newLocks = append(newLocks, lock)
			}
		}
		if len(newLocks) == 0 {
			kv.locks.Delete(key)
		} else {
			kv.locks.Store(key, newLocks)
		}
		return true
	})
}

// Transaction methods
func (kv *KVService) Begin(request *kvs.BeginRequest, response *kvs.BeginResponse) error {
	txnCounter := kv.txnCounter.Add(1)
	txnId := fmt.Sprintf("txn-%d", txnCounter)
	kv.transactions.Store(txnId, NewTransaction(txnId))
	response.TxnId = txnId

	return nil
}

func (kv *KVService) TxnGet(request *kvs.TxnGetRequest, response *kvs.TxnGetResponse) error {
	kv.stats.gets.Add(1)

	txnVal, exists := kv.transactions.Load(request.TxnId)
	if !exists {
		response.Found = false
		return nil
	}
	txn := txnVal.(*Transaction)

	if txn.aborted.Load() {
		response.Found = false
		return nil
	}

	// Check write set first (no lock needed for own writes)
	if value, found := txn.writeSet.Load(request.Key); found {
		response.Value = value.(string)
		response.Found = true
		return nil
	}

	// Need to acquire shared lock - use lockMutex for compound operation
	kv.lockMutex.Lock()
	defer kv.lockMutex.Unlock()

	// Re-check transaction state after acquiring lock
	if txn.aborted.Load() {
		response.Found = false
		return nil
	}

	// Acquire shared lock for read
	if !kv.acquireLock(request.Key, SharedLock, request.TxnId) {
		// Lock acquisition failed - abort transaction
		txn.aborted.Store(true)
		kv.releaseLocks(request.TxnId)
		response.Found = false
		return nil
	}

	// Read from committed data
	if value, found := kv.mp.Load(request.Key); found {
		response.Value = value.(string)
		response.Found = true
		txn.readSet.Store(request.Key, value.(string))
	} else {
		// we assume the map has been initialized
		response.Value = ""
		txn.readSet.Store(request.Key, "")
		response.Found = true
	}

	return nil
}

func (kv *KVService) TxnPut(request *kvs.TxnPutRequest, response *kvs.TxnPutResponse) error {
	kv.lockMutex.Lock()
	defer kv.lockMutex.Unlock()

	kv.stats.puts.Add(1)

	txnVal, exists := kv.transactions.Load(request.TxnId)
	if !exists {
		return nil
	}
	txn := txnVal.(*Transaction)

	if txn.aborted.Load() {
		return nil
	}

	// Acquire exclusive lock for write
	if !kv.acquireLock(request.Key, ExclusiveLock, request.TxnId) {
		// Lock acquisition failed - abort transaction
		txn.aborted.Store(true)
		kv.releaseLocks(request.TxnId)
		return nil
	}

	// Add to write set
	txn.writeSet.Store(request.Key, request.Value)

	return nil
}

func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error {
	kv.lockMutex.Lock()
	defer kv.lockMutex.Unlock()

	txnVal, exists := kv.transactions.Load(request.TxnId)
	if !exists {
		response.Success = false
		kv.releaseLocks(request.TxnId)
		return nil
	}
	txn := txnVal.(*Transaction)

	if txn.aborted.Load() {
		response.Success = false
		kv.releaseLocks(request.TxnId)
		return nil
	}

	// Check if read set is still valid (simple serializability check)
	valid := true
	txn.readSet.Range(func(keyVal, expectedValueVal interface{}) bool {
		key := keyVal.(string)
		expectedValue := expectedValueVal.(string)
		currentValueVal, found := kv.mp.Load(key)

		// If expected value is empty string, accept both "not found" and "empty string"
		if expectedValue == "" {
			if !found {
				return true // Key doesn't exist - matches expectation
			}
			currentValue := currentValueVal.(string)
			if currentValue != "" {
				valid = false // Conflict: expected empty, got non-empty
				return false
			}
			return true
		}

		// Expected value is non-empty
		if !found {
			valid = false
			return false
		}
		currentValue := currentValueVal.(string)
		if currentValue != expectedValue {
			// Conflict detected, abort transaction
			valid = false
			return false
		}
		return true
	})

	if !valid {
		txn.aborted.Store(true)
		response.Success = false
		kv.releaseLocks(request.TxnId)
		kv.transactions.Delete(request.TxnId)
		return nil
	}

	// Apply write set to committed data
	txn.writeSet.Range(func(keyVal, valueVal interface{}) bool {
		key := keyVal.(string)
		value := valueVal.(string)
		kv.mp.Store(key, value)
		return true
	})

	txn.committed.Store(true)
	response.Success = true

	// Only count commits on the leader participant to avoid double counting
	if request.Lead {
		kv.stats.commits.Add(1)
	}

	kv.releaseLocks(request.TxnId) // Release locks after commit
	kv.transactions.Delete(request.TxnId)

	return nil
}

func (kv *KVService) Abort(request *kvs.AbortRequest, response *kvs.AbortResponse) error {
	kv.lockMutex.Lock()
	defer kv.lockMutex.Unlock()

	txnVal, exists := kv.transactions.Load(request.TxnId)
	if exists {
		txn := txnVal.(*Transaction)
		txn.aborted.Store(true)
		kv.releaseLocks(request.TxnId) // Release locks on abort
		kv.transactions.Delete(request.TxnId)
	}

	return nil
}

// 2PC Phase 1: Prepare
func (kv *KVService) Prepare(request *kvs.PrepareRequest, response *kvs.PrepareResponse) error {
	kv.lockMutex.Lock()
	defer kv.lockMutex.Unlock()

	response.TxnId = request.TxnId
	response.Vote = false // default to NO

	txnVal, exists := kv.transactions.Load(request.TxnId)
	if !exists {
		log.Printf("DEBUG SERVER: Transaction %s not found, voting NO\n", request.TxnId)
		return nil // Vote NO
	}
	txn := txnVal.(*Transaction)

	if txn.aborted.Load() {
		log.Printf("DEBUG SERVER: Transaction %s already aborted, voting NO\n", request.TxnId)
		return nil // Vote NO
	}

	// Check if read set is still valid
	valid := true
	txn.readSet.Range(func(keyVal, expectedValueVal interface{}) bool {
		key := keyVal.(string)
		expectedValue := expectedValueVal.(string)
		currentValueVal, found := kv.mp.Load(key)

		// If expected value is empty string, accept both "not found" and "empty string"
		if expectedValue == "" {
			if !found {
				// Key doesn't exist - matches our expectation
				return true
			}
			currentValue := currentValueVal.(string)
			if currentValue != "" {
				// Key exists but has non-empty value - conflict!
				log.Printf("DEBUG SERVER: Conflict on key %s (expected empty, got %s), voting NO\n", key, currentValue)
				valid = false
				return false
			}
			// Key exists with empty value - ok
			return true
		}

		// Expected value is non-empty
		if !found {
			log.Printf("DEBUG SERVER: Key %s not found in database (expected %s), voting NO\n", key, expectedValue)
			valid = false
			return false
		}
		currentValue := currentValueVal.(string)
		if currentValue != expectedValue {
			// Conflict detected, vote NO
			log.Printf("DEBUG SERVER: Conflict detected on key %s (expected=%s, current=%s), voting NO\n", key, expectedValue, currentValue)
			valid = false
			return false
		}
		return true
	})

	if !valid {
		// Vote NO - but DON'T release locks or delete transaction!
		// The coordinator will send GlobalAbort to clean up uniformly across all participants.
		// This is critical for 2PC correctness: locks must be held until coordinator decision.
		return nil
	}

	// Vote YES - mark as prepared but don't commit yet
	// CRITICAL: Keep holding locks until GlobalCommit/GlobalAbort arrives!
	log.Printf("DEBUG SERVER: Transaction %s voting YES\n", request.TxnId)
	txn.prepared.Store(true)
	response.Vote = true
	return nil
}

// 2PC Phase 2: Global Commit
func (kv *KVService) GlobalCommit(request *kvs.GlobalCommitRequest, response *kvs.GlobalCommitResponse) error {
	kv.lockMutex.Lock()
	defer kv.lockMutex.Unlock()

	txnVal, exists := kv.transactions.Load(request.TxnId)
	if !exists {
		return nil // Transaction not found (shouldn't happen in correct 2PC)
	}
	txn := txnVal.(*Transaction)

	if !txn.prepared.Load() {
		// Transaction was not prepared - this violates 2PC protocol
		// Keep for defensive programming
		return nil
	}

	// Note: We don't check txn.aborted here because in correct 2PC flow,
	// coordinator only sends GlobalCommit if all participants voted YES.
	// But we keep it for safety in case of bugs or network issues.
	if txn.aborted.Load() {
		return nil
	}

	// Apply write set to committed data
	txn.writeSet.Range(func(keyVal, valueVal interface{}) bool {
		key := keyVal.(string)
		value := valueVal.(string)
		kv.mp.Store(key, value)
		return true
	})

	txn.committed.Store(true)

	// Only count commits on the leader participant to avoid double counting
	if request.Lead {
		kv.stats.commits.Add(1)
	}

	kv.releaseLocks(request.TxnId) // Release locks after commit
	kv.transactions.Delete(request.TxnId)

	return nil
}

// 2PC Phase 2: Global Abort
func (kv *KVService) GlobalAbort(request *kvs.GlobalAbortRequest, response *kvs.GlobalAbortResponse) error {
	kv.lockMutex.Lock()
	defer kv.lockMutex.Unlock()

	txnVal, exists := kv.transactions.Load(request.TxnId)
	if exists {
		txn := txnVal.(*Transaction)
		txn.aborted.Store(true)
		kv.releaseLocks(request.TxnId) // Release locks on abort
		kv.transactions.Delete(request.TxnId)
	}

	return nil
}

func (kv *KVService) printStats() {
	kv.printMutex.Lock()

	// Read atomic values - don't copy atomic.Uint64 directly
	currentPuts := kv.stats.puts.Load()
	currentGets := kv.stats.gets.Load()
	currentCommits := kv.stats.commits.Load()

	prevPuts := kv.prevStats.puts.Load()
	prevGets := kv.prevStats.gets.Load()
	prevCommits := kv.prevStats.commits.Load()

	// Update prevStats with current values
	kv.prevStats.puts.Store(currentPuts)
	kv.prevStats.gets.Store(currentGets)
	kv.prevStats.commits.Store(currentCommits)

	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.printMutex.Unlock()

	// Calculate differences
	diffPuts := currentPuts - prevPuts
	diffGets := currentGets - prevGets
	diffCommits := currentCommits - prevCommits
	deltaS := now.Sub(lastPrint).Seconds()

	log.Printf("get/s %0.2f\nput/s %0.2f\ncommit/s %0.2f\nops/s %0.2f\n\n",
		float64(diffGets)/deltaS,
		float64(diffPuts)/deltaS,
		float64(diffCommits)/deltaS,
		float64(diffGets+diffPuts)/deltaS)
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	serverId := flag.Int("server-id", 0, "Server ID for account partitioning (0-based)")
	numServers := flag.Int("num-servers", 1, "Total number of servers in the cluster")
	flag.Parse()

	kvs := NewKVServiceWithPartitioning(*serverId, *numServers)
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	log.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
