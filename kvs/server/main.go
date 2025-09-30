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
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Stats struct {
	puts    uint64
	gets    uint64
	commits uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts = s.puts - prev.puts
	r.gets = s.gets - prev.gets
	r.commits = s.commits - prev.commits
	return r
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
	readSet   map[string]string
	writeSet  map[string]string
	locksHeld map[string]LockType // keys locked by this transaction
	committed bool
	aborted   bool
	prepared  bool // for 2PC
	timestamp time.Time
}

func NewTransaction(id string) *Transaction {
	return &Transaction{
		id:        id,
		readSet:   make(map[string]string),
		writeSet:  make(map[string]string),
		locksHeld: make(map[string]LockType),
		committed: false,
		aborted:   false,
		prepared:  false,
		timestamp: time.Now(),
	}
}

type KVService struct {
	sync.Mutex
	mp           map[string]string
	transactions map[string]*Transaction
	locks        map[string][]Lock // key -> list of locks
	txnCounter   uint64
	stats        Stats
	prevStats    Stats
	lastPrint    time.Time
}

func NewKVServiceWithPartitioning(serverId, numServers int) *KVService {
	kvs := &KVService{}
	kvs.mp = make(map[string]string)
	kvs.transactions = make(map[string]*Transaction)
	kvs.locks = make(map[string][]Lock)
	kvs.txnCounter = 0
	kvs.lastPrint = time.Now()

	// Initialize only the bank accounts that this server is responsible for
	// Accounts are partitioned by account_id % num_servers == server_id
	accountsInitialized := []int{}
	for i := 0; i < 10; i++ {
		if i%numServers == serverId {
			kvs.mp[strconv.Itoa(i)] = "1000000"
			accountsInitialized = append(accountsInitialized, i)
		}
	}
	fmt.Printf("%v\n", accountsInitialized)

	return kvs
}

// 2PL Lock management
func (kv *KVService) canAcquireLock(key string, lockType LockType, txnId string) bool {
	locks, exists := kv.locks[key]
	if !exists {
		return true // no locks on this key
	}

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

	locks := kv.locks[key]

	// Check if we already have a lock and need to upgrade
	for i, lock := range locks {
		if lock.txnId == txnId {
			if lockType == ExclusiveLock && lock.lockType == SharedLock {
				// Upgrade to exclusive
				kv.locks[key][i].lockType = ExclusiveLock
			}
			return true
		}
	}

	// Add new lock
	newLock := Lock{lockType: lockType, txnId: txnId}
	kv.locks[key] = append(locks, newLock)

	// Record lock in transaction
	if txn, exists := kv.transactions[txnId]; exists {
		txn.locksHeld[key] = lockType
	}

	return true
}

func (kv *KVService) releaseLocks(txnId string) {
	// Remove all locks held by this transaction
	for key := range kv.locks {
		locks := kv.locks[key]
		newLocks := make([]Lock, 0)
		for _, lock := range locks {
			if lock.txnId != txnId {
				newLocks = append(newLocks, lock)
			}
		}
		if len(newLocks) == 0 {
			delete(kv.locks, key)
		} else {
			kv.locks[key] = newLocks
		}
	}
} // Transaction methods
func (kv *KVService) Begin(request *kvs.BeginRequest, response *kvs.BeginResponse) error {
	kv.Lock()
	defer kv.Unlock()

	kv.txnCounter++
	txnId := fmt.Sprintf("txn-%d", kv.txnCounter)
	kv.transactions[txnId] = NewTransaction(txnId)
	response.TxnId = txnId

	return nil
}

func (kv *KVService) TxnGet(request *kvs.TxnGetRequest, response *kvs.TxnGetResponse) error {
	kv.Lock()
	defer kv.Unlock()

	kv.stats.gets++

	txn, exists := kv.transactions[request.TxnId]
	if !exists || txn.aborted {
		response.Found = false
		return nil
	}

	// Check write set first (no lock needed for own writes)
	if value, found := txn.writeSet[request.Key]; found {
		response.Value = value
		response.Found = true
		return nil
	}

	// Acquire shared lock for read
	if !kv.acquireLock(request.Key, SharedLock, request.TxnId) {
		// Lock acquisition failed - abort transaction
		txn.aborted = true
		kv.releaseLocks(request.TxnId)
		response.Found = false
		return nil
	}

	// Read from committed data
	if value, found := kv.mp[request.Key]; found {
		response.Value = value
		response.Found = true
		txn.readSet[request.Key] = value
	} else {
		// we assume the map has been initialized, since the read far more than writes
		// otherwise, the transaction would always abort since we can't find the key
		response.Value = ""
		txn.readSet[request.Key] = ""
		response.Found = true
	}

	return nil
}

func (kv *KVService) TxnPut(request *kvs.TxnPutRequest, response *kvs.TxnPutResponse) error {
	kv.Lock()
	defer kv.Unlock()

	kv.stats.puts++

	txn, exists := kv.transactions[request.TxnId]
	if !exists || txn.aborted {
		return nil
	}

	// Acquire exclusive lock for write
	if !kv.acquireLock(request.Key, ExclusiveLock, request.TxnId) {
		// Lock acquisition failed - abort transaction
		txn.aborted = true
		kv.releaseLocks(request.TxnId)
		return nil
	}

	// Add to write set
	txn.writeSet[request.Key] = request.Value

	return nil
}

func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error {
	kv.Lock()
	defer kv.Unlock()

	txn, exists := kv.transactions[request.TxnId]
	if !exists || txn.aborted {
		response.Success = false
		kv.releaseLocks(request.TxnId)
		return nil
	}

	// Check if read set is still valid (simple serializability check)
	// why this could be necessary even for 2PC?
	for key, expectedValue := range txn.readSet {
		if currentValue, found := kv.mp[key]; !found || currentValue != expectedValue {
			// Conflict detected, abort transaction
			txn.aborted = true
			response.Success = false
			kv.releaseLocks(request.TxnId)
			delete(kv.transactions, request.TxnId)
			return nil
		}
	}

	// Apply write set to committed data
	for key, value := range txn.writeSet {
		kv.mp[key] = value
	}

	txn.committed = true
	response.Success = true

	// Only count commits on the leader participant to avoid double counting
	if request.Lead {
		kv.stats.commits++
	}

	kv.releaseLocks(request.TxnId) // Release locks after commit
	delete(kv.transactions, request.TxnId)

	return nil
}

func (kv *KVService) Abort(request *kvs.AbortRequest, response *kvs.AbortResponse) error {
	kv.Lock()
	defer kv.Unlock()

	txn, exists := kv.transactions[request.TxnId]
	if exists {
		txn.aborted = true
		kv.releaseLocks(request.TxnId) // Release locks on abort
		delete(kv.transactions, request.TxnId)
	}

	return nil
}

// 2PC Phase 1: Prepare
func (kv *KVService) Prepare(request *kvs.PrepareRequest, response *kvs.PrepareResponse) error {
	kv.Lock()
	defer kv.Unlock()

	response.TxnId = request.TxnId
	response.Vote = false // default to NO

	txn, exists := kv.transactions[request.TxnId]
	if !exists {
		// fmt.Printf("DEBUG SERVER: Transaction %s does not exist, voting NO\n", request.TxnId)
		return nil // Vote NO
	}
	if txn.aborted {
		// fmt.Printf("DEBUG SERVER: Transaction %s already aborted, voting NO\n", request.TxnId)
		return nil // Vote NO
	}

	// Check if read set is still valid
	for key, expectedValue := range txn.readSet {
		currentValue, found := kv.mp[key]

		// is it necessary to check found?
		if currentValue != expectedValue {
			// Conflict detected, vote NO
			fmt.Printf("DEBUG SERVER: Checking key %s: expected=%s, current=%s, found=%t\n",
				key, expectedValue, currentValue, found)
			fmt.Printf("DEBUG SERVER: Conflict detected on key %s, voting NO\n", key)
			txn.aborted = true
			kv.releaseLocks(request.TxnId)
			delete(kv.transactions, request.TxnId)
			return nil
		}
	}

	// Vote YES - mark as prepared but don't commit yet
	// fmt.Printf("DEBUG SERVER: All reads valid, voting YES for txn %s\n", request.TxnId)
	txn.prepared = true
	response.Vote = true
	return nil
}

// 2PC Phase 2: Global Commit
func (kv *KVService) GlobalCommit(request *kvs.GlobalCommitRequest, response *kvs.GlobalCommitResponse) error {
	kv.Lock()
	defer kv.Unlock()

	txn, exists := kv.transactions[request.TxnId]
	if !exists || txn.aborted || !txn.prepared {
		return nil // Transaction not prepared or already aborted
	}

	// Apply write set to committed data
	for key, value := range txn.writeSet {
		kv.mp[key] = value
	}

	txn.committed = true

	// Only count commits on the leader participant to avoid double counting
	if request.Lead {
		kv.stats.commits++
	}

	kv.releaseLocks(request.TxnId) // Release locks after commit
	delete(kv.transactions, request.TxnId)

	return nil
}

// 2PC Phase 2: Global Abort
func (kv *KVService) GlobalAbort(request *kvs.GlobalAbortRequest, response *kvs.GlobalAbortResponse) error {
	kv.Lock()
	defer kv.Unlock()

	txn, exists := kv.transactions[request.TxnId]
	if exists {
		txn.aborted = true
		kv.releaseLocks(request.TxnId) // Release locks on abort
		delete(kv.transactions, request.TxnId)
	}

	return nil
}

func (kv *KVService) printStats() {
	kv.Lock()
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.Unlock()

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\ncommit/s %0.2f\nops/s %0.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.commits)/deltaS,
		float64(diff.gets+diff.puts)/deltaS)
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

	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
