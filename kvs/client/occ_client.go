package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
	"github.com/rstutsman/cs6450-labs/kvs/cache"
)

// Global OCC metrics
// TODO: Can potentially affect efficiency. Low priority.
var (
	occCommits  atomic.Uint64
	occAborts   atomic.Uint64
	cacheHits   atomic.Uint64
	cacheMisses atomic.Uint64
)

// OCCClient wraps RPC client with OCC-specific operations and cache
type OCCClient struct {
	rpcClient     *rpc.Client
	clientId      string
	cacheStrategy cache.CacheStrategy
	callbackHost  string // Host:port for receiving invalidations
}

// OCCDistributedClient manages multiple OCC clients with caching
type OCCDistributedClient struct {
	clients       []*OCCClient // a list of client instances
	numServers    int
	cacheStrategy cache.CacheStrategy
	callbackHost  string
}

// OCCClientInvalidationService handles invalidation RPCs from servers
type OCCClientInvalidationService struct {
	strategy cache.CacheStrategy
}

// ReceiveInvalidation handles invalidation RPC from server
func (s *OCCClientInvalidationService) ReceiveInvalidation(request *kvs.InvalidationRequest, response *kvs.InvalidationResponse) error {
	// log.Printf("Received invalidation for key %s (value=%s, version=%d)\n", request.Key, request.Value, request.Version)
	if proactiveStrategy, ok := s.strategy.(*cache.ProactiveInvalidationStrategy); ok {
		proactiveStrategy.OnInvalidate(request.Key, request.Value, request.Version)
		log.Printf("Updated cache for key %s with new value\n", request.Key)
	}
	return nil
}

// NewOCCClient creates a new OCC client with cache strategy
func NewOCCClient(addr string, clientId string, strategy cache.CacheStrategy, callbackHost string) *OCCClient {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	return &OCCClient{
		rpcClient:     rpcClient,
		clientId:      clientId,
		cacheStrategy: strategy,
		callbackHost:  callbackHost,
	}
}

// NewOCCDistributedClient creates a distributed OCC client
func NewOCCDistributedClient(hosts []string, clientId string, strategy cache.CacheStrategy, callbackHost string) *OCCDistributedClient {
	clients := make([]*OCCClient, len(hosts))
	for i, host := range hosts {
		clients[i] = NewOCCClient(host, fmt.Sprintf("%s-%d", clientId, i), strategy, callbackHost)
	}
	return &OCCDistributedClient{
		clients:       clients,
		numServers:    len(hosts),
		cacheStrategy: strategy,
		callbackHost:  callbackHost,
	}
}

// OCCBegin starts a new OCC transaction
func (client *OCCClient) OCCBegin() string {
	reqClientId := client.clientId
	reqCallbackHost := client.callbackHost
	if client.cacheStrategy.GetName() == "no-cache" {
		reqClientId = ""
		reqCallbackHost = ""
	}

	request := kvs.OCCBeginRequest{
		ClientId:           reqClientId,
		ClientCallbackHost: reqCallbackHost,
	}
	response := kvs.OCCBeginResponse{}
	err := client.rpcClient.Call("OCCKVService.OCCBegin", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	return response.TxnId
}

// OCCTxnGet reads a value, using cache if possible
func (client *OCCClient) OCCTxnGet(txnId string, key string) (string, uint64, bool) {
	// Try cache first
	if cachedEntry, found := client.cacheStrategy.OnRead(key); found {
		cacheHits.Add(1)
		return cachedEntry.Value, cachedEntry.Version, true
	}

	// Cache miss - fetch from server
	cacheMisses.Add(1)

	// Optimization: Don't send ClientId for no-cache strategy to avoid server tracking overhead
	reqClientId := client.clientId
	if client.cacheStrategy.GetName() == "no-cache" {
		reqClientId = ""
	}

	request := kvs.OCCTxnGetRequest{
		TxnId:    txnId,
		Key:      key,
		ClientId: reqClientId,
	}
	response := kvs.OCCTxnGetResponse{}
	err := client.rpcClient.Call("OCCKVService.OCCTxnGet", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	// Update cache with server response
	if response.Found {
		client.cacheStrategy.OnServerRead(key, response.Value, response.Version)
	}

	return response.Value, response.Version, response.Found
}

// OCCTxnPut writes a value (stored locally, committed at validation)
func (client *OCCClient) OCCTxnPut(txnId string, key string, value string) {
	request := kvs.OCCTxnPutRequest{
		TxnId: txnId,
		Key:   key,
		Value: value,
	}
	response := kvs.OCCTxnPutResponse{}
	err := client.rpcClient.Call("OCCKVService.OCCTxnPut", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

// OCCPrepare validates transaction and locks keys (Phase 1 of 2PC)
func (client *OCCClient) OCCPrepare(txnId string, readSet []kvs.VersionedValue, writeSet []kvs.VersionedValue, lead bool) bool {
	request := kvs.OCCValidateRequest{
		TxnId:    txnId,
		ReadSet:  readSet,
		WriteSet: writeSet,
		Lead:     lead,
		ClientId: client.clientId,
	}
	response := kvs.OCCValidateResponse{}
	err := client.rpcClient.Call("OCCKVService.OCCPrepare", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	return response.Success
}

// OCCCommit commits a prepared transaction (Phase 2 of 2PC)
func (client *OCCClient) OCCCommit(txnId string, readSet []kvs.VersionedValue, writeSet []kvs.VersionedValue, lead bool) bool {
	request := kvs.OCCValidateRequest{
		TxnId:    txnId,
		ReadSet:  readSet,
		WriteSet: writeSet,
		Lead:     lead,
		ClientId: client.clientId,
	}
	response := kvs.OCCValidateResponse{}
	err := client.rpcClient.Call("OCCKVService.OCCCommit", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	return response.Success
}

// OCCAbortPrepared aborts a prepared transaction
func (client *OCCClient) OCCAbortPrepared(txnId string, readSet []kvs.VersionedValue, writeSet []kvs.VersionedValue, lead bool) {
	request := kvs.OCCValidateRequest{
		TxnId:    txnId,
		ReadSet:  readSet,
		WriteSet: writeSet,
		Lead:     lead,
		ClientId: client.clientId,
	}
	response := kvs.OCCValidateResponse{}
	err := client.rpcClient.Call("OCCKVService.OCCAbortPrepared", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

// OCCAbort aborts a transaction
func (client *OCCClient) OCCAbort(txnId string) {
	request := kvs.AbortRequest{TxnId: txnId}
	response := kvs.AbortResponse{}
	err := client.rpcClient.Call("OCCKVService.OCCAbort", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

// executeOCCTransaction executes a transaction using OCC
func executeOCCTransaction(dc *OCCDistributedClient, txn kvs.Transaction) bool {
	txnDesc := formatTransaction(txn)
	// log.Printf("OCC TRANSACTION START: %s (TxnType=%d)\n", txnDesc, txn.TxnType)

	// Determine participating servers
	participantIds := make(map[int]bool)
	for _, op := range txn.Operations {
		serverId := int(op.Key) % dc.numServers // Simple server selection based on key hash
		participantIds[serverId] = true
	}

	participants := make([]int, 0, len(participantIds))
	for serverId := range participantIds {
		participants = append(participants, serverId)
	}

	// Begin transaction on all servers
	txnIds := make(map[int]string)
	for _, serverId := range participants {
		txnId := dc.clients[serverId].OCCBegin()
		txnIds[serverId] = txnId
	}

	// Track read and write sets for validation
	readSets := make(map[int][]kvs.VersionedValue)
	writeSets := make(map[int][]kvs.VersionedValue)
	for _, serverId := range participants {
		readSets[serverId] = []kvs.VersionedValue{}
		writeSets[serverId] = []kvs.VersionedValue{}
	}

	// Local cache for this transaction
	localReadCache := make(map[string]*cache.CacheEntry)
	localWriteCache := make(map[string]*cache.CacheEntry)

	// Execute operations optimistically
	readValues := make(map[uint64]string)
	for _, op := range txn.Operations {
		keyStr := strconv.FormatUint(op.Key, 10)
		serverId := int(op.Key) % dc.numServers
		client := dc.clients[serverId]
		txnId := txnIds[serverId]

		if op.OpType == kvs.TxnGet {
			value, version, found := client.OCCTxnGet(txnId, keyStr)
			if !found {
				// Abort all participants
				for _, pServerId := range participants {
					dc.clients[pServerId].OCCAbort(txnIds[pServerId])
				}
				dc.cacheStrategy.OnAbort(localReadCache, localWriteCache)
				log.Printf("OCC TRANSACTION FAILED (KEY_NOT_FOUND): %s\n", txnDesc)
				return false
			}
			readValues[op.Key] = value

			// Track in read set for validation
			readSets[serverId] = append(readSets[serverId], kvs.VersionedValue{
				Key:     keyStr,
				Value:   value,
				Version: version,
			})

			// Track in local cache
			localReadCache[keyStr] = &cache.CacheEntry{
				Key:       keyStr,
				Value:     value,
				Version:   version,
				Timestamp: time.Now(),
			}

		} else { // TxnPut
			var valueToWrite string

			// Handle transaction-specific logic
			if txn.TxnType == kvs.PaymentTxn {
				currentValue, exists := readValues[op.Key]
				if !exists {
					for _, pServerId := range participants {
						dc.clients[pServerId].OCCAbort(txnIds[pServerId])
					}
					dc.cacheStrategy.OnAbort(localReadCache, localWriteCache)
					log.Printf("OCC TRANSACTION FAILED (PAYMENT_ACCOUNT_NOT_FOUND): %s\n", txnDesc)
					return false
				}

				currentBalance, err := strconv.ParseUint(currentValue, 10, 64)
				if err != nil {
					for _, pServerId := range participants {
						dc.clients[pServerId].OCCAbort(txnIds[pServerId])
					}
					dc.cacheStrategy.OnAbort(localReadCache, localWriteCache)
					log.Printf("OCC TRANSACTION FAILED (INVALID_BALANCE): %s\n", txnDesc)
					return false
				}

				fromAccount := txn.Operations[0].Key
				if op.Key == fromAccount {
					if currentBalance < txn.Amount {
						for _, pServerId := range participants {
							dc.clients[pServerId].OCCAbort(txnIds[pServerId])
						}
						dc.cacheStrategy.OnAbort(localReadCache, localWriteCache)
						log.Printf("OCC TRANSACTION FAILED (INSUFFICIENT_FUNDS): %s\n", txnDesc)
						return false
					}
					valueToWrite = strconv.FormatUint(currentBalance-txn.Amount, 10)
				} else {
					valueToWrite = strconv.FormatUint(currentBalance+txn.Amount, 10)
				}
			} else {
				valueToWrite = op.Value
			}

			client.OCCTxnPut(txnId, keyStr, valueToWrite)

			// Track in write set for validation
			writeSets[serverId] = append(writeSets[serverId], kvs.VersionedValue{
				Key:   keyStr,
				Value: valueToWrite,
			})

			// Track in local write cache (version will be incremented on commit)
			if entry, found := localReadCache[keyStr]; found {
				localWriteCache[keyStr] = &cache.CacheEntry{
					Key:       keyStr,
					Value:     valueToWrite,
					Version:   entry.Version, // Will be incremented by server
					Timestamp: time.Now(),
				}
			} else {
				localWriteCache[keyStr] = &cache.CacheEntry{
					Key:       keyStr,
					Value:     valueToWrite,
					Version:   0,
					Timestamp: time.Now(),
				}
			}
		}
	}

	// OCC 2-Phase Commit Protocol
	// Phase 1: Prepare (validate and lock) on all servers
	allValid := true
	leader := participants[0]
	for _, serverId := range participants {
		if serverId < leader {
			leader = serverId
		}
	}

	// Prepare on all servers
	for _, serverId := range participants {
		isLeader := (serverId == leader)
		success := dc.clients[serverId].OCCPrepare(
			txnIds[serverId],
			readSets[serverId],
			writeSets[serverId],
			isLeader,
		)
		if !success {
			allValid = false
			log.Printf("OCC PREPARE FAILED for txn type %d on server %d: %s\n", txn.TxnType, serverId, txnDesc)
			break
		}
	}

	// Phase 2: Commit or Abort based on prepare results
	if allValid {
		// All servers prepared successfully - commit on all
		for _, serverId := range participants {
			isLeader := (serverId == leader)
			dc.clients[serverId].OCCCommit(
				txnIds[serverId],
				readSets[serverId],
				writeSets[serverId],
				isLeader,
			)
		}

		dc.cacheStrategy.OnCommit(localReadCache, localWriteCache)
		occCommits.Add(1)
		// Post-commit verification for verification transactions
		log.Printf("DEBUG: txn.TxnType = %d, VerificationTxn = %d", txn.TxnType, kvs.VerificationTxn)
		if txn.TxnType == kvs.VerificationTxn {
			log.Printf("DEBUG: Inside verification block")
			var totalSum uint64 = 0
			accountBalances := make([]uint64, 10)
			for i := uint64(0); i < 10; i++ {
				if value, exists := readValues[i]; exists {
					if balance, err := strconv.ParseUint(value, 10, 64); err == nil {
						totalSum += balance
						accountBalances[i] = balance
					}
				}
			}

			if totalSum != txn.Amount {
				log.Printf("VERIFICATION FAILED: Expected %d, got %d\n", txn.Amount, totalSum)
			} else {
				log.Printf("VERIFICATION SUCCESS: Total=%d, Balances=%v\n", totalSum, accountBalances)
			}
		}

		return true
	} else {
		// At least one server failed prepare - abort on all
		for _, serverId := range participants {
			isLeader := (serverId == leader)
			dc.clients[serverId].OCCAbortPrepared(
				txnIds[serverId],
				readSets[serverId],
				writeSets[serverId],
				isLeader,
			)
		}
		dc.cacheStrategy.OnAbort(localReadCache, localWriteCache)
		occAborts.Add(1)
		log.Printf("OCC TRANSACTION FAILED (VALIDATION_FAILED): %s\n", txnDesc)
		return false
	}
}

// runOCCTransactionClient runs OCC transactions with retry
func runOCCTransactionClient(hosts []string, done interface{}, workload kvs.TransactionWorkload,
	sleep time.Duration, clientId string, strategy cache.CacheStrategy, callbackHost string) {
	dc := NewOCCDistributedClient(hosts, clientId, strategy, callbackHost)

	// Type assertion for done flag
	var doneFlag interface{ Load() bool }
	if df, ok := done.(interface{ Load() bool }); ok {
		doneFlag = df
	} else {
		log.Fatal("Invalid done flag type")
	}

	for !doneFlag.Load() {
		txn := workload.NextTransaction()

		// Execute with retry on abort
		for attempt := 0; ; attempt++ {
			if doneFlag.Load() {
				return
			}
			if executeOCCTransaction(dc, txn) {
				break
			}
			// Exponential backoff on conflicts
			time.Sleep(time.Millisecond * time.Duration(1+attempt))
		}

		if sleep > 0 {
			time.Sleep(sleep)
		}
	}
}

// startInvalidationServer starts an RPC server to receive invalidations from servers
// Returns the callback host:port address
func startInvalidationServer(strategy cache.CacheStrategy) string {
	invalidationService := &OCCClientInvalidationService{strategy: strategy}

	// Create a NEW RPC server instance (not the global default)
	rpcServer := rpc.NewServer()
	rpcServer.Register(invalidationService)

	// Listen on a random available port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("Failed to start invalidation server:", err)
	}

	// Get the actual port assigned
	port := listener.Addr().(*net.TCPAddr).Port

	// Get the actual hostname (not localhost) for distributed systems
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Warning: Failed to get hostname, using localhost: %v", err)
		hostname = "localhost"
	}

	callbackHost := fmt.Sprintf("%s:%d", hostname, port)

	// Create a new HTTP mux for this RPC server
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)

	// Start serving HTTP in background with the dedicated mux
	go http.Serve(listener, mux)

	return callbackHost
}
