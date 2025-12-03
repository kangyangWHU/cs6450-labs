package main

import (
	"flag"
	"fmt"
	"io"

	// "fmt"
	"log"
	"net/rpc"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
	"github.com/rstutsman/cs6450-labs/kvs/cache"
)

var (
	twoPLCommits atomic.Uint64
	twoPLAborts  atomic.Uint64
)

type HostList []string

// `String` returns a string representation of the HostList.
// It formats the list of hosts into a human-readable string.
func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

// Set implements flag.Value for HostList. It replaces the receiver's contents
// with the result of strings.Split(value, ",") and always returns nil.
// Note: elements are not trimmed and empty elements are preserved (for example,
// "" -> []string{""} and "a,,b" -> []string{"a", "", "b"}). This method does
// not perform hostname validation or normalization.
func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

type Client struct {
	rpcClient *rpc.Client
}

type DistributedClient struct {
	clients    []*Client
	numServers int
}

func NewClient(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	return &Client{rpcClient}
}

func NewDistributedClient(hosts []string) *DistributedClient {
	clients := make([]*Client, len(hosts))
	for i, host := range hosts {
		clients[i] = NewClient(host)
	}
	return &DistributedClient{
		clients:    clients,
		numServers: len(hosts),
	}
}

func (client *Client) Begin() string {
	request := kvs.BeginRequest{}
	response := kvs.BeginResponse{}
	err := client.rpcClient.Call("KVService.Begin", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	return response.TxnId
}

func (client *Client) TxnGet(txnId string, key string) (string, bool) {
	request := kvs.TxnGetRequest{TxnId: txnId, Key: key}
	response := kvs.TxnGetResponse{}
	err := client.rpcClient.Call("KVService.TxnGet", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	return response.Value, response.Found
}

func (client *Client) TxnPut(txnId string, key string, value string) {
	request := kvs.TxnPutRequest{TxnId: txnId, Key: key, Value: value}
	response := kvs.TxnPutResponse{}
	err := client.rpcClient.Call("KVService.TxnPut", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

func (client *Client) Prepare(txnId string) bool {
	request := kvs.PrepareRequest{TxnId: txnId}
	response := kvs.PrepareResponse{}
	err := client.rpcClient.Call("KVService.Prepare", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	return response.Vote
}

func (client *Client) GlobalCommit(txnId string, lead bool) {
	request := kvs.GlobalCommitRequest{TxnId: txnId, Lead: lead}
	response := kvs.GlobalCommitResponse{}
	err := client.rpcClient.Call("KVService.GlobalCommit", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

func (client *Client) GlobalAbort(txnId string) {
	request := kvs.GlobalAbortRequest{TxnId: txnId}
	response := kvs.GlobalAbortResponse{}
	err := client.rpcClient.Call("KVService.GlobalAbort", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

// Helper function to abort transaction on all participants
func abortTransaction(dc *DistributedClient, participants []int, txnIds map[int]string, txnDesc string, reason string) bool {
	for _, pServerId := range participants {
		dc.clients[pServerId].GlobalAbort(txnIds[pServerId])
	}
	log.Printf("TRANSACTION FAILED (%s): %s\n", reason, txnDesc)
	return false
}

// Helper function to format transaction description for logging
func formatTransaction(txn kvs.Transaction) string {
	switch txn.TxnType {
	case kvs.PaymentTxn:
		fromAccount := txn.Operations[0].Key
		toAccount := txn.Operations[1].Key
		return fmt.Sprintf("PAYMENT: Transfer $%d from account %d to account %d", txn.Amount, fromAccount, toAccount)
	case kvs.VerificationTxn:
		return fmt.Sprintf("VERIFICATION: Check sum of accounts 0-9 equals %d", txn.Amount)
	case kvs.RegularTxn:
		opCount := len(txn.Operations)
		readCount := 0
		writeCount := 0
		for _, op := range txn.Operations {
			if op.OpType == kvs.TxnGet {
				readCount++
			} else {
				writeCount++
			}
		}
		return fmt.Sprintf("REGULAR: %d ops (%d reads, %d writes)", opCount, readCount, writeCount)
	default:
		return fmt.Sprintf("UNKNOWN: %d operations", len(txn.Operations))
	}
}

// Unified transaction execution function that handles all transaction types
func executeTransaction(dc *DistributedClient, txn kvs.Transaction) bool {
	// Log transaction start
	txnDesc := formatTransaction(txn)
	log.Printf("TRANSACTION START: %s\n", txnDesc)

	// Determine all servers involved in this transaction
	participantIds := make(map[int]bool)
	for _, op := range txn.Operations {
		if op.OpType == kvs.TxnGet || op.OpType == kvs.TxnPut {
			// Map key to server using same partitioning logic
			serverId := int(op.Key) % dc.numServers
			participantIds[serverId] = true
		}
	}

	// Convert to slice for easier iteration
	participants := make([]int, 0, len(participantIds))
	for serverId := range participantIds {
		participants = append(participants, serverId)
	}

	// Begin transaction on all involved servers
	txnIds := make(map[int]string)
	for _, serverId := range participants {
		txnId := dc.clients[serverId].Begin()
		txnIds[serverId] = txnId
	}

	// Execute operations - unified loop for all transaction types
	readValues := make(map[uint64]string) // Store read values for validation
	for _, op := range txn.Operations {
		// Determine which server handles this key
		// key_str := fmt.Sprintf("%d", op.Key)
		key_str := strconv.FormatUint(op.Key, 10)
		serverId := int(op.Key) % dc.numServers
		client := dc.clients[serverId]
		txnId := txnIds[serverId]

		if op.OpType == kvs.TxnGet {
			value, found := client.TxnGet(txnId, key_str)
			if !found {
				// Key not found, abort transaction
				return abortTransaction(dc, participants, txnIds, txnDesc, "KEY_NOT_FOUND")
			}
			readValues[op.Key] = value
		} else {
			// Handle PUT operations with transaction-specific logic
			var valueToWrite string

			if txn.TxnType == kvs.PaymentTxn {
				// Payment transaction: calculate new balance based on transfer
				currentValue, exists := readValues[op.Key]
				if !exists {
					return abortTransaction(dc, participants, txnIds, txnDesc, "PAYMENT_ACCOUNT_NOT_FOUND")
				}

				currentBalance, err := strconv.ParseUint(currentValue, 10, 64)
				if err != nil {
					return abortTransaction(dc, participants, txnIds, txnDesc, "INVALID_BALANCE")
				}

				// Determine if this is the from account (first PUT) or to account (second PUT)
				fromAccount := txn.Operations[0].Key // First GET is from account
				if op.Key == fromAccount {
					// This is the from account - subtract amount
					if currentBalance < txn.Amount {
						return abortTransaction(dc, participants, txnIds, txnDesc, "INSUFFICIENT_FUNDS")
					}
					newBalance := currentBalance - txn.Amount
					valueToWrite = strconv.FormatUint(newBalance, 10)
				} else {
					// This is the to account - add amount
					newBalance := currentBalance + txn.Amount
					valueToWrite = strconv.FormatUint(newBalance, 10)
				}
			} else {
				// Regular transaction: use the provided value
				valueToWrite = op.Value
			}

			// Execute the PUT operation
			client.TxnPut(txnId, key_str, valueToWrite)
		}
	}

	// Post-execution validation for verification transactions
	if txn.TxnType == kvs.VerificationTxn {
		var totalSum uint64 = 0
		accountBalances := make([]uint64, 10)
		for i := uint64(0); i < 10; i++ {
			if value, exists := readValues[i]; exists {
				if balance, err := strconv.ParseUint(value, 10, 64); err == nil {
					totalSum += balance
					accountBalances[i] = balance
				} else {
					return abortTransaction(dc, participants, txnIds, txnDesc, "VERIFICATION_INVALID_BALANCE")
				}
			} else {
				return abortTransaction(dc, participants, txnIds, txnDesc, "VERIFICATION_ACCOUNT_NOT_FOUND")
			}
		}

		if totalSum != txn.Amount {
			fmt.Printf("VERIFICATION FAILED: Expected sum %d, but got %d\n", txn.Amount, totalSum)
		} else {
			// Verification succeeded - print account balances
			fmt.Printf("VERIFICATION SUCCESS: Total=%d, Balances=%v\n", totalSum, accountBalances)
		}
	}

	// 2PC Phase 1: Prepare
	allVotedYes := true
	for _, serverId := range participants {
		vote := dc.clients[serverId].Prepare(txnIds[serverId])
		if !vote {
			allVotedYes = false
			break
		}
	}

	// 2PC Phase 2: Global decision
	if allVotedYes {
		// Choose the participant with the lowest server ID as the coordinator/leader
		leader := participants[0]
		for _, serverId := range participants {
			if serverId < leader {
				leader = serverId
			}
		}
		for _, serverId := range participants {
			isLeader := (serverId == leader)
			dc.clients[serverId].GlobalCommit(txnIds[serverId], isLeader)
		}
		log.Printf("TRANSACTION SUCCESS: %s\n", txnDesc)
		return true
	} else {
		return abortTransaction(dc, participants, txnIds, txnDesc, "2PC_VOTE_NO")
	}
}

// Unified transaction client with retry logic
func runTransactionClient(hosts []string, done *atomic.Bool, workload kvs.TransactionWorkload, sleep time.Duration) {
	dc := NewDistributedClient(hosts)

	for !done.Load() {
		txn := workload.NextTransaction()

		// Count cross-server transactions
		serverSet := make(map[int]bool)
		for _, op := range txn.Operations {
			serverId := int(op.Key % uint64(dc.numServers))
			serverSet[serverId] = true
		}

		// Execute transaction with retry logic
		for attempt := 0; !done.Load(); attempt++ {
			if executeTransaction(dc, txn) {
				twoPLCommits.Add(1)
				break
			}
			twoPLAborts.Add(1)
			// randomly sleep between 1ms to (1 + attempt)ms
			time.Sleep(time.Millisecond * time.Duration(1+attempt))
		}
		if sleep > 0 {
			time.Sleep(sleep)
		}
	}
}

func main() {
	hosts := HostList{}
	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C, XFER, VERIFY)")
	secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
	numClients := flag.Int("clients", 50, "Number of concurrent client goroutines")
	useOCC := flag.Bool("occ", false, "Use OCC instead of 2PL")
	cacheStrategy := flag.String("cache-strategy", "discard-on-abort", "Cache strategy: discard-on-abort, proactive-invalidation, ttl-reuse, no-cache")
	ttl := flag.Duration("ttl", 100*time.Millisecond, "TTL for ttl-reuse strategy (e.g., 100ms, 1s)")

	verboseFlag := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	if verboseFlag != nil && *verboseFlag {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	} else {
		log.SetOutput(io.Discard)
	}

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf("hosts %v\ntheta %.2f\nworkload %s\nsecs %d\nclients %d\nocc %v\ncache-strategy %s\nttl %v\n",
		hosts, *theta, *workload, *secs, *numClients, *useOCC, *cacheStrategy, *ttl)

	done := atomic.Bool{}

	if *useOCC {
		// Print cache strategy info
		switch *cacheStrategy {
		case "discard-on-abort":
			fmt.Printf("Using Discard-on-Abort cache strategy\n")
		case "proactive-invalidation":
			fmt.Printf("Using Proactive Invalidation cache strategy\n")
		case "ttl-reuse":
			fmt.Printf("Using TTL Reuse cache strategy (TTL=%v)\n", *ttl)
		case "no-cache":
			fmt.Printf("Using No-Cache strategy (always fetch from server)\n")
		default:
			fmt.Printf("Unknown cache strategy: %s\n", *cacheStrategy)
		}

		// Start OCC clients - each with its own cache strategy instance
		for clientId := 0; clientId < *numClients; clientId++ {
			go func(clientId int) {
				// Create a separate cache strategy instance for each client
				var strategy cache.CacheStrategy
				switch *cacheStrategy {
				case "discard-on-abort":
					strategy = cache.NewDiscardOnAbortStrategy()
				case "proactive-invalidation":
					strategy = cache.NewProactiveInvalidationStrategy()
				case "ttl-reuse":
					strategy = cache.NewTTLReuseStrategy(*ttl)
				case "no-cache":
					strategy = cache.NewNoCacheStrategy()
				default:
					strategy = cache.NewDiscardOnAbortStrategy() // Default fallback
				}

				// Start invalidation RPC server for proactive invalidation (per client)
				var callbackHost string
				if *cacheStrategy == "proactive-invalidation" {
					callbackHost = startInvalidationServer(strategy)
				}

				var txnWorkload kvs.TransactionWorkload
				if *workload == "XFER" {
					txnWorkload = kvs.NewPaymentWorkload()
				} else {
					txnWorkload = kvs.NewWorkload(*workload, *theta)
				}
				runOCCTransactionClient(hosts, &done, txnWorkload, 0, fmt.Sprintf("client-%d", clientId), strategy, callbackHost)
			}(clientId)
		}

	} else {
		// Start 2PL clients (original)
		for clientId := 0; clientId < *numClients; clientId++ {
			go func(clientId int) {
				var txnWorkload kvs.TransactionWorkload
				if *workload == "XFER" {
					txnWorkload = kvs.NewPaymentWorkload()
				} else {
					txnWorkload = kvs.NewWorkload(*workload, *theta)
				}
				runTransactionClient(hosts, &done, txnWorkload, 0)
			}(clientId)
		}

		// Start periodic verification if enabled or if workload is XFER
		if *workload == "XFER" {
			go runTransactionClient(hosts, &done, kvs.NewVerificationWorkload(), time.Second*1)
		}
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	if *useOCC {
		// Print OCC metrics
		time.Sleep(time.Second * 2) // Wait for in-flight transactions to finish
		commits := occCommits.Load()
		aborts := occAborts.Load()
		hits := cacheHits.Load()
		misses := cacheMisses.Load()
		totalTxns := commits + aborts
		totalCacheAccess := hits + misses

		if totalTxns > 0 {
			commitRate := float64(commits) / float64(totalTxns) * 100
			abortRate := float64(aborts) / float64(totalTxns) * 100
			fmt.Printf("Commit/Abort Stats:\n")
			fmt.Printf("  Total Transactions: %d\n", totalTxns)
			fmt.Printf("  Commits: %d (%.2f%%)\n", commits, commitRate)
			fmt.Printf("  Aborts: %d (%.2f%%)\n", aborts, abortRate)
		}
		if totalCacheAccess > 0 {
			hitRatio := float64(hits) / float64(totalCacheAccess) * 100
			missRatio := float64(misses) / float64(totalCacheAccess) * 100
			fmt.Printf("Cache Hit Stats:\n")
			fmt.Printf("  Total Cache Accesses: %d\n", totalCacheAccess)
			fmt.Printf("  Cache Hits: %d (%.2f%%)\n", hits, hitRatio)
			fmt.Printf("  Cache Misses: %d (%.2f%%)\n", misses, missRatio)
		}
	} else {
		// Print 2PL metrics
		time.Sleep(time.Second * 2) // Wait for in-flight transactions to finish
		commits := twoPLCommits.Load()
		aborts := twoPLAborts.Load()
		totalTxns := commits + aborts

		if totalTxns > 0 {
			commitRate := float64(commits) / float64(totalTxns) * 100
			abortRate := float64(aborts) / float64(totalTxns) * 100
			fmt.Printf("Commit/Abort Stats:\n")
			fmt.Printf("  Total Transactions: %d\n", totalTxns)
			fmt.Printf("  Commits: %d (%.2f%%)\n", commits, commitRate)
			fmt.Printf("  Aborts: %d (%.2f%%)\n", aborts, abortRate)
		}
	}

	if *useOCC && *workload == "XFER" {
		// All payment clients stopped
		log.Printf("Stopping payment clients, waiting for them to finish...\n")
		time.Sleep(time.Second * 5) // Wait longer for all goroutines to fully exit retry loops

		fmt.Printf("Starting final verification transaction\n")
		txnWorkload := kvs.NewVerificationWorkload()
		strategy := cache.NewDiscardOnAbortStrategy()

		// Single synchronous verification transaction with retry
		dc := NewOCCDistributedClient(hosts, "verifier-client", strategy, "")
		txn := txnWorkload.NextTransaction()

		// Retry verification until success or timeout (10 seconds)
		startTime := time.Now()
		maxRetries := 100
		for attempt := 0; attempt < maxRetries && time.Since(startTime) < 10*time.Second; attempt++ {
			if executeOCCTransaction(dc, txn) {
				fmt.Printf("Final verification succeeded after %d attempts\n", attempt+1)
				break
			}
			// Exponential backoff
			time.Sleep(time.Millisecond * time.Duration(10+attempt*10))
		}
	}
}
