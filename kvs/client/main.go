package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
	// "github.com/rstutsman/cs6450-labs/kvs"
)

type Client struct {
	rpcClient *rpc.Client
}

// Dial establishes an HTTP RPC connection to the specified address and returns a new Client.
// It terminates the program if the connection cannot be established.
func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient}
}

// fnv64a is a small, fast non-crypto hash for partitioning.
func fnv64a(s string) uint64 {
	const (
		offset64 = 1469598103934665603
		prime64  = 1099511628211
	)
	var h uint64 = offset64
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// Get retrieves the value associated with the specified key from the key-value store.
// It returns the value as a string. If the key does not exist, it may return an empty string
// or handle the error based on the implementation.
func (client *Client) Get(key string) string {
	request := kvs.GetRequest{
		Key: key,
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response.Value
}

// Put sends a key-value pair to the key-value store server using an RPC call.
// It constructs a PutRequest with the provided key and value, and sends it to the server.
// If the RPC call fails, the function logs the error and terminates the program.
//
// Parameters:
//   - key:   The key to store in the key-value store.
//   - value: The value associated with the key.
func (client *Client) Put(key string, value string) {
	request := kvs.PutRequest{
		Key:   key,
		Value: value,
	}
	response := kvs.PutResponse{}
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}



// func runClient3(id int, addr string, done *atomic.Bool,
// 		workload *kvs.Workload, resultsCh chan<- uint64) {
// 	client := Dial(addr)
// 	value := strings.Repeat("x", 128)
// 	const batchSize = 1024
// 	opsCompleted := uint64(0)
// 	const bucketSize = 3
// 	// each bucket has 2 buffer, one for get and one for put.
// 	clientBuckets := make([]kvs.ClientBucket, 0, bucketSize)
// 	for i := 0; i < bucketSize; i++ {
// 		clientBuckets = append(clientBuckets, kvs.ClientBucket{
// 			GetBuffer: kvs.GetBatchRequest{},
// 			PutBuffer: kvs.PutBatchRequest{},
// 		})
// 	}

// 	// each bucket is supposed to be
// }

func runClient2(id int, addr string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	client := Dial(addr)

	value := strings.Repeat("x", 128)
	const getBufferSize = 512 // 512 seems to be faster than 1024, 2048
	const putBufferSize = 512
	const batchSize = 512
	opsCompleted := uint64(0)
	getBuffer := make([]kvs.GetRequest, 0, getBufferSize)
	putBuffer := make([]kvs.PutRequest, 0, putBufferSize)
	// RH: closure to flush out getBuffer.
	flushGetBuffer := func() {
		if len(getBuffer) > 0 {
			request := kvs.GetBatchRequest{Keys: getBuffer}
			response := kvs.GetBatchResponse{}
			err := client.rpcClient.Call("KVService.GetBatch", &request, &response)
			if err != nil {
				log.Fatal(err)
			}
			opsCompleted += uint64(len(getBuffer))
			getBuffer = getBuffer[:0] // reset
		}
	}

	flushPutBuffer := func() {
		if len(putBuffer) > 0 {
			request := kvs.PutBatchRequest{Items: putBuffer}
			response := kvs.PutBatchResponse{}
			err := client.rpcClient.Call("KVService.PutBatch", &request, &response)
			if err != nil {
				log.Fatal(err)
			}
			opsCompleted += uint64(len(putBuffer))
			putBuffer = putBuffer[:0] // Reset buffer
		}
	}

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			key := strconv.FormatInt(int64(op.Key), 10)

			if op.IsRead {
				if len(putBuffer) > 0 { // if we already have puts pending, then flush it
					flushPutBuffer()
				}
				getBuffer = append(getBuffer, kvs.GetRequest{Key: key})

				if len(getBuffer) >= batchSize {
					flushGetBuffer()
				}
			} else {
				if len(getBuffer) > 0 {
					flushGetBuffer()
				}
				putBuffer = append(putBuffer, kvs.PutRequest{Key: key, Value: value})

				if len(putBuffer) >= batchSize {
					flushPutBuffer()
				}
			}
		}

		flushGetBuffer()
		flushPutBuffer()
	}

	flushGetBuffer()
	flushPutBuffer()

	fmt.Printf("Client %d finished operations.\n", id)

	resultsCh <- opsCompleted
}

// runClient executes a workload of key-value store operations (Get and Put) against a server at the specified address.
// The client continues to perform operations in batches until the 'done' flag is set to true.
// Each operation is determined by the provided workload generator. The total number of completed operations
// is sent to the resultsCh channel upon completion.
//
// Parameters:
//   - id:        Unique identifier for the client.
//   - addr:      Address of the key-value store server to connect to.
//   - done:      Atomic boolean flag indicating when the client should stop processing.
//   - workload:  Pointer to a Workload generator that provides the next operation (read or write).
//   - resultsCh: Channel to send the total number of completed operations when finished.
// Protobuf-optimized partitioned client for maximum performance
func runClientPartitioned(id int, addr string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	const partitions = 4
	const batchSize = 1024
	
	// One RPC connection per partition to avoid head-of-line blocking
	clients := make([]*Client, partitions)
	for i := range partitions {
		clients[i] = Dial(addr)
	}
	
	type Op struct {
		key    string
		isRead bool
		value  string
	}
	
	// One channel per partition - no shared mutation, no locks needed
	partCh := make([]chan Op, partitions)
	for i := range partitions {
		partCh[i] = make(chan Op, 4096) // Large buffer for burst traffic
	}
	
	var totalOps uint64
	var wg sync.WaitGroup
	
	// One worker goroutine per partition with its own buffers
	for p := range partitions {
		wg.Add(1)
		go func(p int, in <-chan Op, client *Client) {
			defer wg.Done()
			
			// Direct string slices for protobuf - more efficient than struct slices
			getKeys := make([]string, 0, batchSize)
			putItems := make([]struct{ Key, Value string }, 0, batchSize)
			
			flush := func() {
				// Flush GET operations using protobuf-optimized RPC
				if len(getKeys) > 0 {
					req := &kvs.RPCProtoGetBatchRequest{Keys: getKeys}
					var resp kvs.RPCProtoGetBatchResponse
					if err := client.rpcClient.Call("KVService.GetBatchProto", req, &resp); err != nil {
						log.Fatal(err)
					}
					atomic.AddUint64(&totalOps, uint64(len(getKeys)))
					getKeys = getKeys[:0]
				}
				
				// Flush PUT operations using protobuf-optimized RPC
				if len(putItems) > 0 {
					req := &kvs.RPCProtoPutBatchRequest{Items: putItems}
					var resp kvs.RPCProtoPutBatchResponse
					if err := client.rpcClient.Call("KVService.PutBatchProto", req, &resp); err != nil {
						log.Fatal(err)
					}
					atomic.AddUint64(&totalOps, uint64(len(putItems)))
					putItems = putItems[:0]
				}
			}
			
			for {
				o, ok := <-in
				if !ok {
					// Channel closed - drain and exit
					if len(getKeys) > 0 || len(putItems) > 0 {
						flush()
					}
					return
				}
				
				if o.isRead {
					getKeys = append(getKeys, o.key)
					if len(getKeys) >= batchSize {
						flush()
					}
				} else {
					// Flush pending GETs before switching to PUTs (maintain ordering)
					if len(getKeys) > 0 {
						flush()
					}
					putItems = append(putItems, struct{ Key, Value string }{
						Key:   o.key,
						Value: o.value,
					})
					if len(putItems) >= batchSize {
						flush()
					}
				}
			}
		}(p, partCh[p], clients[p])
	}
	
	// Producer: generate operations and route by hash partitioning
	value := strings.Repeat("x", 128)
	for !done.Load() {
		// Super-batch generation to amortize workload.Next() overhead
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			key := strconv.FormatInt(int64(op.Key), 10)
			
			// Hash-based partitioning using FNV
			p := int(fnv64a(key) % uint64(partitions))
			
			if op.IsRead {
				partCh[p] <- Op{key: key, isRead: true}
			} else {
				partCh[p] <- Op{key: key, isRead: false, value: value}
			}
		}
	}
	
	// Signal workers to stop and flush remaining operations
	for i := range partCh {
		close(partCh[i])
	}
	wg.Wait()
	
	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- atomic.LoadUint64(&totalOps)
}

func runClient(id int, addr string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	client := Dial(addr)

	value := strings.Repeat("x", 128)
	const batchSize = 1024

	opsCompleted := uint64(0)

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			key := strconv.FormatInt(int64(op.Key), 10)
			if op.IsRead {
				client.Get(key)
			} else {
				client.Put(key, value)
			}
			opsCompleted++
		}
	}

	fmt.Printf("Client %d finished operations.\n", id)

	resultsCh <- opsCompleted
}

type HostList []string

// String returns a string representation of the HostList by joining all hosts with commas.
func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

// Set parses a comma-separated string and sets the HostList to the resulting slice of strings.
// It implements the flag.Value interface, allowing HostList to be used as a command-line flag.
// Returns an error if the input value is invalid.
func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

func main() {
	hosts := HostList{}

	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C)")
	secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
	flag.Parse()

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf(
		"hosts %v\n"+
			"theta %.2f\n"+
			"workload %s\n"+
			"secs %d\n",
		hosts, *theta, *workload, *secs,
	)

	start := time.Now()

	done := atomic.Bool{}
	// resultsCh := make(chan uint64)
	clientNum := 16 // RH: align with CPU cores
	resultsChs := make([]chan uint64, clientNum)
	for i := 0; i < clientNum; i++ {
		resultsChs[i] = make(chan uint64)
	}

	for clientId := 0; clientId < clientNum; clientId++ {
		host := hosts[clientId%len(hosts)] // Distribute clients across all servers
		go func(clientId int, host string) {
			workload := kvs.NewWorkload(*workload, *theta)
			runClientPartitioned(clientId, host, &done, workload, resultsChs[clientId])
		}(clientId, host)
	}

	// clientId := 0
	// go func(clientId int) {
	// 	workload := kvs.NewWorkload(*workload, *theta)
	// 	runClient2(clientId, host, &done, workload, resultsCh)
	// }(clientId)

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	// opsCompleted := <-resultsCh
	var opsCompleted uint64 = 0
	for _, resultsCh := range resultsChs {
		opsCompleted += <-resultsCh
	}

	elapsed := time.Since(start)

	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
