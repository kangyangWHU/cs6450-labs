package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"

	// "strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
	// "github.com/rstutsman/cs6450-labs/kvs"
)

const shardNum = 64 // NOTE: change to RPC
// each partition should only handles shardNum / partitions shards
const partitions = 4 // partitions are for the client-side.

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

func xorFolding(key uint64, total int) int {
	folded := key ^ (key >> 16) ^ (key >> 32) ^ (key >> 48)
	// return int(folded % 64)
	return int(folded % uint64(total))
}

// Add a fast integer hash function
func fnv64aInt(key uint64) uint64 {
	const (
		offset64 = 1469598103934665603
		prime64  = 1099511628211
	)
	h := uint64(offset64)

	// Hash the 8 bytes of the int64
	for i := 0; i < 8; i++ {
		h ^= uint64((key >> (i * 8)) & 0xFF)
		h *= prime64
	}
	return h
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
func (client *Client) Get(key uint64) string {
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
func (client *Client) Put(key uint64, value string) {
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

// runClientDirect processes operations directly without channels/goroutines
func runClientDirect(
	id int,
	addr string,
	done *atomic.Bool,
	workload *kvs.Workload,
	resultsCh chan<- uint64,
) {
	const batchSize = 1024

	client := Dial(addr)
	value := strings.Repeat("x", 128)

	// Single set of buffers for all shards
	getsPerShard := make([][]kvs.GetRequest, shardNum)
	putsPerShard := make([][]kvs.PutRequest, shardNum)
	for i := 0; i < shardNum; i++ {
		getsPerShard[i] = make([]kvs.GetRequest, 0, batchSize)
		putsPerShard[i] = make([]kvs.PutRequest, 0, batchSize)
	}

	var totalOps uint64

	flushGetShard := func(shardIdx int) {
		if len(getsPerShard[shardIdx]) > 0 {
			req := kvs.GetBatchShardedRequest{Shard: shardIdx, Keys: getsPerShard[shardIdx]}
			var resp kvs.GetBatchResponse
			if err := client.rpcClient.Call("KVService.GetBatchSharded", &req, &resp); err != nil {
				log.Fatal(err)
			}
			totalOps += uint64(len(getsPerShard[shardIdx]))
			getsPerShard[shardIdx] = getsPerShard[shardIdx][:0]
		}
	}

	flushPutShard := func(shardIdx int) {
		if len(putsPerShard[shardIdx]) > 0 {
			req := kvs.PutBatchShardedRequest{Shard: shardIdx, Items: putsPerShard[shardIdx]}
			var resp kvs.PutBatchResponse
			if err := client.rpcClient.Call("KVService.PutBatchSharded", &req, &resp); err != nil {
				log.Fatal(err)
			}
			totalOps += uint64(len(putsPerShard[shardIdx]))
			putsPerShard[shardIdx] = putsPerShard[shardIdx][:0]
		}
	}

	flushAllShards := func() {
		for i := 0; i < shardNum; i++ {
			flushGetShard(i)
			flushPutShard(i)
		}
	}

	// Direct processing loop - no channels!
	for !done.Load() {
		op := workload.Next()

		// Use fastest hash function from our evaluation
		shardIdx := int((op.Key * 0x9e3779b97f4a7c15) % uint64(shardNum))

		if op.IsRead {
			// Flush pending puts for this shard
			if len(putsPerShard[shardIdx]) > 0 {
				flushPutShard(shardIdx)
			}
			getsPerShard[shardIdx] = append(getsPerShard[shardIdx], kvs.GetRequest{Key: op.Key})
			if len(getsPerShard[shardIdx]) >= batchSize {
				flushGetShard(shardIdx)
			}
		} else {
			// Flush pending gets for this shard
			if len(getsPerShard[shardIdx]) > 0 {
				flushGetShard(shardIdx)
			}
			putsPerShard[shardIdx] = append(putsPerShard[shardIdx], kvs.PutRequest{Key: op.Key, Value: value})
			if len(putsPerShard[shardIdx]) >= batchSize {
				flushPutShard(shardIdx)
			}
		}
	}

	flushAllShards()
	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- totalOps
}

// runClientPartitioned sends ops to P independent partitions.
// Each partition keeps its own GET/PUT buffers and flushes them via batch RPCs.
func runClientPartitioned(
	id int,
	addr string,
	done *atomic.Bool,
	workload *kvs.Workload,
	resultsCh chan<- uint64,
) {

	const batchSize = 1024

	// One RPC connection per partition (to avoid HOL on a single conn).
	clients := make([]*Client, partitions)
	for i := range clients {
		clients[i] = Dial(addr)
	}

	type Op struct {
		// key    string
		key    uint64
		value  string
		isRead bool
	}

	// One channel per partition.
	partCh := make([]chan Op, partitions)
	for i := 0; i < partitions; i++ {
		partCh[i] = make(chan Op, 4096) // 4096 Ops buffer for each channel.
	}

	var totalOps uint64
	var wg sync.WaitGroup

	// One flusher goroutine per partition that owns its buffers.
	for p := 0; p < partitions; p++ {
		wg.Add(1)
		go func(p int, in <-chan Op, client *Client) {
			defer wg.Done()

			getsPerShard := make([][]kvs.GetRequest, shardNum)
			putsPerShard := make([][]kvs.PutRequest, shardNum)
			for i := 0; i < shardNum; i++ {
				getsPerShard[i] = make([]kvs.GetRequest, 0, batchSize)
				putsPerShard[i] = make([]kvs.PutRequest, 0, batchSize)
			}

			// puts := make([]kvs.PutRequest, 0, batchSize)

			flushGetPerShardNoCheck := func(shardIdx int) {
				gets := getsPerShard[shardIdx]
				// getsNum := getsPerShardNum[shardIdx]
				req := kvs.GetBatchShardedRequest{Shard: shardIdx, Keys: gets}
				var resp kvs.GetBatchResponse
				if err := client.rpcClient.Call("KVService.GetBatchSharded", &req, &resp); err != nil {
					log.Fatal(err)
				}
				atomic.AddUint64(&totalOps, uint64(len(gets))) // NOTE: can optimize
				getsPerShard[shardIdx] = gets[:0]
			}

			flushPutPerShardNoCheck := func(shardIdx int) {
				puts := putsPerShard[shardIdx]
				req := kvs.PutBatchShardedRequest{Shard: shardIdx, Items: puts}
				var resp kvs.PutBatchResponse
				if err := client.rpcClient.Call("KVService.PutBatchSharded", &req, &resp); err != nil {
					log.Fatal(err)
				}
				atomic.AddUint64(&totalOps, uint64(len(puts)))
				putsPerShard[shardIdx] = puts[:0]
			}

			// flush := func() {
			// flushPerShard := func(shardIdx int) {
			// 	gets := getsPerShard[shardIdx]
			// 	getsNum := getsPerShardNum[shardIdx]
			// 	if getsNum > 0 {
			// 		req := kvs.GetBatchShardedRequest{Shard: shardIdx, Keys: gets}
			// 		var resp kvs.GetBatchResponse
			// 		if err := client.rpcClient.Call("KVService.GetBatchSharded", &req, &resp); err != nil {
			// 			log.Fatal(err)
			// 		}
			// 		atomic.AddUint64(&totalOps, uint64(len(gets))) // NOTE: can optimize
			// 		// gets = gets[:0]
			// 		getsPerShard[shardIdx] = gets[:0]
			// 	}
			// 	// if puts, exists := putsPerShard[shardIdx]; exists && len(puts) > 0 {
			// 	puts := putsPerShard[shardIdx]
			// 	putsNum := putsPerShardNum[shardIdx]
			// 	if putsNum > 0 {
			// 		req := kvs.PutBatchShardedRequest{Shard: shardIdx, Items: puts}
			// 		var resp kvs.PutBatchResponse
			// 		if err := client.rpcClient.Call("KVService.PutBatchSharded", &req, &resp); err != nil {
			// 			log.Fatal(err)
			// 		}
			// 		atomic.AddUint64(&totalOps, uint64(len(puts)))
			// 		// puts = puts[:0]
			// 		// delete(putsPerShard, shardIdx)
			// 		putsPerShard[shardIdx] = puts[:0]
			// 	}
			// }

			flushAllShards := func() {
				for shardIdx := 0; shardIdx < shardNum; shardIdx++ {
					if len(getsPerShard[shardIdx]) > 0 {
						flushGetPerShardNoCheck(shardIdx)
					}
					if len(putsPerShard[shardIdx]) > 0 {
						flushPutPerShardNoCheck(shardIdx)
					}
				}
			}

			// Time-based micro-batching to cap tail latency.
			// ticker := time.NewTicker(1 * time.Millisecond)
			// defer ticker.Stop()

			for {
				// select {
				// case o, ok := <-in:
				o, ok := <-in
				if !ok {
					// Drain & exit.
					flushAllShards()
					return
				}

				// Use fastest hash function from our evaluation
				shardIdx := int((o.key * 0x9e3779b97f4a7c15) % uint64(shardNum))

				if o.isRead {
					if len(putsPerShard[shardIdx]) > 0 {
						flushPutPerShardNoCheck(shardIdx)
					}
					getsPerShard[shardIdx] = append(getsPerShard[shardIdx], kvs.GetRequest{Key: o.key})
					if len(getsPerShard[shardIdx]) >= batchSize {
						flushGetPerShardNoCheck(shardIdx)
					}
				} else {
					if len(getsPerShard[shardIdx]) > 0 {
						flushGetPerShardNoCheck(shardIdx)
					}
					putsPerShard[shardIdx] = append(putsPerShard[shardIdx], kvs.PutRequest{Key: o.key, Value: o.value})
					if len(putsPerShard[shardIdx]) >= batchSize {
						flushPutPerShardNoCheck(shardIdx)
					}
				}
				// case <-ticker.C:
				// flushAllShards()
				// if len(getsPerShard[shardIdx]) > 0 || len(putsPerShard[shardIdx]) > 0 {
				// flushPerShard(shardIdx)
				// }
				// }
			}
		}(p, partCh[p], clients[p])
	}

	// Producer: generate ops and route to a partition by hash(key) % P.
	value := strings.Repeat("x", 128)
	for !done.Load() {
		// Super-batch just to amortize Next(); individual partitions still micro-batch/flush.
		// for j := 0; j < batchSize; j++ {
		op := workload.Next() // existing generator :contentReference[oaicite:7]{index=7}
		// key := strconv.FormatInt(int64(op.Key), 10)
		// p := op.Key % partitions // NOTE: add support to align with distribution.
		// p := op.Key >> (64 - 6)
		// p := int(fnv64aInt(op.Key) % uint64(partitions)) // Better hash for int keys.
		p := xorFolding(op.Key, partitions)
		// p := int(fnv64a(key) % uint64(partitions)) // Speed up here.
		if op.IsRead {
			partCh[p] <- Op{key: op.Key, isRead: true}
		} else {
			partCh[p] <- Op{key: op.Key, isRead: false, value: value}
		}
		// }
	}

	// Signal workers to stop and flush.
	for i := range partCh {
		close(partCh[i])
	}
	wg.Wait()

	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- atomic.LoadUint64(&totalOps)
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
			// key := strconv.FormatInt(int64(op.Key), 10)
			key := op.Key

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
func runClient(id int, addr string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	client := Dial(addr)

	value := strings.Repeat("x", 128)
	const batchSize = 1024
	// const batchSize = 8

	opsCompleted := uint64(0)

	// mutex := sync.Mutex{}
	for !done.Load() {

		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			// key := fmt.Sprintf("%d", op.Key)
			// key := strconv.FormatInt(int64(op.Key), 10)
			key := op.Key
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
	clientNum := 8 // RH: align with CPU cores
	resultsChs := make([]chan uint64, clientNum)
	for i := 0; i < clientNum; i++ {
		resultsChs[i] = make(chan uint64)
	}

	host := hosts[0]
	for clientId := 0; clientId < clientNum; clientId++ {
		go func(clientId int) {
			workload := kvs.NewWorkload(*workload, *theta)
			runClientDirect(clientId, host, &done, workload, resultsChs[clientId])
		}(clientId)
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
